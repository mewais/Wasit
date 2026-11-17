//! wasit-claude — claude stream-json driver for Wasit.
//!
//! Implements `AgentDriver` + `Session` for the `claude` CLI using the
//! `--input-format stream-json --output-format stream-json` protocol.
//!
//! ## Wire protocol overview
//!
//! Claude communicates over stdin/stdout as NDJSON lines.
//! - Inbound (claude→wasit): `system/init`, `assistant`, `user`, `result`,
//!   `control_request` (can_use_tool / elicitation / AskUserQuestion /
//!   ExitPlanMode), `system/compact_boundary`, `rate_limit_event`,
//!   `system/status` (mode change), various `system/*` subtypes (unknown).
//! - Outbound (wasit→claude): user prompt as a `user` message, gate responses
//!   as `control_response` (behavior allow/deny or elicitation accept/decline),
//!   `control_request` initialize (handshake), interrupt control_request,
//!   set_permission_mode control_request.
//!
//! ## Capability report (honest)
//! - `interruptible`: false — the control-protocol interrupt exists but live
//!   verification is incomplete; flagged false for honesty.
//! - `model_switchable`: false — claude does not support live model changes.
//! - `mode_switchable`: true — `set_permission_mode` control_request works.
//! - `persistent_conversation`: true — context lives across turns.
//! - `resumable`: true — `--resume <session-id>`.
//! - `streaming_output`: false — claude delivers full assistant messages per turn.
//! - `reports_usage`: true — per-message and result-level usage.
//! - `supports_gates`: true — can_use_tool / elicitation / question / plan.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::mpsc;
use wasit_core::{
    AgentDriver, AgentEvent, CompactionInfo, DriverCapabilities, DriverError, ElicitationChoice,
    EndReason, GateDecision, GateRequest, ModelUsage, QuestionAnswer, QuestionOption,
    QuestionPrompt, RateLimitInfo, ResumeMode, Session, SessionMode, SpawnSpec, Usage,
};

// ============================================================================
// 1. argv builder (pure)
// ============================================================================

/// Build the exact `claude` invocation for a session launch.
///
/// The first prompt is NOT included here — in `--input-format stream-json`
/// mode the prompt must be delivered via stdin as a stream-json user message.
///
/// - `Fresh`      → nothing extra
/// - `Continue`   → `--continue`
/// - `Resume(id)` → `--resume <id>`
pub fn claude_argv(spec: &SpawnSpec) -> Vec<String> {
    let bin = spec.bin.as_deref().unwrap_or("claude").to_string();

    let mut argv = vec![
        bin,
        "--print".to_string(),
        "--input-format".to_string(),
        "stream-json".to_string(),
        "--output-format".to_string(),
        "stream-json".to_string(),
        "--verbose".to_string(),
    ];

    // Model flag — always include it when set; omit only if None.
    if let Some(ref model) = spec.model {
        argv.push("--model".to_string());
        argv.push(model.clone());
    }

    // Permission mode — omit when empty (backend default).
    if !spec.mode.is_empty() {
        argv.push("--permission-mode".to_string());
        argv.push(spec.mode.clone());
    }

    // Permission-prompt-tool stdio: always present so MCP HITL can route here.
    argv.push("--permission-prompt-tool".to_string());
    argv.push("stdio".to_string());

    // Resume / continue.
    match &spec.resume {
        ResumeMode::Fresh => {}
        ResumeMode::Continue => argv.push("--continue".to_string()),
        ResumeMode::Resume(id) => {
            argv.push("--resume".to_string());
            argv.push(id.clone());
        }
    }

    // Disallowed tools — comma-joined, omitted when empty.
    if !spec.disallowed_tools.is_empty() {
        argv.push("--disallowedTools".to_string());
        argv.push(spec.disallowed_tools.join(","));
    }

    // MCP config.
    if let Some(ref mcp) = spec.mcp_config {
        argv.push("--mcp-config".to_string());
        argv.push(mcp.clone());
    }

    argv
}

// ============================================================================
// 2. Stateful line parser (pure core — testable without IO)
// ============================================================================

/// Stateful stream-json line parser.
///
/// Maintains the state needed to correlate tool_use IDs → names (for
/// `ToolCallFinished`) and track per-turn flag state.
#[derive(Debug, Default)]
pub struct LineParser {
    /// Maps tool_use id → tool name for correlating tool_result blocks.
    tool_use_names: HashMap<String, String>,
    /// Whether `TurnStarted` has been synthesized for the current turn.
    turn_started: bool,
}

impl LineParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process one NDJSON line from claude's stdout.
    ///
    /// Returns zero or more `AgentEvent`s that result from this line.
    pub fn on_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let line = line.trim();
        if line.is_empty() {
            return vec![];
        }
        let val: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        let obj = match val.as_object() {
            Some(o) => o,
            None => return vec![],
        };
        let type_str = match obj.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return vec![],
        };

        match type_str {
            "system" => self.parse_system(obj, val.clone()),
            "assistant" => self.parse_assistant(obj),
            "user" => self.parse_user(obj),
            "result" => self.parse_result(obj),
            "control_request" => self.parse_control_request(obj),
            "rate_limit_event" => self.parse_rate_limit(obj),
            // control_response is the ack to our initialize; ignore silently.
            "control_response" => vec![],
            _ => vec![AgentEvent::Unknown { raw: val }],
        }
    }

    // ── system subtypes ───────────────────────────────────────────────────

    fn parse_system(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
        val: serde_json::Value,
    ) -> Vec<AgentEvent> {
        let subtype = obj.get("subtype").and_then(|v| v.as_str()).unwrap_or("");
        match subtype {
            "init" => self.parse_system_init(obj),
            "status" => self.parse_system_status(obj),
            "compact_boundary" => self.parse_compact_boundary(obj),
            "modes_update" => self.parse_modes_update(obj),
            "mode_update" => {
                // legacy mode_update with current_mode
                let current_mode = obj
                    .get("current_mode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                vec![AgentEvent::ModesUpdated {
                    available_modes: vec![],
                    current_mode,
                }]
            }
            _ => vec![AgentEvent::Unknown { raw: val }],
        }
    }

    fn parse_system_init(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let native_session_id = obj
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let model = obj
            .get("model")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let cwd = obj
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let claude_code_version = obj
            .get("claude_code_version")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let permission_mode = obj
            .get("permissionMode")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Tools list
        let tools: Vec<String> = obj
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        // tools may be strings or objects with a name field
                        t.as_str().map(|s| s.to_string()).or_else(|| {
                            t.get("name")
                                .and_then(|n| n.as_str())
                                .map(|s| s.to_string())
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // MCP servers
        let mcp_servers: Vec<wasit_core::McpServerInfo> = obj
            .get("mcp_servers")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let name = s.get("name").and_then(|n| n.as_str())?.to_string();
                        let status = s
                            .get("status")
                            .and_then(|st| st.as_str())
                            .unwrap_or("")
                            .to_string();
                        Some(wasit_core::McpServerInfo { name, status })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Slash commands
        let slash_commands: Vec<wasit_core::SlashCommandInfo> = obj
            .get("slash_commands")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|sc| {
                        // slash commands may be bare strings or objects
                        if let Some(s) = sc.as_str() {
                            let name = if s.starts_with('/') {
                                s.to_string()
                            } else {
                                format!("/{s}")
                            };
                            return Some(wasit_core::SlashCommandInfo {
                                name,
                                description: String::new(),
                                argument_hint: String::new(),
                            });
                        }
                        let name = sc.get("name").and_then(|n| n.as_str())?.to_string();
                        let description = sc
                            .get("description")
                            .and_then(|d| d.as_str())
                            .unwrap_or("")
                            .to_string();
                        let argument_hint = sc
                            .get("argumentHint")
                            .and_then(|h| h.as_str())
                            .unwrap_or("")
                            .to_string();
                        Some(wasit_core::SlashCommandInfo {
                            name,
                            description,
                            argument_hint,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Derive available modes from version (ported from marsad-events).
        let available_modes = if !claude_code_version.is_empty() {
            claude_available_modes(&claude_code_version)
        } else {
            vec![]
        };
        let current_mode = if !permission_mode.is_empty() {
            permission_mode.clone()
        } else {
            String::new()
        };

        // Reset turn-started state for this new session.
        self.turn_started = false;

        vec![AgentEvent::SessionInit {
            native_session_id,
            model,
            cwd,
            tools,
            mcp_servers,
            slash_commands,
            permission_mode,
            claude_code_version,
            available_modes,
            current_mode,
        }]
    }

    fn parse_system_status(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        // system/status with permissionMode → ModesUpdated (current mode only).
        if let Some(mode) = obj.get("permissionMode").and_then(|v| v.as_str()) {
            vec![AgentEvent::ModesUpdated {
                available_modes: vec![],
                current_mode: Some(mode.to_string()),
            }]
        } else {
            vec![]
        }
    }

    fn parse_compact_boundary(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let meta = obj
            .get("compact_metadata")
            .unwrap_or(&serde_json::Value::Null);
        let info = CompactionInfo {
            trigger: meta
                .get("trigger")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            pre_tokens: meta.get("pre_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            post_tokens: meta
                .get("post_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cumulative_dropped_tokens: meta
                .get("cumulative_dropped_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            duration_ms: meta
                .get("duration_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        };
        vec![AgentEvent::Compaction { info }]
    }

    fn parse_modes_update(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let available_modes = obj
            .get("available_modes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|m| {
                        let id = m.get("id").and_then(|v| v.as_str())?.to_string();
                        let label = m
                            .get("label")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&id)
                            .to_string();
                        let description = m
                            .get("description")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        Some(SessionMode {
                            id,
                            label,
                            description,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let current_mode = obj
            .get("current_mode")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        vec![AgentEvent::ModesUpdated {
            available_modes,
            current_mode,
        }]
    }

    // ── assistant messages ────────────────────────────────────────────────

    fn parse_assistant(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let message = match obj.get("message") {
            Some(m) => m,
            None => return vec![],
        };
        let message_id = message
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Usage from the message.
        let usage_events = if let Some(usage_obj) = message.get("usage") {
            let usage = parse_usage(usage_obj);
            vec![AgentEvent::UsageUpdate {
                usage,
                context_pct: None,
            }]
        } else {
            vec![]
        };

        let content = match message.get("content").and_then(|c| c.as_array()) {
            Some(arr) => arr,
            None => return usage_events,
        };

        let mut events: Vec<AgentEvent> = Vec::new();

        // Synthesize TurnStarted on first assistant message.
        if !self.turn_started {
            self.turn_started = true;
            events.push(AgentEvent::TurnStarted {
                native_turn_id: None,
            });
        }

        for block in content {
            let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match block_type {
                "text" => {
                    let text = block
                        .get("text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    events.push(AgentEvent::AssistantDelta {
                        text,
                        message_id: message_id.clone(),
                    });
                }
                "thinking" => {
                    let text = block
                        .get("thinking")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    events.push(AgentEvent::ThinkingDelta {
                        text,
                        message_id: message_id.clone(),
                    });
                }
                "tool_use" => {
                    let call_id = block
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = block
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let input = block
                        .get("input")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);

                    // Track tool_use_id → name for tool_result correlation.
                    if !call_id.is_empty() {
                        self.tool_use_names.insert(call_id.clone(), name.clone());
                    }

                    // Subagent detection: Task or Agent tools.
                    let is_subagent = name == "Task" || name == "Agent";
                    let subagent_model = if is_subagent {
                        input
                            .get("model")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    };

                    events.push(AgentEvent::ToolCallStarted {
                        name,
                        call_id,
                        input,
                        is_subagent,
                        subagent_model,
                    });
                }
                _ => {} // unknown content block types: skip
            }
        }

        // Append usage events after content.
        events.extend(usage_events);
        events
    }

    // ── user messages (tool_result) ────────────────────────────────────────

    fn parse_user(&mut self, obj: &serde_json::Map<String, serde_json::Value>) -> Vec<AgentEvent> {
        let message = match obj.get("message") {
            Some(m) => m,
            None => return vec![],
        };
        let content = match message.get("content").and_then(|c| c.as_array()) {
            Some(arr) => arr,
            None => return vec![],
        };

        let mut events = Vec::new();
        for block in content {
            let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if block_type == "tool_result" {
                let call_id = block
                    .get("tool_use_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = self
                    .tool_use_names
                    .get(&call_id)
                    .cloned()
                    .unwrap_or_default();
                let is_error = block
                    .get("is_error")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let result = block.get("content").cloned();
                events.push(AgentEvent::ToolCallFinished {
                    call_id,
                    name,
                    ok: !is_error,
                    result,
                });
            }
        }
        events
    }

    // ── result ────────────────────────────────────────────────────────────

    fn parse_result(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let subtype = obj.get("subtype").and_then(|v| v.as_str()).unwrap_or("");
        let is_error = obj
            .get("is_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let was_interrupted = obj
            .get("was_interrupted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let num_turns = obj.get("num_turns").and_then(|v| v.as_u64()).unwrap_or(0);

        let end_reason = if subtype == "error_max_turns" {
            Some(EndReason::MaxTurns)
        } else if is_error {
            if was_interrupted {
                Some(EndReason::Cancelled)
            } else {
                Some(EndReason::ExecutionError)
            }
        } else {
            None
        };

        let usage = if let Some(u) = obj.get("usage") {
            parse_usage(u)
        } else {
            Usage::default()
        };
        // cost from total_cost_usd
        let cost_usd = obj
            .get("total_cost_usd")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let usage = Usage { cost_usd, ..usage };

        // per_model breakdown
        let per_model: HashMap<String, ModelUsage> = obj
            .get("modelUsage")
            .and_then(|v| v.as_object())
            .map(|map| {
                map.iter()
                    .map(|(k, v)| {
                        let mu = ModelUsage {
                            input_tokens: v
                                .get("inputTokens")
                                .and_then(|x| x.as_u64())
                                .unwrap_or(0),
                            output_tokens: v
                                .get("outputTokens")
                                .and_then(|x| x.as_u64())
                                .unwrap_or(0),
                            cost_usd: v.get("costUSD").and_then(|x| x.as_f64()).unwrap_or(0.0),
                            estimated: false,
                        };
                        (k.clone(), mu)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let reply = obj
            .get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Reset turn state for next turn.
        self.turn_started = false;

        vec![AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted,
            num_turns,
            usage,
            per_model,
            reply,
        }]
    }

    // ── control_request ───────────────────────────────────────────────────

    fn parse_control_request(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let request_id = obj
            .get("request_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let request = match obj.get("request") {
            Some(r) => r,
            None => return vec![],
        };
        let subtype = request
            .get("subtype")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match subtype {
            "can_use_tool" => self.parse_can_use_tool(&request_id, request),
            "elicitation" => self.parse_elicitation(&request_id, request),
            // AskUserQuestion and ExitPlanMode are sent as can_use_tool with
            // specific tool names. They arrive as control_request with
            // subtype="can_use_tool" and tool_name="AskUserQuestion"/"ExitPlanMode".
            // But some versions send them as dedicated subtypes too — handled by
            // delegating to can_use_tool which checks tool_name.
            _ => vec![],
        }
    }

    fn parse_can_use_tool(
        &mut self,
        request_id: &str,
        request: &serde_json::Value,
    ) -> Vec<AgentEvent> {
        let tool_name = request
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let input = request
            .get("input")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // ExitPlanMode → GateRequest::Plan
        if tool_name == "ExitPlanMode" {
            let summary = input
                .get("plan")
                .and_then(|v| v.as_str())
                .or_else(|| input.get("content").and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_string();
            return vec![AgentEvent::GateRequested {
                gate: GateRequest::Plan {
                    request_id: request_id.to_string(),
                    summary,
                },
            }];
        }

        // AskUserQuestion → GateRequest::Question
        if tool_name == "AskUserQuestion" {
            let questions: Vec<QuestionPrompt> = input
                .get("questions")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|q| {
                            let question = q
                                .get("question")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let header = q
                                .get("header")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let multi_select = q
                                .get("multi_select")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let options: Vec<QuestionOption> = q
                                .get("options")
                                .and_then(|v| v.as_array())
                                .map(|opts| {
                                    opts.iter()
                                        .map(|o| QuestionOption {
                                            label: o
                                                .get("label")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("")
                                                .to_string(),
                                            description: o
                                                .get("description")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("")
                                                .to_string(),
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            QuestionPrompt {
                                question,
                                header,
                                multi_select,
                                options,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            return vec![AgentEvent::GateRequested {
                gate: GateRequest::Question {
                    request_id: request_id.to_string(),
                    questions,
                },
            }];
        }

        // Normal can_use_tool → GateRequest::Permission
        let command = input
            .get("command")
            .and_then(|v| v.as_str())
            .or_else(|| input.get("description").and_then(|v| v.as_str()))
            .or_else(|| input.get("file_path").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        let cwd = input
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let explanation = input
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let permission_suggestions: Vec<serde_json::Value> = request
            .get("permission_suggestions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        vec![AgentEvent::GateRequested {
            gate: GateRequest::Permission {
                request_id: request_id.to_string(),
                tool_name,
                command,
                cwd,
                explanation,
                is_elicitation: false,
                elicitation_choices: vec![],
                permission_suggestions,
                raw_input: input,
            },
        }]
    }

    fn parse_elicitation(
        &mut self,
        request_id: &str,
        request: &serde_json::Value,
    ) -> Vec<AgentEvent> {
        let server = request
            .get("server_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let message = request
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let schema = request.get("requested_schema").cloned();

        // Parse elicitation choices from schema.
        let elicitation_choices = parse_elicitation_choices(schema.as_ref());

        vec![AgentEvent::GateRequested {
            gate: GateRequest::Permission {
                request_id: request_id.to_string(),
                tool_name: server.clone(),
                command: message.clone(),
                cwd: String::new(),
                explanation: message,
                is_elicitation: true,
                elicitation_choices,
                permission_suggestions: vec![],
                raw_input: serde_json::Value::Null,
            },
        }]
    }

    // ── rate_limit_event ──────────────────────────────────────────────────

    fn parse_rate_limit(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let info_val = obj
            .get("rate_limit_info")
            .unwrap_or(&serde_json::Value::Null);
        let info = RateLimitInfo {
            status: info_val
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            resets_at: info_val
                .get("resetsAt")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            rate_limit_type: info_val
                .get("rateLimitType")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            overage_status: info_val
                .get("overageStatus")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            is_using_overage: info_val
                .get("isUsingOverage")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        };
        vec![AgentEvent::RateLimitNotice { info }]
    }
}

// ============================================================================
// 3. Pure helpers
// ============================================================================

/// Parse a `Usage` from a JSON object (claude's per-message usage shape).
fn parse_usage(v: &serde_json::Value) -> Usage {
    Usage {
        input_tokens: v.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        output_tokens: v.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        cache_read_tokens: v
            .get("cache_read_input_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
        cache_creation_tokens: v
            .get("cache_creation_input_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
        cost_usd: 0.0,
    }
}

/// Parse elicitation choices from a `requested_schema` JSON Schema object.
/// Looks at the FIRST object property with an `enum` array.
fn parse_elicitation_choices(schema: Option<&serde_json::Value>) -> Vec<ElicitationChoice> {
    let props = match schema
        .and_then(|s| s.get("properties"))
        .and_then(|p| p.as_object())
    {
        Some(p) => p,
        None => return vec![],
    };
    for (key, prop) in props {
        let values = match prop.get("enum").and_then(|e| e.as_array()) {
            Some(v) if !v.is_empty() => v,
            _ => continue,
        };
        let names = prop.get("enumNames").and_then(|n| n.as_array());
        let mut out = Vec::with_capacity(values.len());
        for (i, v) in values.iter().enumerate() {
            let value = v
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string());
            let label = names
                .and_then(|n| n.get(i))
                .and_then(|l| l.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| value.clone());
            out.push(ElicitationChoice {
                key: key.clone(),
                value,
                label,
            });
        }
        return out;
    }
    vec![]
}

/// The available permission modes for a claude session, derived from its version.
/// Ported from `marsad_events::claude_available_modes`.
pub fn claude_available_modes(version: &str) -> Vec<SessionMode> {
    let m = |id: &str, label: &str, desc: &str| SessionMode {
        id: id.to_string(),
        label: label.to_string(),
        description: desc.to_string(),
    };
    let mut modes = vec![
        m("default", "Default", "Prompt before edits and commands."),
        m(
            "acceptEdits",
            "Accept edits",
            "Apply edits automatically; commands still ask.",
        ),
        m(
            "plan",
            "Plan",
            "Read-only planning; propose a plan before acting.",
        ),
    ];
    if claude_supports_bypass(version) {
        modes.push(m(
            "bypassPermissions",
            "Bypass permissions",
            "Act without asking. Use only in throwaway sandboxes.",
        ));
    }
    modes
}

fn claude_supports_bypass(version: &str) -> bool {
    match version
        .split('.')
        .next()
        .and_then(|s| s.parse::<u32>().ok())
    {
        Some(major) => major >= 1,
        None => true,
    }
}

// ============================================================================
// 4. Gate-response wire encoders (pure)
// ============================================================================

/// Encode a `can_use_tool` allow response.
///
/// Wire: `{"type":"control_response","response":{"subtype":"success","request_id":"X","response":{"behavior":"allow","updatedInput":{…}}}}`
pub fn encode_allow(
    request_id: &str,
    raw_input: serde_json::Value,
    permission_updates: Option<serde_json::Value>,
) -> String {
    let mut inner = serde_json::json!({
        "behavior": "allow",
        "updatedInput": raw_input,
    });
    if let Some(perms) = permission_updates {
        inner["updatedPermissions"] = perms;
    }
    serde_json::to_string(&serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": inner,
        }
    }))
    .expect("always serializable")
}

/// Encode a `can_use_tool` deny response.
///
/// Wire: `{"type":"control_response","response":{"subtype":"success","request_id":"X","response":{"behavior":"deny","message":"…"}}}`
pub fn encode_deny(request_id: &str, reason: Option<String>) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": {
                "behavior": "deny",
                "message": reason.unwrap_or_default(),
            }
        }
    }))
    .expect("always serializable")
}

/// Encode an MCP elicitation accept response.
///
/// Wire: `{"type":"control_response","response":{"subtype":"success","request_id":"X","response":{"action":"accept","content":{…}}}}`
pub fn encode_elicitation_accept(request_id: &str, content: Option<serde_json::Value>) -> String {
    let inner = serde_json::json!({
        "action": "accept",
        "content": content.unwrap_or_else(|| serde_json::json!({})),
    });
    serde_json::to_string(&serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": inner,
        }
    }))
    .expect("always serializable")
}

/// Encode an MCP elicitation decline response.
///
/// Wire: `{"type":"control_response","response":{"subtype":"success","request_id":"X","response":{"action":"decline"}}}`
pub fn encode_elicitation_decline(request_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": {
                "action": "decline",
            }
        }
    }))
    .expect("always serializable")
}

/// Encode an `AskUserQuestion` answer response.
///
/// Wire: `{"type":"control_response","response":{"subtype":"success","request_id":"X","response":{"behavior":"allow","updatedInput":{"questions":[…],"answers":{…}}}}}`
pub fn encode_question_answer(
    request_id: &str,
    original_input: &serde_json::Value,
    answers: &serde_json::Value,
) -> String {
    let questions = original_input
        .get("questions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    serde_json::to_string(&serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": {
                "behavior": "allow",
                "updatedInput": {
                    "questions": questions,
                    "answers": answers,
                }
            }
        }
    }))
    .expect("always serializable")
}

/// Encode the `initialize` control request.
///
/// Shape: `{"type":"control_request","request_id":"X","request":{"subtype":"initialize","hooks":null}}`
pub fn encode_initialize(request_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "control_request",
        "request_id": request_id,
        "request": {
            "subtype": "initialize",
            "hooks": serde_json::Value::Null,
        }
    }))
    .expect("always serializable")
}

/// Encode the `set_permission_mode` control request.
///
/// Shape: `{"type":"control_request","request_id":"X","request":{"subtype":"set_permission_mode","mode":"plan"}}`
pub fn encode_set_permission_mode(request_id: &str, mode: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "control_request",
        "request_id": request_id,
        "request": {
            "subtype": "set_permission_mode",
            "mode": mode,
        }
    }))
    .expect("always serializable")
}

/// Encode a user message (stream-json `user` with `role: "user"` and content blocks).
///
/// Shape: `{"role":"user","content":[{"type":"text","text":"…"}]}`
pub fn encode_user_message(text: &str) -> String {
    // claude's `--input-format stream-json` keys on a top-level `"type":"user"`
    // envelope; a bare `{"role":"user",...}` line is silently ignored and the
    // CLI waits forever for a real turn (observed as a live hang). Match Marsad's
    // proven `encode_user_input` wire shape exactly.
    serde_json::to_string(&serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{ "type": "text", "text": text }]
        }
    }))
    .expect("always serializable")
}

// ============================================================================
// 5. IO Shell — AgentDriver + Session
// ============================================================================

/// The claude stream-json driver.
pub struct ClaudeDriver {
    /// Path to the `claude` executable. `None` = `"claude"` on PATH.
    bin: Option<String>,
}

impl ClaudeDriver {
    /// Create a driver that uses the given binary path.
    pub fn new(bin: impl Into<String>) -> Self {
        ClaudeDriver {
            bin: Some(bin.into()),
        }
    }

    /// Create a driver that resolves `claude` from PATH.
    pub fn from_path() -> Self {
        ClaudeDriver { bin: None }
    }
}

#[async_trait]
impl AgentDriver for ClaudeDriver {
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            interruptible: false,
            model_switchable: false,
            mode_switchable: true,
            persistent_conversation: true,
            resumable: true,
            streaming_output: false,
            reports_usage: true,
            supports_gates: true,
        }
    }

    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError> {
        let mut spawn_spec = spec.clone();
        // Override bin from driver config if spec doesn't specify one.
        if spawn_spec.bin.is_none() {
            spawn_spec.bin = self.bin.clone();
        }

        let argv = claude_argv(&spawn_spec);
        if argv.is_empty() {
            return Err(DriverError::SpawnFailed {
                detail: "empty argv".to_string(),
            });
        }

        let mut cmd = tokio::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);

        if let Some(ref cwd) = spawn_spec.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &spawn_spec.env {
            cmd.env(k, v);
        }

        let mut child = cmd.spawn().map_err(|e| DriverError::SpawnFailed {
            detail: format!("spawn {}: {e}", argv[0]),
        })?;

        let stdin = child.stdin.take().ok_or_else(|| DriverError::SpawnFailed {
            detail: "no stdin".to_string(),
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DriverError::SpawnFailed {
                detail: "no stdout".to_string(),
            })?;

        // Event channel: reader task → session.
        let (tx, rx) = mpsc::channel::<AgentEvent>(256);

        let first_prompt = spawn_spec.first_prompt.clone();

        // Start the reader task.
        tokio::spawn(reader_task(stdout, tx.clone(), first_prompt.is_none()));

        let mut session = ClaudeSession {
            child,
            stdin: Some(stdin),
            rx,
            mode_req_counter: 1,
        };

        // Send initialize handshake.
        let init = encode_initialize("req_init");
        session
            .write_line(&init)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("initialize: {e}"),
            })?;

        // Deliver first_prompt if any.
        if let Some(prompt) = first_prompt {
            let msg = encode_user_message(&prompt);
            session
                .write_line(&msg)
                .await
                .map_err(|e| DriverError::SpawnFailed {
                    detail: format!("first_prompt: {e}"),
                })?;
        }

        Ok(Box::new(session))
    }
}

/// Background reader task: reads stdout lines and sends AgentEvents.
async fn reader_task(stdout: ChildStdout, tx: mpsc::Sender<AgentEvent>, _send_init: bool) {
    let mut reader = BufReader::new(stdout).lines();
    let mut parser = LineParser::new();
    loop {
        match reader.next_line().await {
            Ok(Some(line)) => {
                let events = parser.on_line(&line);
                for ev in events {
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
            Ok(None) => {
                // EOF — process exited.
                let _ = tx.send(AgentEvent::ProcessExited { code: None }).await;
                return;
            }
            Err(_) => {
                let _ = tx.send(AgentEvent::ProcessExited { code: None }).await;
                return;
            }
        }
    }
}

/// A live claude session (process + stdin/stdout pipes).
pub struct ClaudeSession {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: mpsc::Receiver<AgentEvent>,
    mode_req_counter: u64,
}

impl ClaudeSession {
    async fn write_line(&mut self, line: &str) -> Result<(), std::io::Error> {
        if let Some(ref mut stdin) = self.stdin {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "stdin closed",
            ))
        }
    }
}

#[async_trait]
impl Session for ClaudeSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.rx.recv().await
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        let msg = encode_user_message(text);
        self.write_line(&msg)
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("send_prompt: {e}"),
            })
    }

    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError> {
        let line = match decision {
            GateDecision::Approve {
                updated_input,
                permission_updates,
                elicitation_content,
            } => {
                if elicitation_content.is_some()
                    || updated_input.is_none() && permission_updates.is_none()
                {
                    // Could be elicitation accept or plain can_use_tool approve.
                    // We can't distinguish here without context — caller should use
                    // the right variant. Treat as elicitation accept if content present.
                    if let Some(content) = elicitation_content {
                        encode_elicitation_accept(request_id, Some(content))
                    } else {
                        encode_allow(request_id, serde_json::json!({}), permission_updates)
                    }
                } else {
                    encode_allow(
                        request_id,
                        updated_input.unwrap_or(serde_json::json!({})),
                        permission_updates,
                    )
                }
            }
            GateDecision::Deny { reason } => encode_deny(request_id, reason),
        };
        self.write_line(&line)
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("answer_gate: {e}"),
            })
    }

    async fn answer_question(
        &mut self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<(), DriverError> {
        let answers_obj: serde_json::Value =
            serde_json::to_value(&answer.answers).unwrap_or_else(|_| serde_json::json!({}));
        let line = encode_question_answer(request_id, &serde_json::json!({}), &answers_obj);
        self.write_line(&line)
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("answer_question: {e}"),
            })
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "interrupt (not yet verified for claude in this driver)".to_string(),
        })
    }

    async fn set_model(&mut self, _model: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_model (claude does not support live model changes)".to_string(),
        })
    }

    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError> {
        self.mode_req_counter += 1;
        let req_id = format!("req_mode_{}", self.mode_req_counter);
        let line = encode_set_permission_mode(&req_id, mode_id);
        self.write_line(&line)
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("set_mode: {e}"),
            })
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        // Close stdin by dropping it → claude reads EOF and exits.
        // The child has kill_on_drop, so it will also be killed on session drop.
        self.stdin.take(); // drops ChildStdin, which closes the fd
        let _ = self.child.wait().await;
        Ok(())
    }
}

// ============================================================================
// Unit tests (pure parts only — no IO, no process spawn)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use wasit_core::{AgentEvent, ResumeMode, SpawnSpec};

    // ── helpers ─────────────────────────────────────────────────────────────

    fn base_spec() -> SpawnSpec {
        SpawnSpec {
            model: Some("claude-sonnet-4".to_string()),
            ..SpawnSpec::default()
        }
    }

    fn parse_one(line: &str) -> AgentEvent {
        let mut p = LineParser::new();
        let mut events = p.on_line(line);
        assert_eq!(events.len(), 1, "expected exactly 1 event from: {line}");
        events.remove(0)
    }

    fn parse_all(line: &str) -> Vec<AgentEvent> {
        let mut p = LineParser::new();
        p.on_line(line)
    }

    // ── claude_argv: basic prefix ────────────────────────────────────────────

    #[test]
    fn argv_prefix_exact() {
        let argv = claude_argv(&base_spec());
        assert!(argv.len() >= 7, "need at least 7 elements; got {:?}", argv);
        assert_eq!(argv[0], "claude");
        assert_eq!(argv[1], "--print");
        assert_eq!(argv[2], "--input-format");
        assert_eq!(argv[3], "stream-json");
        assert_eq!(argv[4], "--output-format");
        assert_eq!(argv[5], "stream-json");
        assert_eq!(argv[6], "--verbose");
    }

    #[test]
    fn argv_custom_bin() {
        let mut spec = base_spec();
        spec.bin = Some("genie".to_string());
        let argv = claude_argv(&spec);
        assert_eq!(argv[0], "genie");
        assert_eq!(argv[1], "--print");
    }

    #[test]
    fn argv_model_present() {
        let argv = claude_argv(&base_spec());
        let idx = argv
            .iter()
            .position(|a| a == "--model")
            .expect("--model missing");
        assert_eq!(argv[idx + 1], "claude-sonnet-4");
    }

    #[test]
    fn argv_no_model_when_none() {
        let mut spec = base_spec();
        spec.model = None;
        let argv = claude_argv(&spec);
        assert!(!argv.contains(&"--model".to_string()));
    }

    #[test]
    fn argv_empty_mode_omits_permission_mode() {
        let argv = claude_argv(&base_spec());
        assert!(!argv.contains(&"--permission-mode".to_string()));
    }

    #[test]
    fn argv_plan_mode() {
        let mut spec = base_spec();
        spec.mode = "plan".to_string();
        let argv = claude_argv(&spec);
        let idx = argv
            .iter()
            .position(|a| a == "--permission-mode")
            .expect("--permission-mode missing");
        assert_eq!(argv[idx + 1], "plan");
    }

    #[test]
    fn argv_bypass_mode() {
        let mut spec = base_spec();
        spec.mode = "bypassPermissions".to_string();
        let argv = claude_argv(&spec);
        let idx = argv
            .iter()
            .position(|a| a == "--permission-mode")
            .expect("--permission-mode missing");
        assert_eq!(argv[idx + 1], "bypassPermissions");
    }

    #[test]
    fn argv_permission_prompt_tool_stdio_always() {
        for mode in ["", "default", "plan", "bypassPermissions"] {
            let mut spec = base_spec();
            spec.mode = mode.to_string();
            let argv = claude_argv(&spec);
            let pos = argv
                .iter()
                .position(|a| a == "--permission-prompt-tool")
                .expect("--permission-prompt-tool missing");
            assert_eq!(argv[pos + 1], "stdio", "mode={mode}");
        }
    }

    #[test]
    fn argv_fresh_no_resume() {
        let argv = claude_argv(&base_spec());
        assert!(!argv.contains(&"--resume".to_string()));
        assert!(!argv.contains(&"--continue".to_string()));
    }

    #[test]
    fn argv_continue_mode() {
        let mut spec = base_spec();
        spec.resume = ResumeMode::Continue;
        let argv = claude_argv(&spec);
        assert!(argv.contains(&"--continue".to_string()));
        assert!(!argv.contains(&"--resume".to_string()));
    }

    #[test]
    fn argv_resume_mode() {
        let mut spec = base_spec();
        spec.resume = ResumeMode::Resume("sess-uuid-abc".to_string());
        let argv = claude_argv(&spec);
        let idx = argv
            .iter()
            .position(|a| a == "--resume")
            .expect("--resume missing");
        assert_eq!(argv[idx + 1], "sess-uuid-abc");
    }

    #[test]
    fn argv_disallowed_tools_present() {
        let mut spec = base_spec();
        spec.disallowed_tools = vec!["Bash".to_string(), "Edit".to_string()];
        let argv = claude_argv(&spec);
        let idx = argv
            .iter()
            .position(|a| a == "--disallowedTools")
            .expect("--disallowedTools missing");
        assert_eq!(argv[idx + 1], "Bash,Edit");
    }

    #[test]
    fn argv_disallowed_tools_absent_when_empty() {
        let argv = claude_argv(&base_spec());
        assert!(!argv.contains(&"--disallowedTools".to_string()));
    }

    #[test]
    fn argv_mcp_config_present() {
        let mut spec = base_spec();
        spec.mcp_config = Some("/tmp/mcp.json".to_string());
        let argv = claude_argv(&spec);
        let idx = argv
            .iter()
            .position(|a| a == "--mcp-config")
            .expect("--mcp-config missing");
        assert_eq!(argv[idx + 1], "/tmp/mcp.json");
    }

    #[test]
    fn argv_first_prompt_not_in_argv() {
        let mut spec = base_spec();
        spec.first_prompt = Some("hello world".to_string());
        let argv = claude_argv(&spec);
        assert!(!argv.iter().any(|a| a.contains("hello world")));
    }

    #[test]
    fn argv_deterministic() {
        let spec = base_spec();
        assert_eq!(claude_argv(&spec), claude_argv(&spec));
    }

    // ── on_line: system/init ─────────────────────────────────────────────────

    #[test]
    fn on_line_system_init() {
        let line = r#"{"type":"system","subtype":"init","session_id":"ses-abc","model":"claude-sonnet-4.6[1m]","cwd":"/proj","tools":["Bash","Read"],"mcp_servers":[{"name":"fs","status":"ok"}],"permissionMode":"default","claude_code_version":"1.0.5","slash_commands":["clear"]}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::SessionInit {
                native_session_id,
                model,
                cwd,
                tools,
                mcp_servers,
                slash_commands,
                permission_mode,
                claude_code_version,
                available_modes,
                current_mode,
            } => {
                assert_eq!(native_session_id, Some("ses-abc".to_string()));
                assert_eq!(model, Some("claude-sonnet-4.6[1m]".to_string()));
                assert_eq!(cwd, Some("/proj".to_string()));
                assert_eq!(tools, vec!["Bash", "Read"]);
                assert_eq!(mcp_servers.len(), 1);
                assert_eq!(mcp_servers[0].name, "fs");
                assert_eq!(mcp_servers[0].status, "ok");
                assert_eq!(slash_commands.len(), 1);
                assert!(slash_commands[0].name.contains("clear"));
                assert_eq!(permission_mode, "default");
                assert_eq!(claude_code_version, "1.0.5");
                assert!(!available_modes.is_empty(), "modes should be derived");
                assert_eq!(current_mode, "default");
            }
            other => panic!("expected SessionInit, got {other:?}"),
        }
    }

    #[test]
    fn on_line_system_init_available_modes_version_1() {
        let line = r#"{"type":"system","subtype":"init","session_id":"s","model":"m","cwd":"/","tools":[],"permissionMode":"default","claude_code_version":"1.2.3"}"#;
        let ev = parse_one(line);
        if let AgentEvent::SessionInit {
            available_modes, ..
        } = ev
        {
            // Version 1 → bypassPermissions included
            assert!(available_modes.iter().any(|m| m.id == "bypassPermissions"));
            assert!(available_modes.iter().any(|m| m.id == "plan"));
        } else {
            panic!("expected SessionInit");
        }
    }

    #[test]
    fn on_line_system_init_available_modes_version_0() {
        let line = r#"{"type":"system","subtype":"init","session_id":"s","model":"m","cwd":"/","tools":[],"permissionMode":"default","claude_code_version":"0.9.0"}"#;
        let ev = parse_one(line);
        if let AgentEvent::SessionInit {
            available_modes, ..
        } = ev
        {
            // Version 0 → bypassPermissions NOT included
            assert!(!available_modes.iter().any(|m| m.id == "bypassPermissions"));
        } else {
            panic!("expected SessionInit");
        }
    }

    // ── on_line: system/status → ModesUpdated ───────────────────────────────

    #[test]
    fn on_line_system_status_mode() {
        let line = r#"{"type":"system","subtype":"status","permissionMode":"plan"}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ModesUpdated {
                available_modes,
                current_mode,
            } => {
                assert!(available_modes.is_empty(), "status → no list change");
                assert_eq!(current_mode, Some("plan".to_string()));
            }
            other => panic!("expected ModesUpdated, got {other:?}"),
        }
    }

    #[test]
    fn on_line_system_status_no_mode() {
        // system/status without permissionMode → nothing emitted
        let line = r#"{"type":"system","subtype":"status","foo":"bar"}"#;
        let events = parse_all(line);
        assert!(
            events.is_empty(),
            "no events for status without permissionMode"
        );
    }

    // ── on_line: system/compact_boundary ────────────────────────────────────

    #[test]
    fn on_line_compact_boundary() {
        let line = r#"{"type":"system","subtype":"compact_boundary","compact_metadata":{"trigger":"manual","pre_tokens":5000,"post_tokens":2000,"cumulative_dropped_tokens":3000,"duration_ms":420}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::Compaction { info } => {
                assert_eq!(info.trigger, "manual");
                assert_eq!(info.pre_tokens, 5000);
                assert_eq!(info.post_tokens, 2000);
                assert_eq!(info.cumulative_dropped_tokens, 3000);
                assert_eq!(info.duration_ms, 420);
            }
            other => panic!("expected Compaction, got {other:?}"),
        }
    }

    // ── on_line: assistant text ──────────────────────────────────────────────

    #[test]
    fn on_line_assistant_text() {
        let line = r#"{"type":"assistant","message":{"id":"msg-1","content":[{"type":"text","text":"Hello!"}],"usage":{"input_tokens":10,"output_tokens":5}}}"#;
        let events = parse_all(line);
        // Expect: TurnStarted, AssistantDelta, UsageUpdate
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::TurnStarted { .. })));
        let delta = events
            .iter()
            .find(|e| matches!(e, AgentEvent::AssistantDelta { .. }))
            .expect("AssistantDelta missing");
        if let AgentEvent::AssistantDelta { text, message_id } = delta {
            assert_eq!(text, "Hello!");
            assert_eq!(message_id.as_deref(), Some("msg-1"));
        }
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::UsageUpdate { .. })));
    }

    #[test]
    fn on_line_assistant_turn_started_only_once() {
        // Second assistant line in same turn must NOT produce another TurnStarted.
        let mut p = LineParser::new();
        let line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"text","text":"a"}],"usage":{"input_tokens":1,"output_tokens":1}}}"#;
        let evs1 = p.on_line(line);
        let evs2 = p.on_line(line);
        assert!(evs1
            .iter()
            .any(|e| matches!(e, AgentEvent::TurnStarted { .. })));
        assert!(
            !evs2
                .iter()
                .any(|e| matches!(e, AgentEvent::TurnStarted { .. })),
            "TurnStarted must not appear on second assistant line"
        );
    }

    // ── on_line: assistant thinking ─────────────────────────────────────────

    #[test]
    fn on_line_assistant_thinking() {
        let line = r#"{"type":"assistant","message":{"id":"msg-2","content":[{"type":"thinking","thinking":"Let me think...","signature":"sig"}],"usage":{"input_tokens":5,"output_tokens":2}}}"#;
        let events = parse_all(line);
        let thinking = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ThinkingDelta { .. }))
            .expect("ThinkingDelta missing");
        if let AgentEvent::ThinkingDelta { text, message_id } = thinking {
            assert_eq!(text, "Let me think...");
            assert_eq!(message_id.as_deref(), Some("msg-2"));
        }
    }

    // ── on_line: assistant tool_use ─────────────────────────────────────────

    #[test]
    fn on_line_assistant_tool_use_bash() {
        let line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"command":"ls -la","cwd":"/proj"}}],"usage":{"input_tokens":10,"output_tokens":5}}}"#;
        let events = parse_all(line);
        let tool = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ToolCallStarted { .. }))
            .expect("ToolCallStarted missing");
        if let AgentEvent::ToolCallStarted {
            name,
            call_id,
            input,
            is_subagent,
            subagent_model,
        } = tool
        {
            assert_eq!(name, "Bash");
            assert_eq!(call_id, "toolu_01");
            assert_eq!(input["command"], "ls -la");
            assert!(!is_subagent);
            assert!(subagent_model.is_none());
        }
    }

    #[test]
    fn on_line_assistant_tool_use_subagent_task() {
        let line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"toolu_02","name":"Task","input":{"description":"search","model":"claude-haiku-4"}}],"usage":{"input_tokens":10,"output_tokens":5}}}"#;
        let events = parse_all(line);
        let tool = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ToolCallStarted { .. }))
            .expect("ToolCallStarted missing");
        if let AgentEvent::ToolCallStarted {
            name,
            is_subagent,
            subagent_model,
            ..
        } = tool
        {
            assert_eq!(name, "Task");
            assert!(is_subagent);
            assert_eq!(subagent_model.as_deref(), Some("claude-haiku-4"));
        }
    }

    #[test]
    fn on_line_assistant_tool_use_agent_subagent() {
        let line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"toolu_03","name":"Agent","input":{"description":"sub"}}],"usage":{"input_tokens":5,"output_tokens":2}}}"#;
        let events = parse_all(line);
        let tool = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ToolCallStarted { .. }))
            .expect("ToolCallStarted missing");
        if let AgentEvent::ToolCallStarted {
            name, is_subagent, ..
        } = tool
        {
            assert_eq!(name, "Agent");
            assert!(is_subagent);
        }
    }

    // ── on_line: user tool_result ────────────────────────────────────────────

    #[test]
    fn on_line_user_tool_result() {
        // First emit a tool_use so the name is tracked.
        let mut p = LineParser::new();
        let assistant_line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"command":"ls"}}],"usage":{"input_tokens":5,"output_tokens":2}}}"#;
        p.on_line(assistant_line);

        let user_line = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"file1.rs\nfile2.rs","is_error":false}]}}"#;
        let events = p.on_line(user_line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolCallFinished {
                call_id,
                name,
                ok,
                result,
            } => {
                assert_eq!(call_id, "toolu_01");
                assert_eq!(name, "Bash");
                assert!(ok);
                assert!(result.is_some());
            }
            other => panic!("expected ToolCallFinished, got {other:?}"),
        }
    }

    #[test]
    fn on_line_user_tool_result_error() {
        let mut p = LineParser::new();
        let assistant_line = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"toolu_04","name":"Bash","input":{"command":"rm -rf /"}}],"usage":{"input_tokens":5,"output_tokens":2}}}"#;
        p.on_line(assistant_line);

        let user_line = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_04","content":"Permission denied","is_error":true}]}}"#;
        let events = p.on_line(user_line);
        match &events[0] {
            AgentEvent::ToolCallFinished { ok, .. } => assert!(!ok),
            other => panic!("expected ToolCallFinished, got {other:?}"),
        }
    }

    // ── on_line: result ──────────────────────────────────────────────────────

    #[test]
    fn on_line_result_success() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,"was_interrupted":false,"num_turns":3,"total_cost_usd":0.042,"usage":{"input_tokens":100,"output_tokens":50,"cache_read_input_tokens":20,"cache_creation_input_tokens":10},"result":"Done!"}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns,
                usage,
                reply,
                ..
            } => {
                assert!(end_reason.is_none());
                assert!(!was_interrupted);
                assert_eq!(num_turns, 3);
                assert_eq!(usage.input_tokens, 100);
                assert_eq!(usage.output_tokens, 50);
                assert_eq!(usage.cache_read_tokens, 20);
                assert_eq!(usage.cache_creation_tokens, 10);
                assert!((usage.cost_usd - 0.042).abs() < 1e-6);
                assert_eq!(reply, Some("Done!".to_string()));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_line_result_max_turns() {
        let line = r#"{"type":"result","subtype":"error_max_turns","is_error":true,"was_interrupted":false,"num_turns":10,"total_cost_usd":0.1,"usage":{"input_tokens":500,"output_tokens":200}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, .. } => {
                assert_eq!(end_reason, Some(EndReason::MaxTurns));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_line_result_error_during_execution() {
        let line = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"was_interrupted":false,"num_turns":1,"total_cost_usd":0.01,"usage":{"input_tokens":10,"output_tokens":5}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, .. } => {
                assert_eq!(end_reason, Some(EndReason::ExecutionError));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_line_result_cancelled() {
        let line = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"was_interrupted":true,"num_turns":1,"total_cost_usd":0.01,"usage":{"input_tokens":10,"output_tokens":5}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                ..
            } => {
                assert_eq!(end_reason, Some(EndReason::Cancelled));
                assert!(was_interrupted);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_line_result_per_model() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,"was_interrupted":false,"num_turns":1,"total_cost_usd":0.05,"usage":{"input_tokens":100,"output_tokens":50},"modelUsage":{"claude-sonnet-4":{"inputTokens":100,"outputTokens":50,"costUSD":0.05}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { per_model, .. } => {
                assert!(per_model.contains_key("claude-sonnet-4"));
                let mu = &per_model["claude-sonnet-4"];
                assert_eq!(mu.input_tokens, 100);
                assert_eq!(mu.output_tokens, 50);
                assert!((mu.cost_usd - 0.05).abs() < 1e-6);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_line_result_resets_turn_started() {
        // After a result, the next assistant line should produce TurnStarted again.
        let mut p = LineParser::new();
        let assistant = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":1,"output_tokens":1}}}"#;
        let result = r#"{"type":"result","subtype":"success","is_error":false,"was_interrupted":false,"num_turns":1,"total_cost_usd":0,"usage":{"input_tokens":1,"output_tokens":1}}"#;
        p.on_line(assistant); // First turn.
        p.on_line(result); // Ends turn.
        let evs = p.on_line(assistant); // Second turn — must produce TurnStarted.
        assert!(
            evs.iter()
                .any(|e| matches!(e, AgentEvent::TurnStarted { .. })),
            "second turn must produce TurnStarted"
        );
    }

    // ── on_line: control_request can_use_tool ────────────────────────────────

    #[test]
    fn on_line_can_use_tool_bash() {
        let line = r#"{"type":"control_request","request_id":"req-1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"git status","cwd":"/proj"},"permission_suggestions":[]}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    command,
                    cwd,
                    is_elicitation,
                    permission_suggestions,
                    raw_input,
                    ..
                } => {
                    assert_eq!(request_id, "req-1");
                    assert_eq!(tool_name, "Bash");
                    assert_eq!(command, "git status");
                    assert_eq!(cwd, "/proj");
                    assert!(!is_elicitation);
                    assert!(permission_suggestions.is_empty());
                    assert_eq!(raw_input["command"], "git status");
                }
                other => panic!("expected Permission gate, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    #[test]
    fn on_line_can_use_tool_with_permission_suggestions() {
        let line = r#"{"type":"control_request","request_id":"req-2","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"npm test"},"permission_suggestions":[{"behavior":"allow","destination":"user","rules":["npm test"]}]}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested {
                gate:
                    GateRequest::Permission {
                        permission_suggestions,
                        ..
                    },
            } => {
                assert_eq!(permission_suggestions.len(), 1);
            }
            other => panic!("expected GateRequested(Permission), got {other:?}"),
        }
    }

    // ── on_line: control_request elicitation ─────────────────────────────────

    #[test]
    fn on_line_elicitation_gate() {
        let line = r#"{"type":"control_request","request_id":"req-3","request":{"subtype":"elicitation","server_name":"hares","message":"Grant access?","requested_schema":{"properties":{"grant":{"enum":["once","session","deny"],"enumNames":["Once","Session","Deny"]}}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    is_elicitation,
                    elicitation_choices,
                    ..
                } => {
                    assert_eq!(request_id, "req-3");
                    assert_eq!(tool_name, "hares");
                    assert!(is_elicitation);
                    assert_eq!(elicitation_choices.len(), 3);
                    assert_eq!(elicitation_choices[0].value, "once");
                    assert_eq!(elicitation_choices[0].label, "Once");
                    assert_eq!(elicitation_choices[0].key, "grant");
                }
                other => panic!("expected Permission, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    #[test]
    fn on_line_elicitation_no_schema() {
        let line = r#"{"type":"control_request","request_id":"req-4","request":{"subtype":"elicitation","server_name":"myserver","message":"Proceed?"}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested {
                gate:
                    GateRequest::Permission {
                        is_elicitation,
                        elicitation_choices,
                        ..
                    },
            } => {
                assert!(is_elicitation);
                assert!(elicitation_choices.is_empty());
            }
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    // ── on_line: ExitPlanMode ────────────────────────────────────────────────

    #[test]
    fn on_line_exit_plan_mode() {
        let line = r#"{"type":"control_request","request_id":"req-plan","request":{"subtype":"can_use_tool","tool_name":"ExitPlanMode","input":{"plan":"1. Read files\n2. Write output"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Plan {
                    request_id,
                    summary,
                } => {
                    assert_eq!(request_id, "req-plan");
                    assert!(summary.contains("Read files"));
                }
                other => panic!("expected Plan gate, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    // ── on_line: AskUserQuestion ─────────────────────────────────────────────

    #[test]
    fn on_line_ask_user_question() {
        let line = r#"{"type":"control_request","request_id":"req-q","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[{"question":"What is your goal?","header":"Goal","multi_select":false,"options":[{"label":"Refactor","description":""},{"label":"Debug","description":""}]}]}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Question {
                    request_id,
                    questions,
                } => {
                    assert_eq!(request_id, "req-q");
                    assert_eq!(questions.len(), 1);
                    assert_eq!(questions[0].question, "What is your goal?");
                    assert_eq!(questions[0].header, "Goal");
                    assert!(!questions[0].multi_select);
                    assert_eq!(questions[0].options.len(), 2);
                    assert_eq!(questions[0].options[0].label, "Refactor");
                }
                other => panic!("expected Question, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    // ── on_line: rate_limit_event ────────────────────────────────────────────

    #[test]
    fn on_line_rate_limit_event() {
        let line = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":1700000000,"rateLimitType":"hourly","overageStatus":"","isUsingOverage":false}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::RateLimitNotice { info } => {
                assert_eq!(info.status, "allowed");
                assert_eq!(info.resets_at, 1700000000);
                assert_eq!(info.rate_limit_type, "hourly");
                assert!(!info.is_using_overage);
            }
            other => panic!("expected RateLimitNotice, got {other:?}"),
        }
    }

    // ── on_line: unknown ─────────────────────────────────────────────────────

    #[test]
    fn on_line_unknown_system_subtype() {
        let line = r#"{"type":"system","subtype":"task_started","data":{}}"#;
        let ev = parse_one(line);
        assert!(matches!(ev, AgentEvent::Unknown { .. }));
    }

    #[test]
    fn on_line_unknown_top_level_type() {
        let line = r#"{"type":"some_future_type","data":42}"#;
        let ev = parse_one(line);
        assert!(matches!(ev, AgentEvent::Unknown { .. }));
    }

    #[test]
    fn on_line_empty_string() {
        let events = parse_all("");
        assert!(events.is_empty());
    }

    #[test]
    fn on_line_whitespace_only() {
        let events = parse_all("   ");
        assert!(events.is_empty());
    }

    #[test]
    fn on_line_invalid_json() {
        let events = parse_all("not json {{{");
        assert!(events.is_empty());
    }

    #[test]
    fn on_line_control_response_ignored() {
        // control_response (initialize ack) → silently ignored, no event.
        let line = r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_init","commands":[]}}"#;
        let events = parse_all(line);
        assert!(
            events.is_empty(),
            "control_response ack must be silently ignored"
        );
    }

    // ── gate encoder: allow ──────────────────────────────────────────────────

    #[test]
    fn encode_allow_basic() {
        let json = encode_allow("req-1", serde_json::json!({"command":"ls"}), None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_response");
        assert_eq!(v["response"]["subtype"], "success");
        assert_eq!(v["response"]["request_id"], "req-1");
        assert_eq!(v["response"]["response"]["behavior"], "allow");
        assert_eq!(v["response"]["response"]["updatedInput"]["command"], "ls");
        assert!(v["response"]["response"]["updatedPermissions"].is_null());
    }

    #[test]
    fn encode_allow_with_permission_updates() {
        let perms = serde_json::json!([{"behavior":"allow","destination":"user","rules":["ls"]}]);
        let json = encode_allow("req-2", serde_json::json!({"command":"ls"}), Some(perms));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["response"]["response"]["updatedPermissions"].is_array());
    }

    // ── gate encoder: deny ───────────────────────────────────────────────────

    #[test]
    fn encode_deny_basic() {
        let json = encode_deny("req-3", Some("not allowed".to_string()));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_response");
        assert_eq!(v["response"]["response"]["behavior"], "deny");
        assert_eq!(v["response"]["response"]["message"], "not allowed");
    }

    #[test]
    fn encode_deny_no_reason() {
        let json = encode_deny("req-4", None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["response"]["response"]["message"], "");
    }

    // ── gate encoder: elicitation accept/decline ─────────────────────────────

    #[test]
    fn encode_elicitation_accept_with_content() {
        let content = serde_json::json!({"grant":"session"});
        let json = encode_elicitation_accept("req-5", Some(content));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_response");
        assert_eq!(v["response"]["response"]["action"], "accept");
        assert_eq!(v["response"]["response"]["content"]["grant"], "session");
    }

    #[test]
    fn encode_elicitation_accept_no_content() {
        let json = encode_elicitation_accept("req-6", None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["response"]["response"]["action"], "accept");
        assert_eq!(v["response"]["response"]["content"], serde_json::json!({}));
    }

    #[test]
    fn encode_elicitation_decline_shape() {
        let json = encode_elicitation_decline("req-7");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_response");
        assert_eq!(v["response"]["response"]["action"], "decline");
        // No "content" key on decline.
        assert!(v["response"]["response"]["content"].is_null());
    }

    // ── gate encoder: question answer ────────────────────────────────────────

    #[test]
    fn encode_question_answer_shape() {
        let original_input = serde_json::json!({"questions":[{"question":"Q1?"}]});
        let answers = serde_json::json!({"Q1?":"Option A"});
        let json = encode_question_answer("req-8", &original_input, &answers);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_response");
        assert_eq!(v["response"]["response"]["behavior"], "allow");
        assert!(v["response"]["response"]["updatedInput"]["questions"].is_array());
        assert_eq!(
            v["response"]["response"]["updatedInput"]["answers"]["Q1?"],
            "Option A"
        );
    }

    // ── gate encoder: set_permission_mode ───────────────────────────────────

    #[test]
    fn encode_set_permission_mode_shape() {
        let json = encode_set_permission_mode("req-9", "plan");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_request");
        assert_eq!(v["request"]["subtype"], "set_permission_mode");
        assert_eq!(v["request"]["mode"], "plan");
        assert_eq!(v["request_id"], "req-9");
    }

    // ── gate encoder: initialize ─────────────────────────────────────────────

    #[test]
    fn encode_initialize_shape() {
        let json = encode_initialize("req_init");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "control_request");
        assert_eq!(v["request_id"], "req_init");
        assert_eq!(v["request"]["subtype"], "initialize");
        assert!(v["request"]["hooks"].is_null());
    }

    // ── encode_user_message ──────────────────────────────────────────────────

    #[test]
    fn encode_user_message_shape() {
        let json = encode_user_message("hello");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Must be the `{"type":"user","message":{...}}` envelope claude requires.
        assert_eq!(v["type"], "user");
        assert_eq!(v["message"]["role"], "user");
        assert_eq!(v["message"]["content"][0]["type"], "text");
        assert_eq!(v["message"]["content"][0]["text"], "hello");
    }

    // ── claude_available_modes ───────────────────────────────────────────────

    #[test]
    fn available_modes_version_1_includes_bypass() {
        let modes = claude_available_modes("1.0.5");
        let ids: Vec<&str> = modes.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"default"));
        assert!(ids.contains(&"acceptEdits"));
        assert!(ids.contains(&"plan"));
        assert!(ids.contains(&"bypassPermissions"));
    }

    #[test]
    fn available_modes_version_0_no_bypass() {
        let modes = claude_available_modes("0.9.0");
        assert!(!modes.iter().any(|m| m.id == "bypassPermissions"));
        assert!(modes.iter().any(|m| m.id == "plan"));
    }

    #[test]
    fn available_modes_empty_version_assumes_modern() {
        let modes = claude_available_modes("");
        assert!(modes.iter().any(|m| m.id == "bypassPermissions"));
    }

    // ── parse_elicitation_choices ────────────────────────────────────────────

    #[test]
    fn elicitation_choices_parsed() {
        let schema = serde_json::json!({
            "properties": {
                "grant": {
                    "enum": ["once","session","deny"],
                    "enumNames": ["Once","This Session","Deny"]
                }
            }
        });
        let choices = parse_elicitation_choices(Some(&schema));
        assert_eq!(choices.len(), 3);
        assert_eq!(choices[0].key, "grant");
        assert_eq!(choices[0].value, "once");
        assert_eq!(choices[0].label, "Once");
        assert_eq!(choices[1].value, "session");
        assert_eq!(choices[1].label, "This Session");
        assert_eq!(choices[2].value, "deny");
        assert_eq!(choices[2].label, "Deny");
    }

    #[test]
    fn elicitation_choices_no_schema() {
        assert!(parse_elicitation_choices(None).is_empty());
    }

    #[test]
    fn elicitation_choices_empty_schema() {
        let schema = serde_json::json!({"properties": {}});
        assert!(parse_elicitation_choices(Some(&schema)).is_empty());
    }

    // ── capabilities ────────────────────────────────────────────────────────

    #[test]
    fn driver_capabilities_honest() {
        let driver = ClaudeDriver::from_path();
        let caps = driver.capabilities();
        assert!(
            !caps.interruptible,
            "interruptible must be false (unverified)"
        );
        assert!(!caps.model_switchable);
        assert!(caps.mode_switchable);
        assert!(caps.persistent_conversation);
        assert!(caps.resumable);
        assert!(
            !caps.streaming_output,
            "claude delivers whole messages, not token deltas"
        );
        assert!(caps.reports_usage);
        assert!(caps.supports_gates);
    }
}
