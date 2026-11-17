//! wasit-opencode — opencode ACP (Agent Communication Protocol) JSON-RPC driver for Wasit.
//!
//! Implements `AgentDriver` + `Session` for the `opencode` CLI using the ACP
//! JSON-RPC-over-stdio protocol.
//!
//! ## Wire protocol overview
//!
//! Opencode communicates over stdin/stdout as NDJSON JSON-RPC 2.0 lines.
//! Handshake: `initialize` → `session/new` (or `session/load` for resume).
//! The init response carries `configOptions` with mode and model catalogs.
//! Per-turn: `session/prompt` request → `session/update` notifications
//! (agent_message_chunk, tool_call, tool_call_update, plan, usage_update,
//! current_mode_update, available_commands_update, context_compaction, …)
//! → `session/prompt` result (stopReason + usage).
//! Interrupt: `session/cancel` request.
//! Live config change: `session/set_config_option` (model, mode).
//! Permission gate: `session/request_permission` JSON-RPC request (must be
//! answered before the turn can proceed).
//!
//! ## Capability report (honest)
//! - `interruptible`: true — `session/cancel` request.
//! - `model_switchable`: true — `session/set_config_option` with `id="model"`.
//! - `mode_switchable`: true — `session/set_config_option` with `id="mode"`.
//! - `persistent_conversation`: true — session context persists across turns.
//! - `resumable`: true — `session/load` with a known sessionId.
//! - `streaming_output`: true — `agent_message_chunk` updates.
//! - `reports_usage`: true — `usage_update` and `session/prompt` result usage.
//! - `supports_gates`: true — `session/request_permission` method.
//!
//! ## Gaps vs. claude richness
//! - No per-message `usage` (usage comes as periodic `usage_update` + turn-end
//!   usage in the result; no dedup by message id).
//! - No `AskUserQuestion` gate type (opencode's equivalent is the raw
//!   `session/request_permission` which carries `options`; mapped to a
//!   `GateRequest::Permission` with elicitation_choices).
//! - No `ExitPlanMode`/plan gate — opencode's plan mode surfaces via
//!   `PlanUpdated` events but does not block on an explicit gate.
//! - No `compact_boundary` metadata (only a bare compaction notice).
//! - No `rate_limit_event`.
//! - No `per_model` breakdown in `TurnCompleted` (no `modelUsage` in ACP).
//! - Mode list only available at init and after `set_config_option` — not pushed
//!   spontaneously.
//! - `num_turns` in `TurnCompleted` is always 1 (ACP does not report it).
//! - `answer_question` returns `Unsupported` — multi-option gates route through
//!   `answer_gate` with `elicitation_choices`.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::mpsc;
use wasit_core::{
    AgentDriver, AgentEvent, CompactionInfo, DriverCapabilities, DriverError, ElicitationChoice,
    EndReason, GateDecision, GateRequest, PlanEntry, QuestionAnswer, ResumeMode, Session,
    SessionMode, SlashCommandInfo, SpawnSpec, Usage,
};

// ============================================================================
// 1. argv builder (pure)
// ============================================================================

/// Build the `opencode acp` invocation for a session launch.
///
/// Binary defaults to `"opencode"` when not in spec. Env vars
/// `OPENCODE_DISABLE_PROJECT_CONFIG=1` and `OPENCODE_DISABLE_AUTOCOMPACT=1`
/// are set per Wasl's captured handshake (avoids interference from local
/// opencode config files).
pub fn opencode_argv(spec: &SpawnSpec) -> Vec<String> {
    let bin = spec.bin.as_deref().unwrap_or("opencode").to_string();
    vec![bin, "acp".to_string()]
}

// ============================================================================
// 2. Pure JSON-RPC request builders
// ============================================================================

/// Build the `initialize` request (id=1). Matches Wasl's live-captured shape.
pub fn encode_initialize(client_name: &str, client_version: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {
                "name": client_name,
                "version": client_version,
            }
        }
    }))
    .expect("always serializable")
}

/// Build the `session/new` request (for a fresh session).
pub fn encode_session_new(id: u64, cwd: &str, model: Option<&str>) -> String {
    let mut params = serde_json::json!({
        "cwd": cwd,
        "mcpServers": [],
    });
    if let Some(m) = model.filter(|s| !s.is_empty()) {
        params["model"] = serde_json::json!(m);
    }
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/new",
        "params": params,
    }))
    .expect("always serializable")
}

/// Build the `session/load` request (for resuming a prior session by id).
pub fn encode_session_load(id: u64, session_id: &str, cwd: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/load",
        "params": {
            "sessionId": session_id,
            "cwd": cwd,
            "mcpServers": [],
        },
    }))
    .expect("always serializable")
}

/// Build a `session/prompt` JSON-RPC request.
pub fn encode_session_prompt(id: u64, session_id: &str, text: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [
                { "type": "text", "text": text }
            ],
        },
    }))
    .expect("always serializable")
}

/// Build a `session/cancel` JSON-RPC request (interrupt the active turn).
pub fn encode_session_cancel(id: u64, session_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/cancel",
        "params": {
            "sessionId": session_id,
        },
    }))
    .expect("always serializable")
}

/// Build a `session/set_config_option` request to change the model.
pub fn encode_set_model(id: u64, session_id: &str, model: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/set_config_option",
        "params": {
            "sessionId": session_id,
            "id": "model",
            "value": model,
        },
    }))
    .expect("always serializable")
}

/// Build a `session/set_config_option` request to change the mode.
pub fn encode_set_mode(id: u64, session_id: &str, mode_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/set_config_option",
        "params": {
            "sessionId": session_id,
            "id": "mode",
            "value": mode_id,
        },
    }))
    .expect("always serializable")
}

// ============================================================================
// 3. Gate-response encoders (pure)
// ============================================================================

/// Encode a `session/request_permission` RESPONSE for approval.
///
/// The decision sends back the chosen option id via `optionId`.
/// Ported from Marsad's gate answer path. `request_id_raw` is the JSON-RPC
/// id of the incoming gate request (may be number or string).
pub fn encode_gate_approve(request_id_raw: &serde_json::Value, option_id: Option<&str>) -> String {
    let result = if let Some(oid) = option_id.filter(|s| !s.is_empty()) {
        serde_json::json!({ "optionId": oid })
    } else {
        // No option_id (plain approve) — use the first/default "once" option.
        serde_json::json!({ "optionId": "once" })
    };
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": result,
    }))
    .expect("always serializable")
}

/// Encode a `session/request_permission` RESPONSE for denial.
pub fn encode_gate_deny(request_id_raw: &serde_json::Value) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": { "optionId": "reject" },
    }))
    .expect("always serializable")
}

// ============================================================================
// 4. Pure helpers: configOptions → modes + model
// ============================================================================

/// Parse the `configOptions` array from a `session/new` or `session/load`
/// result (or a `session/set_config_option` result) into a list of
/// `SessionMode`s and the current mode id.
///
/// Returns `(available_modes, current_mode_id)`. Returns empty list and empty
/// string when the `mode` config group is absent.
pub fn parse_config_modes(container: &serde_json::Value) -> (Vec<SessionMode>, String) {
    let opts = match container.get("configOptions").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return (vec![], String::new()),
    };
    let group = match opts
        .iter()
        .find(|o| o.get("id").and_then(|v| v.as_str()) == Some("mode"))
    {
        Some(g) => g,
        None => return (vec![], String::new()),
    };
    let current = group
        .get("currentValue")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let available = group
        .get("options")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|o| {
                    let id = o.get("value").and_then(|v| v.as_str())?;
                    let label = o.get("name").and_then(|v| v.as_str()).unwrap_or(id);
                    let description = o
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    Some(SessionMode {
                        id: id.to_string(),
                        label: label.to_string(),
                        description,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    (available, current)
}

/// Parse the `configOptions` array for the current model value.
///
/// Returns the `currentValue` of the `model` config group, or `None` if absent.
pub fn parse_config_model(container: &serde_json::Value) -> Option<String> {
    let opts = container.get("configOptions")?.as_array()?;
    let group = opts
        .iter()
        .find(|o| o.get("id").and_then(|v| v.as_str()) == Some("model"))?;
    group.get("currentValue")?.as_str().map(|s| s.to_string())
}

/// Parse slash commands from an `available_commands_update` update.
pub fn parse_slash_commands(update: &serde_json::Value) -> Vec<SlashCommandInfo> {
    let raw = match update.get("availableCommands").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return vec![],
    };
    raw.iter()
        .map(|cmd| SlashCommandInfo {
            name: cmd
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            description: cmd
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            argument_hint: cmd
                .get("input")
                .and_then(|inp| inp.get("hint"))
                .and_then(|h| h.as_str())
                .unwrap_or("")
                .to_string(),
        })
        .collect()
}

// ============================================================================
// 5. Stateful message parser (pure core — testable without IO)
// ============================================================================

/// Stored metadata about a pending `session/request_permission` gate request,
/// needed to encode the response.
#[derive(Debug, Clone)]
pub struct PendingGate {
    /// Raw JSON-RPC id (may be number or string).
    pub raw_id: serde_json::Value,
    /// Original params for encoding the response options.
    pub params: serde_json::Value,
}

/// Stateful opencode ACP line parser.
///
/// Maintains:
/// - `pending_tools`: toolCallId → tool name (for ToolCallFinished correlation)
/// - `pending_gates`: routing_id → PendingGate (for gate answer correlation)
/// - `subagent_ids`: set of toolCallIds that are `task` (subagent) calls
/// - `subagent_created`: set of toolCallIds whose ToolCallStarted was already emitted
#[derive(Debug, Default)]
pub struct MessageParser {
    /// Maps toolCallId → tool name for ToolCallFinished correlation.
    pending_tools: HashMap<String, String>,
    /// Maps routing_id → gate metadata for answer correlation.
    pending_gates: HashMap<String, PendingGate>,
    /// Set of toolCallIds that are `task` (subagent) calls.
    subagent_ids: std::collections::HashSet<String>,
    /// Set of toolCallIds whose ToolCallStarted has already been emitted
    /// (for subagent grouping dedup).
    subagent_created: std::collections::HashSet<String>,
    /// The pending prompt request id (for correlating session/prompt results).
    pending_prompt_id: Option<u64>,
}

impl MessageParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the request id for the outstanding `session/prompt` request.
    pub fn set_pending_prompt_id(&mut self, id: u64) {
        self.pending_prompt_id = Some(id);
    }

    /// Look up the stored gate metadata for a routing_id (for answer_gate).
    pub fn pending_gate(&self, routing_id: &str) -> Option<&PendingGate> {
        self.pending_gates.get(routing_id)
    }

    /// Remove a pending gate after it has been answered.
    pub fn remove_gate(&mut self, routing_id: &str) -> Option<PendingGate> {
        self.pending_gates.remove(routing_id)
    }

    /// Process one NDJSON line from opencode ACP stdout.
    ///
    /// Returns zero or more `AgentEvent`s.
    pub fn on_message(&mut self, line: &str) -> Vec<AgentEvent> {
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

        let has_id = obj.contains_key("id");
        let has_method = obj.contains_key("method");
        let has_result = obj.contains_key("result");
        let has_error = obj.contains_key("error");

        // Server-initiated REQUEST (a gate): has method + id, no result/error.
        if has_method && has_id && !has_result && !has_error {
            return self.handle_server_request(&val, obj);
        }

        // Notification: has method, no id.
        if has_method && !has_id {
            let method = match obj.get("method").and_then(|v| v.as_str()) {
                Some(m) => m,
                None => return vec![],
            };
            if method == "session/update" {
                let params = obj
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                return self.handle_session_update(&params);
            }
            return vec![];
        }

        // Response to one of our outgoing requests.
        if has_id && has_result && !has_method {
            return self.handle_response(&val, obj);
        }

        // JSON-RPC error response.
        if has_id && has_error && !has_method {
            let error_msg = val
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("opencode json-rpc error")
                .to_string();
            return vec![AgentEvent::TurnCompleted {
                end_reason: Some(EndReason::ExecutionError),
                was_interrupted: false,
                num_turns: 0,
                usage: Usage::default(),
                per_model: HashMap::new(),
                reply: Some(error_msg),
            }];
        }

        vec![]
    }

    // ── Server-initiated REQUEST (permission gate) ─────────────────────────────

    fn handle_server_request(
        &mut self,
        val: &serde_json::Value,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let method = match obj.get("method").and_then(|v| v.as_str()) {
            Some(m) => m,
            None => return vec![],
        };

        if method != "session/request_permission" {
            return vec![AgentEvent::Unknown { raw: val.clone() }];
        }

        let params = obj
            .get("params")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let raw_id = obj.get("id").cloned().unwrap_or(serde_json::Value::Null);

        // Build the gate routing id: prefer toolCall.toolCallId (globally unique),
        // fall back to session-scoped composite with the raw ACP request id.
        // NEVER use the raw ACP id alone: id=0 is the first gate of every session,
        // making raw-id-as-routing-id produce collisions across sessions.
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("oc");
        let raw_id_str = match &raw_id {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => raw_id.to_string(),
        };
        let routing_id = params
            .get("toolCall")
            .and_then(|tc| tc.get("toolCallId"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{session_id}:acp:{raw_id_str}"));

        // Extract gate fields from params.
        let tool_call = params
            .get("toolCall")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let kind = tool_call.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let title = tool_call
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let raw_input = tool_call
            .get("rawInput")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let tool_name = if kind.is_empty() { "opencode" } else { kind }.to_string();
        let command = if !title.is_empty() {
            format!("{kind}: {title}")
        } else if !kind.is_empty() {
            format!("permission to {kind}")
        } else {
            "opencode permission request".to_string()
        };

        // Parse options into elicitation_choices (carry option id/name as key/value/label).
        let elicitation_choices: Vec<ElicitationChoice> = params
            .get("options")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|opt| {
                        let option_id = opt.get("optionId").and_then(|v| v.as_str())?;
                        let name = opt
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or(option_id);
                        Some(ElicitationChoice {
                            key: "optionId".to_string(),
                            value: option_id.to_string(),
                            label: name.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // permission_suggestions: carry the raw ACP options verbatim.
        let permission_suggestions: Vec<serde_json::Value> = params
            .get("options")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Store gate metadata for answer_gate correlation.
        self.pending_gates.insert(
            routing_id.clone(),
            PendingGate {
                raw_id: raw_id.clone(),
                params: params.clone(),
            },
        );

        vec![AgentEvent::GateRequested {
            gate: GateRequest::Permission {
                request_id: routing_id,
                tool_name,
                command,
                cwd: String::new(),
                explanation: command_explanation(kind, title),
                is_elicitation: !elicitation_choices.is_empty(),
                elicitation_choices,
                permission_suggestions,
                raw_input,
            },
        }]
    }

    // ── session/update notification ────────────────────────────────────────────

    fn handle_session_update(&mut self, params: &serde_json::Value) -> Vec<AgentEvent> {
        let update = match params.get("update") {
            Some(u) => u,
            None => return vec![],
        };

        // ACP tags the update kind in `sessionUpdate`; fall back to `type`.
        let type_str = match update
            .get("sessionUpdate")
            .or_else(|| update.get("type"))
            .and_then(|t| t.as_str())
        {
            Some(t) => t,
            None => return vec![],
        };

        match type_str {
            // ── agent_message_chunk → AssistantDelta ─────────────────────────
            "agent_message_chunk" => {
                let content = match update.get("content") {
                    Some(c) => c,
                    None => return vec![],
                };
                let content_type = content.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if content_type != "text" {
                    return vec![];
                }
                let text = content.get("text").and_then(|v| v.as_str()).unwrap_or("");
                if text.is_empty() {
                    return vec![];
                }
                let message_id = update
                    .get("messageId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                vec![AgentEvent::AssistantDelta {
                    text: text.to_string(),
                    message_id,
                }]
            }

            // ── agent_thought_chunk → ThinkingDelta ──────────────────────────
            "agent_thought_chunk" => {
                // opencode carries reasoning under `content.text` (NOT a top-level
                // `thinking` field). Read content.text first, fall back to `thinking`.
                let thinking = update
                    .get("content")
                    .and_then(|c| c.get("text"))
                    .and_then(|v| v.as_str())
                    .or_else(|| update.get("thinking").and_then(|v| v.as_str()))
                    .unwrap_or("");
                if thinking.is_empty() {
                    // Skip empty thought chunks (avoids spawning empty thinking bubbles).
                    return vec![];
                }
                let message_id = update
                    .get("messageId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                vec![AgentEvent::ThinkingDelta {
                    text: thinking.to_string(),
                    message_id,
                }]
            }

            // ── tool_call → ToolCallStarted ───────────────────────────────────
            "tool_call" => {
                let tool_call_id = update
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("toolu-acp-0")
                    .to_string();
                let title = update
                    .get("title")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let kind = update.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let tool_name = title.unwrap_or(kind).to_string();
                let raw_input = update
                    .get("rawInput")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);

                // Subagent detection: `task` tool is opencode's subagent launcher.
                let is_task = update.get("title").and_then(|v| v.as_str()) == Some("task");

                if is_task {
                    // Track this toolCallId as a subagent (initial `task` call
                    // creates no ToolCallStarted yet — we wait for the enriched
                    // update with a description, exactly as Marsad does).
                    self.subagent_ids.insert(tool_call_id.clone());
                    return vec![];
                }

                // Track tool name for ToolCallFinished correlation.
                self.pending_tools
                    .insert(tool_call_id.clone(), tool_name.clone());

                vec![AgentEvent::ToolCallStarted {
                    name: tool_name,
                    call_id: tool_call_id,
                    input: raw_input,
                    is_subagent: false,
                    subagent_model: None,
                }]
            }

            // ── tool_call_update → ToolCallUpdated / ToolCallFinished / subagent ─
            "tool_call_update" => {
                let tool_call_id = update
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("toolu-acp-upd")
                    .to_string();
                let status = update.get("status").and_then(|v| v.as_str()).unwrap_or("");
                let title = update
                    .get("title")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let kind = update.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let tool_name = self
                    .pending_tools
                    .get(&tool_call_id)
                    .cloned()
                    .or_else(|| title.map(|t| t.to_string()))
                    .unwrap_or_else(|| kind.to_string());

                let raw_input = update
                    .get("rawInput")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);

                let content_arr = update
                    .get("content")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                // Subagent lifecycle: stateful tracking of `task` tool_call_updates.
                if self.subagent_ids.contains(&tool_call_id) {
                    return self.handle_subagent_update(
                        &tool_call_id,
                        status,
                        update,
                        &raw_input,
                        &content_arr,
                    );
                }

                // Terminal statuses → ToolCallFinished.
                let is_terminal = matches!(status, "completed" | "failed");
                if is_terminal {
                    let ok = status == "completed";
                    let result = if content_arr.is_empty() {
                        None
                    } else {
                        Some(serde_json::Value::Array(content_arr))
                    };
                    self.pending_tools.remove(&tool_call_id);
                    return vec![AgentEvent::ToolCallFinished {
                        call_id: tool_call_id,
                        name: tool_name,
                        ok,
                        result,
                    }];
                }

                // Non-terminal → ToolCallUpdated.
                vec![AgentEvent::ToolCallUpdated {
                    call_id: tool_call_id,
                    name: tool_name,
                    content: content_arr,
                    status: if status.is_empty() {
                        None
                    } else {
                        Some(status.to_string())
                    },
                }]
            }

            // ── usage_update → UsageUpdate ────────────────────────────────────
            "usage_update" => {
                let cost = update.get("cost");
                let currency = cost
                    .and_then(|c| c.get("currency"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let cost_usd = if currency == "USD" {
                    cost.and_then(|c| c.get("amount"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0)
                } else {
                    0.0
                };
                let context_used = update.get("used").and_then(|v| v.as_u64()).unwrap_or(0);
                let context_size = update.get("size").and_then(|v| v.as_u64()).unwrap_or(0);

                let context_pct = if context_size > 0 {
                    Some((context_used as f32 / context_size as f32) * 100.0)
                } else {
                    None
                };

                vec![AgentEvent::UsageUpdate {
                    usage: Usage {
                        cost_usd,
                        ..Usage::default()
                    },
                    context_pct,
                }]
            }

            // ── plan → PlanUpdated ────────────────────────────────────────────
            "plan" => {
                let entries = update
                    .get("plan")
                    .and_then(|p| p.get("entries"))
                    .and_then(|e| e.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|e| PlanEntry {
                                content: e
                                    .get("content")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                priority: e
                                    .get("priority")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                vec![AgentEvent::PlanUpdated { entries }]
            }

            // ── available_commands_update → SlashCommandsUpdated ──────────────
            "available_commands_update" => {
                let commands = parse_slash_commands(update);
                vec![AgentEvent::SlashCommandsUpdated { commands }]
            }

            // ── current_mode_update → ModesUpdated ────────────────────────────
            "current_mode_update" => {
                let current_mode = update
                    .get("currentModeId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                vec![AgentEvent::ModesUpdated {
                    available_modes: vec![],
                    current_mode: if current_mode.is_empty() {
                        None
                    } else {
                        Some(current_mode)
                    },
                }]
            }

            // ── context_compaction → Compaction ──────────────────────────────
            "context_compaction" => {
                vec![AgentEvent::Compaction {
                    info: CompactionInfo::default(),
                }]
            }

            // ── user_message_chunk → skip ─────────────────────────────────────
            "user_message_chunk" => vec![],

            // ── unknown update type → Unknown ─────────────────────────────────
            _ => vec![AgentEvent::Unknown {
                raw: update.clone(),
            }],
        }
    }

    // ── Subagent lifecycle (task tool_call_update) ─────────────────────────────

    fn handle_subagent_update(
        &mut self,
        tool_call_id: &str,
        status: &str,
        update: &serde_json::Value,
        raw_input: &serde_json::Value,
        content_arr: &[serde_json::Value],
    ) -> Vec<AgentEvent> {
        let mut events = vec![];

        // Extract description/name for the subagent.
        let name = raw_input
            .get("description")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                update
                    .get("title")
                    .and_then(|v| v.as_str())
                    .filter(|t| !t.is_empty() && *t != "task")
            })
            .map(|s| s.to_string());

        // Emit ToolCallStarted once we have a name (dedup on subagent_created).
        if let Some(ref n) = name {
            if !self.subagent_created.contains(tool_call_id) {
                let mut input = raw_input.clone();
                // Ensure description is present in the input for consumer grouping.
                if input
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.is_empty())
                    .unwrap_or(true)
                {
                    if let Some(obj) = input.as_object_mut() {
                        obj.insert("description".to_string(), serde_json::json!(n));
                    }
                }
                events.push(AgentEvent::ToolCallStarted {
                    name: n.clone(),
                    call_id: tool_call_id.to_string(),
                    input,
                    is_subagent: true,
                    subagent_model: None,
                });
                self.subagent_created.insert(tool_call_id.to_string());
            }
        }

        // Terminal: emit ToolCallFinished.
        if status == "completed" || status == "failed" {
            // If we never created a ToolCallStarted (no name on the terminal update),
            // create a minimal one so consumers see a balanced start+finish.
            if !self.subagent_created.contains(tool_call_id) {
                let fallback_name = name.as_deref().unwrap_or("subagent").to_string();
                events.push(AgentEvent::ToolCallStarted {
                    name: fallback_name.clone(),
                    call_id: tool_call_id.to_string(),
                    input: raw_input.clone(),
                    is_subagent: true,
                    subagent_model: None,
                });
                self.subagent_created.insert(tool_call_id.to_string());
            }
            let ok = status == "completed";
            let result = if content_arr.is_empty() {
                None
            } else {
                Some(serde_json::Value::Array(content_arr.to_vec()))
            };
            events.push(AgentEvent::ToolCallFinished {
                call_id: tool_call_id.to_string(),
                name: name.unwrap_or_else(|| "subagent".to_string()),
                ok,
                result,
            });
        }

        events
    }

    // ── Response to our outgoing requests ─────────────────────────────────────

    fn handle_response(
        &mut self,
        val: &serde_json::Value,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let result = match obj.get("result") {
            Some(r) => r,
            None => return vec![],
        };

        // session/prompt result: carries `stopReason` and optional `usage`.
        if result.get("stopReason").is_some() {
            let stop_reason = result
                .get("stopReason")
                .and_then(|v| v.as_str())
                .unwrap_or("completed");

            let end_reason = match stop_reason {
                "completed" | "end_turn" | "max_tokens" | "max_turn_requests" | "refusal" => None,
                "cancelled" => Some(EndReason::Cancelled),
                "auth_required" | "request_limit_exceeded" => Some(EndReason::ExecutionError),
                _ => None,
            };
            let was_interrupted = end_reason == Some(EndReason::Cancelled);

            // Parse usage from the prompt result.
            let usage_obj = result.get("usage");
            let input_tokens = usage_obj
                .and_then(|u| u.get("inputTokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let output_tokens = usage_obj
                .and_then(|u| u.get("outputTokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let cache_read_tokens = usage_obj
                .and_then(|u| u.get("cachedReadTokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let cache_creation_tokens = usage_obj
                .and_then(|u| u.get("cachedWriteTokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let usage = Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                cost_usd: 0.0,
            };

            self.pending_prompt_id = None;
            return vec![AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns: 1,
                usage,
                per_model: HashMap::new(),
                reply: None,
            }];
        }

        // session/set_config_option result: carries `configOptions`.
        if result.get("configOptions").is_some() {
            let mut events = vec![];

            // Extract model.
            if let Some(model) = parse_config_model(result) {
                events.push(AgentEvent::ModelChanged { model });
            }

            // Extract modes.
            let (available_modes, current_mode) = parse_config_modes(result);
            if !available_modes.is_empty() || !current_mode.is_empty() {
                events.push(AgentEvent::ModesUpdated {
                    available_modes,
                    current_mode: if current_mode.is_empty() {
                        None
                    } else {
                        Some(current_mode)
                    },
                });
            }

            return events;
        }

        // session/new or session/load result: carries `sessionId`.
        if result.get("sessionId").is_some() || result.get("configOptions").is_none() {
            // This is the SessionInit response — handled by the IO shell during
            // handshake (not by on_message). Return empty here so we don't emit
            // a spurious Unknown.
            return vec![];
        }

        // Other responses (initialize ack, cancel ack, etc.) — silently ignore.
        let _ = val;
        vec![]
    }
}

// ── Gate response dispatch ─────────────────────────────────────────────────────

/// Given a stored `PendingGate` and a `GateDecision`, produce the JSON-RPC
/// response line to write to opencode's stdin.
fn encode_gate_response(gate: &PendingGate, decision: &GateDecision) -> String {
    match decision {
        GateDecision::Approve {
            elicitation_content,
            ..
        } => {
            // Extract the chosen optionId from elicitation_content if present.
            let option_id = elicitation_content
                .as_ref()
                .and_then(|c| c.get("optionId"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            encode_gate_approve(&gate.raw_id, option_id.as_deref())
        }
        GateDecision::Deny { .. } => encode_gate_deny(&gate.raw_id),
    }
}

// ── Pure helper ───────────────────────────────────────────────────────────────

fn command_explanation(kind: &str, title: &str) -> String {
    match (kind.is_empty(), title.is_empty()) {
        (false, false) => format!("{kind}: {title}"),
        (true, false) => title.to_string(),
        (false, true) => format!("permission to {kind}"),
        (true, true) => "opencode permission request".to_string(),
    }
}

// ============================================================================
// 6. IO Shell — AgentDriver + Session
// ============================================================================

/// The opencode ACP JSON-RPC driver.
pub struct OpencodeDriver {
    bin: Option<String>,
}

impl OpencodeDriver {
    pub fn new(bin: impl Into<String>) -> Self {
        OpencodeDriver {
            bin: Some(bin.into()),
        }
    }

    pub fn from_path() -> Self {
        OpencodeDriver { bin: None }
    }
}

#[async_trait]
impl AgentDriver for OpencodeDriver {
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            interruptible: true,
            model_switchable: true,
            mode_switchable: true,
            persistent_conversation: true,
            resumable: true,
            streaming_output: true,
            reports_usage: true,
            supports_gates: true,
        }
    }

    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError> {
        let mut spawn_spec = spec.clone();
        if spawn_spec.bin.is_none() {
            spawn_spec.bin = self.bin.clone();
        }

        let argv = opencode_argv(&spawn_spec);
        if argv.is_empty() {
            return Err(DriverError::SpawnFailed {
                detail: "empty argv".to_string(),
            });
        }

        let mut cmd = tokio::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .env("OPENCODE_DISABLE_PROJECT_CONFIG", "1")
            .env("OPENCODE_DISABLE_AUTOCOMPACT", "1")
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

        let mut stdin = child.stdin.take().ok_or_else(|| DriverError::SpawnFailed {
            detail: "no stdin".to_string(),
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DriverError::SpawnFailed {
                detail: "no stdout".to_string(),
            })?;

        // ── Perform the ACP handshake ────────────────────────────────────────

        // 1. Send initialize.
        let init_line = encode_initialize("wasit-opencode", env!("CARGO_PKG_VERSION"));
        write_line_to(&mut stdin, &init_line)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("initialize write: {e}"),
            })?;

        // 2. Read until we find the initialize response (id=1).
        let mut stdout_reader = BufReader::new(stdout);
        wait_for_response_id(&mut stdout_reader, 1)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("initialize response: {e}"),
            })?;

        // 3. Send session/new or session/load.
        let cwd = spawn_spec.cwd.as_deref().unwrap_or(".");
        let model = spawn_spec.model.as_deref();
        let session_req_id: u64 = 2;

        let session_line = match &spawn_spec.resume {
            ResumeMode::Resume(session_id) => encode_session_load(session_req_id, session_id, cwd),
            _ => encode_session_new(session_req_id, cwd, model),
        };
        write_line_to(&mut stdin, &session_line)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("session/new write: {e}"),
            })?;

        // 4. Read until we find the session/new response (id=2) → extract sessionId +
        //    configOptions.
        let session_result = wait_for_response_id(&mut stdout_reader, session_req_id)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("session/new response: {e}"),
            })?;

        let native_session_id = session_result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DriverError::SpawnFailed {
                detail: "session/new result missing sessionId".to_string(),
            })?
            .to_string();

        // Parse modes and model from configOptions at init.
        let (available_modes, current_mode) = parse_config_modes(&session_result);
        let init_model = parse_config_model(&session_result).or_else(|| spawn_spec.model.clone());

        // Event channel: reader task → session.
        let (tx, rx) = mpsc::channel::<AgentEvent>(256);

        // Synthesize SessionInit from the handshake results.
        let init_event = AgentEvent::SessionInit {
            native_session_id: Some(native_session_id.clone()),
            model: init_model,
            cwd: spawn_spec.cwd.clone(),
            tools: vec![],
            mcp_servers: vec![],
            slash_commands: vec![],
            permission_mode: String::new(),
            claude_code_version: String::new(),
            available_modes,
            current_mode,
        };

        // Start the reader task.
        tokio::spawn(reader_task(stdout_reader, tx.clone()));

        // Deliver SessionInit first through the channel so next_event ordering
        // is correct. If receiver dropped, ignore.
        let _ = tx.send(init_event).await;

        let mut session = OpencodeSession {
            child,
            stdin: Some(stdin),
            rx,
            session_id: native_session_id,
            next_request_id: session_req_id + 1,
            parser: MessageParser::new(),
        };

        // Deliver first_prompt if any.
        if let Some(prompt) = spawn_spec.first_prompt.clone() {
            session
                .send_prompt(&prompt)
                .await
                .map_err(|e| DriverError::SpawnFailed {
                    detail: format!("first_prompt: {e}"),
                })?;
        }

        Ok(Box::new(session))
    }
}

/// Write a line to stdin (append newline).
async fn write_line_to(stdin: &mut ChildStdin, line: &str) -> std::io::Result<()> {
    stdin.write_all(line.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await
}

/// Read lines until we find a JSON-RPC response with the given id.
/// Returns the `result` value on success or an error string on timeout/failure.
async fn wait_for_response_id(
    reader: &mut BufReader<ChildStdout>,
    expected_id: u64,
) -> Result<serde_json::Value, String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for response id={expected_id}"));
        }
        let mut line = String::new();
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            reader.read_line(&mut line),
        )
        .await
        {
            Ok(Ok(0)) => return Err("EOF".to_string()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err(format!("timed out waiting for response id={expected_id}")),
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let val: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Skip notifications (no id field) and server-initiated requests (have method+id).
        // We only want responses to OUR requests (have id, no method).
        if val.get("method").is_some() {
            continue;
        }
        let id = val.get("id").and_then(|v| v.as_u64());
        if id == Some(expected_id) {
            if let Some(error) = val.get("error") {
                let msg = error
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("json-rpc error");
                return Err(msg.to_string());
            }
            return Ok(val
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
    }
}

/// Background reader task: reads stdout lines and sends AgentEvents.
async fn reader_task(mut reader: BufReader<ChildStdout>, tx: mpsc::Sender<AgentEvent>) {
    let mut parser = MessageParser::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                let _ = tx.send(AgentEvent::ProcessExited { code: None }).await;
                return;
            }
            Ok(_) => {
                let events = parser.on_message(&line);
                for ev in events {
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
            Err(_) => {
                let _ = tx.send(AgentEvent::ProcessExited { code: None }).await;
                return;
            }
        }
    }
}

/// A live opencode ACP session.
pub struct OpencodeSession {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: mpsc::Receiver<AgentEvent>,
    session_id: String,
    next_request_id: u64,
    /// Parser owned by the session for gate-answer correlation.
    /// NOTE: the reader task has its OWN parser instance. This parser is
    /// used ONLY for tracking pending gates in the session's IO methods.
    parser: MessageParser,
}

impl OpencodeSession {
    async fn write_line(&mut self, line: &str) -> Result<(), DriverError> {
        let stdin = self.stdin.as_mut().ok_or_else(|| DriverError::Transport {
            detail: "stdin closed".to_string(),
        })?;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("write: {e}"),
            })?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| DriverError::Transport {
                detail: format!("write newline: {e}"),
            })?;
        stdin.flush().await.map_err(|e| DriverError::Transport {
            detail: format!("flush: {e}"),
        })
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        id
    }
}

#[async_trait]
impl Session for OpencodeSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        let ev = self.rx.recv().await?;
        // Intercept gate events to track them in this session's parser for
        // answer_gate correlation. We store the gate metadata so answer_gate
        // can retrieve it later.
        if let AgentEvent::GateRequested { ref gate } = ev {
            let routing_id = match gate {
                GateRequest::Permission { request_id, .. } => request_id.clone(),
                GateRequest::Plan { request_id, .. } => request_id.clone(),
                GateRequest::Question { request_id, .. } => request_id.clone(),
            };

            // Reconstruct the raw_id from the routing_id for answer encoding.
            let raw_id_val = if let Some(numeric_part) = routing_id.strip_prefix("codex:") {
                if let Ok(n) = numeric_part.parse::<u64>() {
                    serde_json::json!(n)
                } else {
                    serde_json::json!(routing_id)
                }
            } else if routing_id.contains(":acp:") {
                // "sid:acp:N" pattern — extract the N.
                if let Some(acp_part) = routing_id.rsplit(':').next() {
                    if let Ok(n) = acp_part.parse::<u64>() {
                        serde_json::json!(n)
                    } else {
                        serde_json::json!(acp_part)
                    }
                } else {
                    serde_json::json!(routing_id)
                }
            } else {
                // toolCallId routing: use as-is.
                serde_json::json!(routing_id)
            };

            self.parser.pending_gates.insert(
                routing_id,
                PendingGate {
                    raw_id: raw_id_val,
                    params: serde_json::Value::Null,
                },
            );
        }
        Some(ev)
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        let id = self.next_id();
        self.parser.set_pending_prompt_id(id);
        let line = encode_session_prompt(id, &self.session_id.clone(), text);
        self.write_line(&line).await
    }

    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError> {
        let gate = self
            .parser
            .remove_gate(request_id)
            .ok_or_else(|| DriverError::NotFound {
                detail: format!("no pending gate for request_id={request_id}"),
            })?;
        let line = encode_gate_response(&gate, &decision);
        self.write_line(&line).await
    }

    async fn answer_question(
        &mut self,
        _request_id: &str,
        _answer: QuestionAnswer,
    ) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "answer_question (opencode uses answer_gate with elicitation_choices for multi-option gates)".to_string(),
        })
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        let id = self.next_id();
        let session_id = self.session_id.clone();
        let line = encode_session_cancel(id, &session_id);
        self.write_line(&line).await
    }

    async fn set_model(&mut self, model: &str) -> Result<(), DriverError> {
        let id = self.next_id();
        let session_id = self.session_id.clone();
        let line = encode_set_model(id, &session_id, model);
        self.write_line(&line).await
    }

    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError> {
        let id = self.next_id();
        let session_id = self.session_id.clone();
        let line = encode_set_mode(id, &session_id, mode_id);
        self.write_line(&line).await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.stdin.take();
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
    use wasit_core::{AgentEvent, EndReason, GateRequest, SpawnSpec};

    // ── helpers ─────────────────────────────────────────────────────────────

    fn base_spec() -> SpawnSpec {
        SpawnSpec {
            cwd: Some("/proj".to_string()),
            model: Some("claude-sonnet-4".to_string()),
            ..SpawnSpec::default()
        }
    }

    fn parse_one(line: &str) -> AgentEvent {
        let mut p = MessageParser::new();
        let mut events = p.on_message(line);
        assert_eq!(events.len(), 1, "expected exactly 1 event from: {line}");
        events.remove(0)
    }

    fn parse_all(line: &str) -> Vec<AgentEvent> {
        let mut p = MessageParser::new();
        p.on_message(line)
    }

    // ── opencode_argv ────────────────────────────────────────────────────────

    #[test]
    fn argv_default_bin() {
        let argv = opencode_argv(&SpawnSpec::default());
        assert_eq!(argv[0], "opencode");
        assert_eq!(argv[1], "acp");
    }

    #[test]
    fn argv_custom_bin() {
        let mut spec = base_spec();
        spec.bin = Some("/usr/local/bin/opencode".into());
        let argv = opencode_argv(&spec);
        assert_eq!(argv[0], "/usr/local/bin/opencode");
        assert_eq!(argv[1], "acp");
    }

    #[test]
    fn argv_always_two_elements() {
        let argv = opencode_argv(&base_spec());
        assert_eq!(argv.len(), 2, "opencode_argv must always have 2 elements");
    }

    #[test]
    fn argv_deterministic() {
        assert_eq!(opencode_argv(&base_spec()), opencode_argv(&base_spec()));
    }

    // ── encode_initialize ────────────────────────────────────────────────────

    #[test]
    fn encode_initialize_shape() {
        let json = encode_initialize("wasit-opencode", "0.1.0");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["method"], "initialize");
        assert_eq!(v["params"]["protocolVersion"], 1);
        assert_eq!(v["params"]["clientInfo"]["name"], "wasit-opencode");
    }

    // ── encode_session_new ───────────────────────────────────────────────────

    #[test]
    fn encode_session_new_shape() {
        let json = encode_session_new(2, "/proj", Some("claude-sonnet-4"));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 2);
        assert_eq!(v["method"], "session/new");
        assert_eq!(v["params"]["cwd"], "/proj");
        assert_eq!(v["params"]["model"], "claude-sonnet-4");
        assert_eq!(v["params"]["mcpServers"], serde_json::json!([]));
    }

    #[test]
    fn encode_session_new_no_model() {
        let json = encode_session_new(2, "/proj", None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            v["params"]["model"].is_null(),
            "no model → model must be absent"
        );
    }

    // ── encode_session_load ──────────────────────────────────────────────────

    #[test]
    fn encode_session_load_shape() {
        let json = encode_session_load(2, "ses_abc123", "/proj");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "session/load");
        assert_eq!(v["params"]["sessionId"], "ses_abc123");
        assert_eq!(v["params"]["cwd"], "/proj");
    }

    // ── encode_session_prompt ────────────────────────────────────────────────

    #[test]
    fn encode_session_prompt_shape() {
        let json = encode_session_prompt(3, "ses_abc", "hello world");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "session/prompt");
        assert_eq!(v["params"]["sessionId"], "ses_abc");
        assert_eq!(v["params"]["prompt"][0]["type"], "text");
        assert_eq!(v["params"]["prompt"][0]["text"], "hello world");
    }

    // ── encode_session_cancel ────────────────────────────────────────────────

    #[test]
    fn encode_session_cancel_shape() {
        let json = encode_session_cancel(4, "ses_abc");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "session/cancel");
        assert_eq!(v["params"]["sessionId"], "ses_abc");
    }

    // ── encode_set_model / encode_set_mode ───────────────────────────────────

    #[test]
    fn encode_set_model_shape() {
        let json = encode_set_model(5, "ses_abc", "claude-opus-4");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "session/set_config_option");
        assert_eq!(v["params"]["id"], "model");
        assert_eq!(v["params"]["value"], "claude-opus-4");
        assert_eq!(v["params"]["sessionId"], "ses_abc");
    }

    #[test]
    fn encode_set_mode_shape() {
        let json = encode_set_mode(6, "ses_abc", "plan");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "session/set_config_option");
        assert_eq!(v["params"]["id"], "mode");
        assert_eq!(v["params"]["value"], "plan");
    }

    // ── encode_gate_approve / encode_gate_deny ───────────────────────────────

    #[test]
    fn encode_gate_approve_with_option_id() {
        let raw_id = serde_json::json!(0);
        let json = encode_gate_approve(&raw_id, Some("always"));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 0);
        assert_eq!(v["result"]["optionId"], "always");
    }

    #[test]
    fn encode_gate_approve_no_option_defaults_to_once() {
        let raw_id = serde_json::json!(0);
        let json = encode_gate_approve(&raw_id, None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["optionId"], "once");
    }

    #[test]
    fn encode_gate_deny_shape() {
        let raw_id = serde_json::json!(5);
        let json = encode_gate_deny(&raw_id);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 5);
        assert_eq!(v["result"]["optionId"], "reject");
    }

    // ── on_message: agent_message_chunk → AssistantDelta ─────────────────────

    #[test]
    fn on_message_agent_message_chunk_text() {
        // Real opencode wire line from spike capture.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_f86a99a43001","content":{"type":"text","text":"OK"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::AssistantDelta { text, message_id } => {
                assert_eq!(text, "OK");
                assert_eq!(message_id, Some("msg_f86a99a43001".to_string()));
            }
            other => panic!("expected AssistantDelta, got {other:?}"),
        }
    }

    #[test]
    fn on_message_agent_message_chunk_empty_skipped() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_1","content":{"type":"text","text":""}}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "empty text chunk must be skipped");
    }

    #[test]
    fn on_message_agent_message_chunk_non_text_content_skipped() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_2","content":{"type":"image","data":"..."}}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "non-text content type must be skipped");
    }

    // ── on_message: agent_thought_chunk → ThinkingDelta ──────────────────────

    #[test]
    fn on_message_agent_thought_chunk_content_text() {
        // Real opencode shape: reasoning in content.text.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought_1","content":{"type":"text","text":"Reasoning here."}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ThinkingDelta { text, message_id } => {
                assert_eq!(text, "Reasoning here.");
                assert_eq!(message_id, Some("thought_1".to_string()));
            }
            other => panic!("expected ThinkingDelta, got {other:?}"),
        }
    }

    #[test]
    fn on_message_agent_thought_chunk_fallback_to_thinking_field() {
        // Fallback path: top-level `thinking` field.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought_2","thinking":"Top-level thought."}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ThinkingDelta { text, .. } => {
                assert_eq!(text, "Top-level thought.");
            }
            other => panic!("expected ThinkingDelta, got {other:?}"),
        }
    }

    #[test]
    fn on_message_agent_thought_chunk_empty_skipped() {
        // Empty thought chunks must be dropped (avoids empty "thinking" bubbles).
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought_3","content":{"type":"text","text":""}}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "empty thought chunk must be skipped");
    }

    // ── on_message: tool_call → ToolCallStarted ───────────────────────────────

    #[test]
    fn on_message_tool_call_starts_with_title() {
        // Real opencode tool_call shape.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_1","title":"glob","kind":"search","status":"pending","rawInput":{"pattern":"*"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                input,
                is_subagent,
                ..
            } => {
                assert_eq!(name, "glob", "name = title, not kind");
                assert_eq!(call_id, "toolu_1");
                assert_eq!(input["pattern"], "*");
                assert!(!is_subagent);
            }
            other => panic!("expected ToolCallStarted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_tool_call_falls_back_to_kind() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_2","kind":"read","status":"pending","rawInput":{"path":"/etc/hosts"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ToolCallStarted { name, .. } => {
                assert_eq!(name, "read", "no title → use kind");
            }
            other => panic!("expected ToolCallStarted, got {other:?}"),
        }
    }

    // ── on_message: task tool_call → subagent grouping ───────────────────────

    #[test]
    fn on_message_task_tool_call_emits_nothing_initially() {
        // Bare `task` tool_call: marks the subagent, creates nothing yet.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_task_1","title":"task","kind":"think","status":"pending","rawInput":{}}}}"#;
        let events = parse_all(line);
        assert!(
            events.is_empty(),
            "bare `task` tool_call must emit nothing (wait for enriched update)"
        );
    }

    #[test]
    fn on_message_task_tool_call_update_creates_subagent_on_description() {
        let mut p = MessageParser::new();
        // Bare task call.
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_task_1","title":"task","kind":"think","status":"pending","rawInput":{}}}}"#);
        // Enriched update with description → ToolCallStarted.
        let events = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_task_1","title":"Explore repo","kind":"think","status":"in_progress","rawInput":{"description":"Explore repo","prompt":"..."}}}}"#);
        assert_eq!(
            events.len(),
            1,
            "enriched update must produce exactly ToolCallStarted"
        );
        match &events[0] {
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                is_subagent,
                ..
            } => {
                assert_eq!(name, "Explore repo");
                assert_eq!(call_id, "toolu_task_1");
                assert!(is_subagent, "task tool must have is_subagent=true");
            }
            other => panic!("expected ToolCallStarted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_task_completion_emits_tool_call_finished() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_task_1","title":"task","kind":"think","status":"pending","rawInput":{}}}}"#);
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_task_1","title":"Explore repo","status":"in_progress","rawInput":{"description":"Explore repo"}}}}"#);
        let events = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_task_1","title":"Explore repo","status":"completed","content":[{"type":"content","content":{"type":"text","text":"done"}}]}}}"#);
        let finished = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ToolCallFinished { .. }));
        assert!(
            finished.is_some(),
            "completed task update must emit ToolCallFinished"
        );
        if let Some(AgentEvent::ToolCallFinished { ok, call_id, .. }) = finished {
            assert!(ok);
            assert_eq!(call_id, "toolu_task_1");
        }
    }

    #[test]
    fn on_message_task_no_duplicate_subagent_on_repeated_updates() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_task_1","title":"task","kind":"think","status":"pending","rawInput":{}}}}"#);
        // First enriched update → ToolCallStarted.
        let ev1 = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_task_1","title":"Task A","status":"in_progress","rawInput":{"description":"Task A"}}}}"#);
        assert_eq!(ev1.len(), 1, "first enriched update = one ToolCallStarted");
        // Second in_progress update → NO duplicate ToolCallStarted.
        let ev2 = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_task_1","title":"Task A","status":"in_progress","rawInput":{"description":"Task A"}}}}"#);
        let started_count = ev2
            .iter()
            .filter(|e| matches!(e, AgentEvent::ToolCallStarted { .. }))
            .count();
        assert_eq!(
            started_count, 0,
            "repeat in_progress must not duplicate ToolCallStarted"
        );
    }

    // ── on_message: tool_call_update → ToolCallUpdated / ToolCallFinished ─────

    #[test]
    fn on_message_tool_call_update_intermediate() {
        let mut p = MessageParser::new();
        // Start a tool.
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_edit_1","title":"main.rs","kind":"edit","status":"pending","rawInput":{}}}}"#);
        // Intermediate update.
        let events = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_edit_1","title":"main.rs","status":"in_progress","content":[]}}}"#);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolCallUpdated {
                call_id,
                name,
                status,
                ..
            } => {
                assert_eq!(call_id, "toolu_edit_1");
                assert_eq!(name, "main.rs");
                assert_eq!(status.as_deref(), Some("in_progress"));
            }
            other => panic!("expected ToolCallUpdated, got {other:?}"),
        }
    }

    #[test]
    fn on_message_tool_call_update_completed() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_edit_2","title":"foo.rs","kind":"edit","status":"pending","rawInput":{}}}}"#);
        let events = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_edit_2","title":"foo.rs","status":"completed","content":[{"type":"content","content":{"type":"text","text":"edited"}}]}}}"#);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolCallFinished {
                call_id,
                ok,
                result,
                ..
            } => {
                assert_eq!(call_id, "toolu_edit_2");
                assert!(ok, "completed → ok=true");
                assert!(result.is_some(), "content must be present");
            }
            other => panic!("expected ToolCallFinished, got {other:?}"),
        }
    }

    #[test]
    fn on_message_tool_call_update_failed() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_bash_3","title":"echo hi","kind":"execute","status":"pending","rawInput":{}}}}"#);
        let events = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call_update","toolCallId":"toolu_bash_3","status":"failed","content":[]}}}"#);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolCallFinished { ok, .. } => {
                assert!(!ok, "failed → ok=false");
            }
            other => panic!("expected ToolCallFinished, got {other:?}"),
        }
    }

    // ── on_message: usage_update → UsageUpdate ────────────────────────────────

    #[test]
    fn on_message_usage_update_usd() {
        // Real opencode wire line.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"usage_update","used":3,"size":200000,"cost":{"amount":0.0042,"currency":"USD"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::UsageUpdate { usage, context_pct } => {
                assert!((usage.cost_usd - 0.0042).abs() < 1e-6);
                assert!(
                    context_pct.is_some(),
                    "context_pct must be computed from used/size"
                );
                // 3/200000 * 100 = 0.0015%
                let pct = context_pct.unwrap();
                assert!(
                    pct > 0.0 && pct < 1.0,
                    "context_pct should be ~0.0015, got {pct}"
                );
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
    }

    #[test]
    fn on_message_usage_update_non_usd_cost_zeroed() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"usage_update","used":1000,"size":200000,"cost":{"amount":99.99,"currency":"EUR"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::UsageUpdate { usage, .. } => {
                assert_eq!(usage.cost_usd, 0.0, "non-USD cost must be zeroed");
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
    }

    #[test]
    fn on_message_usage_update_zero_size_no_context_pct() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"usage_update","used":100,"size":0,"cost":{"amount":0,"currency":"USD"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::UsageUpdate { context_pct, .. } => {
                assert!(
                    context_pct.is_none(),
                    "zero size → context_pct must be None"
                );
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
    }

    // ── on_message: plan → PlanUpdated ───────────────────────────────────────

    #[test]
    fn on_message_plan_with_entries() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"plan","plan":{"entries":[{"content":"Research","priority":"high"},{"content":"Implement","priority":"medium"}]}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::PlanUpdated { entries } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].content, "Research");
                assert_eq!(entries[0].priority, "high");
                assert_eq!(entries[1].content, "Implement");
            }
            other => panic!("expected PlanUpdated, got {other:?}"),
        }
    }

    #[test]
    fn on_message_plan_empty_entries() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"plan","plan":{"entries":[]}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::PlanUpdated { entries } => {
                assert!(entries.is_empty());
            }
            other => panic!("expected PlanUpdated, got {other:?}"),
        }
    }

    // ── on_message: available_commands_update → SlashCommandsUpdated ──────────

    #[test]
    fn on_message_available_commands_update() {
        // Real wire line from spike capture.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"/compact","description":"Compact context"},{"name":"/clear","description":"Clear"}]}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::SlashCommandsUpdated { commands } => {
                assert_eq!(commands.len(), 2);
                assert_eq!(commands[0].name, "/compact");
                assert_eq!(commands[1].name, "/clear");
            }
            other => panic!("expected SlashCommandsUpdated, got {other:?}"),
        }
    }

    #[test]
    fn on_message_available_commands_empty() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::SlashCommandsUpdated { commands } => {
                assert!(commands.is_empty());
            }
            other => panic!("expected SlashCommandsUpdated, got {other:?}"),
        }
    }

    // ── on_message: current_mode_update → ModesUpdated ───────────────────────

    #[test]
    fn on_message_current_mode_update_plan() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"current_mode_update","currentModeId":"plan"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ModesUpdated {
                available_modes,
                current_mode,
            } => {
                assert!(
                    available_modes.is_empty(),
                    "current_mode_update does not carry mode list"
                );
                assert_eq!(current_mode, Some("plan".to_string()));
            }
            other => panic!("expected ModesUpdated, got {other:?}"),
        }
    }

    #[test]
    fn on_message_current_mode_update_build() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"current_mode_update","currentModeId":"build"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ModesUpdated { current_mode, .. } => {
                assert_eq!(current_mode, Some("build".to_string()));
            }
            other => panic!("expected ModesUpdated, got {other:?}"),
        }
    }

    // ── on_message: context_compaction → Compaction ───────────────────────────

    #[test]
    fn on_message_context_compaction() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"context_compaction"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::Compaction { .. } => {}
            other => panic!("expected Compaction, got {other:?}"),
        }
    }

    // ── on_message: session/request_permission → GateRequested ───────────────

    #[test]
    fn on_message_request_permission_with_options() {
        // Real opencode wire line from marsad golden tests.
        let line = r#"{"jsonrpc":"2.0","id":0,"method":"session/request_permission","params":{"sessionId":"ses_real","toolCall":{"toolCallId":"toolu_perm","title":"/work/x","kind":"other","rawInput":{"filepath":"/work/x"}},"options":[{"optionId":"once","name":"Allow once","kind":"allow_once"},{"optionId":"always","name":"Always allow","kind":"allow_always"},{"optionId":"reject","name":"Reject","kind":"reject_once"}]}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    elicitation_choices,
                    permission_suggestions,
                    is_elicitation,
                    ..
                } => {
                    // routing_id = toolCallId (not the raw ACP id=0).
                    assert_eq!(request_id, "toolu_perm");
                    assert_ne!(request_id, "0", "routing id must not be the raw ACP id 0");
                    assert_eq!(tool_name, "other");
                    assert!(is_elicitation, "options present → is_elicitation=true");
                    assert_eq!(elicitation_choices.len(), 3);
                    assert_eq!(elicitation_choices[0].value, "once");
                    assert_eq!(elicitation_choices[0].label, "Allow once");
                    assert_eq!(elicitation_choices[0].key, "optionId");
                    assert_eq!(elicitation_choices[1].value, "always");
                    assert_eq!(elicitation_choices[2].value, "reject");
                    assert_eq!(permission_suggestions.len(), 3);
                }
                other => panic!("expected Permission, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
        // Gate is stored for answer correlation.
        assert!(p.pending_gate("toolu_perm").is_some());
    }

    #[test]
    fn on_message_request_permission_routing_id_unique_across_sessions() {
        // The raw ACP id=0 is reused across sessions; routing must be the toolCallId.
        let line1 = r#"{"jsonrpc":"2.0","id":0,"method":"session/request_permission","params":{"sessionId":"ses_A","toolCall":{"toolCallId":"toolu_A1","title":"file","kind":"read"},"options":[]}}"#;
        let line2 = r#"{"jsonrpc":"2.0","id":0,"method":"session/request_permission","params":{"sessionId":"ses_B","toolCall":{"toolCallId":"toolu_B1","title":"file","kind":"read"},"options":[]}}"#;
        let mut p = MessageParser::new();
        p.on_message(line1);
        p.on_message(line2);
        // Both must be stored independently.
        assert!(p.pending_gate("toolu_A1").is_some());
        assert!(p.pending_gate("toolu_B1").is_some());
    }

    #[test]
    fn on_message_request_permission_fallback_routing_id() {
        // No toolCallId in toolCall → routing id falls back to "sid:acp:N".
        let line = r#"{"jsonrpc":"2.0","id":42,"method":"session/request_permission","params":{"sessionId":"ses_fallback","toolCall":{"title":"edit","kind":"edit"},"options":[]}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        if let AgentEvent::GateRequested {
            gate: GateRequest::Permission { request_id, .. },
        } = &events[0]
        {
            assert!(
                request_id.contains("acp") || request_id.contains("42"),
                "fallback routing id must contain acp or rpc id: {request_id}"
            );
            assert_ne!(request_id, "42", "routing id must never be bare rpc id");
        }
    }

    // ── on_message: session/prompt result → TurnCompleted ────────────────────

    #[test]
    fn on_message_prompt_result_end_turn_success() {
        // Real opencode wire line: stopReason="end_turn" is the normal success.
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":2,"outputTokens":855}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns,
                usage,
                ..
            } => {
                assert!(end_reason.is_none(), "end_turn → no end_reason");
                assert!(!was_interrupted);
                assert_eq!(num_turns, 1);
                assert_eq!(usage.input_tokens, 2);
                assert_eq!(usage.output_tokens, 855);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_prompt_result_completed_success() {
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"completed","usage":{}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, .. } => {
                assert!(end_reason.is_none(), "completed → no end_reason");
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_prompt_result_cancelled() {
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"cancelled","usage":{}}}"#;
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
    fn on_message_prompt_result_auth_required() {
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"auth_required","usage":{}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, .. } => {
                assert_eq!(end_reason, Some(EndReason::ExecutionError));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_prompt_result_usage_mapped() {
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":1200,"outputTokens":340,"cachedReadTokens":900,"cachedWriteTokens":50}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { usage, .. } => {
                assert_eq!(usage.input_tokens, 1200);
                assert_eq!(usage.output_tokens, 340);
                assert_eq!(usage.cache_read_tokens, 900);
                assert_eq!(usage.cache_creation_tokens, 50);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    // ── on_message: session/set_config_option result → ModelChanged + ModesUpdated

    #[test]
    fn on_message_set_config_option_result_model_and_modes() {
        // Real opencode set_config_option result shape.
        let line = r#"{"jsonrpc":"2.0","id":5,"result":{"configOptions":[{"id":"model","name":"Model","currentValue":"claude-opus-4","options":[{"value":"claude-opus-4","name":"Opus 4"},{"value":"claude-sonnet-4","name":"Sonnet 4"}]},{"id":"mode","name":"Mode","currentValue":"plan","options":[{"value":"build","name":"build","description":"Default"},{"value":"plan","name":"plan","description":"Plan mode"}]}]}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        // Expect both ModelChanged and ModesUpdated.
        let model_changed = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ModelChanged { .. }));
        let modes_updated = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ModesUpdated { .. }));
        assert!(
            model_changed.is_some(),
            "set_config_option result must emit ModelChanged"
        );
        assert!(
            modes_updated.is_some(),
            "set_config_option result must emit ModesUpdated"
        );
        if let Some(AgentEvent::ModelChanged { model }) = model_changed {
            assert_eq!(model, "claude-opus-4");
        }
        if let Some(AgentEvent::ModesUpdated {
            available_modes,
            current_mode,
        }) = modes_updated
        {
            assert_eq!(current_mode.as_deref(), Some("plan"));
            assert_eq!(available_modes.len(), 2);
            assert_eq!(available_modes[0].id, "build");
            assert_eq!(available_modes[1].id, "plan");
        }
    }

    // ── on_message: JSON-RPC error → TurnCompleted(ExecutionError) ───────────

    #[test]
    fn on_message_jsonrpc_error_response() {
        let line =
            r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"something broke"}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted {
                end_reason, reply, ..
            } => {
                assert_eq!(end_reason, Some(EndReason::ExecutionError));
                assert_eq!(reply.as_deref(), Some("something broke"));
            }
            other => panic!("expected TurnCompleted(ExecutionError), got {other:?}"),
        }
    }

    // ── on_message: misc (empty, invalid, user_message_chunk, unknown) ────────

    #[test]
    fn on_message_empty_string() {
        assert!(parse_all("").is_empty());
    }

    #[test]
    fn on_message_whitespace_only() {
        assert!(parse_all("   \t").is_empty());
    }

    #[test]
    fn on_message_invalid_json() {
        assert!(parse_all("{{{not json").is_empty());
    }

    #[test]
    fn on_message_user_message_chunk_skipped() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"hello"}}}}"#;
        assert!(
            parse_all(line).is_empty(),
            "user_message_chunk must be skipped"
        );
    }

    #[test]
    fn on_message_unknown_update_type_emits_unknown() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"some_future_update_type","data":42}}}"#;
        let events = parse_all(line);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], AgentEvent::Unknown { .. }));
    }

    #[test]
    fn on_message_unknown_server_request_method_emits_unknown() {
        let line = r#"{"jsonrpc":"2.0","id":99,"method":"futureServerMethod/v2","params":{}}"#;
        let events = parse_all(line);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], AgentEvent::Unknown { .. }));
    }

    #[test]
    fn on_message_session_new_response_ack_ignored() {
        // session/new result (has sessionId but no stopReason, no configOptions) → silently ignored.
        let line = r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_abc123"}}"#;
        let events = parse_all(line);
        assert!(
            events.is_empty(),
            "session/new response ack must be silently ignored"
        );
    }

    #[test]
    fn on_message_cancel_ack_ignored() {
        let line = r#"{"jsonrpc":"2.0","id":4,"result":{}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "cancel ack must be silently ignored");
    }

    // ── parse_config_modes ────────────────────────────────────────────────────

    #[test]
    fn parse_config_modes_extracts_modes_and_current() {
        let v = serde_json::json!({
            "sessionId": "ses_real",
            "configOptions": [
                {
                    "id": "model",
                    "currentValue": "claude-sonnet-4",
                    "options": [{"value": "claude-sonnet-4", "name": "Sonnet 4"}]
                },
                {
                    "id": "mode",
                    "currentValue": "build",
                    "options": [
                        {"value": "build", "name": "build", "description": "Default."},
                        {"value": "plan", "name": "plan", "description": "Plan mode."}
                    ]
                }
            ]
        });
        let (modes, current) = parse_config_modes(&v);
        assert_eq!(modes.len(), 2);
        assert_eq!(modes[0].id, "build");
        assert_eq!(modes[0].description, "Default.");
        assert_eq!(modes[1].id, "plan");
        assert_eq!(current, "build");
    }

    #[test]
    fn parse_config_modes_returns_empty_when_no_mode_group() {
        let v = serde_json::json!({ "configOptions": [{"id": "model", "options": []}] });
        let (modes, current) = parse_config_modes(&v);
        assert!(modes.is_empty());
        assert!(current.is_empty());
    }

    #[test]
    fn parse_config_modes_returns_empty_when_no_config_options() {
        let v = serde_json::json!({ "sessionId": "ses_x" });
        let (modes, current) = parse_config_modes(&v);
        assert!(modes.is_empty());
        assert!(current.is_empty());
    }

    // ── parse_config_model ────────────────────────────────────────────────────

    #[test]
    fn parse_config_model_extracts_current_value() {
        let v = serde_json::json!({
            "configOptions": [
                {"id": "model", "currentValue": "amd-anthropic/Claude-Sonnet-5", "options": []}
            ]
        });
        let model = parse_config_model(&v);
        assert_eq!(model.as_deref(), Some("amd-anthropic/Claude-Sonnet-5"));
    }

    #[test]
    fn parse_config_model_none_when_absent() {
        let v = serde_json::json!({ "configOptions": [] });
        assert!(parse_config_model(&v).is_none());
    }

    // ── driver capabilities ───────────────────────────────────────────────────

    #[test]
    fn driver_capabilities_honest() {
        let driver = OpencodeDriver::from_path();
        let caps = driver.capabilities();
        assert!(caps.interruptible, "session/cancel verified");
        assert!(caps.model_switchable, "session/set_config_option id=model");
        assert!(caps.mode_switchable, "session/set_config_option id=mode");
        assert!(caps.persistent_conversation);
        assert!(caps.resumable, "session/load");
        assert!(caps.streaming_output, "agent_message_chunk");
        assert!(caps.reports_usage, "usage_update + prompt result usage");
        assert!(caps.supports_gates, "session/request_permission");
    }

    // ── gate-response encode dispatch ─────────────────────────────────────────

    #[test]
    fn gate_response_approve_sends_option_from_elicitation_content() {
        let gate = PendingGate {
            raw_id: serde_json::json!(0),
            params: serde_json::Value::Null,
        };
        let decision = GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: Some(serde_json::json!({ "optionId": "always" })),
        };
        let json = encode_gate_response(&gate, &decision);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["optionId"], "always");
        assert_eq!(v["id"], 0);
    }

    #[test]
    fn gate_response_deny_sends_reject() {
        let gate = PendingGate {
            raw_id: serde_json::json!(7),
            params: serde_json::Value::Null,
        };
        let decision = GateDecision::Deny { reason: None };
        let json = encode_gate_response(&gate, &decision);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["optionId"], "reject");
        assert_eq!(v["id"], 7);
    }

    #[test]
    fn gate_response_approve_no_content_defaults_to_once() {
        let gate = PendingGate {
            raw_id: serde_json::json!(3),
            params: serde_json::Value::Null,
        };
        let decision = GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: None,
        };
        let json = encode_gate_response(&gate, &decision);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["optionId"], "once");
    }

    // ── Real opencode golden wire-line tests ──────────────────────────────────

    #[test]
    fn golden_real_opencode_turn_text_tool_gate_result() {
        // Replay the full sequence from marsad's golden test using real opencode
        // wire lines, through on_message → AgentEvent.

        let mut p = MessageParser::new();

        // 1. Text chunk.
        let ev1 = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_1","content":{"type":"text","text":"Looking at the codebase."}}}}"#);
        assert_eq!(ev1.len(), 1);
        assert!(matches!(ev1[0], AgentEvent::AssistantDelta { .. }));

        // 2. tool_call → ToolCallStarted (name = title "glob", not "unknown_tool").
        let ev2 = p.on_message(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_real","update":{"sessionUpdate":"tool_call","toolCallId":"toolu_1","title":"glob","kind":"search","status":"pending","rawInput":{"pattern":"*"}}}}"#);
        assert_eq!(ev2.len(), 1);
        if let AgentEvent::ToolCallStarted { name, .. } = &ev2[0] {
            assert_eq!(name, "glob", "tool_call name must be the title 'glob'");
        }

        // 3. session/request_permission → GateRequested with routing_id = toolCallId.
        let ev3 = p.on_message(r#"{"jsonrpc":"2.0","id":0,"method":"session/request_permission","params":{"sessionId":"ses_real","toolCall":{"toolCallId":"toolu_perm","title":"/work/x","kind":"other","rawInput":{"filepath":"/work/x"}},"options":[{"optionId":"once","name":"Allow once","kind":"allow_once"},{"optionId":"always","name":"Always allow","kind":"allow_always"},{"optionId":"reject","name":"Reject","kind":"reject_once"}]}}"#);
        assert_eq!(ev3.len(), 1);
        if let AgentEvent::GateRequested {
            gate:
                GateRequest::Permission {
                    request_id,
                    permission_suggestions,
                    ..
                },
        } = &ev3[0]
        {
            assert_ne!(request_id, "0", "routing id must not be the raw ACP id 0");
            assert_eq!(permission_suggestions.len(), 3, "3 ACP options");
        }

        // 4. session/prompt result end_turn → TurnCompleted(success, no error).
        let ev4 = p.on_message(r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":2,"outputTokens":855}}}"#);
        assert_eq!(ev4.len(), 1);
        if let AgentEvent::TurnCompleted { end_reason, .. } = &ev4[0] {
            assert!(end_reason.is_none(), "end_turn must not be an error");
        }
    }

    #[test]
    fn golden_real_opencode_configoptions_surfaces_modes() {
        // Real session/new result object (trimmed model list).
        let result: serde_json::Value = serde_json::from_str(
            r#"{"sessionId":"ses_07d65356","configOptions":[{"id":"model","name":"Model","category":"model","type":"select","currentValue":"amd-anthropic/Claude-Sonnet-5","options":[{"value":"amd-anthropic/Claude-Haiku-4.5","name":"Claude Haiku 4.5"},{"value":"amd-anthropic/Claude-Opus-4.8","name":"Claude Opus 4.8"}]},{"id":"mode","name":"Session Mode","category":"mode","type":"select","currentValue":"build","options":[{"value":"build","name":"build","description":"Default agent."},{"value":"plan","name":"plan","description":"Plan mode."}]}]}"#,
        ).unwrap();

        let (modes, current) = parse_config_modes(&result);
        assert_eq!(
            modes.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            vec!["build", "plan"]
        );
        assert_eq!(current, "build");

        let model = parse_config_model(&result);
        assert_eq!(model.as_deref(), Some("amd-anthropic/Claude-Sonnet-5"));
    }

    #[test]
    fn golden_real_opencode_mode_switch_reflects_in_set_config_result() {
        // Real session/set_config_option result after switching mode to plan.
        let line = r#"{"jsonrpc":"2.0","id":5,"result":{"configOptions":[{"id":"model","name":"Model","currentValue":"amd-anthropic/Claude-Sonnet-5","options":[{"value":"amd-anthropic/Claude-Haiku-4.5","name":"Haiku"},{"value":"amd-anthropic/Claude-Opus-4.8","name":"Opus"}]},{"id":"mode","name":"Session Mode","currentValue":"plan","options":[{"value":"build","name":"build","description":"Default"},{"value":"plan","name":"plan","description":"Plan mode"}]}]}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        let mode_ev = events
            .iter()
            .find(|e| matches!(e, AgentEvent::ModesUpdated { .. }));
        assert!(mode_ev.is_some());
        if let Some(AgentEvent::ModesUpdated {
            current_mode,
            available_modes,
        }) = mode_ev
        {
            assert_eq!(current_mode.as_deref(), Some("plan"));
            assert_eq!(available_modes.len(), 2);
        }
    }
}
