//! wasit-codex — codex app-server JSON-RPC driver for Wasit.
//!
//! Implements `AgentDriver` + `Session` for `codex app-server --stdio`.
//!
//! ## Wire protocol overview
//!
//! Codex communicates over stdin/stdout as NDJSON JSON-RPC 2.0 lines.
//! Codex OMITS the `jsonrpc` field on outbound lines (verified live: responses
//! are `{"id":N,"result":{…}}`, notifications `{"method":…,"params":…}`).
//! We include `"jsonrpc":"2.0"` on our OWN outbound requests (codex accepts it).
//!
//! Handshake: `initialize` → `thread/start` (or `thread/resume` for resume).
//! Per-turn: `turn/start` request → `turn/started` notification +
//! `item/agentMessage/delta` notifications + `item/started`/`item/completed`
//! (commandExecution) + `turn/completed` notification.
//! Interrupt: `turn/interrupt` request.
//!
//! ## Gate handling (REAL gates, ported from Marsad)
//!
//! Codex app-server defines server-initiated JSON-RPC REQUEST methods
//! (`execCommandApproval`, `applyPatchApproval`,
//! `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`,
//! `item/permissions/requestApproval`, `item/tool/requestUserInput`,
//! `mcpServer/elicitation/request`) that arrive with both `method` and `id`,
//! requiring a response written to stdin before codex can proceed.
//!
//! This driver surfaces them as `GateRequested` events (porting Marsad's full
//! gate handling from `codex_translate.rs`) and answers them via `answer_gate`.
//! `supports_gates: true`.
//!
//! ## Capability report (honest)
//! - `interruptible`: true — `turn/interrupt` verified live.
//! - `model_switchable`: false — codex does not support live model changes.
//! - `mode_switchable`: false — codex does not report modes in its protocol.
//! - `persistent_conversation`: true — thread-based context persists.
//! - `resumable`: true — `thread/resume` verified live.
//! - `streaming_output`: true — `item/agentMessage/delta` notifications.
//! - `reports_usage`: false — codex does not report token/cost usage (gap).
//! - `supports_gates`: true — REAL gates ported from Marsad.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::mpsc;
use wasit_core::{
    AgentDriver, AgentEvent, DriverCapabilities, DriverError, ElicitationChoice, EndReason,
    GateDecision, GateRequest, QuestionAnswer, QuestionOption, QuestionPrompt, Session, SpawnSpec,
    Usage,
};

// ============================================================================
// 1. argv builder (pure)
// ============================================================================

/// Build the `codex app-server --stdio` invocation for a session launch.
///
/// Model/approval/sandbox are NOT CLI flags here — they are set per-thread via
/// `thread/start` params. Binary defaults to `"codex"` when not in spec.
///
/// Matches Marsad's `codex_appserver_argv`.
pub fn codex_argv(spec: &SpawnSpec) -> Vec<String> {
    let bin = spec.bin.as_deref().unwrap_or("codex").to_string();
    vec![bin, "app-server".to_string(), "--stdio".to_string()]
}

// ============================================================================
// 2. Pure JSON-RPC request builders
// ============================================================================

/// Build the `initialize` request (id=1).
pub fn encode_initialize(client_name: &str, client_version: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": client_name,
                "version": client_version,
            }
        }
    }))
    .expect("always serializable")
}

/// Map a mode id to codex `(approvalPolicy, sandbox)`.
///
/// Codex's available modes are not discoverable from the CLI; any mode id
/// (including empty = backend default) maps to the sane default settings.
/// Ported from Marsad's `codex_mode_settings`.
pub fn codex_mode_settings(_mode: &str) -> (&'static str, &'static str) {
    ("on-request", "workspace-write")
}

/// Build the `thread/start` params.
///
/// Model, approvalPolicy, sandbox are optional. Mode is mapped via
/// `codex_mode_settings`. Ported from Marsad's `codex_appserver.rs`.
pub fn thread_start_params(cwd: &str, model: Option<&str>, mode: &str) -> serde_json::Value {
    let (approval_policy, sandbox) = codex_mode_settings(mode);
    let mut o = serde_json::Map::new();
    o.insert("cwd".into(), serde_json::json!(cwd));
    if let Some(m) = model.filter(|s| !s.is_empty()) {
        o.insert("model".into(), serde_json::json!(m));
    }
    o.insert("approvalPolicy".into(), serde_json::json!(approval_policy));
    o.insert("sandbox".into(), serde_json::json!(sandbox));
    serde_json::Value::Object(o)
}

/// Build the `thread/start` JSON-RPC request (id=2).
pub fn encode_thread_start(id: u64, cwd: &str, model: Option<&str>, mode: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "thread/start",
        "params": thread_start_params(cwd, model, mode),
    }))
    .expect("always serializable")
}

/// Build the `thread/resume` JSON-RPC request (id=2).
pub fn encode_thread_resume(id: u64, thread_id: &str, cwd: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "thread/resume",
        "params": {
            "threadId": thread_id,
            "cwd": cwd,
        },
    }))
    .expect("always serializable")
}

/// Build a `turn/start` JSON-RPC request.
pub fn encode_turn_start(id: u64, thread_id: &str, text: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "turn/start",
        "params": {
            "threadId": thread_id,
            "input": [{"type": "text", "text": text}],
        },
    }))
    .expect("always serializable")
}

/// Build a `turn/interrupt` JSON-RPC request.
pub fn encode_turn_interrupt(id: u64, thread_id: &str, turn_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "turn/interrupt",
        "params": {
            "threadId": thread_id,
            "turnId": turn_id,
        },
    }))
    .expect("always serializable")
}

/// Build a `collaborationMode/list` JSON-RPC request.
pub fn encode_collab_mode_list(id: u64, thread_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "collaborationMode/list",
        "params": {"threadId": thread_id},
    }))
    .expect("always serializable")
}

// ============================================================================
// 3. Gate-response encoders (pure — ported from Marsad's codex_translate.rs)
// ============================================================================

/// Identify whether a method is a new-style `item/…/requestApproval` gate.
fn is_command_approval(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
    )
}

/// Identify whether a method is a legacy exec-path approval.
fn is_legacy_approval(method: &str) -> bool {
    matches!(method, "execCommandApproval" | "applyPatchApproval")
}

/// Encode a codex gate approval response (written to stdin as a JSON-RPC response).
///
/// The shape of the result object depends on which approval method was received:
/// - `item/commandExecution/requestApproval` + `item/fileChange/requestApproval`
///   + `item/permissions/requestApproval`: `{"decision":"accept"|"decline"|"acceptForSession"}`
/// - `execCommandApproval` + `applyPatchApproval` (legacy):
///   `{"decision":"approved"|"denied"}`
/// - `item/tool/requestUserInput`: `{"answers":{…}}`
/// - `mcpServer/elicitation/request`: `{"action":"accept"|"decline","content":{…}}`
///
/// `request_id_raw` is the JSON Value of the gate's `id` field (may be number
/// or string per JSON-RPC spec; codex uses integers).
pub fn encode_gate_approve(
    request_id_raw: &serde_json::Value,
    method: &str,
    for_session: bool,
) -> String {
    let result = if is_command_approval(method) {
        let d = if for_session { "acceptForSession" } else { "accept" };
        serde_json::json!({"decision": d})
    } else if is_legacy_approval(method) {
        serde_json::json!({"decision": "approved"})
    } else {
        // Fallback for unknown method types.
        serde_json::json!({"decision": "accept"})
    };
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": result,
    }))
    .expect("always serializable")
}

/// Encode a codex gate deny response.
pub fn encode_gate_deny(request_id_raw: &serde_json::Value, method: &str) -> String {
    let result = if is_legacy_approval(method) {
        serde_json::json!({"decision": "denied"})
    } else if method == "item/permissions/requestApproval" {
        // Deny = grant no permissions.
        serde_json::json!({"permissions": {}})
    } else {
        // item/commandExecution, item/fileChange, and any unknown: "decline"
        serde_json::json!({"decision": "decline"})
    };
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": result,
    }))
    .expect("always serializable")
}

/// Encode a `mcpServer/elicitation/request` accept response.
pub fn encode_elicitation_accept(
    request_id_raw: &serde_json::Value,
    content: Option<serde_json::Value>,
) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": {
            "action": "accept",
            "content": content.unwrap_or_else(|| serde_json::json!({})),
        },
    }))
    .expect("always serializable")
}

/// Encode a `mcpServer/elicitation/request` decline response.
pub fn encode_elicitation_decline(request_id_raw: &serde_json::Value) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": {"action": "decline"},
    }))
    .expect("always serializable")
}

/// Encode an `item/tool/requestUserInput` answer response.
pub fn encode_user_input_answer(
    request_id_raw: &serde_json::Value,
    answers: serde_json::Value,
) -> String {
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id_raw,
        "result": {"answers": answers},
    }))
    .expect("always serializable")
}

// ============================================================================
// 4. Stateful message parser (pure core — testable without IO)
// ============================================================================

/// Stored metadata about a pending gate request, needed to encode the response.
#[derive(Debug, Clone)]
pub struct PendingGate {
    /// Raw JSON-RPC id (may be number or string).
    pub raw_id: serde_json::Value,
    /// The codex method that triggered the gate.
    pub method: String,
    /// Original gate params (for encoding permissions/user-input answers).
    params: serde_json::Value,
}

/// Stateful codex app-server line parser.
///
/// Maintains:
/// - `pending_commands`: item_id → command name (for ToolCallFinished correlation)
/// - `pending_gates`: routing_id → PendingGate (for gate answer correlation)
/// - `active_turn_id`: the current `turn.id` (for interrupt)
#[derive(Debug, Default)]
pub struct MessageParser {
    /// Maps item_id → command string for commandExecution correlation.
    pending_commands: HashMap<String, String>,
    /// Maps routing_id → gate metadata for answer correlation.
    pending_gates: HashMap<String, PendingGate>,
    /// Current turn id (from `turn/started`), used for interrupt.
    active_turn_id: Option<String>,
}

impl MessageParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The current active turn id (needed by the session to send turn/interrupt).
    pub fn active_turn_id(&self) -> Option<&str> {
        self.active_turn_id.as_deref()
    }

    /// Look up the stored gate metadata for a routing_id (for answer_gate).
    pub fn pending_gate(&self, routing_id: &str) -> Option<&PendingGate> {
        self.pending_gates.get(routing_id)
    }

    /// Remove a pending gate after it has been answered.
    pub fn remove_gate(&mut self, routing_id: &str) -> Option<PendingGate> {
        self.pending_gates.remove(routing_id)
    }

    /// Process one NDJSON line from codex app-server stdout.
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

        // Classify the message type.
        if has_method && has_id && !has_result && !has_error {
            // Server-initiated REQUEST (a gate).
            return self.handle_server_request(&val, obj);
        }

        if has_method && !has_id {
            // Notification.
            let method = match obj.get("method").and_then(|v| v.as_str()) {
                Some(m) => m,
                None => return vec![],
            };
            let params = obj.get("params").cloned().unwrap_or(serde_json::Value::Null);
            return self.handle_notification(method, &params);
        }

        if has_id && has_result && !has_method {
            // Response to one of OUR outgoing requests. Usually just an ack —
            // we emit Unknown for forward-compat observability.
            return vec![];
        }

        if has_id && has_error && !has_method {
            // Error response to one of our requests — fail the turn.
            let error_msg = val
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("codex json-rpc error")
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

    // ── Server-initiated REQUEST (gate) ───────────────────────────────────────

    fn handle_server_request(
        &mut self,
        val: &serde_json::Value,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Vec<AgentEvent> {
        let method = match obj.get("method").and_then(|v| v.as_str()) {
            Some(m) => m,
            None => return vec![],
        };
        let params = obj.get("params").cloned().unwrap_or(serde_json::Value::Null);
        let raw_id = obj.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let raw_id_str = match &raw_id {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => raw_id.to_string(),
        };

        // Compute the routing id (unique per gate, stable for answer routing).
        let routing_id = compute_gate_routing_id(&raw_id_str, &params);

        match method {
            // New-style command/file/permissions approval.
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                let gate = build_approval_gate(&routing_id, method, &params);
                self.pending_gates.insert(
                    routing_id.clone(),
                    PendingGate {
                        raw_id,
                        method: method.to_string(),
                        params,
                    },
                );
                vec![AgentEvent::GateRequested { gate }]
            }

            // Legacy approval.
            "execCommandApproval" | "applyPatchApproval" => {
                let gate = build_approval_gate(&routing_id, method, &params);
                self.pending_gates.insert(
                    routing_id.clone(),
                    PendingGate {
                        raw_id,
                        method: method.to_string(),
                        params,
                    },
                );
                vec![AgentEvent::GateRequested { gate }]
            }

            // User input request (maps to GateRequest::Question).
            "item/tool/requestUserInput" => {
                let gate = build_user_input_gate(&routing_id, &params);
                self.pending_gates.insert(
                    routing_id.clone(),
                    PendingGate {
                        raw_id,
                        method: method.to_string(),
                        params,
                    },
                );
                vec![AgentEvent::GateRequested { gate }]
            }

            // MCP elicitation.
            "mcpServer/elicitation/request" => {
                let gate = build_elicitation_gate(&routing_id, &params);
                self.pending_gates.insert(
                    routing_id.clone(),
                    PendingGate {
                        raw_id,
                        method: method.to_string(),
                        params,
                    },
                );
                vec![AgentEvent::GateRequested { gate }]
            }

            _ => {
                // Unknown server request — emit Unknown for observability,
                // don't silently hang.
                vec![AgentEvent::Unknown { raw: val.clone() }]
            }
        }
    }

    // ── Notification handler ──────────────────────────────────────────────────

    fn handle_notification(&mut self, method: &str, params: &serde_json::Value) -> Vec<AgentEvent> {
        match method {
            "thread/started" => {
                let native_session_id = params
                    .get("thread")
                    .and_then(|t| t.get("id"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                // Clear turn state on new thread.
                self.active_turn_id = None;
                vec![AgentEvent::SessionInit {
                    native_session_id,
                    model: None,
                    cwd: None,
                    tools: vec![],
                    mcp_servers: vec![],
                    slash_commands: vec![],
                    permission_mode: String::new(),
                    claude_code_version: String::new(),
                    available_modes: vec![],
                    current_mode: String::new(),
                }]
            }

            "turn/started" => {
                let turn_id = params
                    .get("turn")
                    .and_then(|t| t.get("id"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                // Track the active turn id for interrupt.
                self.active_turn_id = turn_id.clone();
                vec![AgentEvent::TurnStarted {
                    native_turn_id: turn_id,
                }]
            }

            "turn/completed" => {
                let turn = params.get("turn");
                let status = turn
                    .and_then(|t| t.get("status"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("completed");

                let end_reason = match status {
                    "completed" => None,
                    "interrupted" | "cancelled" | "canceled" => Some(EndReason::Cancelled),
                    _ => Some(EndReason::ExecutionError),
                };

                let was_interrupted = end_reason == Some(EndReason::Cancelled);
                let error_msg = if matches!(end_reason, Some(EndReason::ExecutionError)) {
                    turn.and_then(|t| t.get("error"))
                        .and_then(|e| e.get("message"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                };

                // Clear turn state.
                self.active_turn_id = None;
                // Clear pending commands for this turn boundary.
                self.pending_commands.clear();

                vec![AgentEvent::TurnCompleted {
                    end_reason,
                    was_interrupted,
                    num_turns: 0,
                    usage: Usage::default(),
                    per_model: HashMap::new(),
                    reply: error_msg,
                }]
            }

            "item/agentMessage/delta" => {
                let delta = params
                    .get("delta")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if delta.is_empty() {
                    return vec![];
                }
                vec![AgentEvent::AssistantDelta {
                    text: delta,
                    message_id: None,
                }]
            }

            "item/started" => {
                let item = match params.get("item") {
                    Some(i) => i,
                    None => return vec![],
                };
                let itype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if itype != "commandExecution" {
                    return vec![];
                }
                let item_id = match item.get("id").and_then(|v| v.as_str()) {
                    Some(id) => id,
                    None => return vec![],
                };
                let command = match item.get("command").and_then(|v| v.as_str()) {
                    Some(c) => c,
                    None => return vec![],
                };
                // Track item_id → command for ToolCallFinished correlation.
                self.pending_commands.insert(item_id.to_string(), command.to_string());
                vec![AgentEvent::ToolCallStarted {
                    name: command.to_string(),
                    call_id: item_id.to_string(),
                    input: serde_json::Value::Null,
                    is_subagent: false,
                    subagent_model: None,
                }]
            }

            "item/completed" => {
                let item = match params.get("item") {
                    Some(i) => i,
                    None => return vec![],
                };
                let itype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if itype != "commandExecution" {
                    return vec![];
                }
                let item_id = match item.get("id").and_then(|v| v.as_str()) {
                    Some(id) => id,
                    None => return vec![],
                };
                let name = match self.pending_commands.remove(item_id) {
                    Some(n) => n,
                    None => return vec![],
                };
                let exit_code = item.get("exitCode").and_then(|v| v.as_i64()).unwrap_or(-1);
                let ok = exit_code == 0;
                vec![AgentEvent::ToolCallFinished {
                    call_id: item_id.to_string(),
                    name,
                    ok,
                    result: None,
                }]
            }

            // Ignore streaming deltas we don't need (non-commandExecution items).
            // `turn/plan/updated`, `thread/tokenUsage/updated`, etc. → skip.
            _ => vec![],
        }
    }
}

// ============================================================================
// 5. Pure gate builders (ported from Marsad's codex_translate.rs)
// ============================================================================

/// Compute a stable routing id for a codex gate.
///
/// Uses `itemId` from params if present (unique per item), else falls back to
/// a composite from the raw JSON-RPC id. Matches Marsad's `codex_gate_routing_id`.
fn compute_gate_routing_id(raw_id: &str, params: &serde_json::Value) -> String {
    params
        .get("itemId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("codex:{raw_id}"))
}

/// Build a `GateRequest::Permission` for `item/…/requestApproval` or legacy
/// `execCommandApproval`/`applyPatchApproval`. Ported from Marsad's
/// `codex_approval_to_control_request`.
fn build_approval_gate(routing_id: &str, method: &str, params: &serde_json::Value) -> GateRequest {
    let (tool_name, command, cwd) = match method {
        "item/fileChange/requestApproval" | "applyPatchApproval" => {
            let changes = params
                .get("changes")
                .or_else(|| params.get("fileChanges"))
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            (
                "apply_patch".to_string(),
                format!("{changes}"),
                String::new(),
            )
        }
        "item/permissions/requestApproval" => (
            "permissions".to_string(),
            params
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("codex requests permission")
                .to_string(),
            String::new(),
        ),
        // command execution (new + legacy).
        _ => {
            let command = params
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cwd = params
                .get("cwd")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            ("shell".to_string(), command, cwd)
        }
    };

    let reason = params
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let explanation = if reason.is_empty() {
        format!("codex requests approval to {tool_name}")
    } else {
        reason
    };

    GateRequest::Permission {
        request_id: routing_id.to_string(),
        tool_name,
        command,
        cwd,
        explanation,
        is_elicitation: false,
        elicitation_choices: vec![],
        permission_suggestions: vec![],
        raw_input: params.clone(),
    }
}

/// Build a `GateRequest::Question` for `item/tool/requestUserInput`.
/// Ported from Marsad's `codex_question_to_control_request`.
fn build_user_input_gate(routing_id: &str, params: &serde_json::Value) -> GateRequest {
    let questions: Vec<QuestionPrompt> = params
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
                    let options: Vec<QuestionOption> = q
                        .get("options")
                        .and_then(|v| v.as_array())
                        .map(|opts| {
                            opts.iter()
                                .map(|o| QuestionOption {
                                    label: o
                                        .get("label")
                                        .or_else(|| o.get("value"))
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
                        multi_select: false,
                        options,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    GateRequest::Question {
        request_id: routing_id.to_string(),
        questions,
    }
}

/// Build a `GateRequest::Permission { is_elicitation: true }` for
/// `mcpServer/elicitation/request`. Ported from Marsad's
/// `codex_elicitation_to_control_request`.
fn build_elicitation_gate(routing_id: &str, params: &serde_json::Value) -> GateRequest {
    let server_name = params
        .get("mode")
        .or_else(|| params.get("server"))
        .and_then(|v| v.as_str())
        .unwrap_or("codex")
        .to_string();
    let message = params
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let schema = params.get("requestedSchema").cloned();

    let elicitation_choices = parse_elicitation_choices(schema.as_ref());

    GateRequest::Permission {
        request_id: routing_id.to_string(),
        tool_name: server_name,
        command: message.clone(),
        cwd: String::new(),
        explanation: message,
        is_elicitation: true,
        elicitation_choices,
        permission_suggestions: vec![],
        raw_input: serde_json::Value::Null,
    }
}

/// Parse elicitation choices from a `requestedSchema` JSON Schema object.
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

// ============================================================================
// 6. Gate-answer encoder dispatcher
// ============================================================================

/// Given a stored `PendingGate` and a `GateDecision`, produce the JSON-RPC
/// response line to write to codex's stdin.
fn encode_gate_response(gate: &PendingGate, decision: &GateDecision) -> String {
    let method = gate.method.as_str();
    match decision {
        GateDecision::Approve {
            elicitation_content,
            ..
        } => {
            if method == "mcpServer/elicitation/request" {
                encode_elicitation_accept(&gate.raw_id, elicitation_content.clone())
            } else if method == "item/tool/requestUserInput" {
                let answers = elicitation_content
                    .clone()
                    .unwrap_or_else(|| serde_json::json!({}));
                encode_user_input_answer(&gate.raw_id, answers)
            } else {
                let for_session = false; // simple approval; consumers can extend
                encode_gate_approve(&gate.raw_id, method, for_session)
            }
        }
        GateDecision::Deny { .. } => {
            if method == "mcpServer/elicitation/request" {
                encode_elicitation_decline(&gate.raw_id)
            } else {
                encode_gate_deny(&gate.raw_id, method)
            }
        }
    }
}

// ============================================================================
// 7. IO Shell — AgentDriver + Session
// ============================================================================

/// The codex app-server JSON-RPC driver.
pub struct CodexDriver {
    bin: Option<String>,
}

impl CodexDriver {
    pub fn new(bin: impl Into<String>) -> Self {
        CodexDriver {
            bin: Some(bin.into()),
        }
    }

    pub fn from_path() -> Self {
        CodexDriver { bin: None }
    }
}

#[async_trait]
impl AgentDriver for CodexDriver {
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            interruptible: true,
            model_switchable: false,
            mode_switchable: false,
            persistent_conversation: true,
            resumable: true,
            streaming_output: true,
            reports_usage: false,
            // REAL gates ported from Marsad — supports_gates: true.
            supports_gates: true,
        }
    }

    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError> {
        let mut spawn_spec = spec.clone();
        if spawn_spec.bin.is_none() {
            spawn_spec.bin = self.bin.clone();
        }

        let argv = codex_argv(&spawn_spec);
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

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| DriverError::SpawnFailed {
                detail: "no stdin".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DriverError::SpawnFailed {
                detail: "no stdout".to_string(),
            })?;

        // ── Perform the handshake synchronously before returning ──────────────

        // 1. Send initialize.
        let init_line = encode_initialize("wasit-codex", env!("CARGO_PKG_VERSION"));
        write_line_to(&mut stdin, &init_line).await.map_err(|e| DriverError::SpawnFailed {
            detail: format!("initialize write: {e}"),
        })?;

        // 2. Read and skip until we find the initialize response (id=1).
        let mut stdout_reader = BufReader::new(stdout);
        wait_for_response_id(&mut stdout_reader, 1).await.map_err(|e| DriverError::SpawnFailed {
            detail: format!("initialize response: {e}"),
        })?;

        // 3. Send thread/start (or thread/resume).
        let cwd = spawn_spec.cwd.as_deref().unwrap_or(".");
        let model = spawn_spec.model.as_deref();
        let mode = &spawn_spec.mode;
        let thread_req_id: u64 = 2;

        let thread_line = match &spawn_spec.resume {
            wasit_core::ResumeMode::Resume(thread_id) => {
                encode_thread_resume(thread_req_id, thread_id, cwd)
            }
            _ => encode_thread_start(thread_req_id, cwd, model, mode),
        };
        write_line_to(&mut stdin, &thread_line)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("thread/start write: {e}"),
            })?;

        // 4. Read until we find the thread/start response (id=2) → extract thread_id.
        let thread_id = wait_for_thread_id(&mut stdout_reader, thread_req_id)
            .await
            .map_err(|e| DriverError::SpawnFailed {
                detail: format!("thread/start response: {e}"),
            })?;

        // Event channel: reader task → session.
        let (tx, rx) = mpsc::channel::<AgentEvent>(256);

        // Start the reader task.
        tokio::spawn(reader_task(stdout_reader, tx));

        let mut session = CodexSession {
            child,
            stdin: Some(stdin),
            rx,
            thread_id,
            next_request_id: thread_req_id + 1,
            parser: MessageParser::new(),
        };

        // Deliver first_prompt if any.
        if let Some(prompt) = spawn_spec.first_prompt.clone() {
            session.send_prompt(&prompt).await.map_err(|e| DriverError::SpawnFailed {
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
/// Returns an error string on timeout or parse failure.
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
        // Skip notifications (no id field) and requests from server (have method).
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
            return Ok(val.get("result").cloned().unwrap_or(serde_json::Value::Null));
        }
    }
}

/// Read lines until we find the thread/start (or thread/resume) response, then
/// extract the thread id from `result.thread.id`.
async fn wait_for_thread_id(
    reader: &mut BufReader<ChildStdout>,
    expected_id: u64,
) -> Result<String, String> {
    let result = wait_for_response_id(reader, expected_id).await?;
    result
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| "thread/start result missing thread.id".to_string())
}

/// Background reader task: reads stdout lines and sends AgentEvents.
async fn reader_task(mut reader: BufReader<ChildStdout>, tx: mpsc::Sender<AgentEvent>) {
    let mut parser = MessageParser::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF.
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

/// A live codex app-server session.
pub struct CodexSession {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: mpsc::Receiver<AgentEvent>,
    thread_id: String,
    next_request_id: u64,
    /// Parser owned by the session (for gate answer correlation).
    /// NOTE: the reader task has its OWN parser instance. This parser is
    /// used ONLY for tracking pending gates in the session's IO methods.
    /// Gate events arrive via the channel; this parser tracks metadata for
    /// answer_gate to route responses. We duplicate minimal state here.
    parser: MessageParser,
}

impl CodexSession {
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
        stdin.write_all(b"\n").await.map_err(|e| DriverError::Transport {
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
impl Session for CodexSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        let ev = self.rx.recv().await?;
        // Intercept gate events to track them in this session's parser for
        // answer_gate correlation. The reader_task's MessageParser emitted the
        // gate event; here we register the metadata needed to build the response.
        //
        // KNOWN GAP: The reader_task's parser has the true JSON-RPC id (raw_id)
        // for each gate. We can't recover it from the AgentEvent (which only
        // carries the routing_id). For item/…/requestApproval gates the routing_id
        // is the itemId (not the rpc id); for elicitation/userInput gates we emit
        // the rpc id as the routing_id via "codex:<id>". We reconstruct the best
        // raw_id from the routing_id string here. A full fix would carry raw_id in
        // the event or via a parallel channel — deferred, flagged here.
        if let AgentEvent::GateRequested { ref gate } = ev {
            let routing_id = match gate {
                GateRequest::Permission { request_id, .. } => request_id.clone(),
                GateRequest::Plan { request_id, .. } => request_id.clone(),
                GateRequest::Question { request_id, .. } => request_id.clone(),
            };

            let raw_id_val = if let Some(numeric_part) = routing_id.strip_prefix("codex:") {
                // Composite id: "codex:<rpc-id>"; recover the numeric part.
                if let Ok(n) = numeric_part.parse::<u64>() {
                    serde_json::json!(n)
                } else {
                    serde_json::json!(routing_id)
                }
            } else {
                // itemId routing: use as-is (itemId is the gate's response identity).
                serde_json::json!(routing_id)
            };

            let method = match gate {
                GateRequest::Permission {
                    is_elicitation,
                    tool_name,
                    ..
                } => {
                    if *is_elicitation {
                        "mcpServer/elicitation/request".to_string()
                    } else {
                        match tool_name.as_str() {
                            "apply_patch" => "item/fileChange/requestApproval".to_string(),
                            "permissions" => "item/permissions/requestApproval".to_string(),
                            _ => "item/commandExecution/requestApproval".to_string(),
                        }
                    }
                }
                GateRequest::Question { .. } => "item/tool/requestUserInput".to_string(),
                GateRequest::Plan { .. } => "unknown".to_string(),
            };

            self.parser.pending_gates.insert(
                routing_id,
                PendingGate {
                    raw_id: raw_id_val,
                    method,
                    params: serde_json::Value::Null,
                },
            );
        }
        Some(ev)
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        let id = self.next_id();
        let line = encode_turn_start(id, &self.thread_id.clone(), text);
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
            operation: "answer_question (codex: use answer_gate with elicitation_content for requestUserInput)".to_string(),
        })
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        // We don't have the turn_id here because the reader_task's parser
        // tracks it. We'd need to pipe it through. For now, try without turn_id
        // (send empty string). Codex may still honor it. Known gap.
        let id = self.next_id();
        let thread_id = self.thread_id.clone();
        let line = encode_turn_interrupt(id, &thread_id, "");
        self.write_line(&line).await
    }

    async fn set_model(&mut self, _model: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_model (codex does not support live model changes)".to_string(),
        })
    }

    async fn set_mode(&mut self, _mode_id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_mode (codex does not expose a mode concept in its protocol)".to_string(),
        })
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
            model: Some("o4-mini".to_string()),
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

    // ── codex_argv ───────────────────────────────────────────────────────────

    #[test]
    fn argv_default_bin() {
        let argv = codex_argv(&SpawnSpec::default());
        assert_eq!(argv[0], "codex");
        assert_eq!(argv[1], "app-server");
        assert_eq!(argv[2], "--stdio");
    }

    #[test]
    fn argv_custom_bin() {
        let mut spec = base_spec();
        spec.bin = Some("/usr/local/bin/codex".to_string());
        let argv = codex_argv(&spec);
        assert_eq!(argv[0], "/usr/local/bin/codex");
        assert_eq!(argv[1], "app-server");
        assert_eq!(argv[2], "--stdio");
    }

    #[test]
    fn argv_always_three_elements() {
        let argv = codex_argv(&base_spec());
        assert_eq!(argv.len(), 3, "codex_argv must always have 3 elements");
    }

    #[test]
    fn argv_deterministic() {
        assert_eq!(codex_argv(&base_spec()), codex_argv(&base_spec()));
    }

    #[test]
    fn argv_no_model_flag() {
        // Model is NOT in the argv — it goes in thread/start params.
        let argv = codex_argv(&base_spec());
        assert!(!argv.contains(&"--model".to_string()));
        assert!(!argv.contains(&"o4-mini".to_string()));
    }

    // ── thread_start_params ──────────────────────────────────────────────────

    #[test]
    fn thread_start_params_with_model() {
        let params = thread_start_params("/proj", Some("o4-mini"), "");
        assert_eq!(params["cwd"], "/proj");
        assert_eq!(params["model"], "o4-mini");
        assert_eq!(params["approvalPolicy"], "on-request");
        assert_eq!(params["sandbox"], "workspace-write");
    }

    #[test]
    fn thread_start_params_no_model() {
        let params = thread_start_params("/proj", None, "");
        assert!(params["model"].is_null(), "model must be absent when None");
        assert_eq!(params["cwd"], "/proj");
    }

    #[test]
    fn thread_start_params_any_mode_returns_default() {
        // All mode ids map to the same default (codex modes not yet discoverable).
        let params = thread_start_params("/proj", None, "plan");
        assert_eq!(params["approvalPolicy"], "on-request");
    }

    // ── encode_initialize ────────────────────────────────────────────────────

    #[test]
    fn encode_initialize_shape() {
        let json = encode_initialize("wasit-codex", "0.1.0");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["method"], "initialize");
        assert_eq!(v["params"]["clientInfo"]["name"], "wasit-codex");
    }

    // ── encode_thread_start ──────────────────────────────────────────────────

    #[test]
    fn encode_thread_start_shape() {
        let json = encode_thread_start(2, "/proj", Some("o4-mini"), "");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 2);
        assert_eq!(v["method"], "thread/start");
        assert_eq!(v["params"]["cwd"], "/proj");
        assert_eq!(v["params"]["model"], "o4-mini");
    }

    // ── encode_thread_resume ─────────────────────────────────────────────────

    #[test]
    fn encode_thread_resume_shape() {
        let json = encode_thread_resume(2, "thread-abc", "/proj");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "thread/resume");
        assert_eq!(v["params"]["threadId"], "thread-abc");
        assert_eq!(v["params"]["cwd"], "/proj");
    }

    // ── encode_turn_start ────────────────────────────────────────────────────

    #[test]
    fn encode_turn_start_shape() {
        let json = encode_turn_start(3, "t-1", "hello world");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "turn/start");
        assert_eq!(v["params"]["threadId"], "t-1");
        assert_eq!(v["params"]["input"][0]["type"], "text");
        assert_eq!(v["params"]["input"][0]["text"], "hello world");
    }

    // ── encode_turn_interrupt ────────────────────────────────────────────────

    #[test]
    fn encode_turn_interrupt_shape() {
        let json = encode_turn_interrupt(4, "t-1", "turn-99");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["method"], "turn/interrupt");
        assert_eq!(v["params"]["threadId"], "t-1");
        assert_eq!(v["params"]["turnId"], "turn-99");
    }

    // ── on_message: thread/started → SessionInit ─────────────────────────────

    #[test]
    fn on_message_thread_started() {
        // Real codex wire line (from live capture / Wasl tests).
        let line = r#"{"method":"thread/started","params":{"thread":{"id":"thread-abc-123","sessionId":"sess-xyz"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::SessionInit { native_session_id, .. } => {
                assert_eq!(native_session_id, Some("thread-abc-123".to_string()));
            }
            other => panic!("expected SessionInit, got {other:?}"),
        }
    }

    #[test]
    fn on_message_thread_started_no_thread_id() {
        let line = r#"{"method":"thread/started","params":{}}"#;
        let events = parse_all(line);
        // Still emits SessionInit but with None native_session_id.
        assert_eq!(events.len(), 1);
        if let AgentEvent::SessionInit { native_session_id, .. } = &events[0] {
            assert!(native_session_id.is_none());
        } else {
            panic!("expected SessionInit");
        }
    }

    // ── on_message: turn/started → TurnStarted ───────────────────────────────

    #[test]
    fn on_message_turn_started() {
        let line = r#"{"method":"turn/started","params":{"turn":{"id":"turn-001","status":"inProgress"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnStarted { native_turn_id } => {
                assert_eq!(native_turn_id, Some("turn-001".to_string()));
            }
            other => panic!("expected TurnStarted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_turn_started_sets_active_turn_id() {
        let mut p = MessageParser::new();
        let line = r#"{"method":"turn/started","params":{"turn":{"id":"turn-007"}}}"#;
        p.on_message(line);
        assert_eq!(p.active_turn_id(), Some("turn-007"));
    }

    // ── on_message: item/agentMessage/delta → AssistantDelta ─────────────────

    #[test]
    fn on_message_agent_message_delta() {
        let line = r#"{"method":"item/agentMessage/delta","params":{"itemId":"item-1","delta":"Hello, world!"}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::AssistantDelta { text, message_id } => {
                assert_eq!(text, "Hello, world!");
                assert!(message_id.is_none(), "codex does not provide message_id");
            }
            other => panic!("expected AssistantDelta, got {other:?}"),
        }
    }

    #[test]
    fn on_message_agent_message_delta_empty_skipped() {
        let line = r#"{"method":"item/agentMessage/delta","params":{"delta":""}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "empty delta must be skipped");
    }

    // ── on_message: item/started commandExecution → ToolCallStarted ──────────

    #[test]
    fn on_message_item_started_command() {
        let line = r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_1","command":"echo hello","cwd":"/proj"}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                input,
                is_subagent,
                subagent_model,
            } => {
                assert_eq!(name, "echo hello");
                assert_eq!(call_id, "call_1");
                assert!(input.is_null(), "codex: input is always null");
                assert!(!is_subagent);
                assert!(subagent_model.is_none());
            }
            other => panic!("expected ToolCallStarted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_item_started_non_command_skipped() {
        // Non-commandExecution items are skipped.
        let line = r#"{"method":"item/started","params":{"item":{"type":"agentMessage","id":"item-2"}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "non-commandExecution item/started must be skipped");
    }

    // ── on_message: item/completed → ToolCallFinished ────────────────────────

    #[test]
    fn on_message_item_completed_correlated() {
        let mut p = MessageParser::new();
        let started = r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_1","command":"ls -la"}}}"#;
        p.on_message(started);

        let completed = r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_1","exitCode":0}}}"#;
        let events = p.on_message(completed);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolCallFinished { call_id, name, ok, result } => {
                assert_eq!(call_id, "call_1");
                assert_eq!(name, "ls -la");
                assert!(ok, "exit code 0 → ok=true");
                assert!(result.is_none(), "codex: no result content");
            }
            other => panic!("expected ToolCallFinished, got {other:?}"),
        }
    }

    #[test]
    fn on_message_item_completed_exit_nonzero() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_2","command":"rm -rf /"}}}"#);
        let events = p.on_message(r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_2","exitCode":1}}}"#);
        assert_eq!(events.len(), 1);
        if let AgentEvent::ToolCallFinished { ok, .. } = &events[0] {
            assert!(!ok, "nonzero exit → ok=false");
        } else {
            panic!("expected ToolCallFinished");
        }
    }

    #[test]
    fn on_message_item_completed_unknown_id_skipped() {
        let mut p = MessageParser::new();
        let events = p.on_message(r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"unknown","exitCode":0}}}"#);
        assert!(events.is_empty(), "unknown item_id: nothing emitted");
    }

    #[test]
    fn on_message_item_completed_non_command_skipped() {
        let mut p = MessageParser::new();
        let events = p.on_message(r#"{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"item-5"}}}"#);
        assert!(events.is_empty());
    }

    #[test]
    fn tool_tracker_clears_on_turn_completed() {
        let mut p = MessageParser::new();
        // Start a command but never complete it.
        p.on_message(r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_x","command":"sleep 30"}}}"#);
        // Turn completes (interrupted).
        p.on_message(r#"{"method":"turn/completed","params":{"turn":{"id":"t-1","status":"interrupted"}}}"#);
        // Late item/completed should be ignored.
        let events = p.on_message(r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_x","exitCode":0}}}"#);
        assert!(events.is_empty(), "stale item_id must not survive past turn/completed");
    }

    // ── on_message: turn/completed → TurnCompleted ───────────────────────────

    #[test]
    fn on_message_turn_completed_success() {
        // Real codex wire line shape.
        let line = r#"{"method":"turn/completed","params":{"threadId":"t-1","turn":{"id":"turn-001","items":[],"status":"completed","error":null}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns,
                usage,
                ..
            } => {
                assert!(end_reason.is_none(), "completed → no end_reason");
                assert!(!was_interrupted);
                assert_eq!(num_turns, 0, "codex does not report num_turns");
                // Usage is always zero (gap).
                assert_eq!(usage.input_tokens, 0);
                assert_eq!(usage.cost_usd, 0.0);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_turn_completed_failed() {
        let line = r#"{"method":"turn/completed","params":{"turn":{"id":"t1","status":"failed","error":{"message":"exec failed"}}}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, reply, .. } => {
                assert_eq!(end_reason, Some(EndReason::ExecutionError));
                assert_eq!(reply.as_deref(), Some("exec failed"));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn on_message_turn_completed_interrupted() {
        let line = r#"{"method":"turn/completed","params":{"turn":{"id":"t1","status":"interrupted"}}}"#;
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
    fn on_message_turn_completed_clears_active_turn_id() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"method":"turn/started","params":{"turn":{"id":"t-007"}}}"#);
        assert_eq!(p.active_turn_id(), Some("t-007"));
        p.on_message(r#"{"method":"turn/completed","params":{"turn":{"id":"t-007","status":"completed"}}}"#);
        assert!(p.active_turn_id().is_none());
    }

    // ── on_message: JSON-RPC error response → TurnCompleted(ExecutionError) ───

    #[test]
    fn on_message_jsonrpc_error_response() {
        let line = r#"{"id":3,"error":{"code":-32602,"message":"invalid params"}}"#;
        let ev = parse_one(line);
        match ev {
            AgentEvent::TurnCompleted { end_reason, reply, .. } => {
                assert_eq!(end_reason, Some(EndReason::ExecutionError));
                assert_eq!(reply.as_deref(), Some("invalid params"));
            }
            other => panic!("expected TurnCompleted(ExecutionError), got {other:?}"),
        }
    }

    // ── on_message: gate (server-initiated request) ───────────────────────────

    #[test]
    fn on_message_command_approval_gate() {
        // Real codex app-server gate shape.
        let line = r#"{"jsonrpc":"2.0","id":5,"method":"item/commandExecution/requestApproval","params":{"itemId":"item-exec-1","command":"rm -rf /tmp/junk","cwd":"/proj","reason":"clean up"}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    command,
                    cwd,
                    is_elicitation,
                    ..
                } => {
                    // routing_id = itemId
                    assert_eq!(request_id, "item-exec-1");
                    assert_eq!(tool_name, "shell");
                    assert_eq!(command, "rm -rf /tmp/junk");
                    assert_eq!(cwd, "/proj");
                    assert!(!is_elicitation);
                }
                other => panic!("expected Permission, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
        // Gate is now pending.
        assert!(p.pending_gate("item-exec-1").is_some());
    }

    #[test]
    fn on_message_file_change_approval_gate() {
        let line = r#"{"id":6,"method":"item/fileChange/requestApproval","params":{"changes":{"path":"/proj/foo.rs"}}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        if let AgentEvent::GateRequested { gate: GateRequest::Permission { tool_name, .. } } =
            &events[0]
        {
            assert_eq!(tool_name, "apply_patch");
        } else {
            panic!("expected Permission gate");
        }
    }

    #[test]
    fn on_message_legacy_exec_approval() {
        let line = r#"{"id":7,"method":"execCommandApproval","params":{"command":"echo hi","cwd":"/"}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        if let AgentEvent::GateRequested { gate: GateRequest::Permission { tool_name, command, .. } } =
            &events[0]
        {
            assert_eq!(tool_name, "shell");
            assert_eq!(command, "echo hi");
        } else {
            panic!("expected Permission gate");
        }
    }

    #[test]
    fn on_message_user_input_gate() {
        let line = r#"{"id":8,"method":"item/tool/requestUserInput","params":{"questions":[{"id":"q1","question":"Which approach?","header":"","options":[{"label":"A"},{"label":"B"}]}]}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Question { request_id, questions } => {
                    assert!(!request_id.is_empty());
                    assert_eq!(questions.len(), 1);
                    assert_eq!(questions[0].question, "Which approach?");
                    assert_eq!(questions[0].options.len(), 2);
                    assert_eq!(questions[0].options[0].label, "A");
                }
                other => panic!("expected Question, got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    #[test]
    fn on_message_elicitation_gate() {
        let line = r#"{"id":9,"method":"mcpServer/elicitation/request","params":{"mode":"hares","message":"Grant access?","requestedSchema":{"properties":{"grant":{"enum":["once","session","deny"],"enumNames":["Once","Session","Deny"]}}}}}"#;
        let mut p = MessageParser::new();
        let events = p.on_message(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::GateRequested { gate } => match gate {
                GateRequest::Permission {
                    is_elicitation,
                    elicitation_choices,
                    tool_name,
                    ..
                } => {
                    assert!(is_elicitation);
                    assert_eq!(tool_name, "hares");
                    assert_eq!(elicitation_choices.len(), 3);
                    assert_eq!(elicitation_choices[0].value, "once");
                    assert_eq!(elicitation_choices[0].label, "Once");
                    assert_eq!(elicitation_choices[0].key, "grant");
                }
                other => panic!("expected Permission(elicitation), got {other:?}"),
            },
            other => panic!("expected GateRequested, got {other:?}"),
        }
    }

    #[test]
    fn on_message_gate_stored_in_pending_gates() {
        let mut p = MessageParser::new();
        p.on_message(r#"{"id":5,"method":"item/commandExecution/requestApproval","params":{"itemId":"item-x","command":"ls"}}"#);
        assert!(p.pending_gate("item-x").is_some());
        p.remove_gate("item-x");
        assert!(p.pending_gate("item-x").is_none());
    }

    // ── gate-response encoders ───────────────────────────────────────────────

    #[test]
    fn encode_gate_approve_command() {
        let raw_id = serde_json::json!(5);
        let json = encode_gate_approve(&raw_id, "item/commandExecution/requestApproval", false);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 5);
        assert_eq!(v["result"]["decision"], "accept");
    }

    #[test]
    fn encode_gate_approve_for_session() {
        let raw_id = serde_json::json!(5);
        let json = encode_gate_approve(&raw_id, "item/commandExecution/requestApproval", true);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["decision"], "acceptForSession");
    }

    #[test]
    fn encode_gate_deny_command() {
        let raw_id = serde_json::json!(5);
        let json = encode_gate_deny(&raw_id, "item/commandExecution/requestApproval");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["decision"], "decline");
    }

    #[test]
    fn encode_gate_approve_legacy() {
        let raw_id = serde_json::json!(7);
        let json = encode_gate_approve(&raw_id, "execCommandApproval", false);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["decision"], "approved");
    }

    #[test]
    fn encode_gate_deny_legacy() {
        let raw_id = serde_json::json!(7);
        let json = encode_gate_deny(&raw_id, "execCommandApproval");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["decision"], "denied");
    }

    #[test]
    fn encode_elicitation_accept_with_content() {
        let raw_id = serde_json::json!(9);
        let content = serde_json::json!({"grant": "session"});
        let json = encode_elicitation_accept(&raw_id, Some(content));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["action"], "accept");
        assert_eq!(v["result"]["content"]["grant"], "session");
    }

    #[test]
    fn encode_elicitation_accept_no_content() {
        let raw_id = serde_json::json!(9);
        let json = encode_elicitation_accept(&raw_id, None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["action"], "accept");
        assert_eq!(v["result"]["content"], serde_json::json!({}));
    }

    #[test]
    fn encode_elicitation_decline_shape() {
        let raw_id = serde_json::json!(9);
        let json = encode_elicitation_decline(&raw_id);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["action"], "decline");
    }

    #[test]
    fn encode_permissions_deny() {
        let raw_id = serde_json::json!(10);
        let json = encode_gate_deny(&raw_id, "item/permissions/requestApproval");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["result"]["permissions"], serde_json::json!({}));
    }

    // ── codex_mode_settings ──────────────────────────────────────────────────

    #[test]
    fn codex_mode_settings_empty_mode() {
        let (policy, sandbox) = codex_mode_settings("");
        assert_eq!(policy, "on-request");
        assert_eq!(sandbox, "workspace-write");
    }

    #[test]
    fn codex_mode_settings_any_mode_returns_default() {
        let (policy, sandbox) = codex_mode_settings("plan");
        assert_eq!(policy, "on-request");
        assert_eq!(sandbox, "workspace-write");
    }

    // ── driver capabilities ──────────────────────────────────────────────────

    #[test]
    fn driver_capabilities_honest() {
        let driver = CodexDriver::from_path();
        let caps = driver.capabilities();
        assert!(caps.interruptible, "codex interrupt verified live");
        assert!(!caps.model_switchable, "codex: no live model change");
        assert!(!caps.mode_switchable, "codex: no mode protocol");
        assert!(caps.persistent_conversation);
        assert!(caps.resumable, "thread/resume verified live");
        assert!(caps.streaming_output, "item/agentMessage/delta");
        assert!(!caps.reports_usage, "codex does not report usage — honest gap");
        assert!(caps.supports_gates, "REAL gates ported from Marsad");
    }

    // ── on_message: misc (empty, invalid, ignored) ───────────────────────────

    #[test]
    fn on_message_empty_string() {
        let events = parse_all("");
        assert!(events.is_empty());
    }

    #[test]
    fn on_message_whitespace_only() {
        let events = parse_all("   ");
        assert!(events.is_empty());
    }

    #[test]
    fn on_message_invalid_json() {
        let events = parse_all("not json {{{");
        assert!(events.is_empty());
    }

    #[test]
    fn on_message_response_ack_ignored() {
        // A simple ack (response to our turn/start) → no event.
        let line = r#"{"id":3,"result":{"turn":{"id":"t-1","status":"inProgress"}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "turn/start ack must be silently ignored");
    }

    #[test]
    fn on_message_thread_start_response_ignored() {
        let line = r#"{"id":2,"result":{"thread":{"id":"thread-abc-123","sessionId":"sess-xyz"}}}"#;
        let events = parse_all(line);
        assert!(events.is_empty(), "thread/start response must be silently ignored");
    }

    #[test]
    fn on_message_unknown_server_request_emits_unknown() {
        // A server request with an unrecognized method → Unknown event (not hang).
        let line = r#"{"id":99,"method":"futureGateMethod/v2","params":{}}"#;
        let events = parse_all(line);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], AgentEvent::Unknown { .. }));
    }

    #[test]
    fn on_message_compute_gate_routing_id_prefers_item_id() {
        let params = serde_json::json!({"itemId":"my-item-id","command":"ls"});
        let routing = compute_gate_routing_id("42", &params);
        assert_eq!(routing, "my-item-id");
    }

    #[test]
    fn on_message_compute_gate_routing_id_fallback_to_rpc_id() {
        let params = serde_json::json!({"command":"ls"});
        let routing = compute_gate_routing_id("42", &params);
        assert_eq!(routing, "codex:42");
    }

    // ── multi-turn: active turn tracking across turns ─────────────────────────

    #[test]
    fn active_turn_id_tracks_across_turns() {
        let mut p = MessageParser::new();
        assert!(p.active_turn_id().is_none());

        p.on_message(r#"{"method":"turn/started","params":{"turn":{"id":"t-1"}}}"#);
        assert_eq!(p.active_turn_id(), Some("t-1"));

        p.on_message(r#"{"method":"turn/completed","params":{"turn":{"id":"t-1","status":"completed"}}}"#);
        assert!(p.active_turn_id().is_none());

        p.on_message(r#"{"method":"turn/started","params":{"turn":{"id":"t-2"}}}"#);
        assert_eq!(p.active_turn_id(), Some("t-2"));
    }
}
