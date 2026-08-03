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
//!   verification is incomplete; flagged false for honesty (Wasl's existing
//!   claude driver does the same).
//! - `model_switchable`: false — claude does not support live model changes.
//! - `mode_switchable`: true — `set_permission_mode` control_request works.
//! - `persistent_conversation`: true — context lives across turns.
//! - `resumable`: true — `--resume <session-id>`.
//! - `streaming_output`: false — claude delivers full assistant messages per
//!   turn, not token-level SSE deltas (confirmed: no `streaming_output` in
//!   Wasl's existing claude driver).
//! - `reports_usage`: true — per-message and result-level usage.
//! - `supports_gates`: true — can_use_tool / elicitation / question / plan.
//!
//! All bodies are `todo!()`.

#![forbid(unsafe_code)]
#![allow(dead_code, unused_variables)]

use async_trait::async_trait;
use wasit_core::{
    AgentDriver, AgentEvent, DriverCapabilities, DriverError, GateDecision, QuestionAnswer,
    Session, SpawnSpec,
};

// ============================================================================
// Public driver struct
// ============================================================================

/// The claude stream-json driver.
pub struct ClaudeDriver {
    /// Path to the `claude` executable. `None` = `"claude"` on PATH.
    bin: Option<std::path::PathBuf>,
}

impl ClaudeDriver {
    /// Create a driver that uses the given binary path.
    pub fn new(bin: impl Into<std::path::PathBuf>) -> Self {
        ClaudeDriver {
            bin: Some(bin.into()),
        }
    }

    /// Create a driver that resolves `claude` from PATH.
    pub fn from_path() -> Self {
        ClaudeDriver { bin: None }
    }

    fn effective_bin(&self) -> &str {
        self.bin
            .as_deref()
            .and_then(|p| p.to_str())
            .unwrap_or("claude")
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
        todo!("ClaudeDriver::spawn — Phase 2 logic")
    }
}

// ============================================================================
// Session impl (stub)
// ============================================================================

/// A live claude session (process + stdin/stdout pipes).
pub struct ClaudeSession {
    // Phase 2: child process, event channel, pending-gate state, etc.
    _phantom: std::marker::PhantomData<()>,
}

#[async_trait]
impl Session for ClaudeSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        todo!("ClaudeSession::next_event")
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        todo!("ClaudeSession::send_prompt")
    }

    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError> {
        todo!("ClaudeSession::answer_gate")
    }

    async fn answer_question(
        &mut self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<(), DriverError> {
        todo!("ClaudeSession::answer_question")
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "interrupt (not yet verified for claude in this driver)".to_string(),
        })
    }

    async fn set_model(&mut self, model: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_model (claude does not support live model changes)".to_string(),
        })
    }

    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError> {
        todo!("ClaudeSession::set_mode — send set_permission_mode control_request")
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        todo!("ClaudeSession::shutdown")
    }
}
