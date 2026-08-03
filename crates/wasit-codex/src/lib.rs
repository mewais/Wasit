//! wasit-codex — codex app-server JSON-RPC driver for Wasit.
//!
//! Implements `AgentDriver` + `Session` for `codex app-server --stdio`.
//!
//! ## Wire protocol overview
//!
//! Codex communicates over stdin/stdout as NDJSON JSON-RPC 2.0 lines.
//! Handshake: `initialize` → `thread/start` (or `thread/resume` for resume).
//! Per-turn: `turn/start` request → `turn/started` notification +
//! `item/agentMessage/delta` notifications + `turn/completed` notification.
//! Interrupt: `turn/interrupt` request.
//!
//! ## Capability report (honest)
//! - `interruptible`: true — `turn/interrupt` verified live.
//! - `model_switchable`: false — codex does not support live model changes.
//! - `mode_switchable`: false — codex does not report modes in its protocol.
//! - `persistent_conversation`: true — thread-based context persists.
//! - `resumable`: true — `thread/resume` verified live.
//! - `streaming_output`: true — `item/agentMessage/delta` notifications.
//! - `reports_usage`: false — codex does not report token/cost usage.
//! - `supports_gates`: false in practice.
//!   KNOWN GAP: codex app-server defines approval-request methods
//!   (`execCommandApproval`, `applyPatchApproval`, `item/*/requestApproval`,
//!   `item/tool/requestUserInput`, `mcpServer/elicitation/request`) that arrive
//!   as genuine JSON-RPC REQUESTS the driver must answer before codex can
//!   proceed. Under the default config (no explicit `approvalPolicy`/`sandbox`)
//!   these have not been observed in live captures. The driver detects them by
//!   shape (`method` + `id`, no `result`/`error`) and fails the turn immediately
//!   with a `TurnCompleted { end_reason: ExecutionError }` describing the
//!   unanswerable request — a SAFE floor, not a silent hang. The product
//!   decision of whether/how to surface these as `GateRequested` events (and
//!   what the answer wire shape is for each method) is deferred; `supports_gates`
//!   is `false` until that decision is made and verified.
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

/// The codex app-server JSON-RPC driver.
pub struct CodexDriver {
    bin: Option<std::path::PathBuf>,
}

impl CodexDriver {
    pub fn new(bin: impl Into<std::path::PathBuf>) -> Self {
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
            supports_gates: false,
        }
    }

    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError> {
        todo!("CodexDriver::spawn — Phase 2 logic")
    }
}

// ============================================================================
// Session impl (stub)
// ============================================================================

pub struct CodexSession {
    _phantom: std::marker::PhantomData<()>,
}

#[async_trait]
impl Session for CodexSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        todo!("CodexSession::next_event")
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        todo!("CodexSession::send_prompt")
    }

    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "answer_gate (codex gate answering not yet supported; see known gap in crate docs)".to_string(),
        })
    }

    async fn answer_question(
        &mut self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "answer_question (codex does not emit AskUserQuestion gates)".to_string(),
        })
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        todo!("CodexSession::interrupt — send turn/interrupt JSON-RPC")
    }

    async fn set_model(&mut self, model: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_model (codex does not support live model changes)".to_string(),
        })
    }

    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported {
            operation: "set_mode (codex does not expose a mode concept in its protocol)".to_string(),
        })
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        todo!("CodexSession::shutdown")
    }
}
