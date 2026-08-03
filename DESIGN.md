# Wasit — Design Document

Wasit is the single-source-of-truth CLI-driver library for claude, codex, and
opencode.  Its job: spawn the CLI, parse its native wire output into one rich
common event model (`AgentEvent`), and format gate/tool/interrupt responses
back.  It is drivers-only — no session lifecycle, no A2A routing, no UI.

---

## 1. Crate layout

```
Wasit/
  Cargo.toml              [workspace]
  crates/
    wasit-core/           Heart: AgentEvent, SpawnSpec, GateRequest/Decision,
                          Usage, SessionMode, AgentDriver trait, Session trait.
    wasit-claude/         impl AgentDriver for claude stream-json.
    wasit-codex/          impl AgentDriver for codex app-server JSON-RPC.
    wasit-opencode/       impl AgentDriver for opencode ACP JSON-RPC.
```

Consumer dependency trees:
- **Wasl** depends on `wasit-core` + the driver crate(s) it instantiates.
  Its `InteractiveProtocol` / `InteractiveTransport` become thin adapters
  wrapping a `Box<dyn Session>`.
- **Marsad** depends on `wasit-core` + the driver crate(s) it instantiates.
  Its `spawn_session_reader` loop becomes a thin adapter folding `AgentEvent`
  into `StreamMsg` (and the existing `SessionState` reducer).

---

## 2. The common event model — `AgentEvent`

`AgentEvent` is an internally-tagged serde enum (`#[serde(tag = "type",
rename_all = "snake_case")]`).  Every variant serializes to a JSON object with
a `"type"` field.  Naming closely follows Marsad's existing `StreamMsg`/
`SessionState` vocabulary so the mapping is obvious.

### Complete variant list with payload fields

| Variant | Key payload fields | Notes / judgment calls |
|---|---|---|
| `SessionInit` | `native_session_id?`, `model?`, `cwd?`, `tools[]`, `mcp_servers[]`, `slash_commands[]`, `permission_mode`, `claude_code_version`, `available_modes[]`, `current_mode` | claude populates `tools`/`mcp_servers`/`slash_commands`/`claude_code_version`; opencode gets `available_modes`/`current_mode` from `configOptions`; codex leaves most empty |
| `TurnStarted` | `native_turn_id?` | JUDGMENT: claude has no explicit "turn started" signal; driver synthesizes it on first assistant message — so it arrives after the first `AssistantDelta` for claude (unlike codex/opencode) |
| `TurnCompleted` | `end_reason?`, `was_interrupted`, `num_turns`, `usage`, `per_model{}`, `reply?` | `end_reason` = `None` for a normal success; codex: no usage (all zeros); opencode: no `per_model`; claude: `reply` present on success |
| `AssistantDelta` | `text`, `message_id?` | All three CLIs; message_id for dedup on reconnect |
| `ThinkingDelta` | `text`, `message_id?` | claude extended-thinking blocks; opencode `agent_thought_chunk` also mapped here |
| `ToolCallStarted` | `name`, `call_id`, `input`, `is_subagent`, `subagent_model?` | `is_subagent` = true when tool is claude `Task`/`Agent` or opencode `task`; `input` is null for codex (it only carries the command string as `name`) |
| `ToolCallUpdated` | `call_id`, `name`, `content[]`, `status?` | opencode only — intermediate `tool_call_update` with partial results |
| `ToolCallFinished` | `call_id`, `name`, `ok`, `result?` | claude: from `tool_result` blocks; codex: `item/completed` exit code; opencode: terminal `tool_call_update` |
| `GateRequested` | `gate: GateRequest` | claude: from `control_request` (can_use_tool / elicitation / AskUserQuestion / ExitPlanMode); opencode: from `session/request_permission`; codex: see honest gap §4 |
| `UsageUpdate` | `usage`, `context_pct?` | claude: per-message incremental; opencode: periodic cumulative; codex: not emitted |
| `PlanUpdated` | `entries[]` | opencode plan-mode only; each event is a full snapshot |
| `ModesUpdated` | `available_modes[]`, `current_mode?` | claude: `system/status` mid-session + version-derived at init; opencode: `current_mode_update`; codex: not emitted |
| `ModelChanged` | `model` | opencode `session/set_config_option` result for `model`; claude/codex: not emitted |
| `SlashCommandsUpdated` | `commands[]` | opencode `available_commands_update`; also emitted at init for claude's slash_commands list |
| `Compaction` | `info: CompactionInfo` | claude `system/compact_boundary`; opencode `context_compaction` update type |
| `RateLimitNotice` | `info: RateLimitInfo` | claude `rate_limit_event` only |
| `ProcessExited` | `code?` | All three CLIs — terminal event |
| `Unknown` | `raw: Value` | Forward-compat: recognized outer type but unknown subtype — preserves observability without crashing |

### Supporting types (all in `wasit-core`)

- **`SpawnSpec`** — union of Marsad `LaunchSpec` + Wasl `SpawnContext`: `cwd?`, `model?`, `mode`, `bin?`, `env[]`, `setup?`, `disallowed_tools[]`, `mcp_config?`, `resume: ResumeMode`, `first_prompt?`
- **`Usage`** — `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_creation_tokens`, `cost_usd`
- **`ModelUsage`** — per-model row (same fields + `estimated` flag)
- **`SessionMode`** — `id`, `label`, `description` (always verbatim from the CLI)
- **`McpServerInfo`**, **`SlashCommandInfo`** — verbatim from `system/init`
- **`PlanEntry`** — `content`, `priority`
- **`CompactionInfo`** — `trigger`, `pre_tokens`, `post_tokens`, `cumulative_dropped_tokens`, `duration_ms`
- **`RateLimitInfo`** — `status`, `resets_at`, `rate_limit_type`, `overage_status`, `is_using_overage`
- **`SubagentInfo`** — `name`, `call_id`, `model?`, `status: SubagentStatus` (not yet a variant; consumers derive subagent lifecycle from `ToolCallStarted { is_subagent: true }` + `ToolCallFinished`)
- **`GateRequest`** — internally-tagged on `gate_type`: `permission { request_id, tool_name, command, cwd, explanation, is_elicitation, elicitation_choices[], permission_suggestions[], raw_input }`, `plan { request_id, summary }`, `question { request_id, questions[] }`
- **`GateDecision`** — internally-tagged on `action`: `approve { updated_input?, permission_updates?, elicitation_content? }`, `deny { reason? }`
- **`QuestionAnswer`** — `answers: HashMap<String, String>`
- **`ElicitationChoice`** — `key`, `value`, `label`
- **`QuestionPrompt`** / **`QuestionOption`**
- **`EndReason`** — `cancelled`, `max_turns`, `execution_error`, `compaction_needed`
- **`DriverCapabilities`** — 8 independent honest booleans (see §3)

---

## 3. `AgentDriver` and `Session` trait signatures

```rust
// Factory
#[async_trait]
pub trait AgentDriver: Send + Sync {
    fn capabilities(&self) -> DriverCapabilities;
    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError>;
}

// Live session
#[async_trait]
pub trait Session: Send {
    async fn next_event(&mut self) -> Option<AgentEvent>;
    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError>;
    async fn answer_gate(&mut self, request_id: &str, decision: GateDecision)
        -> Result<(), DriverError>;
    async fn answer_question(&mut self, request_id: &str, answer: QuestionAnswer)
        -> Result<(), DriverError>;
    async fn interrupt(&mut self) -> Result<(), DriverError>;
    async fn set_model(&mut self, model: &str) -> Result<(), DriverError>;
    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError>;
    async fn shutdown(&mut self) -> Result<(), DriverError>;
}
```

`DriverCapabilities` (8 independent honest booleans):

| Field | claude | codex | opencode |
|---|---|---|---|
| `interruptible` | false* | true | true |
| `model_switchable` | false | false | true |
| `mode_switchable` | true | false | true |
| `persistent_conversation` | true | true | true |
| `resumable` | true | true | true |
| `streaming_output` | false** | true | true |
| `reports_usage` | true | false | true |
| `supports_gates` | true | false*** | true |

*claude's `interrupt` control_request exists but live round-trip not yet
verified in this driver (Wasl's existing claude driver also reports `false`).

**claude delivers whole assistant messages per turn (not token-level SSE
deltas). The driver will emit one `AssistantDelta` per `text` content block,
which may be a full response at once.

***codex has unanswerable approval-request methods under stricter configs; see
§4.

---

## 4. Per-CLI wire mapping and honest gaps

### 4.1 claude (stream-json duplex)

| Wire line | `AgentEvent` |
|---|---|
| `system/init` | `SessionInit` (tools, mcp_servers, slash_commands, model, cwd, session_id, claude_code_version; available_modes derived from version) |
| `system/status { permissionMode }` | `ModesUpdated { current_mode }` |
| `system/compact_boundary` | `Compaction` |
| `system/plan_update` | NOT from claude — opencode only |
| `system/mode_update` | `ModesUpdated` |
| `system/modes_update` | `ModesUpdated` |
| `system/slash_update` | `SlashCommandsUpdated` |
| `system/usage_update` | `UsageUpdate` |
| `assistant { message { content: [text] } }` | `AssistantDelta` |
| `assistant { message { content: [thinking] } }` | `ThinkingDelta` |
| `assistant { message { content: [tool_use] } }` | `ToolCallStarted` |
| `user { message { content: [tool_result] } }` | `ToolCallFinished` (stateful tracker: `tool_use_id` → name) |
| `result { is_error: false }` | `TurnCompleted { end_reason: None }` |
| `result { subtype: error_max_turns }` | `TurnCompleted { end_reason: MaxTurns }` |
| `result { is_error: true }` | `TurnCompleted { end_reason: ExecutionError }` (or `Cancelled` when `was_interrupted`) |
| `control_request { can_use_tool }` | `GateRequested { gate: Permission }` |
| `control_request { elicitation }` | `GateRequested { gate: Permission { is_elicitation: true } }` |
| `control_request { AskUserQuestion tool }` | `GateRequested { gate: Question }` |
| `control_request { ExitPlanMode tool }` | `GateRequested { gate: Plan }` |
| `rate_limit_event` | `RateLimitNotice` |
| unknown `system/*` subtypes | `Unknown` |

**Gaps vs. richness bar:**
- No native per-turn streaming deltas (full text block per turn).
- `TurnStarted` is synthesized on first `AssistantDelta` — arrives after it.
- No `ModelChanged` event (model only reported at init).
- `num_turns` comes from `result.num_turns`; accurate.
- `per_model` breakdown from `result.modelUsage` (newer claude only).

### 4.2 codex (app-server JSON-RPC)

| Wire message | `AgentEvent` |
|---|---|
| `thread/started` notification | `SessionInit { native_session_id }` |
| `turn/started` notification | `TurnStarted { native_turn_id }` |
| `item/agentMessage/delta { delta }` | `AssistantDelta` |
| `item/started { type: commandExecution, command }` | `ToolCallStarted` |
| `item/completed { type: commandExecution, exitCode }` | `ToolCallFinished` |
| `turn/completed { status: completed }` | `TurnCompleted { end_reason: None }` |
| `turn/completed { status: failed }` | `TurnCompleted { end_reason: ExecutionError }` |
| `turn/completed { status: interrupted }` | `TurnCompleted { end_reason: Cancelled }` |
| JSON-RPC error response (correlated to our request) | `TurnCompleted { end_reason: ExecutionError }` |
| `ProcessExited` (child exits) | `ProcessExited` |

**Honest gaps:**
- No token/cost usage (`reports_usage: false`). `TurnCompleted.usage` fields
  are all zero.
- No mode support (`mode_switchable: false`). `set_mode` returns `Unsupported`.
- No model switching (`model_switchable: false`). `set_model` returns
  `Unsupported`.
- No `SessionMode` list. `ModesUpdated` is never emitted.
- **Gate gap (known, documented):** codex app-server defines server-initiated
  approval-request methods (`execCommandApproval`, `applyPatchApproval`,
  `item/*/requestApproval`, `item/tool/requestUserInput`,
  `mcpServer/elicitation/request`) that arrive as JSON-RPC REQUESTs (carrying
  both `method` and `id`). The driver must answer them before codex can
  proceed. Under the default config (no explicit `approvalPolicy`/`sandbox`)
  these have not been observed in live captures. The driver detects them by
  shape and fails the turn immediately with `TurnCompleted { end_reason:
  ExecutionError }` naming the unanswerable method — a safe floor, not a
  silent hang. `supports_gates: false` until the product decision of
  whether/how to surface these as `GateRequested` events is made and
  wire-verified. JUDGMENT CALL: this gap is carried forward from Wasl's
  existing `wasit-codex` driver.
- No `ThinkingDelta` (codex does not emit reasoning blocks in the wire
  protocol).
- `TurnStarted` carries `native_turn_id` from `turn/started.params.turn.id`.
- `ToolCallStarted.name` = the shell command string (codex's "tool" is a
  command execution); `input` is always `null`.

### 4.3 opencode (ACP JSON-RPC)

| Wire message | `AgentEvent` |
|---|---|
| `initialize` + `session/new` (or `session/load`) results | `SessionInit` (model + modes from `configOptions`, session_id from result) |
| `agent_message_chunk { content: { type: text } }` | `AssistantDelta` |
| `agent_thought_chunk { content: { text } }` | `ThinkingDelta` |
| `tool_call { title, toolCallId, rawInput }` | `ToolCallStarted` |
| `tool_call_update` (intermediate status) | `ToolCallUpdated` |
| `tool_call_update` (terminal status completed/failed) | `ToolCallFinished` |
| `task` tool_call (subagent) | `ToolCallStarted { is_subagent: true }` |
| `plan { plan.entries }` | `PlanUpdated` |
| `available_commands_update` | `SlashCommandsUpdated` |
| `current_mode_update { currentModeId }` | `ModesUpdated { current_mode }` |
| `usage_update { cost, size, used }` | `UsageUpdate { context_pct }` |
| `context_compaction` | `Compaction` |
| `session/prompt` result | `TurnCompleted` (stopReason → end_reason) |
| `session/request_permission` JSON-RPC request | `GateRequested { gate: Permission }` |
| `session/set_config_option model` result | `ModelChanged` |
| `session/set_config_option mode` result | `ModesUpdated` |
| `ProcessExited` (child exits) | `ProcessExited` |

**Honest gaps:**
- No `per_model` breakdown (`TurnCompleted.per_model` is empty).
- No `TurnStarted` — there is no native "turn accepted" notification before
  the first delta.
- No `result.num_turns` — always 1 in `TurnCompleted`.
- No explicit `AskUserQuestion` gate — opencode's permission requests with
  options are mapped to `GateRequest::Permission { elicitation_choices }`.
- No `ExitPlanMode`/`plan` gate — opencode's plan mode publishes a live
  `PlanUpdated` feed but never blocks on an explicit human-approval gate.
- No `rate_limit_event`.
- No `system/compact_boundary` metadata (opencode emits only a bare
  `context_compaction` type; `CompactionInfo` fields are all zero/empty).
- `ThinkingDelta` comes from `agent_thought_chunk.content.text` (not a
  top-level `thinking` field as claude uses).
- JUDGMENT CALL: `answer_question` returns `Unsupported` for opencode; all
  multi-option gates route through `answer_gate` with `elicitation_choices`.

---

## 5. Migration note

**Wasl:** Wasl's current drivers (`claude_stream.rs`, `codex_appserver.rs`,
`opencode_acp.rs`) implement `InteractiveProtocol` / `InteractiveTransport`
and emit the lean `ProtocolEvent` (9 variants). The migration path is:

1. Replace the driver bodies with a thin adapter wrapping a `Box<dyn Session>`
   from the corresponding Wasit crate.
2. Map `AgentEvent` → `ProtocolEvent` in `next_event()`:
   - `SessionInit` → `ProtocolEvent::SessionReady { native }`
   - `TurnStarted` → `ProtocolEvent::TurnAccepted { native_turn }`
   - `AssistantDelta` → `ProtocolEvent::AssistantDelta { text }`
   - `ToolCallStarted` → `ProtocolEvent::ToolStarted { name }`
   - `ToolCallFinished` → `ProtocolEvent::ToolFinished { name, ok }`
   - `GateRequested` → `ProtocolEvent::NativeGateRequested { detail }` (summary + preview extracted from the `GateRequest`)
   - `TurnCompleted { end_reason: None }` → `ProtocolEvent::TurnCompleted { reply }`
   - `TurnCompleted { end_reason: Some(_) }` → `ProtocolEvent::TurnFailed { error }`
   - `ProcessExited` → `ProtocolEvent::ProcessExited { code }`
   - Everything else → drop (the lean model intentionally ignores cost, plan, compaction, etc.)
3. Grow `ProtocolEvent` where Wasl needs richer data (e.g. add a `NativeGateDetail` that carries the full `GateRequest` for Wasl's native-gate resolution path, without polluting the public `TurnEvent`).

No change to Wasl's actor, scheduler, or A2A plumbing is required in this migration.

**Marsad:** Marsad's current `spawn_session_reader` / `reader.rs` loop parses
NDJSON lines directly and calls `parse_stream_line` → `reduce`. The migration
path is:

1. Instantiate the Wasit driver at session launch (replacing `claude_argv` +
   child-process wiring).
2. In the reader loop, call `session.next_event()` and map `AgentEvent` →
   `StreamMsg` (the existing serde types Marsad's reducer already understands):
   - `SessionInit` → `StreamMsg::SystemInit`
   - `AssistantDelta` → `StreamMsg::Assistant` (accumulate deltas per `message_id`)
   - `ThinkingDelta` → `StreamMsg::Assistant` (with `Thinking` content block)
   - `ToolCallStarted` → `StreamMsg::Assistant` (with `ToolUse` content block)
   - `ToolCallFinished` → `StreamMsg::User` (with `ToolResult` content block)
   - `GateRequested` → `StreamMsg::ControlRequest`
   - `TurnCompleted` → `StreamMsg::Result`
   - `PlanUpdated` → `StreamMsg::PlanUpdate`
   - `ModesUpdated` → `StreamMsg::ModesUpdate`
   - `ModelChanged` → `StreamMsg::ModesUpdate { current_mode: Some(model) }` (or a new `StreamMsg::ModelUpdate`)
   - `Compaction` → `StreamMsg::CompactBoundary`
   - `RateLimitNotice` → `StreamMsg::RateLimitEvent`
   - `UsageUpdate` → `StreamMsg::UsageUpdate`
   - `SlashCommandsUpdated` → `StreamMsg::SlashUpdate`
   - `ProcessExited` → signal the session is dead (existing liveness path)
3. Gate responses: replace direct stdin writes with `session.answer_gate()` /
   `session.answer_question()` / `session.interrupt()` / `session.set_mode()`.
4. Delete the per-driver parsing code from `reader.rs`, `codex.rs`,
   `codex_translate.rs`, `acp_translate.rs` — they are replaced by Wasit.

The existing `marsad_proto::StreamMsg` reducer (`marsad_events::reduce`) is
unchanged — it remains the authoritative Marsad event folder.

---

## 6. Judgment calls and open questions (flag for review)

1. **`TurnStarted` ordering for claude.** Claude has no native "turn started"
   signal. The driver will synthesize it from the first `AssistantDelta`.
   This means `TurnStarted` arrives AFTER the first delta for claude but
   BEFORE it for codex/opencode. If Wasl's actor or Marsad's reducer needs a
   strict "no deltas before TurnStarted" invariant, the driver must buffer the
   first delta. Left as `todo!()` for Phase 2 — flag for architectural review.

2. **`interrupt` capability for claude.** Wasl's existing claude driver reports
   `interruptible: false` because live round-trip verification is incomplete.
   The control-protocol `interrupt` request exists and Marsad uses it, but
   Wasit carries the same conservative `false` until it is verified end-to-end
   in this new driver. The `interrupt()` method returns `Unsupported` for now.

3. **codex gate answering.** The codex app-server defines approval-request
   server-initiated JSON-RPC methods. Whether/how to surface these as
   `GateRequested { gate: Permission }` and what the answer wire shape is for
   each method is a product/security decision deferred to Phase 2. The current
   driver fails the turn immediately with `ExecutionError` when it sees such a
   request.

4. **`answer_question` for opencode.** Opencode's multi-option permission
   requests are currently mapped to `GateRequest::Permission {
   elicitation_choices }` and answered via `answer_gate`. If opencode later
   introduces a dedicated question-type gate (AskUserQuestion equivalent),
   `answer_question` becomes the right path. Left as `Unsupported` for now.

5. **`ModelChanged` vs `ModesUpdated` for opencode model updates.** When
   opencode's `set_config_option model` confirms, the new model is in the same
   `configOptions` result as the mode list. Both `ModelChanged` and
   `ModesUpdated` would fire. JUDGMENT CALL: emit `ModelChanged` separately so
   consumers that only care about the model don't need to inspect the modes
   update. Consumers that fold both get a duplicate-safe update.

6. **`ThinkingDelta` for opencode.** Opencode's `agent_thought_chunk` uses
   `content.text` (not a top-level `thinking` field as claude does). The
   driver reads `content.text` first and falls back to `thinking` for
   spec-compliant agents. This matches Marsad's existing `acp_translate.rs`
   behavior.

7. **Serde wire pins.** The `AgentEvent` enum uses
   `#[serde(tag = "type", rename_all = "snake_case")]` and supporting types use
   `#[serde(rename_all = "snake_case")]`. This matches Marsad's existing
   `StreamMsg` conventions. Consumers that need different wire shapes (e.g.
   Wasl's control API uses `snake_case` too) should find the mapping trivial.

8. **`Unknown` variant.** Included for forward compatibility (new CLI subtypes
   don't break parsing). The alternative — silently dropping — loses
   observability. Matches Marsad's `StreamMsg::Unknown`.
