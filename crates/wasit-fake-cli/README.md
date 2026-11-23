# wasit-fake-cli

A scriptable fake of the real coding-agent CLIs. One binary, four personas, speaking the **real
wire protocols** on stdin/stdout, driven by a JSON scenario: deterministic, no network, no model.
Drivers and the runtime are tested against it (ARCHITECTURE.md §1 rule 10).

| Persona | Real CLI it imitates | Selected by |
|---|---|---|
| `claude` | `claude --print --input-format stream-json --output-format stream-json --permission-prompt-tool stdio` | `--input-format stream-json` |
| `codex` | `codex app-server --stdio` | `app-server` |
| `opencode` | `opencode acp` | `acp` |
| `kilo` | `kilo acp` | `acp` + `WASIT_FAKE_PERSONA=kilo` (or an executable name containing `kilo`) |

`WASIT_FAKE_PERSONA=claude|codex|opencode|kilo` overrides detection (needed for `--version` and
the discovery commands, which carry no protocol argv). The fake ignores flags it does not model,
but honours claude's `--model`, `--permission-mode`, `--resume`, `--session-id`, `--continue`,
`--name` and `--include-partial-messages`.

`WASIT_FAKE_SCENARIO=<path.json>` selects the scenario (default: an empty one, which echoes every
prompt). `WASIT_FAKE_TRANSCRIPT=<path>` appends every line in both directions as
`{"dir":"in"|"out","msg":…}`, so a test can assert what its driver sent.
`WASIT_FAKE_SCENARIO_JSON=<json>` passes the scenario inline (a fake on another machine).
`WASIT_FAKE_STATE_DIR=<dir>` is the fake's "agent storage" (see below); the default is a per-user
directory under the system temp directory, never the real CLI's home.
`WASIT_FAKE_CONSOLE_REPORT=<path>` appends `<pid> <none|hidden|visible> <args>`: whether the
process got a visible console window (Windows; the no-console-window tests).

## Using it from another crate

```toml
[dev-dependencies]
wasit-fake-cli = { path = "../wasit-fake-cli" }   # for the scenario builder and fake_cli_path()
```

`env!("CARGO_BIN_EXE_wasit-fake-cli")` only exists for this crate's own tests. Build the binary
first (`cargo build -p wasit-fake-cli`; it lands in `target/<profile>/wasit-fake-cli`) and ask
`wasit_fake_cli::fake_cli_path()`, which finds it relative to the running test executable (or at
`$WASIT_FAKE_CLI`). Then:

```rust
use wasit_fake_cli::scenario::{Scenario, Turn};
let scenario = Scenario::builder()
    .turn(Turn::builder().say("on it")
        .tool("Bash", serde_json::json!({"command": "touch x"})).needs_permission().done()
        .say("done"))
    .build();
scenario.write_to(dir.join("scenario.json"))?;
// spawn fake_cli_path() with WASIT_FAKE_SCENARIO=<that file>, WASIT_FAKE_PERSONA=claude,
// and the argv the driver would give the real CLI.
```

## Scenario format

```jsonc
{
  "session_id": "…",            // id of the first session (claude session_id, codex thread id, ACP sessionId)
  "version": "1.2.3",           // --version / initialize agentInfo (default: the recorded real version)
  "models": [{"id": "p/m", "name": "Display", "description": "…"}],
  "current_model": "p/m",
  "modes":  [{"id": "plan", "name": "Plan", "description": "…"}],   // ACP modes, codex collaboration modes
  "current_mode": "plan",
  "commands": [{"name": "review", "description": "…"}],            // slash commands / skills
  "sessions": [{"id": "ses_old", "title": "…", "cwd": "/w",        // exist before start: list / load / resume
                "history": [{"role": "user", "text": "…"}, {"role": "assistant", "text": "…"}]}],
  "echo_prefix": "echo: ",
  "refuse": [{"when": "regex on the prompt", "message": "…"}],     // codex: turn/start and turn/steer carrying a matching text get this error
  "turns": [ {"when": "regex on prompt (optional)", "steps": [ … ]} ],
  "startup": [ … ]              // steps run before any input: stderr / garbage / hang / exit
}
```

Turn selection per prompt: the first turn whose `when` regex matches (reusable); otherwise the next
unconsumed turn without `when`, in order; otherwise `echo_prefix + prompt`.

Steps (`{"type": "<name>", …}`):

| type | fields | effect |
|---|---|---|
| `say` | `text`, `chunks?` | assistant text, streamed in `chunks` deltas where the protocol streams |
| `think` | `text` | reasoning (claude `thinking` block, ACP `agent_thought_chunk`, codex `reasoning` item) |
| `tool` | `name`, `input`, `needs_permission`, `result_if_allowed`, `result_if_denied`, `title?` | a tool call; with `needs_permission` the fake sends the persona's real permission request and waits for the answer |
| `ask_question` | `question`, `header?`, `options: [{label, description}]`, `multi_select` | claude `AskUserQuestion`, ACP `session/request_permission` with the options, codex `item/tool/requestUserInput` |
| `plan` | `entries: [{content, status, priority}]`, `needs_approval` | ACP `plan` update, codex `turn/plan/updated`, claude `TodoWrite` (or an `ExitPlanMode` gate with `needs_approval`) |
| `usage` | `input_tokens`, `output_tokens`, `cached_tokens`, `thought_tokens`, `cost_usd`, `context_window` | the usage the turn-end message reports |
| `sleep_ms` | `ms` | model a long tool; an interrupt ends it early |
| `wait_for_interrupt` | | block until interrupted / cancelled |
| `fail` | `message` | the turn ends with the persona's real error shape |
| `exit` | `code` | the process dies mid-turn |
| `hang` | | stop answering anything (stdin is drained until EOF) |
| `garbage` | `line?` | one malformed stdout line |
| `stderr` | `text` | write to stderr |
| `background` | `kind` (`local_agent` \| `local_bash`), `description`, `subagent_type?`, `command?`, `after_ms`, `steps`, `status?`, `summary?`, `then`, `on_stop` | claude persona: start a task in the background, as the real CLI does (see below) |
| `mcp_call` | `server`, `tool`, `arguments`, `save_as?` | claude persona: start the stdio MCP server `server` from `--mcp-config`, call `tool`, show it as a tool call `mcp__<server>__<tool>`; `save_as` keeps the result (JSON when it parses) for `{{name.path.0.x}}` placeholders in later `say` texts and `mcp_call` arguments |

Background tasks (claude persona, modelled on claude 2.1.293: `reference/claude/background_bash`,
`background_agent`, `stop_task`, `agent_turn_interrupt`, `agent_turn_prompt`): a `background` step prints
the spawning tool call, `background_tasks_changed`, `task_started` (`is_backgrounded`) and the call's
result, and the turn goes on and ends. The task runs for `after_ms` while the session is idle: a
`local_agent` streams its own `steps` (assistant / user lines tagged `parent_tool_use_id`, `agent_id`,
`subagent_type`, `task_description`, with `task_progress` after each of its tool calls); then
`task_updated`, `task_notification`, `background_tasks_changed` and — when `then` is not empty — a turn
the agent starts by itself: `system/init`, no lifecycle frame, `then`'s steps, a `result` whose `origin`
is `{kind:"task-notification", producer:"session-task", runId}`. `stop_task {task_id}` ends the task
(`task_updated` killed, `task_notification` stopped, the new list, then the answer `{}`; any other id is
answered `{}` too, a missing one is refused); stopping a `local_agent` makes the agent run `on_stop`
as its own turn, stopping a `local_bash` starts none (both recorded). An `interrupt` while only tasks
run is answered and stops nothing. A prompt with a `uuid` read during a turn the agent started by itself
is taken into it at the next step boundary (`started`, then `completed` right before the `result`, which
lists it); one read during a turn of the client's waits for the next turn as before.

Protocol-level requests are answered without scenario help: initialize, session/thread creation,
load/resume/fork/close/list, set mode / model / config option, interrupt / cancel (turn ends as
interrupted), permission responses (continue the tool, or deny it). Closing stdin ends the process.
The codex persona also answers `turn/steer` as codex-cli 0.161.0 does (`reference/codex/steer.jsonl`):
`{"turnId"}` while a turn runs and `expectedTurnId` names it, otherwise the real errors (`no active
turn to steer`, `expected active turn id … but found …`), or the scenario's `refuse` error; an
accepted prompt is folded in when the turn ends (a `userMessage` item, then a reply `<echo_prefix><text>`
in the same turn; an interrupted or failed turn drops it). The engine reads input only while a turn
waits (`sleep_ms`, `wait_for_interrupt`, a gate), so a steer is seen only then.

Discovery commands (opencode / kilo persona): `--version`, `models [provider] [--verbose]`,
`session list [--format json]`, answered from `models` / `sessions` and the sessions earlier
processes stored (below). `session list` shows the sessions of the folder it runs in, newest first,
with the `id`/`title`/`updated`/`created`/`directory` fields the driver's `parse_sessions` reads; a
folder with none prints nothing at all (opencode 1.19.6, kilo 7.8.27). `--version` also works for
claude and codex.

## Agent storage: conversations outlive the process

A conversation can be resumed by the next process only if the CLI stored it, and each real CLI
stores at its own moment. The fake keeps files under `WASIT_FAKE_STATE_DIR` (`CLAUDE_CONFIG_DIR`
for claude's transcripts when it is set, which is where claude discovery reads them) and follows
what the real CLIs were observed to do. Scenario `sessions` count as conversations that already
exist.

* **claude** (2.1.293, real runs in `/tmp/marsad-lifecycle/live`; transcripts are
  `<config>/projects/<cwd with every non-alphanumeric character as "-">/<id>.jsonl`):
  * a conversation is stored with its first prompt (`system/init`), not before: `--session-id U`
    and a close before any prompt leaves no file (exit 0);
  * `--resume U` before that, or for an id never used: no `control_response`; one stdout line
    `{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":U,
    "errors":["No conversation found with session ID: U"],…}`, stderr `No conversation found with
    session ID: U`, exit 1;
  * `--resume U` finds U in whatever project it was stored (also from another directory);
  * `--session-id U` is refused once a transcript for U exists in the working directory's
    project (stderr `Error: Session ID U is already in use.`, exit 1, no stdout) and accepted in
    another directory; a non-UUID is refused (stderr `Error: Invalid session ID. Must be a valid
    UUID.`, exit 1);
  * `--name N` announces `{"type":"system","subtype":"session_title_changed","title":N,
    "session_id":…}` before the initialize response (also with no prompt), and the transcript
    starts with `custom-title` and `agent-name` records when it is first written; `--resume`
    announces the stored title again;
  * `--continue` in a directory with no conversation starts a new one without a word (E6 of the
    lifecycle investigation), else resumes the newest;
  * the transcript has the REAL FILE'S SHAPE, written by one module, `wasit_fake_cli::transcript`
    (the persona and tests share it; its module docs list every field). Taken from claude
    2.1.293's own files (a one-prompt stream-json run; interactive transcripts for structure only):
    `queue-operation` (enqueue, dequeue) before a prompt; `user` (`parentUuid` null for the first,
    `promptId`, `permissionMode`, `promptSource`, `turnOrigin`, `turnPosition`, `message.content` as
    an array of blocks) with the fields every message record has (`uuid`, `parentUuid`,
    `isSidechain`, `timestamp` RFC 3339 ms, `userType`, `entrypoint` `sdk-cli`, `cwd`, `sessionId`,
    `version`, `gitBranch`); `attachment` records chained after a conversation's first prompt
    (`environment`, `model`, `session_context`, `date`) plus `atis-latch`; ONE `assistant` record per
    content block (`message.id` shared by the blocks of a message, `apiBlockIndex`, `requestId`; a
    `say` is a `text` block, a `think` a `thinking` block with empty text and a signature, a tool
    call a `tool_use` block); a tool result is a `user` record whose content is a `tool_result`
    block, with `toolUseResult` and `sourceToolAssistantUUID` (denied: `is_error`; an image: an
    `image` block and `toolUseResult {type:"image", file{…}}`); a background `local_agent` is a
    `tool_use` of `Agent`, its result `toolUseResult {status:"async_launched", agentId, …}` and the
    subagent's own conversation in `<id>/subagents/agent-<agentId>.jsonl` (`isSidechain`, `agentId`,
    own chain from a first `user` record holding the prompt) with `agent-<agentId>.meta.json`
    (`toolUseId`, `agentType`, …); an interrupt is the `[Request interrupted by user]` user record,
    a failed turn a `<synthetic>` assistant error record; after the last message of a turn
    `last-prompt` (`leafUuid`) and `cost-state`; `--name` writes `custom-title` and `agent-name` first.
    Every message record chains to the previous one, and a resumed run appends to the same file with
    its first record chained to the file's last leaf. The builder (`Transcript`) also writes what a
    persona never does, so tests can have it: a rewound branch (file order is not chain order), a
    compaction (`compact_boundary` with `parentUuid` null and `logicalParentUuid`, then the
    `isCompactSummary` record), a synchronous subagent, a user prompt with an image, bookkeeping and
    unknown record kinds, a cut last line, and big files of filler turns. A scenario `sessions[]`
    entry that is resumed (`--resume <its id>`) is written as such a transcript first
    (`transcript::materialise_session`), in its `cwd` or the persona's.
* **codex** (codex-cli 0.161.0, real runs against `codex app-server`): `thread/start` reports a
  thread id at once but writes no rollout; `thread/resume` of it fails with code -32600 `no
  rollout found for thread id <id>`. The rollout exists from the first `turn/start`, so the fake
  stores a thread then. The fake stores each turn with every item it had (user message, reasoning,
  tool items, agent messages, completed), and `thread/resume` hands the conversation back in the
  result as codex-cli 0.169.1 still does for a paginated thread (`reference/codex/resume.jsonl`,
  `wasit-codex/tests/fixtures/resume.ndjson`): a `deprecationNotice` and an idle
  `thread/status/changed` first, then `thread.turns[]` = `{id, rootTurnId, items, itemsView:"full",
  status, error, startedAt, completedAt, durationMs}`, `historyMode:"paginated"`,
  `initialTurnsPage: null` and the two backwards cursors. A scenario session's `history` is paired
  into turns (a user message starts one). The rollout discovery lists is written too:
  `<CODEX_HOME, else <state>/codex-home>/sessions/YYYY/MM/DD/rollout-<YYYY-MM-DDThh-mm-ss>-<id>.jsonl`,
  a `session_meta` line (`id`, `session_id`, `cwd`, `originator` = the client's name, `cli_version`,
  `source`, `history_mode`, …) and one `response_item` message line per user / assistant message
  (`input_text` / `output_text`), the structure of real `~/.codex/sessions` files (read for their
  shape only); a scenario session gets its rollout when the persona starts. Thread and rollout are in
  storage before `turn/completed` is sent. `thread/list` lists the threads of `cwd` newest first.
* **opencode / kilo** (kilo 7.8.8, real run through `kilo acp`): `session/new` stores the session;
  `session/load` of it works in the next process with no prompt in between. opencode is assumed to
  keep every session it opens too (the driver's own comment says so); no opencode CLI was
  available to check. The session keeps what its turns sent (the user's messages, thoughts, text
  messages whole, tool calls with their outcome, the usage after each turn) and `session/load`
  replays it as `session/update` notifications before the result, as `reference/{kilo,opencode}/
  load.jsonl` show (`user_message_chunk`, `agent_thought_chunk`, `agent_message_chunk`; a tool is a
  `tool_call` and its completed `tool_call_update`, which the recordings do not contain: the
  shapes are the live ones); a session known only by its messages replays one chunk per message. The
  session is stored before `session/prompt` is answered. `session/list` and `session list` list the
  stored sessions too (`updatedAt` / `updated` are the real last change), those of `cwd` when
  one is asked for.

## Fidelity

`reference/<persona>/*.jsonl` are real exchanges recorded from claude 2.1.326, opencode 1.19.6,
kilo 7.8.27 and codex-cli 0.169.1 (see `reference/record/README.md` for how);
`reference/claude/lifecycle.jsonl`, `background_bash.jsonl`, `background_agent.jsonl`, `stop_task.jsonl`,
`agent_turn_interrupt.jsonl` and `agent_turn_prompt.jsonl` are from claude 2.1.293 (sanitized: home path,
MCP server, skill and plugin names; recorded with `wasit-smoke --record`, scripts and the sanitizer in
`reference/record/background`; the same data is `wasit-claude/tests/fixtures/*.ndjson`). The claude persona reports the lifecycle of a user message that carries a `uuid`
the way that recording shows: `command_lifecycle` `queued` when the line is read, `started`
before the turn's `system/init`, `completed` / `cancelled` (interrupted) after its `result`,
whose `user_message_uuids` lists it; `cancel_async_message` removes a still-queued message
(`cancelled` frame, then `{cancelled:true}`) and answers `{cancelled:false}` for one that
started; `interrupt` answers `still_queued` with the messages read during the turn, which then
run as their own turns. The personas clone
large static objects (claude `system/init` and the initialize reply, codex thread objects, ACP
`initialize`) straight from those recordings, and `tests/reference.rs` replays the client side of
every recording against the fake and compares message structure (keys and JSON kinds, not values).

`reference/codex/steer.jsonl` is from codex-cli 0.161.0, the version installed where it was recorded
(the rest of `reference/codex` is 0.169.1); only the host name and installation id in the
`remoteControl/status/changed` line were replaced, and `tests/reference.rs` compares just the
`turn/steer` replies of it.

Known gaps: the claude transcript lacks what the real file has besides the records above
(`file-history-snapshot` / `-delta`, `slug`, `session_id`, `queueTranscriptOnly`, `classifier*`,
`permissionDecision`, the big `prompt_snapshot` attachments, `system/turn_duration`, which the
stream-json runs examined did not write, thinking text, `tool-results/` files, the `task-notification`
records of a finished background task), and claude's behaviour for `--resume` together with `--session-id`, `--fork-session` and `--name` on a
resume (the fake renames) was not observed, and `/rename <name>` over stream-json (observed with
2.1.293: a second `session_title_changed`, the transcript's title rewritten) is just a prompt to the fake; a codex thread
persisted by `thread/name/set` without a turn (real codex writes the rollout then: observed
with 0.161.0) is not modelled, nor is the `thread/name/set` request itself; the
history a codex resume or ACP load replays is what the fake's own turns produced (no `fileChange`,
plan or image items), and the real agents' replay of tool calls was not recorded; JSON key order is alphabetical, not the real CLI's order; codex `fileChange` items,
codex reasoning items, ACP `plan` updates (real opencode reports todos as a `todowrite` tool call,
the ACP spec shape is emitted here) and claude `ExitPlanMode` were not captured from the real
CLIs and follow the protocol specs; claude `rate_limit_event` and codex `warning` /
`account/rateLimits/updated` / `thread/readState/changed` noise is not emitted; a claude
message read during a turn of the client's waits for the next turn, while the real CLI may also take
it into the running turn at its next model call (`started` mid-turn, one `result` listing both; the
fake does that only in a turn the agent started itself); tasks end only while the session is idle (a
task that comes due during a turn is ended after it, where the real CLI absorbs the notification into
the running turn); the real CLI sometimes starts two turns of its own after one subagent (a `peer`
hand-back with the subagent's report in `origin.body`, then the task notification), the fake one;
claude `system/session_state_changed` (an opt-in of the CLI) is not emitted; the lifecycle state after a
failed turn was not recorded (`completed` is sent); codex `turn/steer`: only the accepted shape, the
two errors above and the fold-in at turn end were recorded (a real steer is folded in at the next model
call, a turn with tool calls may take it earlier), a steer is accepted only while the scenario's turn
waits, and `refuse` is the fake's own knob for the refusals a real CLI gives for reasons no recording
shows (a vanished thread, a rejected input); the ACP persona refuses a prompt only through a `fail` step
(usage update, then the error: kilo 7.8.8 / 7.8.27 without credentials, `reference/kilo/error.jsonl`); `session/load`
history replay and codex `thread/resume` history are simplified; kilo's `effort` config option is
not advertised.
