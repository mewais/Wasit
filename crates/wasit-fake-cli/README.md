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
but honours claude's `--model`, `--permission-mode`, `--resume`, `--session-id` and
`--include-partial-messages`.

`WASIT_FAKE_SCENARIO=<path.json>` selects the scenario (default: an empty one, which echoes every
prompt). `WASIT_FAKE_TRANSCRIPT=<path>` appends every line in both directions as
`{"dir":"in"|"out","msg":…}`, so a test can assert what its driver sent.
`WASIT_FAKE_SCENARIO_JSON=<json>` passes the scenario inline (a fake on another machine).
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
| `mcp_call` | `server`, `tool`, `arguments`, `save_as?` | claude persona: start the stdio MCP server `server` from `--mcp-config`, call `tool`, show it as a tool call `mcp__<server>__<tool>`; `save_as` keeps the result (JSON when it parses) for `{{name.path.0.x}}` placeholders in later `say` texts and `mcp_call` arguments |

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
`session list [--format json]`, answered from `models` / `sessions`. `--version` also works for
claude and codex.

## Fidelity

`reference/<persona>/*.jsonl` are real exchanges recorded from claude 2.1.326, opencode 1.19.6,
kilo 7.8.27 and codex-cli 0.169.1 (see `reference/record/README.md` for how);
`reference/claude/lifecycle.jsonl` is from claude 2.1.293 (sanitized: home path, MCP server and
skill names). The claude persona reports the lifecycle of a user message that carries a `uuid`
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

Known gaps: JSON key order is alphabetical, not the real CLI's order; codex `fileChange` items,
codex reasoning items, ACP `plan` updates (real opencode reports todos as a `todowrite` tool call,
the ACP spec shape is emitted here) and claude `ExitPlanMode` were not captured from the real
CLIs and follow the protocol specs; claude `rate_limit_event` and codex `warning` /
`account/rateLimits/updated` / `thread/readState/changed` noise is not emitted; claude's
`system/init` is printed once, not at the start of every turn; a claude message read during a
turn waits for the next turn, while the real CLI may also take it into the running turn at its
next model call (`started` mid-turn, one `result` listing both); the lifecycle state after a
failed turn was not recorded (`completed` is sent); codex `turn/steer`: only the accepted shape, the
two errors above and the fold-in at turn end were recorded (a real steer is folded in at the next model
call, a turn with tool calls may take it earlier), a steer is accepted only while the scenario's turn
waits, and `refuse` is the fake's own knob for the refusals a real CLI gives for reasons no recording
shows (a vanished thread, a rejected input); the ACP persona refuses a prompt only through a `fail` step
(usage update, then the error: kilo 7.8.8 / 7.8.27 without credentials, `reference/kilo/error.jsonl`); `session/load`
history replay and codex `thread/resume` history are simplified; kilo's `effort` config option is
not advertised.
