Scripts that produced `../<persona>/*.jsonl` from the real CLIs (claude 2.1.326, opencode 1.19.6,
kilo 7.8.27, codex-cli 0.169.1). They are kept for reproducibility, not run by `cargo test`.

* `rec.mjs` spawns a CLI and logs every stdin line (`in`) and stdout line (`out`).
* `claude1.mjs`, `claude2.mjs <scenario>`: claude stream-json (`--model haiku`).
* `claude3.mjs`: the message lifecycle (`uuid` on user messages, `command_lifecycle`,
  `cancel_async_message`, `interrupt` with a queued message); recorded with claude 2.1.293
  (`REC_CWD` sets the working directory), then `trim.py` and the names sanitized by hand.
* `acp.mjs <opencode|kilo> <scenario>`: ACP (free models).
* `codex.mjs <scenario>`: `codex app-server`, with `CODEX_HOME` pointing at a config whose model
  provider is `http://127.0.0.1:18080/v1`, served by `fake-responses.mjs` (a minimal OpenAI
  Responses API stub: text replies, an `exec_command` call for `RUNTOOL`, `request_user_input`
  for `ASKUSER`, a slow stream for `SLOW`, an HTTP 400 for `FAILME`, and for a message with an
  `input_image` a reply naming its MIME type and size; requests are appended to
  `$FAKE_RESPONSES_LOG`, default `/tmp/fake-responses.log`).
* `codex/steer.jsonl` (not in `codex.mjs`): codex-cli 0.161.0 (the version installed on the recording
  machine), same fake Responses server, port changed. A raw JSON-RPC client: `turn/start` `SLOW please`,
  after the first delta a `turn/steer` with a wrong `expectedTurnId`, one with the right one, and after
  `turn/completed` one more. The host name and installation id were replaced by hand.
* `background/` (claude 2.1.293): the background-task recordings (`wasit-claude/tests/fixtures/{background-bash,
  background-agent,stop-task,agent-turn-interrupt,agent-turn-prompt}.ndjson`, and the `claude/*.jsonl` here made from
  them). Recorded through the driver itself: `wasit-smoke claude --model haiku --cwd <scratch> --record X.raw
  --script X.script.json --timeout 80 "<first prompt>"` (the prompts are in the test docs: a background `Bash`
  `sleep` or an `Agent` with `run_in_background`; `"task_id": "@running"` in a script command names the latest
  task), then `python3 -I background/sanitize.py X.raw > fixture.ndjson` (home path, MCP / skill / plugin names,
  catalogues trimmed; `in` records untouched) and `python3 -I background/to_reference.py fixture.ndjson >
  claude/X.jsonl`.
* `trim.py` shortens long lists (models, commands, tools) so the recordings stay small.

Run from this directory with `REF_DIR=..` set.
