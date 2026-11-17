Scripts that produced `../<persona>/*.jsonl` from the real CLIs (claude 2.1.326, opencode 1.19.6,
kilo 7.8.27, codex-cli 0.169.1). They are kept for reproducibility, not run by `cargo test`.

* `rec.mjs` spawns a CLI and logs every stdin line (`in`) and stdout line (`out`).
* `claude1.mjs`, `claude2.mjs <scenario>`: claude stream-json (`--model haiku`).
* `acp.mjs <opencode|kilo> <scenario>`: ACP (free models).
* `codex.mjs <scenario>`: `codex app-server`, with `CODEX_HOME` pointing at a config whose model
  provider is `http://127.0.0.1:18080/v1`, served by `fake-responses.mjs` (a minimal OpenAI
  Responses API stub: text replies, an `exec_command` call for `RUNTOOL`, `request_user_input`
  for `ASKUSER`, a slow stream for `SLOW`, an HTTP 400 for `FAILME`).
* `trim.py` shortens long lists (models, commands, tools) so the recordings stay small.

Run from this directory with `REF_DIR=..` set.
