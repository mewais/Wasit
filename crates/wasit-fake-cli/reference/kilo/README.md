# Kilo subagent recordings (`subagent_*`)

Recorded from **Kilo 7.8.8** (`kilo --version`), 2026-11-24/25, with a scripted fake OpenAI-compatible
provider (no real model). Driven by `/tmp/kilo/kilo-probe/acp_probe.mjs` (ACP client plus HTTP/SSE
capture) and `/tmp/kilo/kilo-probe/fake_openai_server.py` (scripted provider). Those two scripts are not
vendored in this repo; they are the reproduction recipe.

## How they were recorded

* Each scenario starts `kilo acp --port <free port> --hostname 127.0.0.1` with an isolated environment:
  `HOME` and all `XDG_*` under `/tmp/kilo/kilo-probe`, so the owner's real config and sessions are untouched.
  The config (`config/kilo/kilo.jsonc`) defines a custom provider `fake/fake-model` at
  `http://127.0.0.1:18090/v1` and sets `"permission": { "bash": "ask" }`. Without that line the
  default config ran the child's `bash` with **no** permission request (see `permission_*`).
* `KILO_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true` for every scenario except `fg`.
* The client speaks ACP over stdio (`initialize`, `session/new`, `session/set_config_option` model,
  `session/prompt`, `session/load`, `session/cancel`), answers `session/request_permission` only when the
  scenario says so, and in parallel subscribes to `GET /global/event` and takes snapshots of
  `GET /session/status`, `/session/{parent}/children`, `/kilocode/background-jobs?sessionID={parent}`
  and `GET /permission`. It also sends HTTP calls (`prompt_async`, `abort?scope=session`, `permission/{id}/reply`).
* The fake provider answers the sub-agent with scripted tool calls (`glob`, `read`, or `bash`) and a
  final text. A turn is **held** (not answered) until a gate file exists. The child is held on its
  first request in the `*_SLOW`/`*_PERM` scenarios, so the driver can act while the child is running.
  This makes the timing deterministic; it is the only artificial part.
* Content-free: prompts are the marker strings (`[[PARENT_BG_SLOW]]`, `[[STEER_ACP]]`, ...); model
  text is the fake's `CHILD_DONE`/`PARENT_DONE`; host paths are replaced with `$PROBE`
  (`/tmp/kilo/kilo-probe`) and `$HOME`. `message.part.*`, `session.next.tool.*`, `session.diff` and
  full `sync` dumps are dropped from the SSE files. Session/child ids are real but random per run.

## File formats

* `subagent_<scenario>.acp.jsonl`: one line per ACP message: `{"dir": "in"|"out"|"proc"|"driver", "t": ms since
  scenario start, "proc": kilo instance label, "msg": ...}`. `in` = client to kilo, `out` = kilo to client.
  `proc` lines mark process start/exit/stdout-close; `driver` lines are notes.
* `subagent_<scenario>.sse.jsonl`: `{"dir": "sse", "t", "proc", "msg": {payload: {type, properties}}}`, plus
  `sse_end`/`sse_error` and `proc` markers (filtered as above).
* `subagent_<scenario>.http.json`: `snapshots` (label, endpoint, body with only this scenario's sessions,
  `_other_sessions` count), `http` (every driver HTTP call with status and body), `provider` (one row per
  fake-provider request: role, messages, prior tool calls, decision, `hold` gate, `[[MARKER]]`s seen in
  user text; no text bodies), `driver_notes`, `child_ids`.

## Scenarios and what they show (verified)

* **fg**: parent `task` (foreground). Child id is only in the completed `tool_call_update`
  `rawOutput.metadata.sessionId`. Child is a real session with `parentID`, `session.status` busy then idle.
  Without a load, **no child events reach ACP** (the only sessionId in `session/update` is the parent).
* **bg**: `task` returns immediately with `background-jobs` `running` and the child listed under
  `/children`. The child was held; `session/load <child>` on the same connection succeeds. After release,
  the child's `tool_call`/message updates arrive on ACP **with the child's sessionId** (10 events, 0 under
  other sessions). Child goes `session.idle` (SSE), job `completed`. The parent then runs a Kilo-driven
  completion turn (provider request with the notice; streamed `agent_message_chunk` with the **parent**
  sessionId; no client prompt). `GET /session/status` **omits idle sessions**; an empty map means idle.
* **bg_multi**: three `task` calls in one turn; all three children busy at once (`/session/status` shows
  three busy entries, jobs `running`); each completes independently after release.
* **steer**: child loaded, then (a) `POST /session/{child}/prompt_async` with
  `parts:[{type:"text", text, metadata:{kind:"subagent_steer"}}]` returns **204**; the text appears in the
  child's **next** provider request (after its glob call, i.e. at a step boundary) and the child continues.
  (b) ACP `session/prompt` on the busy child is **queued**: it is not seen in the current turn; it runs as a
  second child turn after the first ends, and the ACP prompt resolves `end_turn` after that turn.
  Child events stream on ACP with the child's sessionId.
* **permission_loaded**: child `bash` asks: `session/request_permission` arrives on ACP with the **child's**
  sessionId (also `permission.asked` on SSE). The client's `allow_once` answer lets the command run; the child completes.
* **permission_unloaded**: child not loaded, same config. **No** request reaches ACP and the command does not run
  for the 60 s wait; the child stays busy. `GET /permission` lists the pending request with
  `sessionID` = child and `permission: "bash"`. `POST /permission/{id}/reply {"reply":"once"}` returns **200**;
  the command then runs and the child completes (job `completed`).
* **interrupt**: ACP `session/cancel` on a held parent turn, with a bg child held. Parent prompt resolves
  `stopReason: cancelled`. **The child is also aborted**: `session.error` `MessageAbortedError` on the child, idle, and
  its job becomes `cancelled`. The child does not survive.
* **interrupt_abort**: `POST /session/{parent}/abort?scope=session` on the same setup returns **200 true**.
  Parent prompt resolves `cancelled`. The child **keeps running** (busy, job `running`) and completes after release.
* **owner_exit**: SIGKILL of the kilo process while the child runs. ACP stdout EOF and the SSE stream error
  are observed at the same instant; no `session.idle` for the child. A new `kilo acp` on the same storage:
  `session/load <child>` succeeds (replays the user message), but the parent's
  `/kilocode/background-jobs` is **empty** and the child is not busy: the job is gone, not reported as lost.

## Not covered

Background child that has already finished before the load (replay of a finished child); `abort?scope=tree`;
steering by `task_id` (parent-side `background.extend`); take-over; and a clean timing for the one
~120 s stall seen before the first provider request in a run (cause not established; likely the model-catalogue
network fetch, not verified).
