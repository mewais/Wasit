# Kilo sub-agent probe (fake provider + wasit-hostd)

Durable reproduction of the Kilo 7.8.8 background sub-agent smoke against the real `kilo` binary,
a scripted OpenAI-compatible provider, and `wasit-hostd`. Recording tooling only: not built, not run
by `cargo test`, not committed. The scripts are the recipe; the outputs live under `/tmp/kilo/e2e`.

## Files

* `fake_openai_server.py`: `POST /v1/chat/completions` with SSE (incl. `tool_calls` deltas and a usage
  chunk). Requests without `tools` are title requests (plain text). Scripts are picked by the marker in
  the first user message: `[[PARENT_BG]]` (parent: `task` with `background: true`, ACK, then `PARENT_DONE`
  after the background completion) and `[[CHILD]]` (child: `glob`, then a HOLD until the release file
  exists, then `read`, then `CHILD_DONE`). "Step" = number of assistant tool-call messages in the whole
  history. Opt-in parent HOLD of the ACK while `$E2E/hold_parent_ack` exists (until `$E2E/release_parent`).
  Every request is one JSON line in `$E2E/requests.jsonl` (content-free: conv, step, message count, last
  role, `steer_seen`, hold begin/end).
* `run.sh`: `env` (isolated HOME/XDG under `$E2E/home`, `kilo.jsonc` with provider `fake/fake-model` at
  `http://127.0.0.1:18090/v1`, `permission: {"*": "allow"}`, fixture files in `$E2E/work`), `fake`, `hostd`
  (`wasit-hostd serve --data $E2E/data`), `drive <args>`. Sets `KILO_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true`
  and puts `/home/mewais/.kilo/bin` first on PATH so `kilo-bridge -- kilo acp` finds the real binary.
* `drive.py`: wasit-control client (newline-delimited JSON frames over `$E2E/data/hostd.sock`, Hello
  first). Subcommands: `create`, `prompt`, `steer`, `interrupt`, `stop_task`, `snap`, `wait`, `call`.
  Prints content-free summaries only.

## Run order

```
./run.sh env
./run.sh fake      &   # tool-managed, listens on 127.0.0.1:18090
./run.sh hostd     &   # prints {"ready":true,...} with the socket path
./run.sh drive create                      # -> {"id": "<sid>"}
./run.sh drive prompt <sid> "[[PARENT_BG]] ..."
./run.sh drive snap <sid>
./run.sh drive steer <sid> <task_id> "[[STEER]] ..."
./run.sh drive interrupt <sid>
touch $E2E/release                          # releases the held child
./run.sh drive stop_task <sid> <task_id>
```

Gates: `touch $E2E/hold_parent_ack` before a prompt keeps the parent turn open (for interrupt);
`$E2E/release` releases the held child; `$E2E/release_parent` releases the held parent ACK.
Remove the gate files between runs. Shut down by PID only: hostd, the fake server, and the
`wasit-hostd shim` / `kilo-bridge` / `kilo acp` processes that the sessions left behind (they outlive
hostd by design).

## Observed results (2026-11-25, Kilo 7.8.8, wasit-hostd 0.1.0 debug build)

1. Working while the bg child runs, Idle after (run 1): `working` with `turn_open=false`,
   `background_running=1`, child `running=true`; after release and the parent's follow-up turn:
   `idle`, `background_running=0`, `turns_completed=2`. Parent's second request was the completion
   (`step 1`, last role `user`).
2. Sub-agent live state, scoped tool calls, tokens: the subagent entry carries `running`, `status`,
   `outcome`, `tool_calls` (1 while held, 2 when done), `last_tool`, `subagent_type=general`, and a `usage`
   object (`input_tokens`, `output_tokens`, `tool_uses`, `total_tokens`; the child's first request
   reported 3540 input tokens). Session usage is reported separately.
3. Steer reaches the child (run 1): `steer_subagent` returned `sent`; the child's next request
   (`conv=child, step=2`) had `steer_seen=true`.
4. Interrupt keeps the child running (run 2): with the parent turn held open (`turn_open=true`), the
   interrupt closed the turn (`turn_open=false`, `turns_completed=1`) while the child kept
   `running=true` and `background_running=1`. After the release, the child's last request
   (`step 2`) followed and the child ended `completed`. OBSERVATION, not verified as a product
   contract: no new parent turn followed the child's completion in run 2, whereas run 1 without
   interrupt got one. Cause not isolated.
5. Stop ends it (run 3): `stop_task` returned `sent`; subagent `running=false`, `status=stopped`,
   `outcome=stopped`, `background_running=0`, phase `idle`.

## Known limits

* Hold/release is artificial by design (the fake holds the request); timing is deterministic only
  because of it.
* The `fg` (foreground) sub-agent path is not scripted here.
* `drive.py wait` polls the snapshot; it does not use the event topic.
