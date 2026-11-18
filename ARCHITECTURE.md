# Marsad / Wasl / Wasit — Target Architecture (v2)

Authoritative. Supersedes every older ARCHITECTURE/DESIGN/GAPS/handoff/spec doc in the three
repos; where an old doc disagrees, this wins and the old doc gets deleted as its area is rebuilt.
Breaking changes to every wire format, store format and config format are allowed; nothing is
migrated.

## 0. Product

- **Layer 1 — dashboard.** One place to launch, continue, watch and steer many coding-agent CLI
  sessions (Claude Code, Codex, OpenCode, Kilo Code) on the local machine and on remote machines.
  At a glance: what is working, what needs an answer, what failed, what finished.
- **Layer 2 — orchestration.** Sessions wired into workstreams across machines; an agent can
  delegate to other agents (pre-declared or spawned on demand), with human gates on handoffs.
- Platforms: Linux, Windows and macOS, each as GUI machine and as session host; WSL as session host only.
  Mobile is parked (do not design it out: any client speaks the control
  protocol in §4).

## 1. Non-negotiable engineering rules

1. **Nothing blocks.** All I/O is async (tokio). No thread may wait on I/O while holding a lock
   or while it is the only thing that can serve other sessions. Every external wait has a
   deadline. The UI never waits synchronously on the backend: commands return immediately,
   results arrive as events.
2. **No sleeps or polls as synchronisation.** Readiness is signalled (a line, a frame, a
   resolved future). `sleep` is allowed only inside a backoff schedule.
3. **One owner per piece of state.** Each session has exactly one actor that owns its transport,
   its pending gates, its status. Everybody else sends it messages.
4. **No silent errors.** A failed operation produces an error response to the request that
   caused it and, when user-relevant, a user-visible event. `let _ =` / `.ok()` on a fallible
   I/O result needs a comment justifying it.
5. **No duplication.** One parser per CLI, one handshake per CLI, one event model, one session
   runtime, one control protocol, one set of wire types — defined once, depended on everywhere.
   No copied DTOs.
6. **No driver names in control flow.** Outside the driver's own crate and the registry that
   lists drivers, code never matches on `"claude"`/`"codex"`/…; it asks the driver descriptor
   for capabilities. An unknown driver id is an error, never a fallback.
7. **No preset client knowledge.** Models, modes, slash commands and resumable sessions are
   discovered live from each CLI.
8. **Never parse rendered terminal output.** Structured protocols only.
9. **Marsad never decides for the human.** Gates are answered by a human click or by a policy
   the human explicitly set (e.g. "always allow on this edge").
10. **Tests exercise real processes.** Every driver and the runtime are tested against a
    scriptable fake CLI binary speaking the real wire protocol, and against the real CLI where
    one is installed. A live test whose binary is missing is reported as skipped by the harness
    (`#[ignore]` + opt-in env/feature), never as a silent pass. No coverage-percentage gate and
    no `cfg(not(coverage))`.

## 2. Repos and layering

```
Wasit   driver + session-runtime library      (knows CLIs; knows nothing about A2A or UI)
Wasl    agent-to-agent layer on top of Wasit  (tasks, peers, handoff gates, MCP tools)
Marsad  product: host daemon binary, GUI core, desktop UI (uses Wasit + Wasl)
```

Dependencies are pinned cargo **git** dependencies (Marsad → Wasl, Wasit; Wasl → Wasit), one
resolved copy of Wasit in any build. Local development uses a `[patch]` section in an untracked
`.cargo/config.toml` pointing at sibling checkouts. No nested checkouts, no submodules.

### 2.1 Wasit crates

| Crate | Role |
|---|---|
| `wasit-core` | `AgentEvent`, gate/usage/mode types, `DriverDescriptor`, `DriverRegistry`, `Protocol` trait (§3.1). No I/O. |
| `wasit-claude`, `wasit-codex`, `wasit-opencode` | One `Protocol` impl each. `wasit-opencode` is parameterised by a flavor so Kilo Code is a second descriptor over the same implementation. |
| `wasit-shim` | Per-session process holder (lib + `wasit-shim` binary entry) (§3.2). |
| `wasit-runtime` | Session actor, journal reader, session state reducer, host-level session manager (§3.3). |
| `wasit-control` | Control protocol: wire types, framing, client, server, link supervisor (§4). |
| `wasit-fake-cli` | Scriptable fake agent CLI speaking each real wire protocol, for tests. |
| `wasit-process` | The ONLY place processes are created, in all three repos: `command` / `std_command` (`CREATE_NO_WINDOW` on Windows: no console window ever flashes) and `detached_command` + `spawn_detached` for daemons; `decode_console_output` (UTF-16LE from `wsl.exe`). A source-scan test per repo fails on any other `Command::new`. |

### 2.2 Wasl crates (after rebuild)

`wasl-proto` (task/peer/gate-policy types), `wasl-node` (the A2A layer: peer table, task
engine, handoff gates, router; embeddable library), `wasl-mcp` (the stdio MCP tool server an
agent CLI attaches to; thin client of the local node), `wasl-audit` (+ file sink), and a small
standalone binary. Everything else (one-shot adapters, `wasl-runtime*`, `wasl-session`,
`wasl-operator`, legacy dispatch/registry/server/gate crates) is deleted or folded in.

### 2.3 Marsad crates (after rebuild)

| Crate | Role |
|---|---|
| `marsad-agent` | The **host daemon** binary: `wasit-runtime` host + embedded `wasl-node` + Marsad services (git changes/commit, pty terminal, capability discovery, config rendering) served over `wasit-control`. Same binary on every machine; subcommands `serve`, `bridge` (stdio↔daemon for SSH), `shim` (wasit-shim entry), `mcp` (wasl-mcp entry). |
| `marsad-core` | GUI-side client: connection to the local daemon, view-model state, commands. |
| `marsad-store` | Persistence of Marsad-level data (hosts, workstreams, config layers, settings, seen-markers). |
| `marsad-viewmodel`, `marsad-diff`, `marsad-format` | Pure presentation logic. |
| `apps/desktop` | Tauri shell + React UI. |

Deleted: `marsad-transport`, `marsad-lifecycle`, `marsad-winshim`, `marsad-hosts` (ssh-script
plumbing), `marsad-wasl`, `marsad-wasl-session`, `marsad-events` (reducer moves to
`wasit-runtime`), `marsad-gates`, `marsad-notify` (logic moves next to the one notifier), tmux,
FIFOs, `apps/ios`, `apps/android` (parked; removed from tree).

## 3. Session runtime (Wasit)

### 3.1 Protocol = pure, transport-free driver

```rust
pub trait Protocol: Send {
    /// Lines to write right after the process starts (handshake), given the spawn spec.
    fn start(&mut self, spec: &SpawnSpec) -> Vec<OutLine>;
    /// Feed one line the CLI wrote; get events plus any lines to write back (handshake
    /// continuation, protocol-level acks).
    fn on_line(&mut self, line: &str) -> Step;          // Step { events, write }
    /// Encode a command. Err(Unsupported) only when the descriptor says so.
    fn encode(&mut self, cmd: Command) -> Result<Vec<OutLine>, DriverError>;
}
```

`Command` = Prompt, AnswerGate, AnswerQuestion, Interrupt, SetModel, SetMode, RunSlash,
Shutdown. The state machine is deterministic in its inputs (start spec, lines read, commands
encoded), so replaying a journal reproduces state exactly. Request ids are counters inside the
protocol state, never clock-derived. `DriverDescriptor` carries: id, display name, default
binary name, `argv(spec)`, `env(spec)`, capabilities, a `new_protocol()` factory, discovery
hooks (list models, list resumable sessions, version) and a `ConfigRenderer` (§6).

### 3.2 Shim = the only process that owns a CLI

One small process per session, detached from whoever started it (new session/process group on
Unix; `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP` + job object on Windows). It:

- spawns the CLI with piped stdio in the session directory `<data>/sessions/<session_id>/`;
- appends every stdout line, stderr line, every line written to stdin, and lifecycle records
  (`spawned {pid, argv, started_at}`, `exited {code|signal}`) to `journal.ndjson`, each record
  `{seq, t, kind, data}` with a per-session monotonically increasing `seq`, fsync on a short
  coalescing timer;
- listens on a local endpoint (`ctl.sock`; named pipe `\\.\pipe\wasit-<session_id>` on Windows)
  for ONE controller at a time: `attach {from_seq}` → streams journal records from `from_seq`
  then live, with no gap and no duplicate; `write {line}`, `signal {int|term|kill}`, `close`;
- holds an exclusive lock on `shim.lock` for its lifetime (liveness = lock held; no pidfile
  guessing), and exits once the CLI has exited and the final record is durable.

The shim knows nothing about any CLI protocol and should almost never need to change, which is
what lets the daemon be upgraded/restarted while sessions run.

### 3.3 Runtime = one actor per session

`SessionActor` (tokio task) owns the shim connection and the `Protocol` state. Inputs: journal
records from the shim, commands from its mailbox (bounded). Outputs: `SessionEvent`s
(`{cursor, AgentEvent | StateChanged | …}`, cursor derived from journal seq) on a broadcast with
replay-from-cursor, and a continuously reduced `SessionSnapshot` (phase, pending gates,
usage/cost/context, model/mode, subagents, last activity, exit). Rules:

- Phase is derived only from facts: journal says the process exited ⇒ `Exited{code}` (shown as
  Done/Failed), shim gone without an exit record ⇒ `Lost`; never "working" for a dead process.
- Pending gates are part of reduced state, so they survive daemon restart via replay.
- Transcript = the journal. Backfill = replay from cursor. No separate transcript store, no
  1 MiB caps; paging by cursor.
- Every command gets a result (ok / typed error) delivered to the requester.
- Watchdogs are timers owned by the actor (not checked opportunistically on message arrival).

`SessionManager` (per daemon): create (spawn shim), continue (new shim with driver resume),
re-adopt on daemon start (scan session dirs, attach to live shims, replay finished ones lazily),
stop, delete. It persists `session.json` (spec, driver id, native session id, labels) beside the
journal.

## 4. Control protocol (`wasit-control`)

One protocol for GUI↔daemon, daemon↔daemon and (later) phone↔daemon.

- Framing: newline-delimited JSON over any duplex byte stream. Transports: Unix socket / Windows
  named pipe (local), child-process stdio (SSH).
- `Hello {protocol, build, host_id, capabilities}` both ways first; incompatible major ⇒ typed
  refusal the UI can explain ("helper on host X is older; updating").
- `Request {id, method, params}` → `Response {id, result | error{code,message}}`; long-running
  work replies immediately with an operation id and finishes with an event.
- `Subscribe {topic, from_cursor}` → `Event {sub, cursor, payload}` …; topics: host (session
  list + snapshots, link states, capability inventory), session (events), task/edge (Wasl).
  Resubscribing with the last cursor is the whole reconnect story.
- Bounded per-connection queues. A subscriber that falls behind is disconnected with a
  `Lagged` close and simply resubscribes from its cursor. No unbounded buffers.
- Single-instance: the daemon holds an exclusive lock file next to its socket; a second
  instance exits.

### 4.1 Topology

- Every machine that hosts sessions runs `marsad-agent serve` (started on demand, detached).
- The GUI talks only to its **local** daemon. The local daemon is the **hub**: for each remote
  host it keeps one long-lived `ssh <host> marsad-agent bridge` child whose stdio carries the
  control protocol to that host's daemon. Requests carry a `host_id`; the hub routes.
- `LinkSupervisor` per remote host: explicit state machine
  (`Connecting → Provisioning → Connected → Reconnecting{attempt, next_at} | AuthNeeded |
  Failed{reason}`), exponential backoff, state published as events. ssh runs with
  `BatchMode=yes`, `ServerAliveInterval`, no ControlMaster; anything interactive (password,
  host-key confirmation) surfaces as `AuthNeeded` with an action in the UI, never a hang.
- **Provisioning:** on connect the hub runs one bounded probe (`marsad-agent --version` + OS/arch).
  Missing or different build ⇒ copy the matching binary over the same ssh (scp/sftp, or stdin
  upload) into `~/.marsad/bin/` (`%LOCALAPPDATA%\Marsad\bin`), atomically renamed, then start.
  Running sessions are unaffected (they are held by shims). Binaries for other targets come from
  the app's bundled resources / release assets; in development from `MARSAD_AGENT_BIN_DIR`.
- Windows hosts are reached with the same mechanism via OpenSSH for Windows; WSL distros are
  hosts reached via `wsl.exe -d <distro> -- marsad-agent bridge`. The transport is "a command
  whose stdio is the link" — ssh, wsl and (later) anything else are just different commands.

## 5. Orchestration (Wasl on Wasit)

- **Addressing:** a peer is a *session* — `AgentId` ↔ `(host_id, session_id)`. The node's peer
  table is live (updated over the control protocol), never baked into a config file.
- **Routing:** node-to-node messages travel over the control links (worker host → hub → target
  host). No listening TCP ports, no extra auth. A `PeerTransport` trait keeps direct HTTP
  possible for standalone Wasl, but the relay is the default and the only one Marsad uses.
- **Tasks are asynchronous and durable.** MCP tools offered to an agent:
  `list_peers`, `delegate(peer|peers, instruction, mode: consult|command) → task_id(s)`,
  `task_status(ids)`, `task_wait(ids, timeout) → results`, `task_cancel(id)`,
  `message(peer, text)` (fire-and-forget), `spawn_agent(spec) → peer` (phase 2),
  `transfer(...)` (reworked last). `consult` remains as sugar for delegate + wait. Task state is
  journaled by the node; results are structured (`status`, `summary`, `artifacts`, `usage`).
  Fan-out never serialises: no lock is held across a delegation.
- **Delivery:** a task for a session is a turn submitted to that session's actor with a
  provenance header; progress events stream back to the task; completion = the turn's result.
- **Handoff gates:** raised by the *sender's* node as a gate on the sending session (kind
  `Handoff`), so they surface through the same pipeline as any tool permission, for every CLI,
  with no dependence on MCP elicitation. Policy per edge: `ask` (default) / `always_allow` /
  `deny`; "always allow" chosen in a gate writes the edge policy. Standalone Wasl (no
  supervisor) falls back to MCP elicitation, else deny.
- **Safety:** hop count and causation chain are propagated and enforced (cycle ⇒ rejected with a
  clear error to the agent); per-task deadline; optional per-workstream budget.
- Workstream graph (Marsad) ⇒ peer table + edge policies pushed to the nodes. Agents spawned on
  demand appear as new nodes attached to their spawner.

## 6. Agent configuration layers

`AgentConfig { mcp_servers, skills, plugins, extra_instructions, env, tool_policy }`, stored by
Marsad in layers **Global → Host → Workstream node → Session**; each layer can add, override or
disable an item. The effective config (with provenance per item) is computed in one pure
function and rendered by the driver's `ConfigRenderer` into launch-time injection only (flags,
env, generated files in the session directory). The user's own CLI config files are never
edited. A driver that cannot honour an item reports it as unsupported with a reason; the UI
shows that, the launch does not fail. The Wasl MCP server is just an item injected by the
orchestration layer through the same path.

## 7. GUI side

- `marsad-core`: one async connection to the local daemon under a supervisor (reconnect +
  resubscribe from cursors); state is a projection of daemon events; every command is
  `request → response`, surfaced errors go to a toast/event channel. No fake fallback backend in
  production paths (the mock backend is a separate, explicit dev/test mode).
- Tauri commands are async and never hold a lock across an await on I/O.
- "Seen" markers (finished/failed since last look) and notification preferences live in
  `marsad-store` and are honoured by the single notifier.
- Notifications: needs-you, failed, finished, host lost — each individually switchable.

## 8. Build order

1. **Wasit**: Protocol refactor + registry + Kilo flavor; shim; control; runtime; fake CLI.
2. **Marsad daemon + core** on the new runtime (local, then remote links + provisioning);
   delete old plumbing. Layer 1 complete here.
3. **Wasl** rebuilt on Wasit runtime; tasks, relay routing, handoff gates. Marsad workstreams
   wired to it. Layer 2 complete here.
4. **UI** rebuilt screen by screen from owner-approved mockups, in parallel from step 2 on.
5. Windows verification on CI runners throughout; packaging/release assets last.
