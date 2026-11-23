//! The `codex` persona: `codex app-server --stdio` (JSON-RPC 2.0 without the `jsonrpc` member
//! on responses and notifications).

use crate::engine::{Incoming, Outcome, Persona, TurnStats};
use crate::io::Io;
use crate::reference::find_out;
use crate::scenario::{AskStep, HistoryMessage, PlanEntry, Scenario, ToolStep};
use crate::util::{alnum, iso_from_ms, now_ms, now_secs, set, uuid7};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Where the persona keeps its threads (see [`crate::store`]).
const STORE: &str = "codex-threads";

#[derive(Clone, Serialize, Deserialize)]
struct Thread {
    id: String,
    cwd: String,
    model: String,
    preview: String,
    history: Vec<HistoryMessage>,
    created: u64,
    /// When the last turn ended (seconds; `0`: never, `created` stands in).
    #[serde(default)]
    updated: u64,
    /// The turns the thread ran, each as `turn/completed` reported it but with every item the
    /// turn had (what `thread/resume` hands back as `thread.turns`).
    #[serde(default)]
    turns: Vec<Value>,
}

/// Where the real CLI keeps its state: `CODEX_HOME`, else (the fake never touches the real home)
/// `<state>/codex-home`.
fn codex_home() -> std::path::PathBuf {
    match std::env::var_os("CODEX_HOME") {
        Some(d) => d.into(),
        None => crate::store::state_dir().join("codex-home"),
    }
}

/// `<home>/sessions/YYYY/MM/DD/rollout-<YYYY-MM-DDThh-mm-ss>-<id>.jsonl` of a thread created at
/// `created` (seconds), the name the real CLI gives a rollout.
fn rollout_path(id: &str, created: u64) -> std::path::PathBuf {
    let stamp = iso_from_ms(created * 1000);
    let (date, time) = (&stamp[..10], &stamp[11..19]);
    let (y, rest) = date.split_at(4);
    let (m, d) = (&rest[1..3], &rest[4..6]);
    codex_home()
        .join("sessions")
        .join(y)
        .join(m)
        .join(d)
        .join(format!(
            "rollout-{date}T{}-{id}.jsonl",
            time.replace(':', "-")
        ))
}

/// The turns of a conversation known only by its messages (a scenario session's `history`): a
/// user message starts a turn, the assistant messages after it are its replies.
fn turns_from_history(history: &[HistoryMessage], created: u64) -> Vec<Value> {
    let mut turns: Vec<Value> = Vec::new();
    let mut at = created * 1000;
    for m in history {
        at += 10;
        let user = m.role == "user";
        if user || turns.is_empty() {
            let id = uuid7();
            turns.push(
                json!({"id":id,"rootTurnId":id,"items":[],"itemsView":"full",
                "status":"completed","error":null,"startedAt":at/1000,
                "completedAt":at/1000,"durationMs":10}),
            );
        }
        let item = if user {
            json!({"type":"userMessage","id":uuid7(),"clientId":null,
                "content":[{"type":"text","text":m.text,"text_elements":[]}],
                "startedAtMs":at,"completedAtMs":at})
        } else {
            json!({"type":"agentMessage","moderation":null,"id":format!("msg_{}", alnum(8)),
                "text":m.text,"phase":null,"memoryCitation":null,"delivery":null,
                "questions":null,"startedAtMs":at,"completedAtMs":at})
        };
        if let Some(items) = turns.last_mut().and_then(|t| t["items"].as_array_mut()) {
            items.push(item);
        }
    }
    turns
}

pub struct Codex {
    scenario: Scenario,
    version: String,
    threads: Vec<Thread>,
    /// First thread id handed out is the scenario's `session_id`, if any.
    first_id: Option<String>,
    next_request_id: u64,
    item_seq: u64,
    sent_remote_status: bool,
    // Current turn.
    thread_id: String,
    turn_id: String,
    turn_request: Value,
    pending_item: Option<(String, String)>,
    items: Vec<Value>,
    /// Prompts accepted by `turn/steer` during the running turn, answered when it ends.
    steered: Vec<String>,
    /// `clientInfo.name` of `initialize`: what the rollout's `session_meta` says started it.
    originator: String,
}

/// A rollout line (`{"timestamp","type","payload"}`).
fn rollout_line(kind: &str, payload: Value) -> Value {
    json!({"timestamp": iso_from_ms(now_ms()), "type": kind, "payload": payload})
}

/// The `response_item` of a message, as the real rollouts have it (`input_text` for the user,
/// `output_text` for the model).
fn message_line(role: &str, text: &str) -> Value {
    let ty = if role == "user" {
        "input_text"
    } else {
        "output_text"
    };
    rollout_line(
        "response_item",
        json!({"type":"message","role":role,"content":[{"type":ty,"text":text}]}),
    )
}

/// Append `lines` to the thread's rollout, creating it with its `session_meta` first when it
/// does not exist (the real CLI writes the rollout with the first turn). A rollout that cannot
/// be written is a conversation discovery cannot list: say so.
fn append_rollout(t: &Thread, version: &str, originator: &str, lines: &[Value]) {
    use std::io::Write;
    let path = rollout_path(&t.id, t.created);
    let mut all: Vec<Value> = Vec::new();
    if !path.is_file() {
        all.push(
            json!({"timestamp": iso_from_ms(t.created * 1000), "type": "session_meta",
            "payload": {"id": t.id, "session_id": t.id,
                "timestamp": iso_from_ms(t.created * 1000), "cwd": t.cwd,
                "originator": originator, "cli_version": version, "source": "vscode",
                "model_provider": "fake", "history_mode": "paginated",
                "base_instructions": {"text": ""}, "context_window": 258_400}}),
        );
    }
    all.extend(lines.iter().cloned());
    let written = path
        .parent()
        .map(std::fs::create_dir_all)
        .unwrap_or(Ok(()))
        .and_then(|_| {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            for l in &all {
                writeln!(f, "{l}")?;
            }
            Ok(())
        });
    if let Err(e) = written {
        eprintln!("wasit-fake-cli: cannot write {}: {e}", path.display());
    }
}

fn rec(name: &str, pred: impl Fn(&Value) -> bool) -> Value {
    find_out("codex", name, pred)
}

fn is_file_change(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "edit" | "write" | "apply_patch" | "filechange" | "multiedit"
    )
}

fn shell_command(tool: &ToolStep) -> String {
    let raw = tool.input["command"]
        .as_str()
        .or_else(|| tool.input["cmd"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{} {}", tool.name, tool.input));
    format!("/bin/bash -lc '{raw}'")
}

impl Codex {
    pub fn new(scenario: &Scenario) -> Codex {
        let default_model = scenario
            .current_model
            .clone()
            .or_else(|| scenario.models.first().map(|m| m.id.clone()))
            .unwrap_or_else(|| "fake".into());
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "/".into());
        let mut threads: Vec<Thread> = scenario
            .sessions
            .iter()
            .map(|s| Thread {
                id: s.id.clone(),
                cwd: s.cwd.clone().unwrap_or_else(|| cwd.clone()),
                model: default_model.clone(),
                preview: s
                    .history
                    .iter()
                    .find(|m| m.role == "user")
                    .map(|m| m.text.clone())
                    .unwrap_or_default(),
                history: s.history.clone(),
                created: now_secs(),
                updated: 0,
                turns: vec![],
            })
            .collect();
        // Threads earlier processes stored: codex keeps a rollout per thread on disk.
        for t in crate::store::list::<Thread>(STORE) {
            if !threads.iter().any(|x| x.id == t.id) {
                threads.push(t);
            }
        }
        let version = scenario
            .version
            .clone()
            .unwrap_or_else(|| "0.169.1".to_string());
        // A scenario session is a conversation that already exists: its rollout does too.
        for t in threads
            .iter()
            .filter(|t| t.updated == 0 && !t.history.is_empty())
        {
            if !scenario.sessions.iter().any(|s| s.id == t.id) {
                continue;
            }
            let lines: Vec<Value> = t
                .history
                .iter()
                .map(|m| message_line(&m.role, &m.text))
                .collect();
            append_rollout(t, &version, "codex_cli_rs", &lines);
        }
        Codex {
            version,
            scenario: scenario.clone(),
            threads,
            first_id: scenario.session_id.clone(),
            next_request_id: 0,
            item_seq: 0,
            sent_remote_status: false,
            thread_id: String::new(),
            turn_id: String::new(),
            turn_request: Value::Null,
            pending_item: None,
            items: vec![],
            steered: vec![],
            originator: "codex_cli_rs".into(),
        }
    }

    /// The error the scenario refuses `text` with, if it does.
    fn refusal(&self, text: &str) -> Option<String> {
        self.scenario
            .refuse
            .iter()
            .find(|r| {
                regex::Regex::new(&r.when)
                    .map(|re| re.is_match(text))
                    .unwrap_or(false)
            })
            .map(|r| r.message.clone())
    }

    fn default_model(&self) -> String {
        self.scenario
            .current_model
            .clone()
            .or_else(|| self.scenario.models.first().map(|m| m.id.clone()))
            .unwrap_or_else(|| "fake".into())
    }

    fn reply(&self, io: &mut Io, id: &Value, result: Value) {
        io.send(&json!({"id": id, "result": result}));
    }

    fn error(&self, io: &mut Io, id: &Value, code: i64, message: &str) {
        io.send(&json!({"error": {"code": code, "message": message}, "id": id}));
    }

    fn notify(&self, io: &mut Io, method: &str, params: Value) {
        io.send(&json!({"method": method, "params": params, "emittedAtMs": now_ms()}));
    }

    fn thread_json(&self, t: &Thread, loaded: bool) -> Value {
        let mut th = rec("simple", |m| {
            m["id"] == 2 && m["result"]["thread"].is_object()
        })["result"]["thread"]
            .clone();
        set(&mut th, "id", json!(t.id));
        set(&mut th, "sessionId", json!(t.id));
        set(&mut th, "cwd", json!(t.cwd));
        set(&mut th, "model", json!(t.model));
        set(&mut th, "preview", json!(t.preview));
        set(&mut th, "createdAt", json!(t.created));
        let updated = if t.updated > 0 { t.updated } else { t.created };
        set(&mut th, "updatedAt", json!(updated));
        set(&mut th, "recencyAt", json!(updated));
        set(&mut th, "cliVersion", json!(self.version));
        set(
            &mut th,
            "path",
            json!(rollout_path(&t.id, t.created).display().to_string()),
        );
        set(
            &mut th,
            "environments",
            json!([{"environmentId":"local","cwd":t.cwd,"runtimeWorkspaceRoots":[t.cwd]}]),
        );
        if !loaded {
            set(&mut th, "status", json!({"type":"notLoaded"}));
            set(&mut th, "environments", Value::Null);
        }
        th
    }

    fn start_result(
        &self,
        template: Value,
        t: &Thread,
        approval: &str,
        sandbox: Option<Value>,
    ) -> Value {
        let mut r = template;
        r["thread"] = self.thread_json(t, true);
        set(&mut r, "cwd", json!(t.cwd));
        set(&mut r, "runtimeWorkspaceRoots", json!([t.cwd]));
        set(&mut r, "model", json!(t.model));
        set(&mut r, "approvalPolicy", json!(approval));
        if let Some(s) = sandbox {
            set(&mut r, "sandbox", s);
        }
        r
    }

    fn sandbox_for(name: &str) -> Option<Value> {
        let base = rec("simple", |m| {
            m["id"] == 2 && m["result"]["sandbox"].is_object()
        })["result"]["sandbox"]
            .clone();
        match name {
            "workspace-write" => Some(base),
            "read-only" => Some(json!({"type":"readOnly","networkAccess":false})),
            "danger-full-access" => Some(json!({"type":"dangerFullAccess"})),
            _ => None,
        }
    }

    fn turn_obj(&self, status: &str, items: Value, done: bool, started_ms: u64) -> Value {
        let mut t = rec("simple", |m| m["method"] == "turn/completed")["params"]["turn"].clone();
        set(&mut t, "id", json!(self.turn_id));
        set(&mut t, "rootTurnId", json!(self.turn_id));
        set(&mut t, "status", json!(status));
        set(&mut t, "items", items);
        set(&mut t, "startedAt", json!(started_ms / 1000));
        if done {
            set(&mut t, "completedAt", json!(now_secs()));
            set(
                &mut t,
                "durationMs",
                json!(now_ms().saturating_sub(started_ms)),
            );
        } else {
            set(&mut t, "completedAt", Value::Null);
            set(&mut t, "durationMs", Value::Null);
        }
        t
    }

    fn item(&mut self, tmpl_msg: &str, kind: &str) -> Value {
        // Build an item from a recorded `item/<which>` notification of type `kind`.
        let file = if kind == "commandExecution" {
            "tool_allow"
        } else {
            "simple"
        };
        let m = rec(file, |m| {
            m["method"] == tmpl_msg && m["params"]["item"]["type"] == kind
        });
        m["params"]["item"].clone()
    }

    fn next_item_id(&mut self) -> String {
        self.item_seq += 1;
        format!("msg_{}", self.item_seq)
    }

    fn item_note(&self, io: &mut Io, method: &str, item: &Value) {
        let started = item["startedAtMs"].clone();
        let mut params = json!({"item": item, "threadId": self.thread_id, "turnId": self.turn_id});
        if method == "item/started" {
            params["startedAtMs"] = started;
        } else {
            params["contentTokenCount"] = Value::Null;
            params["completedAtMs"] = item["completedAtMs"].clone();
        }
        self.notify(io, method, params);
    }

    fn model_list(&self) -> Value {
        let mut r = rec("handshake", |m| {
            m["id"] == 4 && m["result"]["data"].is_array()
        })["result"]
            .clone();
        if !self.scenario.models.is_empty() {
            let proto = r["data"][0].clone();
            r["data"] = Value::Array(
                self.scenario
                    .models
                    .iter()
                    .map(|m| {
                        let mut e = proto.clone();
                        e["id"] = json!(m.id);
                        e["model"] = json!(m.id);
                        e["displayName"] = json!(m.name.clone().unwrap_or_else(|| m.id.clone()));
                        e["description"] = json!(m.description.clone().unwrap_or_default());
                        e
                    })
                    .collect(),
            );
        }
        r
    }

    fn collab_list(&self) -> Value {
        let mut r = rec("handshake", |m| {
            m["id"] == 3 && m["result"]["data"].is_array()
        })["result"]
            .clone();
        if !self.scenario.modes.is_empty() {
            r["data"] = Value::Array(
                self.scenario
                    .modes
                    .iter()
                    .map(|m| {
                        json!({"name": m.name.clone().unwrap_or_else(|| m.id.clone()),
                               "mode": m.id, "model": null, "reasoning_effort": null})
                    })
                    .collect(),
            );
        }
        r
    }

    fn find_thread(&self, id: &str) -> Option<&Thread> {
        self.threads.iter().find(|t| t.id == id)
    }

    /// Write the thread's rollout. Real codex-cli 0.161.0 writes none when a thread is opened:
    /// `thread/resume` of a thread that never had a turn fails with "no rollout found for
    /// thread id" (the rollout appears with the first turn), so a thread is stored from its
    /// first `turn/start` on.
    fn store_thread(&self, id: &str) {
        if let Some(t) = self.find_thread(id) {
            crate::store::put(STORE, id, t);
        }
    }

    fn handle_request(&mut self, io: &mut Io, v: &Value, in_turn: bool) -> Incoming {
        let id = v["id"].clone();
        let params = &v["params"];
        let method = v["method"].as_str().unwrap_or("");
        match method {
            "initialize" => {
                let mut r = rec("simple", |m| {
                    m["id"] == 1 && m["result"]["userAgent"].is_string()
                })["result"]
                    .clone();
                let client = params["clientInfo"]["name"].as_str().unwrap_or("client");
                self.originator = client.to_string();
                let cver = params["clientInfo"]["version"].as_str().unwrap_or("0");
                set(
                    &mut r,
                    "userAgent",
                    json!(format!(
                        "{client}/{} (fake; fake) fake ({client}; {cver})",
                        self.version
                    )),
                );
                self.reply(io, &id, r);
                Incoming::Handled
            }
            "thread/start" => {
                let cwd = params["cwd"]
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| {
                        std::env::current_dir()
                            .ok()
                            .map(|p| p.display().to_string())
                    })
                    .unwrap_or_else(|| "/".into());
                let model = params["model"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| self.default_model());
                let thread = Thread {
                    id: self.first_id.take().unwrap_or_else(uuid7),
                    cwd,
                    model,
                    preview: String::new(),
                    history: vec![],
                    created: now_secs(),
                    updated: 0,
                    turns: vec![],
                };
                self.remote_status_once(io);
                let approval = params["approvalPolicy"]
                    .as_str()
                    .unwrap_or("on-request")
                    .to_string();
                let sandbox = params["sandbox"].as_str().and_then(Self::sandbox_for);
                let template = rec("simple", |m| {
                    m["id"] == 2 && m["result"]["thread"].is_object()
                })["result"]
                    .clone();
                let r = self.start_result(template, &thread, &approval, sandbox);
                let tj = r["thread"].clone();
                self.reply(io, &id, r);
                self.notify(io, "thread/started", json!({"thread": tj}));
                self.threads.push(thread);
                Incoming::Handled
            }
            "thread/resume" => {
                let tid = params["threadId"].as_str().unwrap_or("");
                match self.find_thread(tid).cloned() {
                    None => {
                        self.error(
                            io,
                            &id,
                            -32600,
                            &format!("no rollout found for thread id {tid}"),
                        );
                    }
                    Some(t) => {
                        self.remote_status_once(io);
                        let template = rec("resume", |m| {
                            m["id"] == 2 && m["result"]["thread"].is_object()
                        })["result"]
                            .clone();
                        let turns = if t.turns.is_empty() {
                            turns_from_history(&t.history, t.created)
                        } else {
                            t.turns.clone()
                        };
                        let thread = Thread {
                            cwd: params["cwd"].as_str().unwrap_or(&t.cwd).to_string(),
                            ..t.clone()
                        };
                        let mut r = self.start_result(template, &thread, "on-request", None);
                        // The conversation comes back inside the result, as 0.169.1 still does
                        // for a paginated thread (and says so in a `deprecationNotice`).
                        r["thread"]["turns"] = Value::Array(turns);
                        set(&mut r, "thread.historyMode", json!("paginated"));
                        set(
                            &mut r,
                            "turnsBackwardsCursor",
                            json!(json!({"requestedThreadId":t.id,"rolloutOrdinal":1,
                                "includeAnchor":true,"scope":{"kind":"turns"}})
                            .to_string()),
                        );
                        set(
                            &mut r,
                            "itemsBackwardsCursor",
                            json!(json!({"requestedThreadId":t.id,"rolloutOrdinal":9,
                                "includeAnchor":true,"scope":{"kind":"itemsByCreatedAtOrdinal"}})
                            .to_string()),
                        );
                        set(&mut r, "initialTurnsPage", Value::Null);
                        let mut n = rec("resume", |m| m["method"] == "deprecationNotice");
                        set(&mut n, "emittedAtMs", json!(now_ms()));
                        io.send(&n);
                        self.notify(
                            io,
                            "thread/status/changed",
                            json!({"threadId": t.id, "status": {"type": "idle"}}),
                        );
                        self.reply(io, &id, r);
                    }
                }
                Incoming::Handled
            }
            "thread/list" => {
                let mut r = rec("lifecycle", |m| {
                    m["id"] == 6 && m["result"]["data"].is_array()
                })["result"]
                    .clone();
                let mut listed: Vec<&Thread> = self
                    .threads
                    .iter()
                    .filter(|t| params["cwd"].as_str().is_none_or(|c| c == t.cwd))
                    .collect();
                listed.sort_by_key(|t| std::cmp::Reverse(t.updated.max(t.created)));
                r["data"] = Value::Array(
                    listed
                        .into_iter()
                        .map(|t| self.thread_json(t, false))
                        .collect(),
                );
                self.reply(io, &id, r);
                Incoming::Handled
            }
            "thread/read" => {
                let tid = params["threadId"].as_str().unwrap_or("");
                match self.find_thread(tid) {
                    Some(t) => {
                        let mut r = rec("lifecycle", |m| {
                            m["id"] == 7 && m["result"]["thread"].is_object()
                        })["result"]
                            .clone();
                        r["thread"] = self.thread_json(t, true);
                        self.reply(io, &id, r);
                    }
                    None => self.error(io, &id, -32600, &format!("thread not found: {tid}")),
                }
                Incoming::Handled
            }
            "collaborationMode/list" => {
                let r = self.collab_list();
                self.reply(io, &id, r);
                Incoming::Handled
            }
            "model/list" => {
                let r = self.model_list();
                self.reply(io, &id, r);
                Incoming::Handled
            }
            "turn/start" => {
                let tid = params["threadId"].as_str().unwrap_or("").to_string();
                if self.find_thread(&tid).is_none() {
                    let msg = if uuid_like(&tid) {
                        format!("thread not found: {tid}")
                    } else {
                        "invalid thread id: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()
                    };
                    self.error(io, &id, -32600, &msg);
                    return Incoming::Handled;
                }
                let text = params["input"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|i| i["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();
                if let Some(message) = self.refusal(&text) {
                    self.error(io, &id, -32600, &message);
                    return Incoming::Handled;
                }
                self.thread_id = tid.clone();
                self.turn_id = uuid7();
                self.turn_request = id;
                if params["collaborationMode"].is_object() {
                    let mut n = rec("mode", |m| m["method"] == "thread/settings/updated");
                    set(&mut n, "params.threadId", json!(tid));
                    set(&mut n, "emittedAtMs", json!(now_ms()));
                    io.send(&n);
                }
                if let Some(t) = self.threads.iter_mut().find(|t| t.id == tid) {
                    if t.preview.is_empty() {
                        t.preview = text.clone();
                    }
                    t.history.push(HistoryMessage {
                        role: "user".into(),
                        text: text.clone(),
                    });
                }
                self.store_thread(&tid);
                if let Some(t) = self.find_thread(&tid) {
                    append_rollout(
                        t,
                        &self.version,
                        &self.originator,
                        &[message_line("user", &text)],
                    );
                }
                Incoming::Prompt(text)
            }
            // Shapes recorded from codex-cli 0.161.0 (`reference/codex/steer.jsonl`): the result
            // names the running turn; the prompt joins it and is answered with the turn.
            "turn/steer" => {
                let text = params["input"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|i| i["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();
                let expected = params["expectedTurnId"].as_str().unwrap_or("");
                if !in_turn {
                    self.error(io, &id, -32600, "no active turn to steer");
                } else if expected != self.turn_id {
                    let msg = format!(
                        "expected active turn id `{expected}` but found `{}`",
                        self.turn_id
                    );
                    self.error(io, &id, -32600, &msg);
                } else if let Some(message) = self.refusal(&text) {
                    self.error(io, &id, -32600, &message);
                } else {
                    self.steered.push(text);
                    let turn = self.turn_id.clone();
                    self.reply(io, &id, json!({"turnId": turn}));
                }
                Incoming::Handled
            }
            "thread/settings/update" => {
                let tid = params["threadId"].as_str().unwrap_or("").to_string();
                let Some(t) = self.threads.iter_mut().find(|t| t.id == tid) else {
                    self.error(io, &id, -32600, &format!("thread not found: {tid}"));
                    return Incoming::Handled;
                };
                if let Some(m) = params["model"].as_str() {
                    t.model = m.to_string();
                }
                let model = t.model.clone();
                self.reply(io, &id, json!({}));
                let mut n = rec("mode", |m| m["method"] == "thread/settings/updated");
                set(&mut n, "params.threadId", json!(tid));
                set(&mut n, "params.threadSettings.model", json!(model));
                match params["collaborationMode"]["mode"].as_str() {
                    Some(mode) => {
                        set(
                            &mut n,
                            "params.threadSettings.collaborationMode.mode",
                            json!(mode),
                        );
                        set(
                            &mut n,
                            "params.threadSettings.collaborationMode.settings.model",
                            json!(model),
                        );
                    }
                    None => set(
                        &mut n,
                        "params.threadSettings.collaborationMode",
                        Value::Null,
                    ),
                }
                set(&mut n, "emittedAtMs", json!(now_ms()));
                io.send(&n);
                Incoming::Handled
            }
            "turn/interrupt" => {
                if in_turn {
                    self.reply(io, &id, json!({}));
                    Incoming::Interrupt
                } else {
                    self.error(io, &id, -32600, "no active turn to interrupt");
                    Incoming::Handled
                }
            }
            other => {
                self.error(
                    io,
                    &id,
                    -32600,
                    &format!(
                        "Invalid request: unknown variant `{other}`, expected one of `initialize`, `thread/start`, `thread/resume`, `turn/start`, `turn/steer`, `turn/interrupt`"
                    ),
                );
                Incoming::Handled
            }
        }
    }

    /// Real codex announces its remote-control status once, around the first thread request.
    fn remote_status_once(&mut self, io: &mut Io) {
        if self.sent_remote_status {
            return;
        }
        self.sent_remote_status = true;
        let mut n = rec("simple", |m| m["method"] == "remoteControl/status/changed");
        set(&mut n, "emittedAtMs", json!(now_ms()));
        io.send(&n);
    }

    fn request_to_client(&mut self, io: &mut Io, method: &str, params: Value) -> Value {
        let id = json!(self.next_request_id);
        self.next_request_id += 1;
        io.send(&json!({"method": method, "id": id, "params": params}));
        id
    }

    fn resolved(&self, io: &mut Io, request_id: &Value) {
        self.notify(
            io,
            "serverRequest/resolved",
            json!({"threadId": self.thread_id, "requestId": request_id}),
        );
    }
}

fn uuid_like(s: &str) -> bool {
    s.len() == 36 && s.matches('-').count() == 4
}

fn plan_status(s: &str) -> &'static str {
    match s {
        "in_progress" | "inProgress" => "inProgress",
        "completed" => "completed",
        _ => "pending",
    }
}

impl Persona for Codex {
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return Incoming::Handled;
        };
        let has_method = v["method"].is_string();
        let has_id = !v["id"].is_null();
        match (has_method, has_id) {
            (true, true) => self.handle_request(io, &v, in_turn),
            (false, true) => {
                let body = if v["error"].is_object() {
                    json!({"decision": "decline"})
                } else {
                    v["result"].clone()
                };
                Incoming::Response {
                    id: v["id"].clone(),
                    body,
                }
            }
            _ => Incoming::Handled, // `initialized` and other notifications
        }
    }

    fn turn_begin(&mut self, io: &mut Io, prompt: &str, _id: Option<&str>, stats: &mut TurnStats) {
        stats.started = Some(std::time::Instant::now());
        self.items.clear();
        self.steered.clear();
        let turn = self.turn_obj("inProgress", json!([]), false, now_ms());
        let mut turn_start = turn.clone();
        set(&mut turn_start, "startedAt", Value::Null);
        let req = self.turn_request.clone();
        self.reply(io, &req, json!({"turn": turn_start}));
        self.notify(
            io,
            "thread/status/changed",
            json!({"threadId": self.thread_id, "status": {"type":"active","activeFlags":[]}}),
        );
        self.notify(
            io,
            "turn/started",
            json!({"threadId": self.thread_id, "turn": turn}),
        );
        self.user_message(io, prompt);
    }

    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, _stats: &mut TurnStats) {
        let id = self.next_item_id();
        let started = now_ms();
        let mut item = self.item("item/started", "agentMessage");
        set(&mut item, "id", json!(id));
        set(&mut item, "text", json!(""));
        set(&mut item, "startedAtMs", json!(started));
        self.item_note(io, "item/started", &item);
        for c in crate::util::chunk_text(text, chunks) {
            self.notify(
                io,
                "item/agentMessage/delta",
                json!({"threadId": self.thread_id, "turnId": self.turn_id, "itemId": id, "delta": c}),
            );
        }
        set(&mut item, "text", json!(text));
        set(&mut item, "completedAtMs", json!(now_ms()));
        self.item_note(io, "item/completed", &item);
        self.items.push(item);
    }

    fn think(&mut self, io: &mut Io, text: &str, _stats: &mut TurnStats) {
        let id = format!("rs_{}", alnum(16));
        let mut item = json!({"type":"reasoning","id":id,"summary":[],"content":[],"startedAtMs":now_ms(),"completedAtMs":null});
        self.item_note(io, "item/started", &item);
        self.notify(
            io,
            "item/reasoning/textDelta",
            json!({"threadId": self.thread_id, "turnId": self.turn_id, "itemId": id, "contentIndex": 0, "delta": text}),
        );
        item["summary"] = json!([text]);
        item["completedAtMs"] = json!(now_ms());
        self.item_note(io, "item/completed", &item);
        self.items.push(item);
    }

    fn tool_call(&mut self, io: &mut Io, tool: &ToolStep, _stats: &mut TurnStats) -> Option<Value> {
        let call_id = format!("call_{}", alnum(12));
        let started = now_ms();
        let cwd = self
            .find_thread(&self.thread_id)
            .map(|t| t.cwd.clone())
            .unwrap_or_default();
        let mut item;
        let (req_method, req_params);
        if is_file_change(&tool.name) {
            let path = tool.input["path"]
                .as_str()
                .or_else(|| tool.input["file_path"].as_str())
                .unwrap_or("file")
                .to_string();
            item = json!({"type":"fileChange","id":call_id,
                "changes":[{"path":path,"kind":{"type":"update","move_path":null},"diff":""}],
                "status":"inProgress","startedAtMs":started,"completedAtMs":null});
            req_method = "item/fileChange/requestApproval";
            req_params = json!({"threadId":self.thread_id,"turnId":self.turn_id,"itemId":call_id,
                "startedAtMs":started,"reason":null,"grantRoot":null});
        } else {
            let command = shell_command(tool);
            let raw = command
                .trim_start_matches("/bin/bash -lc '")
                .trim_end_matches('\'')
                .to_string();
            item = self.item("item/started", "commandExecution");
            set(&mut item, "id", json!(call_id));
            set(&mut item, "command", json!(command));
            set(&mut item, "cwd", json!(cwd));
            set(
                &mut item,
                "commandActions",
                json!([{"type":"unknown","command":raw}]),
            );
            set(&mut item, "startedAtMs", json!(started));
            let amend: Vec<&str> = raw.split_whitespace().collect();
            let mut p = rec("tool_allow", |m| {
                m["method"] == "item/commandExecution/requestApproval"
            })["params"]
                .clone();
            set(&mut p, "threadId", json!(self.thread_id));
            set(&mut p, "turnId", json!(self.turn_id));
            set(&mut p, "itemId", json!(call_id));
            set(&mut p, "startedAtMs", json!(started));
            set(&mut p, "command", json!(command));
            set(&mut p, "cwd", json!(cwd));
            set(
                &mut p,
                "commandActions",
                json!([{"type":"unknown","command":raw}]),
            );
            set(&mut p, "proposedExecpolicyAmendment", json!(amend));
            set(
                &mut p,
                "availableDecisions",
                json!(["accept", {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": amend}}, "cancel"]),
            );
            req_method = "item/commandExecution/requestApproval";
            req_params = p;
        }
        self.item_note(io, "item/started", &item);
        self.pending_item = Some((call_id, req_method.to_string()));
        self.items.push(item);
        if tool.needs_permission {
            return Some(self.request_to_client(io, req_method, req_params));
        }
        self.finish_item(io, tool, true);
        None
    }

    fn tool_answered(
        &mut self,
        io: &mut Io,
        tool: &ToolStep,
        body: &Value,
        _stats: &mut TurnStats,
    ) {
        let d = &body["decision"];
        let allow = d.as_str().is_some_and(|s| s.starts_with("accept")) || d.is_object();
        // The request id was the last one handed out.
        let rid = json!(self.next_request_id.saturating_sub(1));
        self.resolved(io, &rid);
        self.finish_item(io, tool, allow);
    }

    fn question(&mut self, io: &mut Io, ask: &AskStep, _stats: &mut TurnStats) -> Value {
        let item_id = format!("call_{}", alnum(12));
        let mut p =
            rec("question", |m| m["method"] == "item/tool/requestUserInput")["params"].clone();
        set(&mut p, "threadId", json!(self.thread_id));
        set(&mut p, "turnId", json!(self.turn_id));
        set(&mut p, "itemId", json!(item_id));
        set(
            &mut p,
            "questions",
            json!([{"id":"q1","header":ask.header.clone().unwrap_or_default(),
                    "question":ask.question,"isOther":true,"isSecret":false,
                    "options":ask.options.iter().map(|o| json!({"label":o.label,"description":o.description})).collect::<Vec<_>>()}]),
        );
        self.request_to_client(io, "item/tool/requestUserInput", p)
    }

    fn question_answered(
        &mut self,
        io: &mut Io,
        _ask: &AskStep,
        _body: &Value,
        _stats: &mut TurnStats,
    ) {
        let rid = json!(self.next_request_id.saturating_sub(1));
        self.resolved(io, &rid);
    }

    fn plan(
        &mut self,
        io: &mut Io,
        entries: &[PlanEntry],
        _needs_approval: bool,
        _stats: &mut TurnStats,
    ) -> Option<Value> {
        self.notify(
            io,
            "turn/plan/updated",
            json!({"threadId": self.thread_id, "turnId": self.turn_id, "explanation": null,
                   "plan": entries.iter().map(|e| json!({"step": e.content, "status": plan_status(&e.status)})).collect::<Vec<_>>()}),
        );
        None
    }

    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats) {
        // Prompts steered into the turn are taken in when the model call in flight ends, and
        // answered by one more (codex-cli 0.161.0, `reference/codex/steer.jsonl`): a `userMessage`
        // item and a reply, inside the same turn. An interrupted or failed turn drops them.
        let steered = std::mem::take(&mut self.steered);
        if matches!(outcome, Outcome::Completed) {
            let prefix = self
                .scenario
                .echo_prefix
                .clone()
                .unwrap_or_else(|| "echo: ".into());
            for text in steered {
                self.user_message(io, &text);
                let reply = format!("{prefix}{text}");
                self.say(io, &reply, 1, stats);
                stats.final_text = reply.clone();
                stats.said.push(reply);
            }
        }
        let started = stats
            .started
            .map(|s| now_ms().saturating_sub(s.elapsed().as_millis() as u64))
            .unwrap_or_else(now_ms);
        let u = &stats.usage;
        let usage = json!({"totalTokens":u.input_tokens+u.output_tokens,"inputTokens":u.input_tokens,
            "cachedInputTokens":u.cached_tokens,"cacheWriteInputTokens":0,
            "outputTokens":u.output_tokens,"reasoningOutputTokens":u.thought_tokens});
        let (status, items, error) = match outcome {
            Outcome::Completed => (
                "completed",
                Value::Array(
                    self.items
                        .iter()
                        .filter(|i| i["type"] == "agentMessage")
                        .cloned()
                        .collect(),
                ),
                Value::Null,
            ),
            Outcome::Interrupted => ("interrupted", json!([]), Value::Null),
            Outcome::Failed(msg) => {
                let e = json!({"message": msg, "codexErrorInfo": "other", "additionalDetails": null, "misalignment": null});
                self.notify(
                    io,
                    "error",
                    json!({"error": e, "willRetry": false, "threadId": self.thread_id, "turnId": self.turn_id}),
                );
                ("failed", json!([]), e)
            }
        };
        if matches!(outcome, Outcome::Completed) {
            self.notify(
                io,
                "thread/tokenUsage/updated",
                json!({"threadId": self.thread_id, "turnId": self.turn_id,
                       "tokenUsage": {"total": usage, "last": usage, "modelContextWindow": u.context_window}}),
            );
        }
        self.notify(
            io,
            "thread/status/changed",
            json!({"threadId": self.thread_id, "status": {"type": "idle"}}),
        );
        let mut turn = self.turn_obj(status, items, true, started);
        set(&mut turn, "error", error);
        if matches!(outcome, Outcome::Interrupted | Outcome::Failed(_)) {
            set(&mut turn, "itemsView", json!("notLoaded"));
        }
        // What the conversation keeps of the turn: every item, completed.
        let mut kept = turn.clone();
        set(&mut kept, "items", Value::Array(self.items.clone()));
        set(&mut kept, "itemsView", json!("full"));
        // Rollout messages: the replies, and the prompts steered into the turn (the first
        // `userMessage` is the prompt, written when the turn started).
        let lines: Vec<Value> = self
            .items
            .iter()
            .skip(1)
            .filter_map(|i| match i["type"].as_str() {
                Some("agentMessage") => Some(message_line("assistant", i["text"].as_str()?)),
                Some("userMessage") => {
                    Some(message_line("user", i["content"][0]["text"].as_str()?))
                }
                _ => None,
            })
            .collect();
        if let Some(th) = self.threads.iter_mut().find(|th| th.id == self.thread_id) {
            th.history.push(HistoryMessage {
                role: "assistant".into(),
                text: stats.final_text.clone(),
            });
            th.turns.push(kept);
            th.updated = now_secs();
        }
        let tid = self.thread_id.clone();
        self.store_thread(&tid);
        if let Some(t) = self.find_thread(&tid) {
            append_rollout(t, &self.version, &self.originator, &lines);
        }
        // The turn is in storage before the client hears it ended: a client that resumes the
        // thread right after `turn/completed` finds all of it.
        self.notify(io, "turn/completed", json!({"threadId": tid, "turn": turn}));
    }
}

impl Codex {
    /// A `userMessage` item for `text`, started and completed.
    fn user_message(&mut self, io: &mut Io, text: &str) {
        let now = now_ms();
        let mut item = self.item("item/started", "userMessage");
        set(&mut item, "id", json!(uuid7()));
        set(
            &mut item,
            "content",
            json!([{"type":"text","text":text,"text_elements":[]}]),
        );
        set(&mut item, "startedAtMs", json!(now));
        self.item_note(io, "item/started", &item);
        set(&mut item, "completedAtMs", json!(now));
        self.item_note(io, "item/completed", &item);
        self.items.push(item);
    }

    fn finish_item(&mut self, io: &mut Io, tool: &ToolStep, allow: bool) {
        let Some((call_id, _)) = self.pending_item.take() else {
            return;
        };
        let Some(pos) = self.items.iter().position(|i| i["id"] == call_id) else {
            return;
        };
        let mut item = self.items[pos].clone();
        let text = if allow {
            // The image has no codex shape here; the file write still happens.
            let _ = tool.run();
            tool.result_if_allowed
                .clone()
                .unwrap_or_else(|| "ok".into())
        } else {
            tool.result_if_denied.clone().unwrap_or_default()
        };
        let is_file = item["type"] == "fileChange";
        set(
            &mut item,
            "status",
            json!(if allow { "completed" } else { "declined" }),
        );
        set(&mut item, "completedAtMs", json!(now_ms()));
        if !is_file {
            set(
                &mut item,
                "aggregatedOutput",
                if allow { json!(text) } else { Value::Null },
            );
            set(
                &mut item,
                "exitCode",
                if allow { json!(0) } else { Value::Null },
            );
            if allow {
                set(&mut item, "durationMs", json!(0));
                set(
                    &mut item,
                    "processId",
                    json!(format!("{}", 40_000 + self.item_seq)),
                );
                set(&mut item, "source", json!("unifiedExecStartup"));
            } else {
                set(&mut item, "durationMs", Value::Null);
            }
        }
        self.item_note(io, "item/completed", &item);
        self.items[pos] = item;
    }
}
