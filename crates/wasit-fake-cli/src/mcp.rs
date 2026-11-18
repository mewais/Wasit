//! A minimal MCP client for the `mcp_call` scenario step: start one of the MCP servers the
//! fake CLI was configured with (stdio), `initialize`, `tools/call`, read the result, stop it.
//! Blocking: the turn waits for the tool, as a real CLI waits for a tool call.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::process::Stdio;

use serde_json::{json, Value};

/// How to start one stdio MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpLaunch {
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

/// The stdio servers of a claude `--mcp-config` JSON (`{"mcpServers": {name: {command, args, env}}}`).
pub fn servers_from_config(cfg: &str) -> BTreeMap<String, McpLaunch> {
    let v: Value = match serde_json::from_str(cfg) {
        Ok(v) => v,
        // A path to a file holding the JSON.
        Err(_) => std::fs::read_to_string(cfg)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Value::Null),
    };
    let mut out = BTreeMap::new();
    if let Some(m) = v.get("mcpServers").and_then(Value::as_object) {
        for (name, s) in m {
            let Some(command) = s.get("command").and_then(Value::as_str) else {
                continue;
            };
            let args = s
                .get("args")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let env = s
                .get("env")
                .and_then(Value::as_object)
                .map(|e| {
                    e.iter()
                        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            out.insert(
                name.clone(),
                McpLaunch {
                    command: command.to_string(),
                    args,
                    env,
                },
            );
        }
    }
    out
}

/// Call `tool` with `arguments`. `Ok(text)` with the tool's text content, `Err(text)` when the
/// tool reported an error (or the server could not be used).
pub fn call(server: &McpLaunch, tool: &str, arguments: &Value) -> Result<String, String> {
    let mut child = wasit_process::std_command(&server.command)
        .args(&server.args)
        .envs(server.env.iter().cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("cannot start MCP server {}: {e}", server.command))?;
    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    let mut out = BufReader::new(child.stdout.take().ok_or("no stdout")?);
    let mut send = |v: Value| -> Result<(), String> {
        writeln!(stdin, "{v}").map_err(|e| e.to_string())?;
        stdin.flush().map_err(|e| e.to_string())
    };
    let mut read_reply = |id: u64| -> Result<Value, String> {
        let mut line = String::new();
        loop {
            line.clear();
            if out.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                return Err("the MCP server closed its output".into());
            }
            let Ok(v) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if v.get("id").and_then(Value::as_u64) == Some(id) && v.get("method").is_none() {
                return Ok(v);
            }
        }
    };
    let result = (|| {
        send(
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},
            "clientInfo":{"name":"wasit-fake-cli","version":env!("CARGO_PKG_VERSION")}}}),
        )?;
        read_reply(1)?;
        send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))?;
        send(
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":tool,"arguments":arguments}}),
        )?;
        let r = read_reply(2)?;
        if let Some(e) = r.get("error") {
            return Err(format!("MCP error: {e}"));
        }
        let res = &r["result"];
        let text: String = res["content"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|c| c["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        if res["isError"].as_bool() == Some(true) {
            Err(text)
        } else {
            Ok(text)
        }
    })();
    // The server's job is done; it may linger on its own connections.
    let _ = child.kill();
    let _ = child.wait();
    result
}

/// Replace `{{name.path.0.x}}` with values saved by earlier steps (strings as they are, other
/// JSON as text). Unknown names are left in place.
pub fn substitute(s: &str, vars: &BTreeMap<String, Value>) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(i) = rest.find("{{") {
        out.push_str(&rest[..i]);
        let after = &rest[i + 2..];
        let Some(j) = after.find("}}") else {
            out.push_str(&rest[i..]);
            return out;
        };
        let path = after[..j].trim();
        let mut parts = path.split('.');
        let found = parts.next().and_then(|n| vars.get(n)).and_then(|v| {
            parts.try_fold(v, |cur, p| match p.parse::<usize>() {
                Ok(n) => cur.get(n),
                Err(_) => cur.get(p),
            })
        });
        match found {
            Some(Value::String(s)) => out.push_str(s),
            Some(v) => out.push_str(&v.to_string()),
            None => out.push_str(&rest[i..i + 2 + j + 2]),
        }
        rest = &after[j + 2..];
    }
    out.push_str(rest);
    out
}

/// [`substitute`] in every string of a JSON value.
pub fn substitute_json(v: &Value, vars: &BTreeMap<String, Value>) -> Value {
    match v {
        Value::String(s) => Value::String(substitute(s, vars)),
        Value::Array(a) => Value::Array(a.iter().map(|x| substitute_json(x, vars)).collect()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| (k.clone(), substitute_json(x, vars)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_saved_values_and_parses_configs() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "s".to_string(),
            json!({"task": {"id": "t1"}, "tasks": [{"result": "42"}]}),
        );
        assert_eq!(
            substitute("id {{s.task.id}} → {{s.tasks.0.result}}", &vars),
            "id t1 → 42"
        );
        assert_eq!(substitute("{{nope.x}} stays", &vars), "{{nope.x}} stays");
        let cfg = r#"{"mcpServers":{"wasl":{"type":"stdio","command":"/bin/x","args":["mcp","--session","s1"],"env":{"A":"1"}}}}"#;
        let m = servers_from_config(cfg);
        assert_eq!(m["wasl"].args, ["mcp", "--session", "s1"]);
        assert_eq!(m["wasl"].env, [("A".to_string(), "1".to_string())]);
    }
}
