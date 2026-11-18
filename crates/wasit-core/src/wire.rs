//! Small pure helpers shared by the driver crates: JSON-RPC message
//! classification/encoding (codex app-server and ACP both speak JSON-RPC 2.0
//! over NDJSON) and JSON field access.

use serde_json::{json, Value};

use crate::event::{GateChoice, GateChoiceKind};
use crate::protocol::OutLine;

/// A classified JSON-RPC message read from a CLI.
#[derive(Debug, Clone, PartialEq)]
pub enum RpcIn {
    /// A request from the CLI that needs our response.
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    /// A notification from the CLI.
    Notification { method: String, params: Value },
    /// A success response to one of our requests.
    Response { id: Value, result: Value },
    /// An error response to one of our requests.
    Error {
        id: Value,
        code: i64,
        message: String,
    },
}

impl RpcIn {
    /// Classify a parsed JSON value; `None` when it is not JSON-RPC shaped.
    pub fn classify(v: &Value) -> Option<RpcIn> {
        let obj = v.as_object()?;
        let method = obj.get("method").and_then(Value::as_str);
        let id = obj.get("id").cloned();
        match (method, id) {
            (Some(m), Some(id)) => Some(RpcIn::Request {
                id,
                method: m.to_string(),
                params: obj.get("params").cloned().unwrap_or(Value::Null),
            }),
            (Some(m), None) => Some(RpcIn::Notification {
                method: m.to_string(),
                params: obj.get("params").cloned().unwrap_or(Value::Null),
            }),
            (None, Some(id)) => {
                if let Some(err) = obj.get("error") {
                    Some(RpcIn::Error {
                        id,
                        code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
                        message: err
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("json-rpc error")
                            .to_string(),
                    })
                } else {
                    Some(RpcIn::Response {
                        id,
                        result: obj.get("result").cloned().unwrap_or(Value::Null),
                    })
                }
            }
            (None, None) => None,
        }
    }
}

/// `{"jsonrpc":"2.0","id":id,"method":method,"params":params}`.
pub fn rpc_request(id: u64, method: &str, params: Value) -> OutLine {
    OutLine::json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
}

/// `{"jsonrpc":"2.0","method":method[,"params":params]}`.
pub fn rpc_notification(method: &str, params: Option<Value>) -> OutLine {
    match params {
        Some(p) => OutLine::json(&json!({"jsonrpc": "2.0", "method": method, "params": p})),
        None => OutLine::json(&json!({"jsonrpc": "2.0", "method": method})),
    }
}

/// `{"jsonrpc":"2.0","id":id,"result":result}`.
pub fn rpc_result(id: &Value, result: Value) -> OutLine {
    OutLine::json(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

/// `{"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}`.
pub fn rpc_error(id: &Value, code: i64, message: &str) -> OutLine {
    OutLine::json(&json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}))
}

/// The id of a JSON-RPC message as a `u64`, when it is one.
pub fn id_u64(id: &Value) -> Option<u64> {
    id.as_u64()
}

/// A JSON-RPC id rendered as a routing-id fragment (`7`, `"abc"` → `abc`).
pub fn id_text(id: &Value) -> String {
    match id {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `v[key]` as a `&str`, or `""`.
pub fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// `v[key]` as an owned non-empty `String`.
pub fn opt_s(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|x| !x.is_empty())
        .map(str::to_string)
}

/// `v[key]` as a `u64`, or 0.
pub fn u(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// The choices of an MCP elicitation (claude `elicitation`, codex
/// `mcpServer/elicitation/request`), each with the `content` to send back on accept (`None`
/// for decline / cancel).
///
/// The first enum property of the requested schema
/// (`{"properties": {"grant": {"enum": [...], "enumNames": [...]}}}`) gives one accept choice
/// per value, labelled by the server's `enumNames` (its own wording); without one there is a
/// single neutral "Accept". MCP's `decline` and `cancel` follow, neutral.
pub fn elicitation_choices(schema: Option<&Value>) -> Vec<(GateChoice, Option<Value>)> {
    let mut out = Vec::new();
    let props = schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object);
    for (key, prop) in props.into_iter().flatten() {
        let Some(values) = prop.get("enum").and_then(Value::as_array) else {
            continue;
        };
        if values.is_empty() {
            continue;
        }
        let names = prop.get("enumNames").and_then(Value::as_array);
        for (i, v) in values.iter().enumerate() {
            let value = v
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string());
            let name = names.and_then(|n| n.get(i)).and_then(Value::as_str);
            let choice = match name {
                Some(n) => {
                    GateChoice::agent(format!("accept:{value}"), n, GateChoiceKind::AllowOnce)
                }
                None => GateChoice::agent(
                    format!("accept:{value}"),
                    value.clone(),
                    GateChoiceKind::AllowOnce,
                ),
            };
            out.push((choice, Some(json!({ key.clone(): v.clone() }))));
        }
        break;
    }
    if out.is_empty() {
        out.push((
            GateChoice::neutral("accept", "Accept", GateChoiceKind::AllowOnce),
            Some(json!({})),
        ));
    }
    out.push((
        GateChoice::neutral("decline", "Decline", GateChoiceKind::RejectOnce),
        None,
    ));
    out.push((
        GateChoice::neutral("cancel", "Cancel", GateChoiceKind::Cancel),
        None,
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_all_shapes() {
        let req = json!({"id": 3, "method": "m", "params": {"a": 1}});
        assert!(
            matches!(RpcIn::classify(&req), Some(RpcIn::Request { method, .. }) if method == "m")
        );
        let note = json!({"method": "n"});
        assert!(matches!(
            RpcIn::classify(&note),
            Some(RpcIn::Notification {
                params: Value::Null,
                ..
            })
        ));
        let resp = json!({"id": 1, "result": {"x": 1}});
        assert!(matches!(
            RpcIn::classify(&resp),
            Some(RpcIn::Response { .. })
        ));
        let err = json!({"id": 1, "error": {"code": -32600, "message": "bad"}});
        assert_eq!(
            RpcIn::classify(&err),
            Some(RpcIn::Error {
                id: json!(1),
                code: -32600,
                message: "bad".into()
            })
        );
        assert_eq!(RpcIn::classify(&json!({"type": "x"})), None);
    }

    #[test]
    fn encoders_are_compact_single_lines() {
        let l = rpc_request(5, "turn/start", json!({"threadId": "t"}));
        assert!(!l.0.contains('\n'));
        assert_eq!(
            l.0,
            r#"{"id":5,"jsonrpc":"2.0","method":"turn/start","params":{"threadId":"t"}}"#
        );
        assert_eq!(
            rpc_notification("initialized", None).0,
            r#"{"jsonrpc":"2.0","method":"initialized"}"#
        );
    }

    #[test]
    fn elicitation_choices_with_names() {
        let schema = json!({"properties": {"grant": {"enum": ["once", "deny"], "enumNames": ["Once", "Deny"]}}});
        let c = elicitation_choices(Some(&schema));
        assert_eq!(c.len(), 4);
        assert_eq!(c[0].0.id, "accept:once");
        assert_eq!(c[0].1, Some(json!({"grant": "once"})));
        assert_eq!(c[1].0.label, "Deny");
        assert!(!c[1].0.neutral_label);
        assert_eq!(c[2].0.kind, GateChoiceKind::RejectOnce);
        let plain = elicitation_choices(None);
        assert_eq!(plain[0].0.id, "accept");
        assert!(plain[0].0.neutral_label);
    }
}
