//! The file edits a Claude Code tool call makes, as [`ContentPart::Diff`] parts.
//!
//! Claude's tool input says exactly what an edit replaces, so the driver can show the change
//! without the CLI's own diff. Only the three file-writing tools are read; other tools are not
//! edits as far as the transcript is concerned.

use serde_json::Value;
use wasit_core::wire::opt_s;
use wasit_core::ContentPart;

/// The diff parts of the tool call `name` with `input`: one for `Edit`, one per entry of
/// `MultiEdit`'s `edits`, one for `Write` (a whole-file write whose old text is not known).
/// Empty for any other tool, and for an input missing the fields the edit needs.
///
/// These describe what the call asked for. The caller attaches them only when the call
/// succeeded, since a failed edit changed nothing.
pub(crate) fn edit_diffs(name: &str, input: &Value) -> Vec<ContentPart> {
    let Some(path) = opt_s(input, "file_path") else {
        return Vec::new();
    };
    let replace = |old: Option<&Value>, new: Option<&Value>| -> Option<ContentPart> {
        let new_text = new?.as_str()?.to_string();
        let old_text = match old {
            Some(v) => Some(v.as_str()?.to_string()),
            None => None,
        };
        Some(ContentPart::Diff {
            path: path.clone(),
            old_text,
            new_text,
            hunk: None,
        })
    };
    match name {
        "Edit" => replace(input.get("old_string"), input.get("new_string"))
            .into_iter()
            .collect(),
        "MultiEdit" => input
            .get("edits")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|e| replace(e.get("old_string"), e.get("new_string")))
            .collect(),
        // A write replaces whatever the file held; the transcript does not know what that was.
        "Write" => replace(None, input.get("content")).into_iter().collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn edit_becomes_one_diff_with_both_texts() {
        let input = json!({"file_path": "/w/a.rs", "old_string": "x", "new_string": "y"});
        assert_eq!(
            edit_diffs("Edit", &input),
            vec![ContentPart::Diff {
                path: "/w/a.rs".into(),
                old_text: Some("x".into()),
                new_text: "y".into(),
                hunk: None,
            }]
        );
    }

    #[test]
    fn multi_edit_becomes_one_diff_per_edit_in_order() {
        let input = json!({"file_path": "/w/a.rs", "edits": [
            {"old_string": "a", "new_string": "b"},
            {"old_string": "c", "new_string": "d", "replace_all": true},
        ]});
        let parts = edit_diffs("MultiEdit", &input);
        assert_eq!(parts.len(), 2);
        assert!(
            matches!(&parts[1], ContentPart::Diff { old_text: Some(o), new_text, .. } if o == "c" && new_text == "d")
        );
    }

    #[test]
    fn write_is_a_diff_without_old_text() {
        let input = json!({"file_path": "/w/new.txt", "content": "hello\n"});
        assert_eq!(
            edit_diffs("Write", &input),
            vec![ContentPart::Diff {
                path: "/w/new.txt".into(),
                old_text: None,
                new_text: "hello\n".into(),
                hunk: None,
            }]
        );
    }

    #[test]
    fn other_tools_and_incomplete_inputs_give_no_diff() {
        assert!(edit_diffs("Bash", &json!({"command": "ls"})).is_empty());
        assert!(edit_diffs("Edit", &json!({"file_path": "/w/a", "old_string": "x"})).is_empty());
        assert!(edit_diffs("Edit", &json!({"old_string": "x", "new_string": "y"})).is_empty());
        assert!(edit_diffs("MultiEdit", &json!({"file_path": "/w/a"})).is_empty());
        assert!(edit_diffs("Edit", &Value::Null).is_empty());
    }
}
