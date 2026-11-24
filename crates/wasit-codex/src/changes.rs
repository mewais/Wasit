//! The file changes of a codex `fileChange` item, as [`ContentPart::Diff`] parts.
//!
//! Codex reports each changed file as `{path, kind: {type}, diff}` where `diff` depends on the
//! kind: the whole new content for an `add`, the removed content for a `delete`, and for an
//! `update` a unified-style patch of `@@` hunks (`-` and `+` lines, no context lines, no file
//! headers, optionally a trailing `Moved to:` note).

use serde_json::Value;
use wasit_core::wire::{opt_s, s};
use wasit_core::ContentPart;

/// One diff part per change of the `fileChange` item `item`. Empty for other items, and for an
/// `update` whose patch is empty: the change is known but not its text, so nothing is shown.
///
/// The caller attaches these only when the item completed, since a declined change was not
/// written.
pub(crate) fn change_diffs(item: &Value) -> Vec<ContentPart> {
    if s(item, "type") != "fileChange" {
        return Vec::new();
    }
    item.get("changes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(change_diff)
        .collect()
}

fn change_diff(change: &Value) -> Option<ContentPart> {
    let path = opt_s(change, "path")?;
    let diff = s(change, "diff");
    let kind = change.get("kind").map(|k| s(k, "type")).unwrap_or_default();
    Some(match kind {
        "add" => ContentPart::Diff {
            path,
            old_text: None,
            new_text: diff.to_string(),
            hunk: None,
        },
        "delete" => ContentPart::Diff {
            path,
            old_text: Some(diff.to_string()),
            new_text: String::new(),
            hunk: None,
        },
        "update" if !diff.is_empty() => {
            let (old, new) = hunk_sides(diff);
            ContentPart::Diff {
                path,
                old_text: Some(old),
                new_text: new,
                hunk: Some(diff.to_string()),
            }
        }
        _ => return None,
    })
}

/// The old and new lines of a codex patch: `-` and context lines form the old side, `+` and
/// context lines the new side. The `@@` headers, `*** End of File` markers and notes are not
/// lines of either side.
fn hunk_sides(patch: &str) -> (String, String) {
    let (mut old, mut new) = (String::new(), String::new());
    for line in patch.lines() {
        let mut chars = line.chars();
        let Some(sigil) = chars.next() else { continue };
        let text = chars.as_str();
        match sigil {
            '-' => push_line(&mut old, text),
            '+' => push_line(&mut new, text),
            ' ' => {
                push_line(&mut old, text);
                push_line(&mut new, text);
            }
            _ => {}
        }
    }
    (old, new)
}

fn push_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(changes: Value) -> Value {
        json!({"type": "fileChange", "id": "i1", "status": "completed", "changes": changes})
    }

    #[test]
    fn add_carries_the_new_content_and_no_old_text() {
        let parts = change_diffs(&item(json!([
            {"path": "/w/n.txt", "kind": {"type": "add"}, "diff": "hello\n"}
        ])));
        assert_eq!(
            parts,
            vec![ContentPart::Diff {
                path: "/w/n.txt".into(),
                old_text: None,
                new_text: "hello\n".into(),
                hunk: None,
            }]
        );
    }

    #[test]
    fn delete_carries_the_removed_content_as_old_text() {
        let parts = change_diffs(&item(json!([
            {"path": "/w/o.txt", "kind": {"type": "delete"}, "diff": "bye\n"}
        ])));
        assert_eq!(
            parts,
            vec![ContentPart::Diff {
                path: "/w/o.txt".into(),
                old_text: Some("bye\n".into()),
                new_text: String::new(),
                hunk: None,
            }]
        );
    }

    #[test]
    fn update_keeps_the_patch_and_splits_it_into_both_sides() {
        let patch = "@@ fn main\n-let a = 1;\n+let a = 2;\n*** End of File";
        let parts = change_diffs(&item(json!([
            {"path": "/w/m.rs", "kind": {"type": "update", "move_path": null}, "diff": patch}
        ])));
        assert_eq!(
            parts,
            vec![ContentPart::Diff {
                path: "/w/m.rs".into(),
                old_text: Some("let a = 1;\n".into()),
                new_text: "let a = 2;\n".into(),
                hunk: Some(patch.into()),
            }]
        );
    }

    #[test]
    fn update_with_context_lines_puts_them_on_both_sides() {
        let patch = "@@\n keep\n-old\n+new\n";
        let parts = change_diffs(&item(json!([
            {"path": "/w/c.txt", "kind": {"type": "update", "move_path": null}, "diff": patch}
        ])));
        assert!(
            matches!(&parts[0], ContentPart::Diff { old_text: Some(o), new_text, .. }
            if o == "keep\nold\n" && new_text == "keep\nnew\n")
        );
    }

    #[test]
    fn one_part_per_change_in_order() {
        let parts = change_diffs(&item(json!([
            {"path": "/w/a", "kind": {"type": "add"}, "diff": "a"},
            {"path": "/w/b", "kind": {"type": "delete"}, "diff": "b"},
        ])));
        assert_eq!(parts.len(), 2);
        assert!(matches!(&parts[1], ContentPart::Diff { path, .. } if path == "/w/b"));
    }

    #[test]
    fn an_update_without_patch_text_and_other_items_give_no_part() {
        assert!(change_diffs(&item(json!([
            {"path": "/w/x", "kind": {"type": "update", "move_path": null}, "diff": ""}
        ])))
        .is_empty());
        assert!(change_diffs(&json!({"type": "commandExecution", "command": "ls"})).is_empty());
        assert!(change_diffs(&item(json!([{"kind": {"type": "add"}, "diff": "x"}]))).is_empty());
    }
}
