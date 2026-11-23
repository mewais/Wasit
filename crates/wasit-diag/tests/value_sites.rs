//! The set of field types and the free-text entry point stay reviewable: outside `wasit-diag`,
//! nothing implements `DiagValue`/`DiagEvent` by hand, every `diag_value!` registration and every
//! `DiagText::new(` call is in the allow-list below. Adding one is a reviewed change here.

use std::path::Path;

use wasit_process::scan;

/// Patterns that extend what may reach a log record.
const PATTERNS: &[&str] = &[
    "diag_value!",
    "DiagValue for ",
    "DiagEvent for ",
    "Sealed for ",
    "SealedEvent for ",
    "DiagText::new(",
];

/// Files allowed to contain them. Later steps add here, e.g. `wasit-control/src/wire.rs` for
/// `TraceId` (design S10) and the `DiagText::new` sites of design §4.3.
const ALLOW: &[&str] = &[
    "wasit-diag/src/value.rs",
    "wasit-diag/src/text.rs",
    "wasit-diag/src/macros.rs",
    "wasit-diag/src/panic.rs",
];

#[test]
fn field_types_and_free_text_only_in_reviewed_places() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let found = scan::find(&root, ALLOW, |line| {
        PATTERNS.iter().any(|p| scan::mentions(line, p))
    });
    assert!(
        found.is_empty(),
        "new diagnostics field types or free-text sites need review (allow-list in {}):\n{}",
        file!(),
        found.join("\n")
    );
}

#[test]
fn the_scan_sees_each_pattern() {
    for line in [
        "wasit_diag::diag_value!(TraceId => Ident, |t| Ident::new(&t.0));",
        "impl DiagValue for Prompt {",
        "impl wasit_diag::DiagEvent for X {",
        "impl wasit_diag::__private::Sealed for X {}",
        "let t = DiagText::new(Origin::Own, &e.to_string());",
    ] {
        assert!(PATTERNS.iter().any(|p| scan::mentions(line, p)), "{line}");
    }
    assert!(!PATTERNS
        .iter()
        .any(|p| scan::mentions("let t = MyDiagText::new(x);", p)));
}
