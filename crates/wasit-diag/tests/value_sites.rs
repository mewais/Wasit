//! The set of field types and the free-text entry points stay reviewable: outside `wasit-diag`,
//! nothing implements `DiagValue`/`DiagEvent` by hand, every `diag_value!` registration and every
//! `DiagText::new(` call is in the allow-list below, and so is every `ErrorInfo::ours(` call
//! (it makes free text from any `Display` as `DiagText::new` does from a `&str`). Adding one is
//! a reviewed change here.

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
///
/// `wasit-control/src/diag.rs` makes `DiagText::new(Origin::Transport, …)` in two places, both
/// text of a link and never of a session: the text that goes with a link state (a transport
/// command's stderr tail, a parked link's reason, why provisioning was needed, a last error
/// whose peer-written parts were replaced by their codes first, see `connect_why` and
/// `close_why`) and the stderr tail of a closed link's transport child.
const ALLOW: &[&str] = &[
    "wasit-diag/src/value.rs",
    "wasit-diag/src/text.rs",
    "wasit-diag/src/macros.rs",
    "wasit-diag/src/panic.rs",
    "wasit-control/src/diag.rs",
];

/// `ErrorInfo::ours(` call sites outside `wasit-diag`: the `Display` of one of our own error
/// types whose text cannot carry session content (every use says why).
const OURS: &str = "ErrorInfo::ours(";

/// Files allowed to call it, and what they pass:
/// * `wasit-shim/src/diag.rs`: the shim's own `Error` (argument, spawn and lock errors; none
///   formats an environment value, see `cli.rs`);
/// * `wasit-runtime/src/bin/wasit-hostd.rs`: the runtime's `RuntimeError` for a session scan;
/// * `wasit-control/src/diag.rs`: `connect_error`, the `Display` of wasit-control's own
///   `ConnectError` for the variants whose text is fixed by this crate (a version pair, a
///   timeout, a closed handshake) or is the OS's reason for a failed read/write/connect or the
///   transport program we could not start (`Io`, `Spawn`). What a peer, a parser or a
///   transport's stderr wrote (`Refused`, `Protocol`, `Command`) goes through
///   `ErrorInfo::foreign`, so only its length and fingerprint are kept.
const OURS_ALLOW: &[&str] = &[
    "wasit-shim/src/diag.rs",
    "wasit-runtime/src/bin/wasit-hostd.rs",
    "wasit-control/src/diag.rs",
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
fn error_text_from_display_only_in_reviewed_places() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let found = scan::find(&root, OURS_ALLOW, |line| scan::mentions(line, OURS));
    assert!(
        found.is_empty(),
        "new `ErrorInfo::ours` sites need review (allow-list in {}):\n{}",
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
    assert!(scan::mentions(
        "error: ErrorInfo::ours(Code::st(\"spawn\"), &e),",
        OURS
    ));
    assert!(scan::mentions("wasit_diag::ErrorInfo::ours(c, &e)", OURS));
    assert!(!scan::mentions("MyErrorInfo::ours(c, &e)", OURS));
}
