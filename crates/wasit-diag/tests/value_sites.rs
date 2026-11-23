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
    "diag_data!",
    "DiagValue for ",
    "DiagEvent for ",
    "Sealed for ",
    "SealedEvent for ",
    "DiagText::new(",
];

/// Files allowed to contain them. Later steps add here, e.g. the `DiagText::new` sites of
/// design §4.3.
///
/// `wasit-control/src/diag.rs` makes `DiagText::new(Origin::Transport, …)` in three places, all
/// text of a link and never of a session: the text that goes with a link state (a transport
/// command's stderr tail, a parked link's reason, why provisioning was needed, a last error
/// whose peer-written parts were replaced by their codes first, see `connect_why` and
/// `close_why`), the stderr tail of a closed link's transport child, and the text of a failed
/// transport command in `ConnectDiag::of` (the reason the user is shown, for a connector that
/// records its own failure).
///
/// * `wasit-control/src/diag.rs` also registers `TraceId` (`diag_value!`: it renders as an
///   `Ident`, so it can be a field of a record and of a trail entry).
/// * `wasit-runtime/src/trail.rs` declares the session trail (`diag_data!`): a typed projection
///   of a session's events whose every field is a value type, served over the control protocol.
///   It makes free text in one place only: the reason a session's process was lost or failed
///   (`DiagText::new(Origin::Own, …)`), which the runtime itself words (its fixed phrases, the
///   exit status, the OS's reason for a failed shim connection) and never takes from the CLI, the
///   user or a peer. Texts the CLI wrote (a turn's error, a rejected command, a notice) and the
///   reason an input was refused go through `ErrorInfo::foreign`: length and fingerprint only.
const ALLOW: &[&str] = &[
    "wasit-diag/src/value.rs",
    "wasit-diag/src/text.rs",
    "wasit-diag/src/macros.rs",
    "wasit-diag/src/panic.rs",
    "wasit-control/src/diag.rs",
    "wasit-runtime/src/trail.rs",
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
///   `ErrorInfo::foreign`, so only its length and fingerprint are kept;
/// * `wasit-runtime/src/trail.rs`: the shim's `stdin_error` note, `write to child stdin: <the
///   OS's reason>`, worded by the shim from the failed write alone (no line of it, no CLI text).
const OURS_ALLOW: &[&str] = &[
    "wasit-shim/src/diag.rs",
    "wasit-runtime/src/bin/wasit-hostd.rs",
    "wasit-control/src/diag.rs",
    "wasit-runtime/src/trail.rs",
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
        "wasit_diag::diag_data! {",
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
