//! The panic hook (design §6.3): records where and on which thread a panic happened and what
//! kind of payload it had — the text only of a payload fixed in the source.

use std::any::Any;
use std::time::Duration;

use crate::internal::SinkEvent;
use crate::sink::{IN_EMIT, WRITER_THREAD};
use crate::text::{DiagText, Origin};
use crate::value::{Code, Ident, SrcLocation};

/// How long the hook waits for the writer to write the panic record.
pub const PANIC_FLUSH: Duration = Duration::from_secs(1);

/// Installs the hook in this process: a `diag.panic` record, a flush bounded by
/// [`PANIC_FLUSH`], then the previously installed hook only when `chain` is set (a developer
/// asked for stderr output; else nothing, since the default hook prints the payload).
pub fn install_panic_hook(chain: bool) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name();
        // Never from the writer itself (it would wait for itself), nor from inside `emit`
        // (the sink's lock may be held by this very thread).
        if name != Some(WRITER_THREAD) && !IN_EMIT.with(|f| f.get()) {
            crate::emit(&panic_event(
                info.location(),
                name.unwrap_or("unnamed"),
                info.payload(),
            ));
            crate::flush(PANIC_FLUSH);
        }
        if chain {
            previous(info);
        }
    }));
}

pub(crate) fn panic_event(
    location: Option<&std::panic::Location<'_>>,
    thread: &str,
    payload: &(dyn Any + Send),
) -> SinkEvent {
    let (kind, len, text) = if let Some(s) = payload.downcast_ref::<&'static str>() {
        (
            "static_str",
            s.chars().count(),
            Some(DiagText::new(Origin::Static, s)),
        )
    } else if let Some(s) = payload.downcast_ref::<String>() {
        ("string", s.chars().count(), None)
    } else {
        ("other", 0, None)
    };
    SinkEvent::Panic {
        location: location.map_or_else(SrcLocation::unknown, SrcLocation::of),
        thread: Ident::new(thread),
        payload_kind: Code::st(kind),
        len: u32::try_from(len).unwrap_or(u32::MAX),
        static_text: text,
    }
}
