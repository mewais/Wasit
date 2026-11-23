//! The `tracing` bridge (design §4.5): `tracing` stays the logging API inside libraries, but
//! what reaches the sink is only how many events each `(target, level)` produced per minute
//! (`trace.counted`) — never a message, never a field value.

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

/// The layer every process adds to its subscriber. Events are counted into the installed sink;
/// without one they are ignored.
#[derive(Debug, Clone, Copy, Default)]
pub struct CountingLayer;

/// The counts-only layer.
pub fn tracing_layer() -> CountingLayer {
    CountingLayer
}

impl<S: Subscriber> Layer<S> for CountingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let Some(sink) = crate::installed() else {
            return;
        };
        let meta = event.metadata();
        let level = match *meta.level() {
            Level::TRACE => "trace",
            Level::DEBUG => "debug",
            Level::INFO => "info",
            Level::WARN => "warn",
            Level::ERROR => "error",
        };
        sink.count_trace(meta.target(), level);
    }
}
