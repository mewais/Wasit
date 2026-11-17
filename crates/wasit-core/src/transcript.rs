//! A transcript format for recording and replaying a protocol session: what
//! the CLI printed, what we wrote, and which commands caused the writes. Used
//! by the driver fixture tests and the `wasit-smoke` recorder; replaying is
//! pure.

use serde::{Deserialize, Serialize};

use crate::event::AgentEvent;
use crate::protocol::{Command, DriverError, Encoded, Protocol, Step};
use crate::spec::SpawnSpec;

/// One transcript record (one NDJSON line, externally tagged).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Record {
    /// `Protocol::start` was called with this spec.
    Start(SpawnSpec),
    /// The CLI printed this line.
    Out(String),
    /// We wrote this line to the CLI.
    In(String),
    /// This command was encoded.
    Cmd(Command),
    /// We closed the CLI's stdin.
    CloseStdin,
}

/// What a replay produced.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Replay {
    pub events: Vec<AgentEvent>,
    /// Lines the protocol asked to write, in order.
    pub written: Vec<String>,
    /// Lines the transcript says were written, in order.
    pub expected_written: Vec<String>,
    pub closed_stdin: bool,
    /// Errors returned by `encode`, with the command name.
    pub encode_errors: Vec<(String, DriverError)>,
}

/// Parse NDJSON transcript text. Blank lines are skipped.
pub fn parse(text: &str) -> Result<Vec<Record>, String> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1)))
        .collect()
}

/// Serialize one record as an NDJSON line (no newline).
pub fn to_line(r: &Record) -> String {
    serde_json::to_string(r).expect("records are always serializable")
}

/// Feed `records` through `protocol`.
pub fn replay(protocol: &mut dyn Protocol, records: &[Record]) -> Replay {
    let mut out = Replay::default();
    let apply = |out: &mut Replay, step: Step| {
        out.events.extend(step.events);
        out.written.extend(step.write.into_iter().map(|l| l.0));
        out.closed_stdin |= step.close_stdin;
    };
    for r in records {
        match r {
            Record::Start(spec) => {
                let step = protocol.start(spec);
                apply(&mut out, step);
            }
            Record::Out(line) => {
                let step = protocol.on_line(line);
                apply(&mut out, step);
            }
            Record::Cmd(cmd) => match protocol.encode(cmd.clone()) {
                Ok(Encoded::Sent(step)) => apply(&mut out, step),
                Ok(Encoded::Queued) => {}
                Err(e) => out.encode_errors.push((cmd.name().to_string(), e)),
            },
            Record::In(line) => out.expected_written.push(line.clone()),
            Record::CloseStdin => {}
        }
    }
    out
}
