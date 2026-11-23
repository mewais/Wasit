//! A transcript format for recording and replaying a protocol session: what
//! the CLI printed, what we wrote, and which commands caused the writes. Used
//! by the driver fixture tests and the `wasit-smoke` recorder; replaying is
//! pure.

use serde::{Deserialize, Serialize};

use crate::event::AgentEvent;
use crate::protocol::{Command, DriverError, Encoded, InputId, Protocol, Step};
use crate::spec::SpawnSpec;

/// One transcript record (one NDJSON line, externally tagged).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Record {
    /// `Protocol::start` was called with this spec.
    Start(Box<SpawnSpec>),
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

/// The [`InputId`] of the command carried by the record at `index` (0-based) of a transcript:
/// its 1-based position, the way a journal's seq names a runtime note. A `start` record's first
/// prompt gets the start record's id.
pub fn input_id(index: usize) -> InputId {
    InputId(index as u64 + 1)
}

/// Feed `records` through `protocol`. A `start` record's first prompt is encoded right after
/// `start`, as the runtime does ([`SpawnSpec::split_first_prompt`]).
///
/// [`SpawnSpec::split_first_prompt`]: crate::SpawnSpec::split_first_prompt
pub fn replay(protocol: &mut dyn Protocol, records: &[Record]) -> Replay {
    let mut out = Replay::default();
    let apply = |out: &mut Replay, step: Step| {
        out.events.extend(step.events);
        out.written.extend(step.write.into_iter().map(|l| l.0));
        out.closed_stdin |= step.close_stdin;
    };
    let encode =
        |protocol: &mut dyn Protocol, out: &mut Replay, input, cmd: &Command| match protocol
            .encode(input, cmd.clone())
        {
            Ok(Encoded::Sent(step)) => apply(out, step),
            Ok(Encoded::Queued) => {}
            Err(e) => out.encode_errors.push((cmd.name().to_string(), e)),
        };
    for (i, r) in records.iter().enumerate() {
        match r {
            Record::Start(spec) => {
                let (spec, first) = spec.split_first_prompt();
                let step = protocol.start(&spec);
                apply(&mut out, step);
                if let Some(cmd) = first {
                    encode(protocol, &mut out, input_id(i), &cmd);
                }
            }
            Record::Out(line) => {
                let step = protocol.on_line(line);
                apply(&mut out, step);
            }
            Record::Cmd(cmd) => encode(protocol, &mut out, input_id(i), cmd),
            Record::In(line) => out.expected_written.push(line.clone()),
            Record::CloseStdin => {}
        }
    }
    out
}
