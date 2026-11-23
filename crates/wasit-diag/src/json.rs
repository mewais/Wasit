//! The output a [`DiagValue`](crate::DiagValue) writes into.
//!
//! [`Json`] is opaque outside this crate: a value implemented elsewhere (through
//! [`diag_value!`](crate::diag_value) or [`diag_enum!`](crate::diag_enum)) can only produce output
//! by delegating to a value type of this crate, so the set of output shapes is decided here.

use std::fmt::Write as _;

/// A JSON value being written. Only this crate's value types write into it directly.
pub struct Json {
    pub(crate) buf: String,
}

impl Json {
    pub(crate) fn new() -> Self {
        Json { buf: String::new() }
    }

    pub(crate) fn raw(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    pub(crate) fn null(&mut self) {
        self.buf.push_str("null");
    }

    pub(crate) fn bool(&mut self, v: bool) {
        self.buf.push_str(if v { "true" } else { "false" });
    }

    pub(crate) fn uint(&mut self, v: u64) {
        // Writing into a String cannot fail.
        let _ = write!(self.buf, "{v}");
    }

    pub(crate) fn int(&mut self, v: i64) {
        // Writing into a String cannot fail.
        let _ = write!(self.buf, "{v}");
    }

    pub(crate) fn float(&mut self, v: f32) {
        if v.is_finite() {
            // Writing into a String cannot fail.
            let _ = write!(self.buf, "{v}");
        } else {
            self.null();
        }
    }

    pub(crate) fn str(&mut self, s: &str) {
        push_str(&mut self.buf, s);
    }

    /// `"<other:N>"`: what a value that failed validation renders as.
    pub(crate) fn other(&mut self, chars: usize) {
        // Writing into a String cannot fail.
        let _ = write!(self.buf, "\"<other:{chars}>\"");
    }

    /// A `CamelCase` identifier written as a `snake_case` string (`diag_enum!` variants).
    pub(crate) fn camel_as_snake(&mut self, ident: &'static str) {
        self.buf.push('"');
        for (i, c) in ident.chars().enumerate() {
            if c.is_ascii_uppercase() {
                if i > 0 {
                    self.buf.push('_');
                }
                self.buf.push(c.to_ascii_lowercase());
            } else if c.is_ascii_alphanumeric() || c == '_' {
                self.buf.push(c);
            }
        }
        self.buf.push('"');
    }
}

/// Appends `s` as a JSON string literal (quotes included).
pub(crate) fn push_str(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                // Writing into a String cannot fail.
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

/// `s` as a JSON string literal.
pub(crate) fn quoted(s: &str) -> String {
    let mut b = String::with_capacity(s.len() + 2);
    push_str(&mut b, s);
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaping_round_trips_through_serde_json() {
        for s in ["plain", "q\"b\\s", "nl\ncr\rtab\t", "\u{1}\u{1f}\u{7f}é…"] {
            let back: String = serde_json::from_str(&quoted(s)).unwrap();
            assert_eq!(back, s);
        }
    }

    #[test]
    fn camel_to_snake() {
        let mut j = Json::new();
        j.camel_as_snake("ActivityWhileIdle");
        assert_eq!(j.buf, "\"activity_while_idle\"");
        let mut j = Json::new();
        j.camel_as_snake("Io");
        assert_eq!(j.buf, "\"io\"");
    }
}
