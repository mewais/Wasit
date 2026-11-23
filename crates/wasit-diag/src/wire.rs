//! The value types on a wire: `serde` (and, with the `ts` feature, TypeScript) support for the
//! types a [`diag_data!`](crate::diag_data) declaration may hold.
//!
//! There is one JSON form of a value: the one its `DiagValue` implementation writes into a log
//! record. [`Serialize`] *is* that form (it renders the value the way a record does and hands
//! the result on), so a value on a wire and in a log line can never differ, and the types that
//! have no `Serialize` (`String`, `PathBuf`, `serde_json::Value`, …) still have none. Reading is
//! the inverse: a word that failed validation on the writing side (`"<other:N>"`) stays one; a
//! text was scrubbed where it was made and is kept as it is; nothing that was not a value when
//! it was written becomes one.
//!
//! Only the types a `diag_data!` type holds are covered: numbers and flags (serde's own),
//! [`Ms`], [`Bytes`], [`TimeMs`], [`Ident`], [`Code`], [`Fp`], [`ErrorInfo`], [`DiagText`],
//! [`ProjectPath`]; `Option` and `Vec` of them come from serde.

use serde::de::Error as _;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::json::Json;
use crate::text::DiagText;
use crate::value::{
    Bytes, Code, DiagValue, ErrorInfo, Fp, Ident, Ms, PathKind, PathTag, ProjectPath, TimeMs,
};

/// What `v` writes into a log record, as a JSON value.
fn rendered<V: DiagValue>(v: &V) -> Result<serde_json::Value, serde_json::Error> {
    let mut out = Json::new();
    v.__write(&mut out);
    serde_json::from_str(&out.buf)
}

macro_rules! serialize_as_rendered {
    ($($t:ty),* $(,)?) => {$(
        impl Serialize for $t {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                rendered(self).map_err(S::Error::custom)?.serialize(s)
            }
        }
    )*};
}
serialize_as_rendered!(
    Ms,
    Bytes,
    TimeMs,
    Ident,
    Code,
    Fp,
    ErrorInfo,
    DiagText,
    ProjectPath
);

macro_rules! deserialize_number {
    ($($t:ident),* $(,)?) => {$(
        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<$t, D::Error> {
                u64::deserialize(d).map($t)
            }
        }
    )*};
}
deserialize_number!(Ms, Bytes, TimeMs);

impl<'de> Deserialize<'de> for Ident {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Ident, D::Error> {
        Ok(Ident::from_wire(&String::deserialize(d)?))
    }
}

impl<'de> Deserialize<'de> for Code {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Code, D::Error> {
        Ok(Code::from_wire(&String::deserialize(d)?))
    }
}

impl<'de> Deserialize<'de> for Fp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Fp, D::Error> {
        let s = String::deserialize(d)?;
        Fp::from_hex(&s).ok_or_else(|| D::Error::custom("a fingerprint is eight hex digits"))
    }
}

impl<'de> Deserialize<'de> for DiagText {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<DiagText, D::Error> {
        Ok(DiagText::from_wire(String::deserialize(d)?))
    }
}

impl<'de> Deserialize<'de> for ErrorInfo {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<ErrorInfo, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            code: Code,
            #[serde(default)]
            text: Option<DiagText>,
            #[serde(default)]
            len: Option<u32>,
            #[serde(default)]
            fp: Option<Fp>,
            #[serde(default)]
            os: Option<i32>,
        }
        let w = Wire::deserialize(d)?;
        Ok(ErrorInfo::from_wire(w.code, w.text, w.len, w.fp, w.os))
    }
}

impl<'de> Deserialize<'de> for ProjectPath {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<ProjectPath, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            kind: String,
            depth: u32,
            hash: String,
        }
        let w = Wire::deserialize(d)?;
        let kind =
            PathKind::from_code(&w.kind).ok_or_else(|| D::Error::custom("unknown path kind"))?;
        PathTag::from_parts(kind, w.depth, &w.hash)
            .map(ProjectPath::from_tag)
            .ok_or_else(|| D::Error::custom("a path hash is ten hex digits"))
    }
}

#[cfg(feature = "ts")]
mod ts {
    use super::*;

    macro_rules! leaf {
        ($($t:ty => $name:literal),* $(,)?) => {$(
            impl ts_rs::TS for $t {
                type WithoutGenerics = Self;
                type OptionInnerType = Self;
                fn name(_: &ts_rs::Config) -> String {
                    $name.to_owned()
                }
                fn inline(cfg: &ts_rs::Config) -> String {
                    <Self as ts_rs::TS>::name(cfg)
                }
            }
        )*};
    }
    leaf!(
        Ms => "number",
        Bytes => "number",
        TimeMs => "number",
        Ident => "string",
        Code => "string",
        Fp => "string",
        DiagText => "string",
        ErrorInfo => "{ code: string, text?: string, len?: number, fp?: string, os?: number }",
        ProjectPath => "{ kind: string, depth: number, hash: string }",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::Origin;

    fn round_trip<T>(v: &T)
    where
        T: Serialize + for<'de> Deserialize<'de> + DiagValue + PartialEq + std::fmt::Debug,
    {
        let wire = serde_json::to_string(v).unwrap();
        let mut log = Json::new();
        v.__write(&mut log);
        let a: serde_json::Value = serde_json::from_str(&wire).unwrap();
        let b: serde_json::Value = serde_json::from_str(&log.buf).unwrap();
        assert_eq!(a, b, "the wire form is the log form");
        let back: T = serde_json::from_str(&wire).unwrap();
        assert_eq!(&back, v, "{wire}");
    }

    #[test]
    fn every_value_type_reads_back_what_it_wrote_in_the_form_of_a_log_record() {
        round_trip(&Ms(12));
        round_trip(&Bytes(3));
        round_trip(&TimeMs(1_763_851_935_123));
        round_trip(&Ident::new("sess-01.a:b@c+d_e"));
        round_trip(&Code::st("not_found"));
        round_trip(&Fp::of("connection 12 refused"));
        round_trip(&DiagText::new(Origin::Own, "could not start: no such file"));
        round_trip(&ErrorInfo::foreign(
            Code::st("execution_error"),
            "some text",
        ));
        round_trip(&ErrorInfo::ours(Code::st("spawn"), &"could not start"));
        round_trip(&ErrorInfo::io(&std::io::Error::from_raw_os_error(2)));
        round_trip(&ProjectPath::from_text("/home/u/proj"));
        round_trip(&ProjectPath::from_text("C:\\Users\\U\\Proj"));
        round_trip(&ProjectPath::from_text(""));
    }

    #[test]
    fn a_word_that_failed_validation_stays_one_with_its_length() {
        let id = Ident::new("has space");
        let wire = serde_json::to_string(&id).unwrap();
        assert_eq!(wire, "\"<other:9>\"");
        assert_eq!(serde_json::from_str::<Ident>(&wire).unwrap(), id);
        assert!(!wire.contains("space"));
        let code = Code::new("a/b");
        assert_eq!(
            serde_json::from_str::<Code>(&serde_json::to_string(&code).unwrap()).unwrap(),
            code
        );
        // A text that is not a rendering is validated like any other.
        assert_eq!(
            serde_json::from_str::<Ident>("\"two words\"").unwrap(),
            Ident::new("two words")
        );
    }

    #[test]
    fn malformed_values_are_refused() {
        assert!(serde_json::from_str::<Fp>("\"xyz\"").is_err());
        assert!(serde_json::from_str::<Fp>("\"0123456\"").is_err());
        assert!(
            serde_json::from_str::<ProjectPath>(r#"{"kind":"posix","depth":1,"hash":"zz"}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<ProjectPath>(
            r#"{"kind":"elsewhere","depth":1,"hash":"0123456789"}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Ms>("\"7\"").is_err());
    }
}
