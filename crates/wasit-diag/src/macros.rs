//! `diag_events!`, `diag_enum!`, `diag_data!` and `diag_value!`: the only ways to declare what a
//! log record, or a typed projection served over a wire, may contain.

/// Declares an event enum. Per variant: the event name `ev` (`[a-z0-9_.]`, dotted), the level
/// (`debug`, `info` (default), `warn`, `error`), optionally a rate budget per minute (default
/// [`DEFAULT_BUDGET`](crate::DEFAULT_BUDGET)), and named fields whose types implement
/// [`DiagValue`](crate::DiagValue).
///
/// ```
/// wasit_diag::diag_events! {
///     /// Things a link does.
///     pub enum LinkEvent {
///         /// An attempt to connect began.
///         #[diag(ev = "link.attempt_begin")]
///         AttemptBegin { link: wasit_diag::Ident, n: u64 },
///         #[diag(ev = "link.attempt_end", lvl = warn, budget = 120)]
///         AttemptEnd {
///             link: wasit_diag::Ident,
///             n: u64,
///             error: Option<wasit_diag::ErrorInfo>,
///             ms: wasit_diag::Ms,
///         },
///     }
/// }
/// wasit_diag::emit(&LinkEvent::AttemptBegin { link: wasit_diag::Ident::st("hosts:gpu"), n: 1 });
/// ```
///
/// The enum derives `Debug` and `Clone` (every value type has both).
///
/// Checked at compile time, at the declaration: every field type implements `DiagValue`; no
/// field is named like a record header key (`b, seq, t, m, src, lvl, ev`); `ev` is well formed.
#[macro_export]
macro_rules! diag_events {
    (
        $(#[$emeta:meta])*
        $vis:vis enum $Name:ident {
            $(
                $(#[doc = $vdoc:literal])*
                #[diag(ev = $ev:literal $(, lvl = $lvl:ident)? $(, budget = $budget:expr)? $(,)?)]
                $V:ident { $( $(#[doc = $fdoc:literal])* $f:ident : $t:ty ),* $(,)? }
            ),* $(,)?
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, Clone)]
        $vis enum $Name {
            $(
                $(#[doc = $vdoc])*
                $V { $( $(#[doc = $fdoc])* $f : $t ),* }
            ),*
        }

        const _: () = {
            fn __diag_value<T: $crate::DiagValue + ?Sized>() {}
            $(
                $( let _ = __diag_value::<$t>; )*
                // A field named like a header key is a duplicate field of this struct.
                #[allow(dead_code, non_camel_case_types, non_snake_case)]
                struct $V { b: (), seq: (), t: (), m: (), src: (), lvl: (), ev: (), $( $f: (), )* }
                assert!($crate::__private::valid_ev($ev), concat!("malformed ev name: ", $ev));
            )*
        };

        impl $crate::__private::SealedEvent for $Name {}

        impl $crate::DiagEvent for $Name {
            fn ev(&self) -> &'static str {
                match self { $( Self::$V { .. } => $ev, )* }
            }

            fn level(&self) -> $crate::Level {
                match self { $( Self::$V { .. } => $crate::__diag_level!($($lvl)?), )* }
            }

            fn __budget(&self) -> &'static $crate::__private::Budget {
                match self {
                    $( Self::$V { .. } => {
                        static B: $crate::__private::Budget =
                            $crate::__private::Budget::new($ev, $crate::__diag_budget!($($budget)?));
                        &B
                    } )*
                }
            }

            #[allow(unused_variables)]
            fn __fields(&self, out: &mut $crate::__private::Fields) {
                match self {
                    $( Self::$V { $( $f ),* } => { $( out.field(stringify!($f), $f); )* } )*
                }
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __diag_level {
    () => {
        $crate::Level::Info
    };
    (debug) => {
        $crate::Level::Debug
    };
    (info) => {
        $crate::Level::Info
    };
    (warn) => {
        $crate::Level::Warn
    };
    (error) => {
        $crate::Level::Error
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __diag_budget {
    () => {
        $crate::DEFAULT_BUDGET
    };
    ($b:expr) => {
        $b
    };
}

/// Declares an enum of unit variants usable as a field; a variant is written as its name in
/// `snake_case` (`ActivityWhileIdle` → `"activity_while_idle"`).
///
/// ```
/// wasit_diag::diag_enum! {
///     /// How an attempt ended.
///     pub enum Outcome { Connected, Backoff, Park }
/// }
/// ```
#[macro_export]
macro_rules! diag_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $Name:ident { $( $(#[doc = $vdoc:literal])* $V:ident ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $Name { $( $(#[doc = $vdoc])* $V ),+ }

        impl $crate::__private::Sealed for $Name {}

        impl $crate::DiagValue for $Name {
            fn __write(&self, out: &mut $crate::__private::Json) {
                $crate::__private::write_variant(out, match self { $( Self::$V => stringify!($V), )+ })
            }
        }
    };
}

/// Registers a validated newtype of another Wasit crate as a field type, rendered as one of this
/// crate's value types (so it cannot produce any output those types cannot).
///
/// The set of field types must stay reviewable: every use of this macro outside `wasit-diag`
/// is listed in the allow-list of the source-scan test `tests/value_sites.rs`.
///
/// ```
/// #[derive(Debug, Clone)]
/// pub struct TraceId(String);
/// wasit_diag::diag_value!(TraceId => wasit_diag::Ident, |t| wasit_diag::Ident::new(&t.0));
///
/// wasit_diag::diag_events! {
///     pub enum E { #[diag(ev = "rpc.traced")] Traced { trace: TraceId } }
/// }
/// ```
#[macro_export]
macro_rules! diag_value {
    ($ty:ty => $shown:ty, |$v:ident| $conv:expr) => {
        impl $crate::__private::Sealed for $ty {}
        impl $crate::DiagValue for $ty {
            fn __write(&self, out: &mut $crate::__private::Json) {
                let $v: &$ty = self;
                let shown: $shown = $conv;
                $crate::DiagValue::__write(&shown, out)
            }
        }
    };
}

/// Declares an enum with data that can be served over a wire and is made only of value types: a
/// projection of something that carries content into something that cannot (the session trail,
/// design §12.5). Every field's type must implement [`DiagValue`](crate::DiagValue), so the
/// declaration cannot name a `String`, a path, a JSON value or a content type: that is a compile
/// error at the declaration, exactly as for [`diag_events!`].
///
/// The enum is internally tagged (`#[diag(tag = "ev")]` names the tag key); a variant's wire name
/// is its `ev` (`[a-z0-9_.]`, dotted). The `#[diag(tag = …)]` attribute comes first, before the
/// enum's documentation. Fields before the `;` are required; fields after it are
/// optional, declared with the type of their value, stored as `Option<T>` and left out of the
/// JSON when absent.
///
/// It derives `Debug`, `Clone`, `PartialEq` and serde's `Serialize`/`Deserialize` (the invoking
/// crate depends on `serde`), is itself a [`DiagValue`](crate::DiagValue) (so a `diag_data!` type
/// may hold another and may be a field of a log record) and has `DECLARED`, the wire name and
/// field names of every variant, and `ev()`. Attributes of the enum, for example
/// `#[cfg_attr(feature = "ts", derive(ts_rs::TS))]`, are passed through. A field named like the
/// tag, or declared twice, is refused (E0124).
///
/// The use of this macro outside `wasit-diag` is reviewed like `diag_value!`: every file that
/// invokes it is in the allow-list of `tests/value_sites.rs`.
///
/// ```
/// wasit_diag::diag_data! {
///     #[diag(tag = "ev")]
///     /// What happened to an input.
///     pub enum Step {
///         #[diag(ev = "input.sent")]
///         Sent { input: u64 },
///         #[diag(ev = "input.dropped")]
///         Dropped { input: u64, reason: wasit_diag::Code ; error: wasit_diag::ErrorInfo },
///     }
/// }
/// let s = Step::Dropped { input: 3, reason: wasit_diag::Code::st("interrupted"), error: None };
/// assert_eq!(
///     serde_json::to_string(&s).unwrap(),
///     r#"{"ev":"input.dropped","input":3,"reason":"interrupted"}"#
/// );
/// assert_eq!(Step::DECLARED[0], ("input.sent", &["input"][..]));
/// ```
///
/// A `String` field is refused:
///
/// ```compile_fail,E0277
/// wasit_diag::diag_data! {
///     #[diag(tag = "ev")]
///     pub enum E { #[diag(ev = "x.y")] V { text: String } }
/// }
/// ```
///
/// and so is an optional one:
///
/// ```compile_fail,E0277
/// wasit_diag::diag_data! {
///     #[diag(tag = "ev")]
///     pub enum E { #[diag(ev = "x.y")] V { ; path: std::path::PathBuf } }
/// }
/// ```
///
/// and a field that clashes with the tag:
///
/// ```compile_fail,E0080
/// wasit_diag::diag_data! {
///     #[diag(tag = "ev")]
///     pub enum E { #[diag(ev = "x.y")] V { ev: u64 } }
/// }
/// ```
#[macro_export]
macro_rules! diag_data {
    (
        #[diag(tag = $tag:literal)]
        $(#[$emeta:meta])*
        $vis:vis enum $Name:ident {
            $(
                $(#[doc = $vdoc:literal])*
                #[diag(ev = $ev:literal)]
                $V:ident {
                    $( $(#[doc = $fdoc:literal])* $f:ident : $t:ty ),* $(,)?
                    $( ; $( $(#[doc = $odoc:literal])* $of:ident : $ot:ty ),* $(,)? )?
                }
            ),* $(,)?
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(tag = $tag)]
        $vis enum $Name {
            $(
                $(#[doc = $vdoc])*
                #[serde(rename = $ev)]
                $V {
                    $( $(#[doc = $fdoc])* $f : $t, )*
                    $($(
                        $(#[doc = $odoc])*
                        #[serde(default, skip_serializing_if = "Option::is_none")]
                        $of : Option<$ot>,
                    )*)?
                }
            ),*
        }

        const _: () = {
            fn __diag_value<T: $crate::DiagValue + ?Sized>() {}
            $(
                $( let _ = __diag_value::<$t>; )*
                $($( let _ = __diag_value::<$ot>; )*)?
                // A field named like the tag would make serde write the key twice.
                $( assert!(!$crate::__private::same_str($tag, stringify!($f)), "a field is named like the tag"); )*
                $($( assert!(!$crate::__private::same_str($tag, stringify!($of)), "a field is named like the tag"); )*)?
                assert!($crate::__private::valid_ev($ev), concat!("malformed ev name: ", $ev));
            )*
        };

        impl $Name {
            /// The wire name and the field names of every variant, in declaration order
            /// (required fields, then optional ones).
            pub const DECLARED: &'static [(&'static str, &'static [&'static str])] = &[
                $( ($ev, &[ $( stringify!($f), )* $($( stringify!($of), )*)? ]), )*
            ];

            /// This variant's wire name.
            pub fn ev(&self) -> &'static str {
                match self { $( Self::$V { .. } => $ev, )* }
            }
        }

        impl $crate::__private::Sealed for $Name {}

        impl $crate::DiagValue for $Name {
            fn __write(&self, out: &mut $crate::__private::Json) {
                $crate::__private::write_serde(out, self)
            }
        }
    };
}
