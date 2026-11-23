//! `diag_events!`, `diag_enum!` and `diag_value!`: the only ways to declare what a log record may
//! contain.

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
