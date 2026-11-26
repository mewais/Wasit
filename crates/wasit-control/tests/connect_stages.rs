//! A connect attempt without a deadline, the stages a running attempt publishes as the link
//! state, and the wire shape of `LinkState::Connecting`.
//!
//! Nothing here touches the home directory, a transport command or the diagnostics sink: the
//! supervisor runs with no link label and every connector is in-process.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::within;
use tokio::sync::{mpsc, oneshot, Notify};
use wasit_control::{
    connector_reporting, Attempt, ConnectError, ControlClient, LinkState, Supervised,
    SupervisedConfig, SupervisedOptions,
};

/// Every state the link publishes, in order, delivered through the synchronous `on_state` tap
/// (so a publication is seen even if a later one replaces it before anyone looks).
fn tap() -> (SupervisedOptions, mpsc::UnboundedReceiver<LinkState>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let opts = SupervisedOptions {
        config: SupervisedConfig {
            connect_timeout: None,
            ..SupervisedConfig::default()
        },
        on_state: Some(Arc::new(move |s: &LinkState| {
            let _ = tx.send(s.clone());
        })),
        ..SupervisedOptions::default()
    };
    (opts, rx)
}

/// The sender the connector's guard signals through when it is dropped.
type Dropped = Arc<Mutex<Option<oneshot::Sender<()>>>>;

/// Sends a signal when dropped: a connector future that owns one tells the test it was dropped.
struct DroppedSignal(Dropped);

impl Drop for DroppedSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = tx.send(());
        }
    }
}

async fn next_state(rx: &mut mpsc::UnboundedReceiver<LinkState>) -> LinkState {
    within(rx.recv())
        .await
        .expect("the link published no more states")
}

/// The stage and `since_ms` of a `Connecting` state; panics on any other state.
fn connecting(s: &LinkState) -> (Option<&str>, u64) {
    match s {
        LinkState::Connecting { stage, since_ms } => (stage.as_deref(), *since_ms),
        other => panic!("expected connecting, got {other:?}"),
    }
}

/// The current wall-clock time in epoch milliseconds.
fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is before the epoch")
        .as_millis() as u64
}

/// A stage entered at `since` must be a wall-clock epoch time: no earlier than `before` (taken
/// before the attempt was spawned) and no later than now (it was entered before it was seen).
fn assert_entered_between(since: u64, before: u64) {
    let now = epoch_ms();
    assert!(
        before <= since && since <= now,
        "since_ms {since} is not an epoch time between {before} (before the attempt) and {now} (now)"
    );
}

/// With no deadline a connect that never finishes stays in `Connecting` for as long as it runs.
/// The window is the observation that nothing ended it; the control test below shows a deadline
/// of a fraction of it does end the same stall, so the window was long enough to matter.
#[tokio::test]
async fn an_attempt_without_a_deadline_never_times_out() {
    let (opts, mut states) = tap();
    let before = epoch_ms();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let dropped: Dropped = Arc::new(Mutex::new(Some(dropped_tx)));
    let sup = Supervised::spawn(
        connector_reporting(move |attempt: Attempt| {
            let guard = DroppedSignal(dropped.clone());
            async move {
                let _guard = guard;
                attempt.reached("waiting for the host's report");
                std::future::pending::<Result<ControlClient, ConnectError>>().await
            }
        }),
        opts,
    );
    assert_eq!(connecting(&next_state(&mut states).await), (None, 0));
    let st = next_state(&mut states).await;
    let (stage, since) = connecting(&st);
    assert_eq!(stage, Some("waiting for the host's report"));
    assert_entered_between(since, before);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        states.try_recv().is_err(),
        "a state was published while the attempt ran"
    );
    assert!(matches!(sup.current_state(), LinkState::Connecting { .. }));

    // Stop ends the attempt: its connector future is dropped before `stop` returns.
    sup.stop().await;
    within(dropped_rx)
        .await
        .expect("the connector future was not dropped");
    assert!(matches!(next_state(&mut states).await, LinkState::Stopped));
}

/// Dropping the last handle ends a deadline-free attempt too, and drops its connector future.
#[tokio::test]
async fn dropping_the_link_ends_an_attempt_without_a_deadline() {
    let (opts, mut states) = tap();
    let before = epoch_ms();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let dropped: Dropped = Arc::new(Mutex::new(Some(dropped_tx)));
    let sup = Supervised::spawn(
        connector_reporting(move |attempt: Attempt| {
            let guard = DroppedSignal(dropped.clone());
            async move {
                let _guard = guard;
                attempt.reached("sending the helper");
                std::future::pending::<Result<ControlClient, ConnectError>>().await
            }
        }),
        opts,
    );
    assert_eq!(connecting(&next_state(&mut states).await), (None, 0));
    let stage = next_state(&mut states).await;
    let (name, since) = connecting(&stage);
    assert_eq!(name, Some("sending the helper"));
    assert_entered_between(since, before);
    let dropped_at = std::time::Instant::now();
    drop(sup);
    within(dropped_rx)
        .await
        .expect("the connector future was not dropped");
    assert!(
        dropped_at.elapsed() < Duration::from_secs(5),
        "dropping took {:?}",
        dropped_at.elapsed()
    );
}

/// The control for the test above: the same stall with a short deadline does end the attempt
/// well inside the window the deadline-free test watches.
#[tokio::test]
async fn an_attempt_with_a_deadline_times_out_under_the_same_stall() {
    let (mut opts, mut states) = tap();
    opts.config.connect_timeout = Some(Duration::from_millis(20));
    opts.config.backoff = wasit_control::Backoff {
        initial: Duration::from_secs(3600),
        max: Duration::from_secs(3600),
        multiplier: 2.0,
        jitter: 0.0,
    };
    let sup = Supervised::spawn(
        connector_reporting(|attempt: Attempt| async move {
            attempt.reached("waiting for the host's report");
            std::future::pending::<Result<ControlClient, ConnectError>>().await
        }),
        opts,
    );
    assert_eq!(connecting(&next_state(&mut states).await), (None, 0));
    let st = next_state(&mut states).await;
    assert_eq!(connecting(&st).0, Some("waiting for the host's report"));
    let s = next_state(&mut states).await;
    let LinkState::Reconnecting { last_error, .. } = &s else {
        panic!("{s:?}");
    };
    assert!(
        last_error.starts_with("connect timed out while waiting"),
        "{last_error}"
    );
    sup.stop().await;
}

/// Each new stage is published when it is reached, in order, with `since_ms` never decreasing;
/// a repeated report of the current stage publishes nothing.
#[tokio::test]
async fn each_stage_is_published_as_it_is_reached_with_a_non_decreasing_since() {
    let (opts, mut states) = tap();
    let before = epoch_ms();
    let gate_one = Arc::new(Notify::new());
    let gate_two = Arc::new(Notify::new());
    let (g1, g2) = (gate_one.clone(), gate_two.clone());
    let sup = Supervised::spawn(
        connector_reporting(move |attempt: Attempt| {
            let (g1, g2) = (g1.clone(), g2.clone());
            async move {
                attempt.reached("first");
                g1.notified().await;
                attempt.reached("second");
                attempt.reached("second");
                g2.notified().await;
                attempt.reached("third");
                std::future::pending::<Result<ControlClient, ConnectError>>().await
            }
        }),
        opts,
    );

    assert_eq!(connecting(&next_state(&mut states).await), (None, 0));
    let st = next_state(&mut states).await;
    let (first, t1) = connecting(&st);
    assert_eq!(first, Some("first"));
    gate_one.notify_one();
    let st = next_state(&mut states).await;
    let (second, t2) = connecting(&st);
    assert_eq!(second, Some("second"));
    gate_two.notify_one();
    let st = next_state(&mut states).await;
    let (third, t3) = connecting(&st);
    assert_eq!(third, Some("third"));
    assert!(
        t1 <= t2 && t2 <= t3,
        "since must not decrease: {t1} {t2} {t3}"
    );
    for t in [t1, t2, t3] {
        assert_entered_between(t, before);
    }
    // The duplicate "second" published nothing, and nothing else has been published since.
    assert!(
        states.try_recv().is_err(),
        "an unexpected state was published"
    );
    assert!(
        matches!(sup.current_state(), LinkState::Connecting { stage: Some(s), .. } if s == "third")
    );
    sup.stop().await;
}

/// A report from an attempt that has already ended is not published over the state that
/// replaced it. The next attempt's first publication shows no stage left over from the old one.
#[tokio::test]
async fn a_report_from_an_ended_attempt_is_not_published() {
    let (mut opts, mut states) = tap();
    // Failures after an attempt wait for `reconnect_now` rather than a timer.
    opts.config.backoff = wasit_control::Backoff {
        initial: Duration::from_secs(3600),
        max: Duration::from_secs(3600),
        multiplier: 2.0,
        jitter: 0.0,
    };
    let kept: Arc<std::sync::Mutex<Vec<Attempt>>> = Arc::default();
    let second_gate = Arc::new(Notify::new());
    let (k, g) = (kept.clone(), second_gate.clone());
    let sup = Supervised::spawn(
        connector_reporting(move |attempt: Attempt| {
            let (k, g) = (k.clone(), g.clone());
            async move {
                let first = attempt.n() == 1;
                k.lock().unwrap().push(attempt.clone());
                if first {
                    attempt.reached("old step");
                    return Err(ConnectError::Io("refused".into()));
                }
                g.notified().await;
                attempt.reached("new step");
                std::future::pending::<Result<ControlClient, ConnectError>>().await
            }
        }),
        opts,
    );
    connecting(&next_state(&mut states).await);
    assert_eq!(
        connecting(&next_state(&mut states).await).0,
        Some("old step")
    );
    let reconnecting = next_state(&mut states).await;
    assert!(
        matches!(reconnecting, LinkState::Reconnecting { .. }),
        "{reconnecting:?}"
    );

    // The first attempt is over. Its late report must not reach the link state, not even while
    // the second attempt is connecting (the state is `Connecting` then, so only the attempt's
    // identity tells the two apart).
    let old = kept.lock().unwrap()[0].clone();
    sup.reconnect_now();
    let st = next_state(&mut states).await;
    let (stage, _) = connecting(&st);
    assert_eq!(
        stage, None,
        "the new attempt starts without the old attempt's stage"
    );
    old.reached("late step");
    assert!(
        states.try_recv().is_err(),
        "a report of an ended attempt was published"
    );
    second_gate.notify_one();
    let st = next_state(&mut states).await;
    let (stage, _) = connecting(&st);
    assert_eq!(stage, Some("new step"));
    sup.stop().await;
}

/// The wire shape: an old `connecting` (no stage, no since) parses; a new one round-trips; a
/// state without a stage serializes without the stage key, as before.
#[test]
fn connecting_parses_from_the_old_wire_shape_and_round_trips() {
    let old: LinkState = serde_json::from_str(r#"{"state":"connecting"}"#).unwrap();
    assert_eq!(
        old,
        LinkState::Connecting {
            stage: None,
            since_ms: 0
        }
    );

    let with: LinkState = serde_json::from_str(
        r#"{"state":"connecting","stage":"sending the helper","since_ms":1500}"#,
    )
    .unwrap();
    assert_eq!(
        with,
        LinkState::Connecting {
            stage: Some("sending the helper".into()),
            since_ms: 1500
        }
    );

    let bare = serde_json::to_value(LinkState::Connecting {
        stage: None,
        since_ms: 0,
    })
    .unwrap();
    assert_eq!(
        bare,
        serde_json::json!({"state": "connecting", "since_ms": 0})
    );
    let back: LinkState = serde_json::from_value(serde_json::to_value(&with).unwrap()).unwrap();
    assert_eq!(back, with);
}
