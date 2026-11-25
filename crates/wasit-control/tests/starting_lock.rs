//! The starting lock a daemon start holds until it is ready, and the wait a process that lost
//! the instance lock to that start makes. Nothing here is polled: the waiter's completion is
//! driven by the holder's release.
#![cfg(unix)]

mod common;

use std::time::Duration;

use common::*;
use wasit_control::{starting_lock_path, wait_until_started, LockError, StartingLock};

#[tokio::test]
async fn a_second_start_sees_the_first_and_a_waiter_returns_when_it_is_ready() {
    let dir = scratch_dir("starting");
    let sock = dir.join("d.sock");

    let first = StartingLock::try_acquire(&sock).await.unwrap();
    // While the first start is in progress a second one cannot take the lock ...
    assert!(matches!(
        StartingLock::try_acquire(&sock).await,
        Err(LockError::Held(p)) if p == starting_lock_path(&sock)
    ));
    // ... and a waiter does not return before the start is ready.
    let mut waiter = tokio::spawn(wait_until_started(sock.clone()));
    // A negative check can only look for a bounded window: the waiter is still pending after it.
    let early = tokio::time::timeout(Duration::from_millis(300), &mut waiter).await;
    assert!(
        early.is_err(),
        "the wait ended before the start was ready: {early:?}"
    );

    // Ready: the start releases the lock, the waiter returns, and the lock can be taken again.
    drop(first);
    within(waiter)
        .await
        .unwrap()
        .expect("the wait ends cleanly");
    assert!(StartingLock::try_acquire(&sock).await.is_ok());
}

#[tokio::test]
async fn a_wait_with_no_start_in_progress_returns_at_once() {
    let dir = scratch_dir("starting-none");
    let sock = dir.join("d.sock");
    within(wait_until_started(sock))
        .await
        .expect("nothing to wait for");
}
