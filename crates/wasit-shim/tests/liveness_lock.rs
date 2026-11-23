//! The liveness lock (`shim.lock`) must mean exactly one thing: a shim is alive.
//!
//! `flock` locks belong to the open file *description*. A probe that takes the lock and then
//! simply closes its descriptor releases the lock only if no other copy of the description
//! exists; a `fork` on another thread of the same process creates such a copy for the time
//! between `fork` and the child's `exec`. These tests run probes (and starting shims) while
//! other threads fork as fast as they can, and count the false answers. Nothing here waits on
//! the clock: every loop is bounded by a count.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common::*;
use wasit_shim::ShimState;

/// Threads forking short-lived children through `wasit-process` until `stop` is set; returns
/// the number of children started.
fn spawner_threads(
    n: usize,
    stop: &Arc<AtomicBool>,
    started: &Arc<AtomicU64>,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..n)
        .map(|_| {
            let stop = stop.clone();
            let started = started.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let status = wasit_process::std_command("true")
                        .status()
                        .expect("spawn true");
                    assert!(status.success());
                    started.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect()
}

fn run_dir_without_a_shim() -> TempDir {
    let t = TempDir::new();
    // The lock file exists (a shim ran here once) and nobody holds it.
    std::fs::File::create(t.path().join(wasit_shim::LOCK_FILE)).unwrap();
    t
}

/// One prober, no other prober: every answer must be `Dead` although other threads fork all
/// the time. Before the fix the lock a probe had just released stayed held through a forked
/// child's copy of the descriptor until that child exec'ed, and the next probe said `Running`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_free_lock_is_never_reported_held_while_other_threads_fork() {
    const PROBES: u32 = 3000;
    let t = run_dir_without_a_shim();
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let spawners = spawner_threads(6, &stop, &started);

    let mut running = 0u32;
    for _ in 0..PROBES {
        match wasit_shim::probe(t.path()).await.unwrap() {
            ShimState::Running => running += 1,
            ShimState::Dead => {}
            other => panic!("unexpected state {other:?}"),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for s in spawners {
        s.join().unwrap();
    }
    assert!(started.load(Ordering::Relaxed) > 0, "no child was forked");
    assert_eq!(
        running,
        0,
        "{running} of {PROBES} probes reported a free lock as held ({} children forked meanwhile)",
        started.load(Ordering::Relaxed)
    );
}

/// Probes of one session from several tasks at once (the manager adopting sessions while an
/// actor and a snapshot loader probe the same one): a probe must never see another probe's
/// momentary hold as a shim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_probes_do_not_see_each_other_as_a_shim() {
    const TASKS: u32 = 6;
    const PROBES: u32 = 600;
    let t = run_dir_without_a_shim();
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let spawners = spawner_threads(3, &stop, &started);

    let dir: Arc<Path> = Arc::from(t.path());
    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let dir = dir.clone();
            tokio::spawn(async move {
                let mut running = 0u32;
                for _ in 0..PROBES {
                    if wasit_shim::probe(&dir).await.unwrap() == ShimState::Running {
                        running += 1;
                    }
                }
                running
            })
        })
        .collect();
    let mut running = 0;
    for task in tasks {
        running += task.await.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for s in spawners {
        s.join().unwrap();
    }
    assert_eq!(
        running,
        0,
        "{running} of {} probes saw a free lock as held",
        TASKS * PROBES
    );
}

/// A new shim started right after the previous one ended always gets the lock, although
/// probes of the same session and forks run all the time in the starting process: the previous
/// shim released the lock explicitly, and a probe's momentary hold never excludes a starting
/// shim for good (it waits for the probe, it does not fail with "already holds the lock").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_shim_right_after_the_previous_one_always_acquires() {
    const ROUNDS: u32 = 40;
    let t = TempDir::new();
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let spawners = spawner_threads(4, &stop, &started);
    let prober = {
        let dir = t.path().to_path_buf();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                wasit_shim::probe(&dir).await.unwrap();
                n += 1;
                tokio::task::yield_now().await;
            }
            n
        })
    };

    for round in 0..ROUNDS {
        // `start_child` fails when the shim cannot acquire the lock.
        start_child(t.path(), "cat", &[]).await;
        let c = wasit_shim::ShimClient::connect(t.path()).await.unwrap();
        let mut s = c.attach(1).await.unwrap();
        c.close_stdin().await.unwrap();
        until_exited(&mut s).await;
        until_end(&mut s).await;
        // The connections close after the lock is released: the session is not running now.
        assert_ne!(
            wasit_shim::probe(t.path()).await.unwrap(),
            ShimState::Running,
            "round {round}: the lock was still held after the shim closed its connections"
        );
    }
    stop.store(true, Ordering::Relaxed);
    for s in spawners {
        s.join().unwrap();
    }
    assert!(prober.await.unwrap() > 0);
}

/// The premise of the fix, stated once: a copy of the descriptor (what a forked child holds
/// until its `exec`) keeps a lock alive across `close`, but not across an explicit unlock.
#[test]
fn unlock_releases_a_lock_that_another_copy_of_the_descriptor_keeps_open() {
    use std::os::fd::{AsRawFd, FromRawFd};

    let t = run_dir_without_a_shim();
    let path = t.path().join(wasit_shim::LOCK_FILE);
    let open = || {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    };

    // Closing alone: the copy keeps the description, and with it the lock.
    let f = open();
    f.try_lock().unwrap();
    // SAFETY: dup of a descriptor we own; the new one is owned by `copy`.
    let copy = unsafe { std::fs::File::from_raw_fd(libc::dup(f.as_raw_fd())) };
    drop(f);
    assert!(
        open().try_lock().is_err(),
        "closing one descriptor released a lock another copy keeps open: the premise is gone"
    );
    drop(copy);
    open().try_lock().expect("all copies closed");

    // Explicit unlock: released although the copy is still open.
    let f = open();
    f.try_lock().unwrap();
    // SAFETY: as above.
    let copy = unsafe { std::fs::File::from_raw_fd(libc::dup(f.as_raw_fd())) };
    f.unlock().unwrap();
    drop(f);
    let g = open();
    g.try_lock()
        .expect("an explicit unlock must release the lock whoever holds a copy");
    drop(g);
    drop(copy);
}

/// Descriptors of `pid` that name `target`.
#[cfg(target_os = "linux")]
fn fds_naming(pid: u32, target: &Path) -> usize {
    let target = target.canonicalize().unwrap();
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.unwrap().path()).ok())
        .filter(|l| *l == target)
        .count()
}

/// The shim is the only process that keeps the lock open: neither the guard it re-executes
/// itself as nor the CLI the guard starts inherits it (the descriptor is close-on-exec), so
/// the lock is free the instant the shim dies, whatever its children are still doing.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_shim_keeps_the_lock_open_and_its_death_frees_it() {
    let t = TempDir::new();
    let h = start_child(t.path(), "cat", &[]).await;
    let lock = t.path().join(wasit_shim::LOCK_FILE);
    let guard_pid: u32 = std::fs::read_to_string(format!("/proc/{}/stat", h.child_pid))
        .unwrap()
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(
        guard_pid, h.shim_pid,
        "the CLI's parent is the guard, not the shim"
    );
    assert_eq!(fds_naming(h.shim_pid, &lock), 1);
    assert_eq!(
        fds_naming(guard_pid, &lock),
        0,
        "the guard keeps the lock open"
    );
    assert_eq!(
        fds_naming(h.child_pid, &lock),
        0,
        "the CLI keeps the lock open"
    );
    assert_eq!(
        wasit_shim::probe(t.path()).await.unwrap(),
        ShimState::Running
    );

    kill_hard(h.shim_pid);
    assert!(await_pid_exit(h.shim_pid, T).await);
    // The guard and the CLI are still being taken down; the lock does not wait for them.
    assert_eq!(wasit_shim::probe(t.path()).await.unwrap(), ShimState::Dead);
}

/// A live shim's exclusive lock is an error for a second shim of the same session, reported at
/// once: the wait for the lock (see `acquire`) is only for momentary holders.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_shim_of_a_live_session_is_refused_not_waited_for() {
    let t = TempDir::new();
    start_child(t.path(), "cat", &[]).await;
    let err = wasit_shim::spawn_detached(&child_spec(t.path(), "cat", &[]))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("already holds the lock"), "{err}");
    assert_eq!(
        wasit_shim::probe(t.path()).await.unwrap(),
        ShimState::Running
    );

    // Let the first shim end: no process is left behind.
    let c = wasit_shim::ShimClient::connect(t.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    c.close_stdin().await.unwrap();
    until_end(&mut s).await;
}
