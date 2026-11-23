//! The single-instance lock must be free the moment its owner lets go, even while other threads
//! of the process fork: `flock` locks belong to the open file description, and a forked child
//! holds a copy of it until its `exec`. A release by closing the descriptor alone would leave
//! the lock held through such a copy, and the next owner (a daemon restarted in the same
//! process) would be told that another instance is running. Every loop is bounded by a count.
#![cfg(unix)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use wasit_control::{InstanceLock, LockError};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lock_is_free_as_soon_as_its_owner_drops_it_while_other_threads_fork() {
    const ROUNDS: u32 = 3000;
    let dir = std::env::temp_dir().join(format!("wic-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("d.sock.lock");

    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let spawners: Vec<_> = (0..6)
        .map(|_| {
            let (stop, started) = (stop.clone(), started.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    assert!(wasit_process::std_command("true")
                        .status()
                        .unwrap()
                        .success());
                    started.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let mut held = 0u32;
    for _ in 0..ROUNDS {
        match InstanceLock::try_acquire(&path).await {
            Ok(lock) => drop(lock),
            Err(LockError::Held(_)) => held += 1,
            Err(e) => panic!("{e}"),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for s in spawners {
        s.join().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(started.load(Ordering::Relaxed) > 0, "no child was forked");
    assert_eq!(
        held,
        0,
        "{held} of {ROUNDS} acquisitions found the lock held although its owner had dropped it \
         ({} children forked meanwhile)",
        started.load(Ordering::Relaxed)
    );
}
