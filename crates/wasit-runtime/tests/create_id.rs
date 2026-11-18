//! `CreateSession::id`: a caller-chosen session id is used, and refused when taken.

mod common;

use common::*;
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{RuntimeError, SessionId};

#[tokio::test]
async fn caller_chosen_id_is_used_and_refused_when_taken() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = SessionId::random();
    let mut req = create(d.path(), "claude", &Scenario::builder().build());
    req.id = Some(id.clone());
    assert_eq!(mgr.create(req.clone()).await.unwrap().id, id);
    wait_phase(&mgr, &id, "idle").await;
    assert!(matches!(
        mgr.create(req).await,
        Err(RuntimeError::InvalidRequest(_))
    ));
}

#[tokio::test]
async fn display_name_is_stored_with_the_session_and_survives_restart() {
    let d = TempDir::new();
    let id = {
        let mgr = manager(d.path()).await;
        let id = mgr
            .create(create(d.path(), "claude", &Scenario::builder().build()))
            .await
            .unwrap()
            .id;
        wait_phase(&mgr, &id, "idle").await;
        mgr.rename(&id, Some("  Docs revamp  ".into()))
            .await
            .unwrap();
        let s = mgr.snapshot(&id).unwrap();
        assert_eq!(s.display_name.as_deref(), Some("Docs revamp"));
        // Blank clears it.
        mgr.rename(&id, Some("   ".into())).await.unwrap();
        assert_eq!(mgr.snapshot(&id).unwrap().display_name, None);
        mgr.rename(&id, Some("Docs revamp".into())).await.unwrap();
        mgr.stop(&id).await.unwrap();
        wait_snapshot(&mgr, &id, "exited", |s| !s.phase.is_live()).await;
        id
    };
    // A new manager over the same data re-adopts the session with its name.
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let s = wait_snapshot(&mgr, &id, "loaded", |s| s.loaded).await;
    assert_eq!(s.display_name.as_deref(), Some("Docs revamp"));
    let file = std::fs::read_to_string(
        d.path()
            .join("sessions")
            .join(id.as_str())
            .join("session.json"),
    )
    .unwrap();
    assert!(
        file.contains("\"display_name\": \"Docs revamp\"")
            || file.contains("\"display_name\":\"Docs revamp\""),
        "{file}"
    );
    assert!(matches!(
        mgr.rename(&SessionId::random(), None).await,
        Err(RuntimeError::SessionNotFound(_))
    ));
}
