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
