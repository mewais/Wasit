//! [`RuntimeClient`]: the typed client of [`crate::RuntimeService`] over a supervised
//! `wasit-control` link. Every consumer (GUI core, hub, tests) uses it, so the wire shapes live
//! only in [`crate::wire`].

use std::marker::PhantomData;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use wasit_control::{
    connector, CallError, ClientOptions, ControlClient, Cursor, DurableOpts, DurableSubscription,
    Endpoint, Hello, HostId, RequestOpts, RpcError, Supervised, SupervisedOptions,
};
use wasit_core::{Command, DriverId};

use crate::wire::{
    methods, topics, Ack, CommandError, CommandOutcome, CommandReply, CommandRequest,
    CreateSession, CreateSessionReply, HistoryPage, HistoryRequest, HostEvent, HostInfo,
    InventoryReply, InventoryRequest, RefreshRequest, RenameSession, RuntimeError, SessionEvent,
    SessionId, SessionList, SessionRef, SessionSnapshot,
};

/// A failed client call.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ClientError {
    /// The session refused or could not apply a command.
    #[error(transparent)]
    Command(CommandError),
    /// The runtime refused the operation.
    #[error(transparent)]
    Runtime(RuntimeError),
    /// Transport / protocol failure, or an error without typed data.
    #[error(transparent)]
    Call(CallError),
}

impl ClientError {
    fn from_call(e: CallError) -> Self {
        match e {
            CallError::Rpc(rpc) => Self::from_rpc(rpc),
            other => ClientError::Call(other),
        }
    }

    /// Typed data first: a [`RuntimeError`] always carries `message`, a [`CommandError`]
    /// never does, so the two cannot be confused.
    fn from_rpc(rpc: RpcError) -> Self {
        if let Some(data) = &rpc.data {
            if let Ok(r) = serde_json::from_value::<RuntimeError>(data.clone()) {
                return ClientError::Runtime(r);
            }
            if let Ok(c) = serde_json::from_value::<CommandError>(data.clone()) {
                return ClientError::Command(c);
            }
        }
        ClientError::Call(CallError::Rpc(rpc))
    }
}

/// A durable, typed subscription: survives reconnects and `Lagged` closes by resubscribing from
/// its last cursor (the service replays exactly).
pub struct TypedSubscription<T> {
    inner: DurableSubscription,
    _t: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> TypedSubscription<T> {
    /// Next event; `Some(Err)` once for a terminal error; `None` when the topic ended.
    pub async fn next(&mut self) -> Option<Result<(Cursor, T), ClientError>> {
        match self.inner.next().await? {
            Ok((c, v)) => Some(
                serde_json::from_value(v)
                    .map(|t| (c, t))
                    .map_err(|e| ClientError::Call(CallError::Decode(e.to_string()))),
            ),
            Err(e) => Some(Err(ClientError::from_rpc(e))),
        }
    }

    pub fn last_cursor(&self) -> Option<&Cursor> {
        self.inner.last_cursor()
    }

    /// Number of successful (re)subscribes so far.
    pub fn subscribe_count(&self) -> u64 {
        self.inner.subscribe_count()
    }
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct RuntimeClient {
    link: Supervised,
    host: Option<HostId>,
    deadline: Duration,
    wait_connected: Duration,
}

impl RuntimeClient {
    /// Use an existing supervised link.
    pub fn new(link: Supervised) -> Self {
        RuntimeClient {
            link,
            host: None,
            deadline: Duration::from_secs(60),
            wait_connected: Duration::from_secs(10),
        }
    }

    /// Supervise a link to a local daemon endpoint (reconnecting with backoff).
    pub fn connect_endpoint(endpoint: Endpoint, hello: Hello) -> Self {
        let conn = connector(move || {
            let endpoint = endpoint.clone();
            let hello = hello.clone();
            async move { ControlClient::connect_endpoint(&endpoint, ClientOptions::new(hello)).await }
        });
        Self::new(Supervised::spawn(conn, SupervisedOptions::default()))
    }

    /// Address requests to `host` (routed by a hub).
    pub fn with_host(mut self, host: impl Into<HostId>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Per-call deadline and how long a call waits for a disconnected link.
    pub fn with_deadlines(mut self, deadline: Duration, wait_connected: Duration) -> Self {
        self.deadline = deadline;
        self.wait_connected = wait_connected;
        self
    }

    pub fn link(&self) -> &Supervised {
        &self.link
    }

    async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, ClientError> {
        let mut opts = RequestOpts::new(self.deadline);
        opts.host = self.host.clone();
        self.link
            .request(method, params, opts, self.wait_connected)
            .await
            .map_err(ClientError::from_call)
    }

    fn durable<T>(
        &self,
        topic: String,
        params: serde_json::Value,
        from: Option<Cursor>,
    ) -> TypedSubscription<T> {
        let opts = DurableOpts {
            host: self.host.clone(),
            params,
            from,
            ..DurableOpts::default()
        };
        TypedSubscription {
            inner: self.link.subscribe_durable(topic, opts),
            _t: PhantomData,
        }
    }

    pub async fn host_info(&self) -> Result<HostInfo, ClientError> {
        self.call(methods::HOST_INFO, ()).await
    }

    pub async fn inventory(&self, project: Option<String>) -> Result<InventoryReply, ClientError> {
        self.call(methods::DRIVERS_INVENTORY, InventoryRequest { project })
            .await
    }

    pub async fn refresh(
        &self,
        driver: Option<DriverId>,
        project: Option<String>,
    ) -> Result<(), ClientError> {
        let _: Ack = self
            .call(methods::DRIVERS_REFRESH, RefreshRequest { driver, project })
            .await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<SessionSnapshot>, ClientError> {
        let l: SessionList = self.call(methods::SESSIONS_LIST, ()).await?;
        Ok(l.sessions)
    }

    pub async fn create(&self, req: CreateSession) -> Result<CreateSessionReply, ClientError> {
        self.call(methods::SESSIONS_CREATE, req).await
    }

    pub async fn continue_session(&self, id: &SessionId) -> Result<(), ClientError> {
        let _: Ack = self
            .call(methods::SESSIONS_CONTINUE, SessionRef { id: id.clone() })
            .await?;
        Ok(())
    }

    pub async fn stop(&self, id: &SessionId) -> Result<(), ClientError> {
        let _: Ack = self
            .call(methods::SESSIONS_STOP, SessionRef { id: id.clone() })
            .await?;
        Ok(())
    }

    pub async fn delete(&self, id: &SessionId) -> Result<(), ClientError> {
        let _: Ack = self
            .call(methods::SESSIONS_DELETE, SessionRef { id: id.clone() })
            .await?;
        Ok(())
    }

    pub async fn rename(&self, id: &SessionId, name: Option<String>) -> Result<(), ClientError> {
        let _: Ack = self
            .call(
                methods::SESSIONS_RENAME,
                RenameSession {
                    id: id.clone(),
                    display_name: name,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn snapshot(&self, id: &SessionId) -> Result<SessionSnapshot, ClientError> {
        self.call(methods::SESSION_SNAPSHOT, SessionRef { id: id.clone() })
            .await
    }

    pub async fn history(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: u32,
    ) -> Result<HistoryPage, ClientError> {
        self.call(
            methods::SESSION_HISTORY,
            HistoryRequest {
                id: id.clone(),
                before,
                limit,
            },
        )
        .await
    }

    pub async fn command(
        &self,
        id: &SessionId,
        command: Command,
    ) -> Result<CommandOutcome, ClientError> {
        let r: CommandReply = self
            .call(
                methods::SESSION_COMMAND,
                CommandRequest {
                    id: id.clone(),
                    command,
                },
            )
            .await?;
        Ok(r.outcome)
    }

    /// `session.attachment.put`: one chunk of an upload.
    pub async fn attachment_put(
        &self,
        r: crate::media::AttachmentPut,
    ) -> Result<crate::media::AttachmentPutReply, ClientError> {
        self.call(methods::SESSION_ATTACHMENT_PUT, r).await
    }

    /// Upload `bytes` as `name` in 1 MiB chunks; the attachment to send with a prompt.
    pub async fn upload(
        &self,
        id: &SessionId,
        upload: &str,
        name: &str,
        bytes: &[u8],
    ) -> Result<wasit_core::Attachment, ClientError> {
        let size = bytes.len() as u64;
        let chunks: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[][..]]
        } else {
            bytes.chunks(crate::media::MAX_UPLOAD_CHUNK).collect()
        };
        let n = chunks.len();
        let mut offset = 0u64;
        for (i, c) in chunks.into_iter().enumerate() {
            let r = self
                .attachment_put(crate::media::AttachmentPut {
                    id: id.clone(),
                    upload: upload.to_string(),
                    name: name.to_string(),
                    size,
                    offset,
                    data: wasit_core::content::b64_encode(c),
                    last: i + 1 == n,
                })
                .await?;
            offset = r.received;
            if let Some(a) = r.attachment {
                return Ok(a);
            }
        }
        Err(ClientError::Runtime(RuntimeError::Io(
            "the upload did not finish".into(),
        )))
    }

    /// `session.file.read`: a range of a blob or a file.
    pub async fn file_read(
        &self,
        r: crate::media::FileReadRequest,
    ) -> Result<crate::media::FileChunk, ClientError> {
        self.call(methods::SESSION_FILE_READ, r).await
    }

    /// `session.files.stat`.
    pub async fn files_stat(
        &self,
        id: &SessionId,
        paths: Vec<String>,
    ) -> Result<crate::media::FilesStatReply, ClientError> {
        self.call(
            methods::SESSION_FILES_STAT,
            crate::media::FilesStatRequest {
                id: id.clone(),
                paths,
            },
        )
        .await
    }

    /// A session's events strictly after `from` (from the beginning when `None`).
    pub fn subscribe_session(
        &self,
        id: &SessionId,
        from: Option<u64>,
    ) -> TypedSubscription<SessionEvent> {
        self.durable(
            topics::session_events(id.as_str()),
            serde_json::Value::Null,
            from.map(Cursor::from_u64),
        )
    }

    /// The session list: a `Reset` first (unless resuming from a still-valid cursor), then
    /// changes.
    pub fn subscribe_host(&self, from: Option<Cursor>) -> TypedSubscription<HostEvent> {
        self.durable(topics::HOST_SESSIONS.into(), serde_json::Value::Null, from)
    }

    pub fn subscribe_inventory(
        &self,
        project: Option<String>,
    ) -> TypedSubscription<InventoryReply> {
        let params = serde_json::to_value(InventoryRequest { project }).unwrap_or_default();
        self.durable(topics::DRIVERS_INVENTORY.into(), params, None)
    }
}
