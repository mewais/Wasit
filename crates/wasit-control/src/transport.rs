//! Byte-stream transports: duplex streams, local endpoints, listeners and the single-instance
//! lock.

use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// Any bidirectional byte stream.
pub trait Duplex: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Duplex for T {}

pub type BoxDuplex = Box<dyn Duplex>;

/// Combine a reader and a writer (e.g. stdin/stdout) into one duplex.
pub fn join_duplex<R, W>(reader: R, writer: W) -> BoxDuplex
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    Box::new(tokio::io::join(reader, writer))
}

/// An in-memory connected pair (for tests and in-process links).
pub fn memory_pair(buffer: usize) -> (BoxDuplex, BoxDuplex) {
    let (a, b) = tokio::io::duplex(buffer);
    (Box::new(a), Box::new(b))
}

/// A local daemon endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// Unix domain socket path (Unix only).
    Unix(PathBuf),
    /// Windows named pipe name, e.g. `\\.\pipe\marsad` (Windows only).
    NamedPipe(String),
}

impl Endpoint {
    /// Connect to the endpoint.
    pub async fn connect(&self) -> io::Result<BoxDuplex> {
        match self {
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let s = tokio::net::UnixStream::connect(path).await?;
                Ok(Box::new(s))
            }
            #[cfg(windows)]
            Endpoint::NamedPipe(name) => {
                use std::os::windows::io::AsRawHandle;
                use tokio::net::windows::named_pipe::ClientOptions;
                // ERROR_PIPE_BUSY: every server instance is connected right now. The server keeps
                // several instances waiting, so this is brief; wait for one with `WaitNamedPipe`
                // (no sleeps), bounded only by the caller's own deadline.
                const ERROR_PIPE_BUSY: i32 = 231;
                loop {
                    match ClientOptions::new().open(name) {
                        Ok(c) => {
                            // The name is predictable: refuse a server that is not this user's.
                            wasit_process::pipe_security::verify_server(c.as_raw_handle())?;
                            return Ok(Box::new(c));
                        }
                        Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                            wait_for_pipe(name.clone()).await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            #[allow(unreachable_patterns)]
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("endpoint {other:?} is not supported on this platform"),
            )),
        }
    }

    /// Whether a connect error means "nobody is serving this endpoint" (absent or stale).
    pub fn is_absent_error(e: &io::Error) -> bool {
        matches!(
            e.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        )
    }
}

/// Wait (in a blocking thread) until an instance of the named pipe is free to connect to, or
/// the pipe is gone (an error). The wait is not bounded here: the caller's deadline bounds the
/// connect. A thread left waiting after its caller gave up ends when an instance frees up or the
/// pipe is removed.
#[cfg(windows)]
async fn wait_for_pipe(name: String) -> io::Result<()> {
    use windows_sys::Win32::System::Pipes::{WaitNamedPipeW, NMPWAIT_WAIT_FOREVER};
    on_own_thread("wasit-pipe-wait", move || {
        let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        // SAFETY: `wide` is NUL-terminated and lives across the call.
        if unsafe { WaitNamedPipeW(wide.as_ptr(), NMPWAIT_WAIT_FOREVER) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })
    .await
}

/// Run `work` on a dedicated thread and await its result.
///
/// Used for waits that have no bound of their own. `spawn_blocking` would pin a thread of
/// tokio's blocking pool for as long as the wait lasts, and a wait that never ends would take
/// that pool thread for good. Here only the wait's own thread is taken; dropping the returned
/// future abandons the wait (the thread ends when `work` does), and the pool is untouched.
async fn on_own_thread<T, F>(name: &str, work: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            // The receiver is gone when the waiter was dropped: nobody wants the answer.
            let _ = tx.send(work());
        })?;
    rx.await
        .map_err(|_| io::Error::other("the wait ended without an answer"))?
}

/// The local endpoint for a daemon identified by `path`: the Unix socket at `path`, or on
/// Windows the named pipe `\\.\pipe\wasit-<hash of path>` (pipes have no filesystem path).
pub fn local_endpoint(path: &Path) -> Endpoint {
    #[cfg(unix)]
    {
        Endpoint::Unix(path.to_path_buf())
    }
    #[cfg(windows)]
    {
        // FNV-1a of the path: one pipe per daemon path. Pipe names are machine-wide; the data
        // path lies in the user's profile (`%LOCALAPPDATA%`), so two users never share one.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in path.to_string_lossy().bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        Endpoint::NamedPipe(format!(r"\\.\pipe\wasit-{h:016x}"))
    }
}

/// Listener type of [`bind_local_exclusive`].
#[cfg(unix)]
pub type LocalListener = UnixSocketListener;
/// Listener type of [`bind_local_exclusive`].
#[cfg(windows)]
pub type LocalListener = NamedPipeListener;

/// Bind the endpoint [`local_endpoint`] names for `path`, holding the single-instance lock
/// `<path>.lock`. A second instance gets `BindError::Lock(LockError::Held)`.
pub async fn bind_local_exclusive(path: impl Into<PathBuf>) -> Result<LocalListener, BindError> {
    let path = path.into();
    #[cfg(unix)]
    {
        UnixSocketListener::bind_exclusive(path).await
    }
    #[cfg(windows)]
    {
        let Endpoint::NamedPipe(name) = local_endpoint(&path) else {
            unreachable!("local endpoints are named pipes on Windows");
        };
        NamedPipeListener::bind_exclusive(name, lock_path_for(&path)).await
    }
}

/// Source of incoming connections for [`crate::ControlServer`].
#[async_trait]
pub trait Listener: Send + 'static {
    /// Next connection; `Ok(None)` when the listener will produce no more.
    async fn accept(&mut self) -> io::Result<Option<BoxDuplex>>;
}

/// Serves exactly one connection made of a reader/writer pair (bridge mode: this process's
/// stdio).
pub struct StdioListener {
    io: Option<BoxDuplex>,
}

impl StdioListener {
    pub fn new<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        StdioListener {
            io: Some(join_duplex(reader, writer)),
        }
    }

    /// This process's stdin/stdout.
    pub fn process() -> Self {
        Self::new(tokio::io::stdin(), tokio::io::stdout())
    }
}

#[async_trait]
impl Listener for StdioListener {
    async fn accept(&mut self) -> io::Result<Option<BoxDuplex>> {
        Ok(self.io.take())
    }
}

/// Error acquiring the single-instance lock.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("another instance holds {0}")]
    Held(PathBuf),
    #[error("lock file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Exclusive advisory lock on a file; held until dropped. Liveness of the owner = lock held.
#[derive(Debug)]
pub struct InstanceLock {
    _file: std::fs::File,
    path: PathBuf,
}

/// Open (creating it) a lock file: the file is never truncated, its content is nothing.
fn open_lock_file(path: &Path) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

impl InstanceLock {
    /// Try to take the lock without waiting.
    pub async fn try_acquire(path: impl Into<PathBuf>) -> Result<InstanceLock, LockError> {
        let path = path.into();
        let p = path.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<std::fs::File, LockError> {
            let file = open_lock_file(&p).map_err(|source| LockError::Io {
                path: p.clone(),
                source,
            })?;
            match file.try_lock() {
                Ok(()) => Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => Err(LockError::Held(p)),
                Err(std::fs::TryLockError::Error(source)) => Err(LockError::Io { path: p, source }),
            }
        })
        .await
        .map_err(|e| LockError::Io {
            path: path.clone(),
            source: io::Error::other(e),
        })?;
        Ok(InstanceLock { _file: res?, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InstanceLock {
    /// Released explicitly, not only by closing the file: `flock` locks belong to the open file
    /// description, and a child forked by another thread of this process holds a copy of it
    /// until its `exec`, which would keep the lock (and so "an instance is running") alive for
    /// the next owner in this process. `LOCK_UN` releases it whoever has a copy.
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

/// The lock file used for an endpoint path: `<path>.lock`.
pub fn lock_path_for(endpoint_path: &Path) -> PathBuf {
    let mut s = endpoint_path.as_os_str().to_owned();
    s.push(".lock");
    PathBuf::from(s)
}

/// The starting lock file of an endpoint path: `<path>.starting` (see [`StartingLock`]).
pub fn starting_lock_path(endpoint_path: &Path) -> PathBuf {
    let mut s = endpoint_path.as_os_str().to_owned();
    s.push(".starting");
    PathBuf::from(s)
}

/// The lock a daemon start holds, exclusively, from before it takes the instance lock until its
/// endpoint is bound and it is ready (then it is dropped). A process that lost the instance lock
/// to that start waits on it with [`wait_until_started`]: the wait ends exactly when the start is
/// ready, so the endpoint can be dialled once, with no retries.
#[derive(Debug)]
pub struct StartingLock {
    _file: std::fs::File,
}

impl StartingLock {
    /// Take the starting lock of `endpoint_path` without waiting: `LockError::Held` while another
    /// start holds it.
    pub async fn try_acquire(endpoint_path: impl Into<PathBuf>) -> Result<StartingLock, LockError> {
        let path = starting_lock_path(&endpoint_path.into());
        let p = path.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<std::fs::File, LockError> {
            let file = open_lock_file(&p).map_err(|source| LockError::Io {
                path: p.clone(),
                source,
            })?;
            match file.try_lock() {
                Ok(()) => Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => Err(LockError::Held(p)),
                Err(std::fs::TryLockError::Error(source)) => Err(LockError::Io { path: p, source }),
            }
        })
        .await
        .map_err(|e| LockError::Io {
            path,
            source: io::Error::other(e),
        })?;
        Ok(StartingLock { _file: res? })
    }
}

impl Drop for StartingLock {
    /// Released explicitly, for the reason [`InstanceLock`] is: a fork-window copy of the file
    /// must not keep a start "in progress" for a waiter after this process is ready.
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

/// Wait until no start holds the starting lock of `endpoint_path`: returns at once when none
/// does, and when the one in progress is ready (it releases the lock then). The wait has no
/// bound of its own and runs on a dedicated thread (see `on_own_thread`), so a start that never
/// releases its lock cannot pin tokio's blocking pool; dropping the future abandons the wait.
/// Nothing is polled.
pub async fn wait_until_started(endpoint_path: impl Into<PathBuf>) -> io::Result<()> {
    let path = starting_lock_path(&endpoint_path.into());
    on_own_thread("wasit-start-wait", move || {
        let file = open_lock_file(&path)?;
        file.lock_shared()?;
        file.unlock()
    })
    .await
}

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error("bind {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
}

#[cfg(unix)]
pub use unix::UnixSocketListener;

#[cfg(unix)]
mod unix {
    use super::*;

    /// Unix socket listener, optionally holding the single-instance lock.
    pub struct UnixSocketListener {
        inner: tokio::net::UnixListener,
        path: PathBuf,
        lock: Option<InstanceLock>,
    }

    impl UnixSocketListener {
        /// Bind without any lock (fails if the path exists).
        pub fn bind(path: impl Into<PathBuf>) -> Result<Self, BindError> {
            let path = path.into();
            let inner = tokio::net::UnixListener::bind(&path).map_err(|source| BindError::Io {
                path: path.display().to_string(),
                source,
            })?;
            Ok(UnixSocketListener {
                inner,
                path,
                lock: None,
            })
        }

        /// Take the exclusive lock `<path>.lock`; only once it is held, remove a stale socket
        /// file left by a dead instance, then bind. A second instance gets
        /// `BindError::Lock(LockError::Held)` and must not touch the socket.
        pub async fn bind_exclusive(path: impl Into<PathBuf>) -> Result<Self, BindError> {
            let path = path.into();
            let lock = InstanceLock::try_acquire(lock_path_for(&path)).await?;
            match tokio::fs::remove_file(&path).await {
                Ok(()) => tracing::debug!(path = %path.display(), "removed stale socket"),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(BindError::Io {
                        path: path.display().to_string(),
                        source,
                    })
                }
            }
            let mut l = Self::bind(path)?;
            l.lock = Some(lock);
            Ok(l)
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for UnixSocketListener {
        fn drop(&mut self) {
            if self.lock.is_some() {
                // Only the lock owner may remove the socket; a failure leaves a stale file
                // that the next owner removes.
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }

    #[async_trait]
    impl Listener for UnixSocketListener {
        async fn accept(&mut self) -> io::Result<Option<BoxDuplex>> {
            let (s, _) = self.inner.accept().await?;
            Ok(Some(Box::new(s)))
        }
    }
}

#[cfg(windows)]
pub use windows::NamedPipeListener;

#[cfg(windows)]
mod windows {
    use super::*;
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

    use wasit_process::pipe_security::OwnerOnly;

    /// Pipe instances kept waiting for clients. A client that finds one free connects at once;
    /// with a single waiting instance it would find `ERROR_PIPE_BUSY` between one accept and
    /// the creation of the next.
    const WAITING_INSTANCES: usize = 8;

    /// Windows named pipe listener. The first instance is created with
    /// `first_pipe_instance(true)`, so a second daemon fails to bind (single instance). Every
    /// instance carries the owner-only security descriptor (see `pipe_security`): only the
    /// current user and SYSTEM may connect.
    pub struct NamedPipeListener {
        name: String,
        security: OwnerOnly,
        /// Instances waiting for a client; each accept replaces the one it hands out.
        waiting: Vec<NamedPipeServer>,
        _lock: Option<InstanceLock>,
    }

    /// One pipe instance of `name`. Only the first may be created with `first`.
    fn create_instance(
        name: &str,
        security: &OwnerOnly,
        first: bool,
    ) -> io::Result<NamedPipeServer> {
        let mut options = ServerOptions::new();
        options.first_pipe_instance(first);
        // SAFETY: `security.attributes()` points at a SECURITY_ATTRIBUTES that lives as long as
        // `security`, which the listener owns for as long as it creates instances.
        unsafe { options.create_with_security_attributes_raw(name, security.attributes()) }
    }

    impl NamedPipeListener {
        pub fn bind(name: impl Into<String>) -> Result<Self, BindError> {
            let name = name.into();
            let io_err = |source| BindError::Io {
                path: name.clone(),
                source,
            };
            let security = OwnerOnly::for_current_user().map_err(io_err)?;
            let mut waiting = Vec::with_capacity(WAITING_INSTANCES);
            waiting.push(create_instance(&name, &security, true).map_err(io_err)?);
            for _ in 1..WAITING_INSTANCES {
                waiting.push(create_instance(&name, &security, false).map_err(io_err)?);
            }
            Ok(NamedPipeListener {
                name,
                security,
                waiting,
                _lock: None,
            })
        }

        /// Bind while holding an exclusive lock file (e.g. in the data directory).
        pub async fn bind_exclusive(
            name: impl Into<String>,
            lock_file: impl Into<PathBuf>,
        ) -> Result<Self, BindError> {
            let lock = InstanceLock::try_acquire(lock_file).await?;
            let mut l = Self::bind(name)?;
            l._lock = Some(lock);
            Ok(l)
        }
    }

    #[async_trait]
    impl Listener for NamedPipeListener {
        async fn accept(&mut self) -> io::Result<Option<BoxDuplex>> {
            // The first waiting instance a client connects to. Dropping the losing connect
            // futures loses no connection event (tokio's `connect` is cancel safe).
            let (connected, index, losers) = {
                let waits: Vec<_> = self.waiting.iter().map(|s| Box::pin(s.connect())).collect();
                futures::future::select_all(waits).await
            };
            drop(losers);
            connected?;
            let fresh = create_instance(&self.name, &self.security, false)?;
            let served = std::mem::replace(&mut self.waiting[index], fresh);
            Ok(Some(Box::new(served)))
        }
    }
}
