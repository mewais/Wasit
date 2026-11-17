//! Local control endpoint: Unix domain socket `<dir>/ctl.sock`, or Windows named pipe
//! `\\.\pipe\wasit-<basename(dir)>`.

use std::io;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) type BoxRead = Box<dyn AsyncRead + Send + Unpin>;
pub(crate) type BoxWrite = Box<dyn AsyncWrite + Send + Unpin>;

#[cfg(unix)]
pub(crate) fn name(dir: &Path) -> PathBuf {
    dir.join("ctl.sock")
}

#[cfg(windows)]
pub(crate) fn name(dir: &Path) -> PathBuf {
    let base: String = dir
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
        .chars()
        .map(|c| if c == '\\' || c == '/' { '_' } else { c })
        .collect();
    PathBuf::from(format!(r"\\.\pipe\wasit-{base}"))
}

fn split<S: AsyncRead + AsyncWrite + Send + 'static>(s: S) -> (BoxRead, BoxWrite) {
    let (r, w) = tokio::io::split(s);
    (Box::new(r), Box::new(w))
}

#[cfg(unix)]
pub(crate) struct Listener {
    inner: tokio::net::UnixListener,
    path: PathBuf,
}

#[cfg(unix)]
impl Listener {
    /// Bind the endpoint. The caller holds the session lock, so a leftover socket file belongs
    /// to a dead shim and is removed.
    pub(crate) fn bind(dir: &Path) -> io::Result<Self> {
        let path = name(dir);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let inner = tokio::net::UnixListener::bind(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("bind {}: {e}", path.display())))?;
        Ok(Listener { inner, path })
    }

    pub(crate) async fn accept(&mut self) -> io::Result<(BoxRead, BoxWrite)> {
        let (s, _) = self.inner.accept().await?;
        Ok(split(s))
    }

    pub(crate) fn remove(&self) -> io::Result<()> {
        std::fs::remove_file(&self.path)
    }
}

#[cfg(unix)]
pub(crate) async fn connect(dir: &Path) -> io::Result<(BoxRead, BoxWrite)> {
    let path = name(dir);
    let s = tokio::net::UnixStream::connect(&path)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("connect {}: {e}", path.display())))?;
    Ok(split(s))
}

#[cfg(windows)]
pub(crate) struct Listener {
    name: PathBuf,
    next: tokio::net::windows::named_pipe::NamedPipeServer,
}

#[cfg(windows)]
impl Listener {
    pub(crate) fn bind(dir: &Path) -> io::Result<Self> {
        use tokio::net::windows::named_pipe::ServerOptions;
        let name = name(dir);
        let next = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&name)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("create pipe {}: {e}", name.display()))
            })?;
        Ok(Listener { name, next })
    }

    pub(crate) async fn accept(&mut self) -> io::Result<(BoxRead, BoxWrite)> {
        use tokio::net::windows::named_pipe::ServerOptions;
        self.next.connect().await?;
        let fresh = ServerOptions::new().create(&self.name)?;
        let conn = std::mem::replace(&mut self.next, fresh);
        Ok(split(conn))
    }

    pub(crate) fn remove(&self) -> io::Result<()> {
        // A named pipe disappears with its last handle.
        Ok(())
    }
}

#[cfg(windows)]
pub(crate) async fn connect(dir: &Path) -> io::Result<(BoxRead, BoxWrite)> {
    use std::time::Duration;
    use tokio::net::windows::named_pipe::ClientOptions;
    const ERROR_PIPE_BUSY: i32 = 231;
    let name = name(dir);
    let mut backoff = Duration::from_millis(5);
    loop {
        match ClientOptions::new().open(&name) {
            Ok(c) => return Ok(split(c)),
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                // Backoff schedule; the caller bounds the whole connect with a deadline.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(200));
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("open pipe {}: {e}", name.display()),
                ))
            }
        }
    }
}
