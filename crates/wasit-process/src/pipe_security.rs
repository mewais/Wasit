//! Who may use a Windows named pipe, and who answers on it.
//!
//! A pipe name is machine-wide and derived from the daemon's path, so it is predictable. Two
//! protections follow, both Windows-only:
//!
//! - The listener creates every pipe instance with a security descriptor that admits only the
//!   current user and SYSTEM ([`owner_only_sddl`]). Other local users cannot connect.
//! - A client checks that the process serving the pipe runs as the current user
//!   ([`verify_server`]). Without this, a process that created the pipe first (before the
//!   daemon) would receive the requests, including sign-in answers.
//!
//! The Windows code is not exercised by the Unix test runs; it is type-checked with
//! `cargo xwin check`. The SDDL text is pure and tested everywhere.

/// The SDDL of a pipe that only `user_sid` (a SID string such as `S-1-5-21-…`) and SYSTEM may
/// open: a protected DACL (nothing inherited) with one full-access allow ACE for each.
#[cfg(any(windows, test))]
pub fn owner_only_sddl(user_sid: &str) -> String {
    format!("D:P(A;;GA;;;{user_sid})(A;;GA;;;SY)")
}

#[cfg(windows)]
pub use win::{verify_server, OwnerOnly};

#[cfg(windows)]
mod win {
    use std::ffi::c_void;
    use std::io;
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{LocalFree, FALSE, HANDLE};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        EqualSid, GetTokenInformation, TokenUser, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
        TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    use super::owner_only_sddl;

    /// A token's user record. The SID points into `words`, so the buffer is kept as `usize`
    /// words (pointer alignment) and lives as long as the record.
    struct UserRecord {
        words: Vec<usize>,
    }

    impl UserRecord {
        fn sid(&self) -> PSID {
            // SAFETY: `words` holds the TOKEN_USER that GetTokenInformation(TokenUser) wrote into
            // it, with pointer alignment; the SID it points at lives in the same buffer.
            unsafe { (*(self.words.as_ptr() as *const TOKEN_USER)).User.Sid }
        }
    }

    fn last_error() -> io::Error {
        io::Error::last_os_error()
    }

    /// The user record of `token`.
    fn user_record(token: RawHandle) -> io::Result<UserRecord> {
        let mut len = 0u32;
        // SAFETY: a null buffer of length 0 only asks for the required size (the call fails).
        unsafe { GetTokenInformation(token, TokenUser, null_mut(), 0, &mut len) };
        let mut words = vec![0usize; (len as usize).div_ceil(size_of::<usize>())];
        // SAFETY: `words` has room for `len` bytes.
        let ok = unsafe {
            GetTokenInformation(token, TokenUser, words.as_mut_ptr().cast(), len, &mut len)
        };
        if ok == 0 {
            return Err(last_error());
        }
        Ok(UserRecord { words })
    }

    /// The current process's user.
    fn current_user() -> io::Result<UserRecord> {
        let mut token: HANDLE = null_mut();
        // SAFETY: the pseudo-handle from GetCurrentProcess needs no closing; on success `token`
        // is a new handle, which `OwnedHandle` closes.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(last_error());
        }
        // SAFETY: OpenProcessToken succeeded, so `token` is a valid handle we own.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        user_record(token.as_raw_handle())
    }

    /// A SID as text (`S-1-…`).
    fn sid_string(sid: PSID) -> io::Result<String> {
        let mut out: *mut u16 = null_mut();
        // SAFETY: `sid` is a valid SID (from a record we hold); `out` receives a LocalAlloc'd
        // string that is freed below.
        if unsafe { ConvertSidToStringSidW(sid, &mut out) } == 0 {
            return Err(last_error());
        }
        // SAFETY: ConvertSidToStringSidW returns a NUL-terminated UTF-16 string.
        let s = unsafe {
            let mut n = 0;
            while *out.add(n) != 0 {
                n += 1;
            }
            String::from_utf16_lossy(std::slice::from_raw_parts(out, n))
        };
        // SAFETY: `out` was allocated with LocalAlloc by the call above and is freed once.
        unsafe { LocalFree(out.cast()) };
        Ok(s)
    }

    /// Security attributes for pipe instances that only the current user and SYSTEM may open.
    pub struct OwnerOnly {
        /// Points at `descriptor`; boxed so its address stays fixed.
        attributes: Box<SECURITY_ATTRIBUTES>,
        descriptor: PSECURITY_DESCRIPTOR,
    }

    // SAFETY: after construction both fields are only read, and the descriptor is freed once,
    // by `Drop`, on whichever thread owns the value.
    unsafe impl Send for OwnerOnly {}

    impl OwnerOnly {
        /// Owner-only attributes for the current user.
        pub fn for_current_user() -> io::Result<Self> {
            let user = current_user()?;
            let sddl = owner_only_sddl(&sid_string(user.sid())?);
            let wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
            let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
            // SAFETY: `wide` is NUL-terminated; the descriptor is LocalAlloc'd and freed in Drop.
            let ok = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    null_mut(),
                )
            };
            if ok == 0 {
                return Err(last_error());
            }
            let attributes = Box::new(SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: descriptor,
                bInheritHandle: FALSE,
            });
            Ok(OwnerOnly {
                attributes,
                descriptor,
            })
        }

        /// The `SECURITY_ATTRIBUTES` to pass to `ServerOptions::create_with_security_attributes_raw`.
        pub fn attributes(&self) -> *mut c_void {
            (&*self.attributes as *const SECURITY_ATTRIBUTES)
                .cast_mut()
                .cast()
        }
    }

    impl Drop for OwnerOnly {
        fn drop(&mut self) {
            // SAFETY: `descriptor` came from ConvertStringSecurityDescriptor… (LocalAlloc) and is
            // freed exactly once.
            unsafe { LocalFree(self.descriptor) };
        }
    }

    /// Refuses a pipe served by a process that does not run as the current user. `pipe` is a
    /// connected client handle; the check is about the server instance it is connected to.
    pub fn verify_server(pipe: RawHandle) -> io::Result<()> {
        let mut pid = 0u32;
        // SAFETY: `pipe` is a live pipe handle owned by the caller; `pid` is written on success.
        if unsafe { GetNamedPipeServerProcessId(pipe, &mut pid) } == 0 {
            return Err(last_error());
        }
        // SAFETY: a plain query; a null result means failure (checked below).
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid) };
        if process.is_null() {
            // Fails closed: a server we cannot inspect is not trusted.
            return Err(last_error());
        }
        // SAFETY: OpenProcess succeeded, so `process` is a valid handle we own.
        let process = unsafe { OwnedHandle::from_raw_handle(process) };
        let mut token: HANDLE = null_mut();
        // SAFETY: `token` receives a new handle on success, which `OwnedHandle` closes.
        if unsafe { OpenProcessToken(process.as_raw_handle(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(last_error());
        }
        // SAFETY: OpenProcessToken succeeded, so `token` is a valid handle we own.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let server = user_record(token.as_raw_handle())?;
        let me = current_user()?;
        // SAFETY: both SIDs live in records that outlive this call.
        if unsafe { EqualSid(server.sid(), me.sid()) } == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("the pipe is served by process {pid}, which runs as another user"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::owner_only_sddl;

    #[test]
    fn owner_only_sddl_grants_only_the_user_and_system() {
        let sddl = owner_only_sddl("S-1-5-21-1-2-3-1001");
        // Protected DACL (no inherited ACEs), then exactly one allow ACE per principal.
        assert_eq!(sddl, "D:P(A;;GA;;;S-1-5-21-1-2-3-1001)(A;;GA;;;SY)");
        let aces = sddl.matches("(A;").count();
        assert_eq!(aces, 2);
        assert!(sddl.starts_with("D:P"), "DACL must be protected: {sddl}");
        assert!(!sddl.contains("WD") && !sddl.contains("AU") && !sddl.contains("BU"));
    }
}
