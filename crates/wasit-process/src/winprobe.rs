//! Windows-only checks used by the no-console-window tests (here and in Wasl/Marsad): does this
//! process have a visible console window, and which visible top-level windows belong to a set
//! of processes.

use windows_sys::Win32::Foundation::{BOOL, HWND, LPARAM};
use windows_sys::Win32::System::Console::GetConsoleWindow;
use windows_sys::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowTextW, GetWindowThreadProcessId, IsWindowVisible,
};

/// The calling process's console, as far as a window is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleWindow {
    /// No console window at all (no console, or a console created with `CREATE_NO_WINDOW`).
    None,
    /// A console window exists but is not visible.
    Hidden,
    /// A visible console window: the flashing this crate prevents.
    Visible,
}

impl ConsoleWindow {
    pub fn as_str(self) -> &'static str {
        match self {
            ConsoleWindow::None => "none",
            ConsoleWindow::Hidden => "hidden",
            ConsoleWindow::Visible => "visible",
        }
    }
}

/// The calling process's console window state.
pub fn own_console_window() -> ConsoleWindow {
    // SAFETY: plain Win32 queries without arguments / on a handle the call returned.
    unsafe {
        let h = GetConsoleWindow();
        if h.is_null() {
            ConsoleWindow::None
        } else if IsWindowVisible(h) != 0 {
            ConsoleWindow::Visible
        } else {
            ConsoleWindow::Hidden
        }
    }
}

/// Visible top-level windows owned (per `GetWindowThreadProcessId`; for console windows that
/// is the console's client process) by any of `pids`, as `(pid, title)`.
pub fn visible_windows_of(pids: &[u32]) -> Vec<(u32, String)> {
    struct Ctx<'a> {
        pids: &'a [u32],
        out: Vec<(u32, String)>,
    }
    unsafe extern "system" fn cb(hwnd: HWND, lp: LPARAM) -> BOOL {
        // SAFETY: `lp` is the `Ctx` passed to EnumWindows below, alive for the whole call.
        let ctx = unsafe { &mut *(lp as *mut Ctx) };
        let mut pid = 0u32;
        // SAFETY: valid window handle from EnumWindows; out-pointer to a local.
        unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
        // SAFETY: as above.
        if ctx.pids.contains(&pid) && unsafe { IsWindowVisible(hwnd) } != 0 {
            let mut buf = [0u16; 256];
            // SAFETY: buffer and its length match.
            let n = unsafe { GetWindowTextW(hwnd, buf.as_mut_ptr(), buf.len() as i32) };
            ctx.out
                .push((pid, String::from_utf16_lossy(&buf[..n.max(0) as usize])));
        }
        1
    }
    let mut ctx = Ctx {
        pids,
        out: Vec::new(),
    };
    // SAFETY: the callback only touches `ctx`, which outlives the call.
    unsafe { EnumWindows(Some(cb), &mut ctx as *mut Ctx as LPARAM) };
    ctx.out
}
