//! Setup commands on native Windows: PowerShell in the captured environment; `$env:` changes
//! are taken, then variables; a native command's exit status, a thrown error, an `exit` and a
//! hang are reported. Non-ASCII text survives both PowerShell runs (a registry variable read by
//! the capture, setup output and the `$env:` values it leaves). One test function: the base
//! environment is process-wide.
#![cfg(windows)]

use std::time::{Duration, Instant};

use wasit_runtime::environment::{CaptureOptions, HostEnv};
use wasit_runtime::wire::{EnvSource, EnvVar, HostEnvConfig};

fn cfg(setup: &str) -> HostEnvConfig {
    HostEnvConfig {
        setup: setup.into(),
        ..HostEnvConfig::default()
    }
}

fn base(name: &str) -> Option<String> {
    wasit_process::child_var(name).map(|v| v.to_string_lossy().into_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn powershell_setup() {
    // A user variable only the registry has (set after this process started), not ASCII.
    let set_user = |value: &str| {
        let st = wasit_process::std_command("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command"])
            .arg(format!(
                "[Environment]::SetEnvironmentVariable('WASIT_E2E_UNI', {value}, 'User')"
            ))
            .status()
            .unwrap();
        assert!(st.success());
    };
    set_user("'ünï 日本 ✓'");
    let env = HostEnv::new(CaptureOptions {
        setup_deadline: Duration::from_secs(20),
        ..CaptureOptions::default()
    });
    let r = env.refresh().await;
    set_user("$null");
    assert_eq!(r.source, EnvSource::Registry, "{r:?}");
    assert_eq!(base("WASIT_E2E_UNI").as_deref(), Some("ünï 日本 ✓"));

    let mut c = cfg("$env:FROM_SETUP = 'yes'\n$env:ORDER = 'setup'\n$env:Path = \"C:\\wasit-setup-bin;$env:Path\"\nWrite-Output 'hello from setup'\n$env:GREETING = 'grüße 日本'\nWrite-Output 'héllo ✓ 日本語'\ncmd /c echo native-ascii");
    c.vars = vec![EnvVar {
        name: "ORDER".into(),
        value: "variable".into(),
        secret: false,
    }];
    let (r, _) = env.configure(c, "1".into(), false).await;
    let s = r.setup.clone().unwrap();
    assert!(s.ok && s.applied, "{s:?}");
    assert_eq!(s.shell, "powershell -NoProfile");
    assert!(s.output.contains("hello from setup"), "{s:?}");
    assert!(s.output.contains("héllo ✓ 日本語"), "{s:?}");
    assert!(s.output.contains("native-ascii"), "{s:?}");
    assert_eq!(base("FROM_SETUP").as_deref(), Some("yes"));
    assert_eq!(base("GREETING").as_deref(), Some("grüße 日本"));
    assert_eq!(base("ORDER").as_deref(), Some("variable"));
    assert_eq!(
        r.path.first().map(String::as_str),
        Some("C:\\wasit-setup-bin"),
        "{r:?}"
    );

    // A native command's exit status.
    let (r, _) = env
        .configure(cfg("$env:AFTER = '1'\ncmd /c exit 4"), "2".into(), false)
        .await;
    let s = r.setup.unwrap();
    assert!(!s.ok && s.applied, "{s:?}");
    assert_eq!(s.exit_code, Some(4));
    assert_eq!(base("AFTER").as_deref(), Some("1"));

    // A thrown error.
    let (r, _) = env.configure(cfg("throw 'boom'"), "3".into(), false).await;
    let s = r.setup.unwrap();
    assert!(!s.ok, "{s:?}");
    assert_eq!(s.exit_code, Some(1));
    assert!(s.output.contains("boom"), "{s:?}");

    // `exit` ends the shell: the captured environment.
    let (r, _) = env
        .configure(cfg("$env:HALF = '1'\nexit 3"), "4".into(), false)
        .await;
    let s = r.setup.unwrap();
    assert!(!s.ok && !s.applied, "{s:?}");
    assert_eq!(s.exit_code, Some(3));
    assert_eq!(base("HALF"), None);

    // A hang is bounded.
    let quick = HostEnv::new(CaptureOptions {
        setup_deadline: Duration::from_secs(3),
        ..CaptureOptions::default()
    });
    let started = Instant::now();
    let (r, _) = quick
        .configure(
            cfg("Write-Output 'before'\nStart-Sleep -Seconds 300"),
            "5".into(),
            false,
        )
        .await;
    let s = r.setup.unwrap();
    assert!(!s.ok && !s.applied, "{s:?}");
    assert!(
        s.error.as_deref().unwrap_or("").contains("did not finish"),
        "{s:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "{:?}",
        started.elapsed()
    );
    wasit_process::set_base_env(None);
}
