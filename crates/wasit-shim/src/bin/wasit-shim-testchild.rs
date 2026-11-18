//! Portable stand-in child for the shim's tests (no reliance on `sh`, `cat`, `seq`, ...).
//!
//! Modes (first argument):
//! * `cat`: echo stdin to stdout line by line (flushing each), exit 0 at EOF.
//! * `seqloop`: for every input line `n`, print `1..=n`, one per line.
//! * `yes <count>`: print `count` fixed 40-digit lines.
//! * `exit <code>`: exit with that code.
//! * `sleep`: sleep (about) forever.
//! * `spawn-sleep`: start a grandchild in `sleep` mode, print its pid, wait for it.
//! * `huge`: 20 MB of `x` and a newline, then `after`.
//! * `utf8`: 19 times `é` and a newline.
//! * `bytes`: invalid UTF-8 with CRLF on stdout, a line on stderr, an unterminated tail.
//! * `print <text>`: print the text and a newline.

use std::io::{BufRead, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    match mode {
        "cat" => {
            let stdin = std::io::stdin();
            let mut inp = stdin.lock();
            let mut line = Vec::new();
            while inp.read_until(b'\n', &mut line).unwrap_or(0) > 0 {
                let _ = out.write_all(&line);
                let _ = out.flush();
                line.clear();
            }
        }
        "seqloop" => {
            let stdin = std::io::stdin();
            for l in stdin.lock().lines() {
                let Ok(l) = l else { break };
                let Ok(n) = l.trim().parse::<u64>() else {
                    continue;
                };
                let mut buf = String::new();
                for i in 1..=n {
                    buf.push_str(&i.to_string());
                    buf.push('\n');
                    if buf.len() > 32 * 1024 {
                        let _ = out.write_all(buf.as_bytes());
                        buf.clear();
                    }
                }
                let _ = out.write_all(buf.as_bytes());
                let _ = out.flush();
            }
        }
        "yes" => {
            let n: usize = args[2].parse().unwrap();
            let line = "0123456789012345678901234567890123456789\n";
            for _ in 0..n {
                if out.write_all(line.as_bytes()).is_err() {
                    break;
                }
            }
        }
        "exit" => {
            let _ = out.flush();
            std::process::exit(args[2].parse().unwrap());
        }
        "sleep" => loop {
            std::thread::sleep(std::time::Duration::from_secs(1000));
        },
        "spawn-sleep" => {
            let mut c = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("sleep")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let _ = writeln!(out, "{}", c.id());
            let _ = out.flush();
            let _ = c.wait();
        }
        "huge" => {
            let chunk = vec![b'x'; 1 << 20];
            for _ in 0..19 {
                let _ = out.write_all(&chunk);
            }
            let _ = out.write_all(&chunk[..20_000_000 - 19 * (1 << 20)]);
            let _ = out.write_all(b"\nafter\n");
        }
        "utf8" => {
            let _ = out.write_all(format!("{}\n", "é".repeat(19)).as_bytes());
        }
        "bytes" => {
            let _ = out.write_all(b"a\xffb\r\n");
            let _ = out.flush();
            let _ = std::io::stderr().write_all(b"e1\n");
            let _ = std::io::stderr().flush();
            // Keep stdout ordering independent of stderr: give the reader time.
            std::thread::sleep(std::time::Duration::from_millis(200));
            let _ = out.write_all(b"no-newline");
        }
        "print" => {
            let _ = writeln!(out, "{}", args[2]);
        }
        other => {
            eprintln!("wasit-shim-testchild: unknown mode {other:?}");
            std::process::exit(64);
        }
    }
    let _ = out.flush();
}
