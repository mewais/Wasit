//! Images and files through the real stack: blobs out of tool results (stored once, served
//! by `session.file.read`, rebuilt from the journal after a restart), attachment uploads with
//! limits, prompts that carry them, and read restrictions (traversal, symlinks, ranges).

mod common;

use common::*;
use serde_json::json;
use wasit_core::content::{b64_decode, b64_encode};
use wasit_core::{AgentEvent, Attachment, Command, ContentPart, MediaSource};
use wasit_fake_cli::scenario::{ImageStep, Scenario, Step, ToolStep, Turn, WriteFileStep};
use wasit_runtime::media::{AttachmentPut, FileReadRequest, FilesStatRequest};
use wasit_runtime::wire::{SessionEventKind, SessionId};
use wasit_runtime::SessionManager;

/// A valid 40x30 PNG (large enough that its base64 is scrubbed from raw values).
fn png() -> Vec<u8> {
    fn crc(data: &[u8]) -> u32 {
        let mut c = 0xffff_ffffu32;
        for &b in data {
            c ^= b as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xedb8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
        }
        !c
    }
    fn chunk(out: &mut Vec<u8>, t: &[u8], d: &[u8]) {
        out.extend((d.len() as u32).to_be_bytes());
        let mut td = t.to_vec();
        td.extend(d);
        out.extend(&td);
        out.extend(crc(&td).to_be_bytes());
    }
    let (w, h) = (40u32, 30u32);
    let mut raw = Vec::new();
    for y in 0..h {
        raw.push(0);
        for x in 0..w {
            raw.extend([(x * 6) as u8, (y * 8) as u8, 200]);
        }
    }
    // Stored (uncompressed) deflate blocks inside a zlib stream.
    let mut z = vec![0x78, 0x01];
    let mut adler = (1u32, 0u32);
    for b in &raw {
        adler.0 = (adler.0 + *b as u32) % 65521;
        adler.1 = (adler.1 + adler.0) % 65521;
    }
    let blocks: Vec<&[u8]> = raw.chunks(65535).collect();
    for (i, blk) in blocks.iter().enumerate() {
        z.push(u8::from(i + 1 == blocks.len()));
        let n = blk.len() as u16;
        z.extend(n.to_le_bytes());
        z.extend((!n).to_le_bytes());
        z.extend(*blk);
    }
    z.extend(((adler.1 << 16) | adler.0).to_be_bytes());
    let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = Vec::new();
    ihdr.extend(w.to_be_bytes());
    ihdr.extend(h.to_be_bytes());
    ihdr.extend([8, 2, 0, 0, 0]);
    chunk(&mut out, b"IHDR", &ihdr);
    chunk(&mut out, b"IDAT", &z);
    chunk(&mut out, b"IEND", &[]);
    out
}

fn image_turn(data: &str, write: Option<(&str, &str)>) -> Turn {
    Turn {
        when: None,
        steps: vec![
            Step::Tool(ToolStep {
                name: "Read".into(),
                input: json!({"file_path": "shot.png"}),
                image: Some(ImageStep {
                    mime: Some("image/png".into()),
                    path: None,
                    data: Some(data.into()),
                }),
                write_file: write.map(|(p, c)| WriteFileStep {
                    path: p.into(),
                    content: c.into(),
                }),
                ..ToolStep::default()
            }),
            Step::Say {
                text: "done".into(),
                chunks: None,
            },
        ],
    }
}

async fn read_all(mgr: &SessionManager, id: &SessionId, blob: &str) -> Vec<u8> {
    let c = mgr
        .file_read(&FileReadRequest {
            id: id.clone(),
            blob: Some(blob.into()),
            path: None,
            offset: 0,
            length: None,
            outside: false,
            grant: None,
        })
        .await
        .expect("blob read");
    assert!(c.eof);
    b64_decode(&c.data).unwrap()
}

fn blob_of(events: &[wasit_runtime::wire::SessionEvent]) -> (String, u64, Option<u32>) {
    events
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::ToolCallFinished { parts, result, .. },
            } => {
                let r = serde_json::to_string(result).unwrap();
                assert!(
                    !r.contains("iVBORw0KGgo"),
                    "raw result still holds base64: {r}"
                );
                parts.iter().find_map(|p| match p {
                    ContentPart::Image {
                        source: MediaSource::Blob { id, size },
                        width,
                        ..
                    } => Some((id.clone(), *size, *width)),
                    _ => None,
                })
            }
            _ => None,
        })
        .expect("an image blob in a tool result")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_result_images_are_blobs_served_once_and_rebuilt_after_a_restart() {
    let d = TempDir::new();
    let bytes = png();
    let sc = Scenario::builder()
        .turn(image_turn(
            &b64_encode(&bytes),
            Some(("out/made.svg", "<svg xmlns='http://www.w3.org/2000/svg'/>")),
        ))
        .build();
    let mgr = manager(d.path()).await;
    let r = mgr.create(create(d.path(), "claude", &sc)).await.unwrap();
    let id = r.id;
    wait_phase(&mgr, &id, "idle").await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("look")).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    let (blob, size, width) = blob_of(&events);
    assert_eq!(size, bytes.len() as u64);
    assert_eq!(width, Some(40));
    assert_eq!(read_all(&mgr, &id, &blob).await, bytes);
    // The tool's file exists and is readable under the cwd.
    let f = mgr
        .file_read(&FileReadRequest {
            id: id.clone(),
            blob: None,
            path: Some("out/made.svg".into()),
            offset: 0,
            length: None,
            outside: false,
            grant: None,
        })
        .await
        .unwrap();
    assert_eq!(f.mime, "image/svg+xml");
    let page = mgr.history(&id, None, 500).await.unwrap();
    let before = serde_json::to_value(&page.events).unwrap();

    // Restart without the blob store: the journal rebuilds the same blob ids.
    drop(s);
    drop(mgr);
    let blobs = d.path().join("sessions").join(id.as_str()).join("blobs");
    std::fs::remove_dir_all(&blobs).unwrap();
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "loaded", |s| s.loaded).await;
    let page = mgr.history(&id, None, 500).await.unwrap();
    // The same records give the same events (adoption may add its own after them).
    let after = serde_json::to_value(&page.events).unwrap();
    let n = before.as_array().unwrap().len();
    assert_eq!(
        after.as_array().unwrap()[..n],
        before.as_array().unwrap()[..]
    );
    assert_eq!(read_all(&mgr, &id, &blob).await, bytes);
    mgr.stop(&id).await.unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn put(
    mgr: &SessionManager,
    id: &SessionId,
    upload: &str,
    name: &str,
    size: u64,
    offset: u64,
    chunk: &[u8],
    last: bool,
) -> Result<wasit_runtime::media::AttachmentPutReply, wasit_runtime::wire::RuntimeError> {
    mgr.attachment_put(&AttachmentPut {
        id: id.clone(),
        upload: upload.into(),
        name: name.into(),
        size,
        offset,
        data: b64_encode(chunk),
        last,
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uploads_are_chunked_limited_and_sent_with_the_prompt() {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("seen"))
        .turn(Turn::builder().say("seen again"))
        .build();
    let mgr = manager(d.path()).await;
    let id = mgr
        .create(create(d.path(), "claude", &sc))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let bytes = png();
    let (a, b) = bytes.split_at(bytes.len() / 2);
    let n = bytes.len() as u64;
    assert!(put(&mgr, &id, "up1abcde", "../../evil.png", n, 0, a, false)
        .await
        .unwrap()
        .attachment
        .is_none());
    // Out of order, past the end, bad ids, too large: refused.
    assert!(put(&mgr, &id, "up1abcde", "x.png", n, 3, b, false)
        .await
        .is_err());
    assert!(put(&mgr, &id, "up1abcde", "x.png", 4, 0, a, false)
        .await
        .is_err());
    assert!(put(&mgr, &id, "BAD", "x.png", n, 0, a, false)
        .await
        .is_err());
    assert!(put(
        &mgr,
        &id,
        "up2abcde",
        "x.png",
        60 * 1024 * 1024,
        0,
        a,
        false
    )
    .await
    .is_err());
    // The refused restarts above did not disturb upload 1? Offset 0 restarts it: resend.
    put(&mgr, &id, "up1abcde", "../../evil.png", n, 0, a, false)
        .await
        .unwrap();
    let att = put(
        &mgr,
        &id,
        "up1abcde",
        "../../evil.png",
        n,
        a.len() as u64,
        b,
        true,
    )
    .await
    .unwrap()
    .attachment
    .expect("finished");
    assert_eq!(att.name, "evil.png");
    assert_eq!(att.mime, "image/png");
    assert!(att.path.contains("attachments"), "{}", att.path);
    assert_eq!(std::fs::read(&att.path).unwrap(), bytes);
    let note = put(
        &mgr,
        &id,
        "up3abcde",
        "notes.bin",
        3,
        0,
        b"\0\x01\x02",
        true,
    )
    .await
    .unwrap()
    .attachment
    .unwrap();

    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(
        &id,
        Command::Prompt {
            text: "what is this".into(),
            attachments: vec![att.clone(), note.clone()],
        },
    )
    .await
    .unwrap();
    let events = until(&mut s, is_turn_completed).await;
    let sent = events
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Input {
                command: Command::Prompt { attachments, .. },
                ..
            } => Some(attachments.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(sent.len(), 2);
    assert!(
        sent.iter().all(|a| a.data.is_none()),
        "published input carries no bytes"
    );
    // What reached claude: an image block, and the binary file named by path.
    let journal = journal_text(&d, &id);
    assert!(
        journal.contains(r#"\"media_type\":\"image/png\""#),
        "image block not written"
    );
    assert!(
        journal.contains("[Attached file: notes.bin"),
        "path reference not written"
    );

    // An attachment that is not an upload of this session is refused.
    let stray = d.path().join("stray.png");
    std::fs::write(&stray, &bytes).unwrap();
    let r = mgr
        .command(
            &id,
            Command::Prompt {
                text: "x".into(),
                attachments: vec![Attachment {
                    path: stray.to_string_lossy().into_owned(),
                    ..att
                }],
            },
        )
        .await;
    assert!(r.is_err(), "{r:?}");
    mgr.stop(&id).await.unwrap();
}

fn journal_text(d: &TempDir, id: &SessionId) -> String {
    let dir = d.path().join("sessions").join(id.as_str());
    let mut s = String::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_file() && p.file_name().unwrap().to_string_lossy().contains("journal") {
            s.push_str(&std::fs::read_to_string(p).unwrap_or_default());
        }
    }
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_stay_inside_the_session_roots_and_honour_ranges() {
    let d = TempDir::new();
    let work = d.path().join("work");
    std::fs::create_dir_all(work.join("sub")).unwrap();
    std::fs::write(work.join("sub/a.txt"), "0123456789").unwrap();
    let secret = d.path().join("secret.txt");
    std::fs::write(&secret, "top secret").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&secret, work.join("link.txt")).unwrap();
    let sc = Scenario::builder().turn(Turn::builder().say("hi")).build();
    let mgr = manager(d.path()).await;
    let mut req = create(d.path(), "claude", &sc);
    req.cwd = Some(work.to_string_lossy().into_owned());
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    let rd = |path: &str, offset: u64, length: Option<u64>, outside: bool| FileReadRequest {
        id: id.clone(),
        blob: None,
        path: Some(path.into()),
        offset,
        length,
        outside,
        grant: None,
    };
    let c = mgr
        .file_read(&rd("sub/a.txt", 3, Some(4), false))
        .await
        .unwrap();
    assert_eq!(b64_decode(&c.data).unwrap(), b"3456");
    assert_eq!(
        (c.size, c.offset, c.eof, c.mime.as_str()),
        (10, 3, false, "text/plain")
    );
    let c = mgr
        .file_read(&rd("sub/a.txt", 8, Some(100), false))
        .await
        .unwrap();
    assert!(c.eof && b64_decode(&c.data).unwrap() == b"89");
    for bad in [
        "../secret.txt",
        "sub/../../secret.txt",
        secret.to_str().unwrap(),
    ] {
        assert!(
            mgr.file_read(&rd(bad, 0, None, false)).await.is_err(),
            "{bad} was readable"
        );
    }
    #[cfg(unix)]
    assert!(
        mgr.file_read(&rd("link.txt", 0, None, false))
            .await
            .is_err(),
        "symlink escaped"
    );
    // `outside` alone is not enough: the user's click is a grant of exactly that file.
    let secret_s = secret.to_str().unwrap();
    assert!(
        mgr.file_read(&rd(secret_s, 0, None, true)).await.is_err(),
        "blanket outside"
    );
    let g = mgr
        .file_grant(&wasit_runtime::media::FileGrantRequest {
            id: id.clone(),
            path: secret_s.into(),
            parent_grant: None,
        })
        .await
        .unwrap();
    let with = |path: &str, token: &str| FileReadRequest {
        grant: Some(token.into()),
        ..rd(path, 0, None, true)
    };
    let c = mgr.file_read(&with(secret_s, &g.token)).await.unwrap();
    assert_eq!(b64_decode(&c.data).unwrap(), b"top secret");
    // The grant covers that one file, not its siblings, and not a made-up token.
    std::fs::write(d.path().join("other.txt"), "x").unwrap();
    let other = d.path().join("other.txt");
    assert!(mgr
        .file_read(&with(other.to_str().unwrap(), &g.token))
        .await
        .is_err());
    assert!(mgr.file_read(&with(secret_s, "00")).await.is_err());
    // A dotted path to the same file is the same canonical file; a sibling needs a child grant
    // of a parent grant: allowed inside the parent's directory only.
    let child = mgr
        .file_grant(&wasit_runtime::media::FileGrantRequest {
            id: id.clone(),
            path: other.to_str().unwrap().into(),
            parent_grant: Some(g.token.clone()),
        })
        .await;
    assert!(child.is_ok(), "{child:?}");
    let up = d.path().parent().unwrap().join("outside-of-parent.txt");
    std::fs::write(&up, "y").unwrap();
    assert!(mgr
        .file_grant(&wasit_runtime::media::FileGrantRequest {
            id: id.clone(),
            path: up.to_str().unwrap().into(),
            parent_grant: Some(g.token.clone()),
        })
        .await
        .is_err());
    // Stat sees outside paths only when asked, and never reads them.
    let ask = |outside| FilesStatRequest {
        id: id.clone(),
        paths: vec![secret_s.into()],
        outside,
    };
    assert!(mgr.files_stat(&ask(false)).await.unwrap().files.is_empty());
    assert_eq!(mgr.files_stat(&ask(true)).await.unwrap().files.len(), 1);
    // Bad blob ids never touch the file system.
    let bad_blob = FileReadRequest {
        id: id.clone(),
        blob: Some("../../secret.txt".into()),
        path: None,
        offset: 0,
        length: None,
        outside: false,
        grant: None,
    };
    assert!(mgr.file_read(&bad_blob).await.is_err());
    let st = mgr
        .files_stat(&FilesStatRequest {
            id: id.clone(),
            paths: vec![
                "sub/a.txt".into(),
                "missing.txt".into(),
                "../secret.txt".into(),
                "link.txt".into(),
            ],
            outside: false,
        })
        .await
        .unwrap();
    assert_eq!(st.files.len(), 1, "{st:?}");
    assert_eq!(st.files[0].path, "sub/a.txt");
    assert_eq!(st.files[0].size, 10);
    mgr.stop(&id).await.unwrap();
}
