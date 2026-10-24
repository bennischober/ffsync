//! Tree sync tests (D17, protocol v3 §16): full tree sessions over real TCP
//! loopback — the same code path the CLI uses.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::tree_sync::{
    TreeOutcome, TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send,
};
use ffsync::tree::{EntryKind, TreeManifest};

/// A temporary tree with a guard that removes it on drop.
struct TempTree(PathBuf);

impl TempTree {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("ffsync_tree_test_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempTree(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn write(&self, rel: &str, content: &[u8]) {
        let p = self.0.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, content).unwrap();
    }

    fn read(&self, rel: &str) -> Option<Vec<u8>> {
        fs::read(self.0.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR))).ok()
    }

    fn exists(&self, rel: &str) -> bool {
        self.0
            .join(rel.replace('/', std::path::MAIN_SEPARATOR_STR))
            .exists()
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Runs one full tree session over TCP loopback with per-socket timeouts,
/// so a protocol deadlock surfaces as a timeout error instead of hanging
/// the test binary forever.
fn run_tree_session(
    sender_root: &Path,
    receiver_root: &Path,
    checksum: bool,
    no_delete: bool,
) -> (
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
) {
    run_tree_session_ex(sender_root, receiver_root, checksum, no_delete, &[], &[])
}

/// Like [`run_tree_session`], but with exclude patterns: the sender's
/// govern both scans (§16.7), the receiver's only guard its delete pass.
fn run_tree_session_ex(
    sender_root: &Path,
    receiver_root: &Path,
    checksum: bool,
    no_delete: bool,
    sender_excludes: &[&str],
    receiver_excludes: &[&str],
) -> (
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
) {
    run_tree_session_full(
        sender_root,
        receiver_root,
        checksum,
        no_delete,
        sender_excludes,
        receiver_excludes,
        1,
    )
}

/// The full session runner: excludes on both sides plus the parallel
/// connection count (§16.8; 1 = sequential).
#[allow(clippy::too_many_arguments)]
fn run_tree_session_full(
    sender_root: &Path,
    receiver_root: &Path,
    checksum: bool,
    no_delete: bool,
    sender_excludes: &[&str],
    receiver_excludes: &[&str],
    parallel: u16,
) -> (
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let listener2 = listener.try_clone().unwrap();

    let recv_root = receiver_root.to_path_buf();
    let recv_excludes: Vec<String> = receiver_excludes.iter().map(|s| s.to_string()).collect();
    let server = std::thread::spawn(move || {
        let (tcp, _peer) = listener.accept().unwrap();
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
        tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
        tree_receive(
            &mut &tcp,
            &TreeReceiveRequest {
                root: recv_root,
                force_checksum: checksum,
                no_delete,
                excludes: recv_excludes,
                accept_worker: std::sync::Arc::new(move || {
                    let (tcp, _wpeer) = listener2
                        .accept()
                        .map_err(|e| std::io::Error::other(format!("cannot accept worker: {e}")))?;
                    tcp.set_nodelay(true).ok();
                    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
                    tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
                    Ok(Box::new(tcp))
                }),
                dry_run: false,
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        )
    });

    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
    let send_addr = addr;
    let send_excludes: Vec<String> = sender_excludes.iter().map(|s| s.to_string()).collect();
    let send_res = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender_root.to_path_buf(),
            force_checksum: checksum,
            no_delete,
            excludes: send_excludes,
            parallel,
            dry_run: false,
            connect_worker: std::sync::Arc::new(move |_index: u16| {
                let tcp = std::net::TcpStream::connect(send_addr)
                    .map_err(|e| std::io::Error::other(format!("cannot connect worker: {e}")))?;
                tcp.set_nodelay(true).ok();
                tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
                tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
                Ok(Box::new(tcp))
            }),
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let recv_res = server.join().unwrap();
    (send_res, recv_res)
}

/// Sets a file's mtime (helper for fast-path tests).
fn set_mtime(path: &Path, secs: u64) {
    let f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
        .unwrap();
}

#[test]
fn tree_converges_mixed_content() {
    // Sender tree: nested dirs, files of several sizes, an empty file, an
    // empty dir. Receiver tree: starts empty.
    let sender = TempTree::new("conv_sender");
    let receiver = TempTree::new("conv_receiver");

    sender.write("big.bin", &vec![7u8; 3 * 1024 * 1024]);
    sender.write("sub/medium.bin", &vec![1u8; 300 * 1024]);
    sender.write("sub/deep/tiny.txt", b"hello tree");
    sender.write("empty.txt", b"");
    fs::create_dir_all(sender.path().join("empty_dir")).unwrap();

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, r.files);
    assert!(s.transferred_bytes > 0);
    assert_eq!(s.deleted, 0);

    // Every file converged byte-for-byte.
    assert_eq!(receiver.read("big.bin"), sender.read("big.bin"));
    assert_eq!(
        receiver.read("sub/medium.bin"),
        sender.read("sub/medium.bin")
    );
    assert_eq!(
        receiver.read("sub/deep/tiny.txt"),
        sender.read("sub/deep/tiny.txt")
    );
    assert_eq!(receiver.read("empty.txt"), Some(Vec::new()));
    // Empty dir created; nested dirs exist.
    assert!(receiver.exists("empty_dir"));
    assert!(receiver.exists("sub/deep"));

    // Second session: everything metadata-matches (mtime preserved at
    // commit) → zero conversations, both sides report 0 files.
    let (s2, r2) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s2 = s2.expect("sender2 ok");
    let r2 = r2.expect("receiver2 ok");
    assert_eq!(s2.files, 0, "second sync must skip all files (fast path)");
    assert_eq!(r2.files, 0);
    assert_eq!(s2.transferred_bytes, 0);
}

#[test]
fn tree_syncs_modified_and_new_files_only() {
    let sender = TempTree::new("mod_sender");
    let receiver = TempTree::new("mod_receiver");

    // Shared initial state: sync once.
    sender.write("a.bin", &vec![1u8; 100 * 1024]);
    sender.write("b.bin", &vec![2u8; 100 * 1024]);
    let _ = run_tree_session(sender.path(), receiver.path(), false, false);

    // Diverge: modify a.bin, add c.bin, leave b.bin alone.
    sender.write("a.bin", &vec![9u8; 100 * 1024]);
    sender.write("c.bin", &vec![3u8; 50 * 1024]);

    let (s, _r) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s = s.expect("sender ok");
    // Exactly two conversations (a.bin modified, c.bin new); b.bin skipped.
    assert_eq!(s.files, 2, "only changed/new files get conversations");
    assert_eq!(receiver.read("a.bin"), sender.read("a.bin"));
    assert_eq!(receiver.read("c.bin"), sender.read("c.bin"));
    assert_eq!(receiver.read("b.bin"), sender.read("b.bin"));
}

#[test]
fn tree_deletes_receiver_only_files_after_success() {
    let sender = TempTree::new("del_sender");
    let receiver = TempTree::new("del_receiver");

    // Initial state includes content the sender will not have. keep.bin gets
    // a fixed mtime on both sides so its triage is deterministically Skip
    // (two fs::write calls can straddle a timer tick and produce different
    // mtimes for identical content — that would turn keep.bin into a
    // conversation and this into a different test).
    sender.write("keep.bin", b"keep");
    receiver.write("keep.bin", b"keep");
    set_mtime(&sender.path().join("keep.bin"), 1600000000);
    set_mtime(&receiver.path().join("keep.bin"), 1600000000);
    receiver.write("stale.bin", b"stale content");
    receiver.write("sub/old.bin", b"old");

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, 0, "no conversations needed");
    // Three deletions: stale.bin, sub/old.bin, and the now-receiver-only
    // `sub` dir itself (sender-authoritative tree, rsync --delete parity).
    assert_eq!(r.deleted, 3);
    assert_eq!(r.deleted_bytes, 13 + 3, "dirs carry no bytes");
    assert!(!receiver.exists("stale.bin"));
    assert!(!receiver.exists("sub/old.bin"));
    assert!(receiver.exists("keep.bin"));
    // The emptied dir is removed bottom-up.
    assert!(!receiver.exists("sub"));
}

#[test]
fn tree_no_delete_keeps_receiver_only_files() {
    let sender = TempTree::new("nodel_sender");
    let receiver = TempTree::new("nodel_receiver");

    // Fixed mtimes: keep.bin must deterministically triage to Skip (see the
    // delete test above for why raw fs::write mtimes are flaky here).
    sender.write("keep.bin", b"keep");
    receiver.write("keep.bin", b"keep");
    set_mtime(&sender.path().join("keep.bin"), 1600000000);
    set_mtime(&receiver.path().join("keep.bin"), 1600000000);
    receiver.write("stale.bin", b"stale");

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, true);
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, 0);
    assert_eq!(r.deleted, 0, "--no-delete must suppress deletions");
    assert!(receiver.exists("stale.bin"));
}

#[test]
fn tree_checksum_mode_ignores_metadata() {
    let sender = TempTree::new("csum_sender");
    let receiver = TempTree::new("csum_receiver");

    // Same size, same mtime — but different content (mtime-preserving edit,
    // the exact case --checksum exists for).
    sender.write("a.bin", &vec![1u8; 64 * 1024]);
    receiver.write("a.bin", &vec![2u8; 64 * 1024]);
    set_mtime(&sender.path().join("a.bin"), 1600000000);
    set_mtime(&receiver.path().join("a.bin"), 1600000000);

    let (s, _r) = run_tree_session(sender.path(), receiver.path(), true, false);
    let s = s.expect("sender ok");
    assert_eq!(s.files, 1, "checksum mode must not trust metadata");
    assert_eq!(receiver.read("a.bin"), sender.read("a.bin"));
}

#[test]
fn tree_empty_sender_file_converges_nonempty_receiver_file() {
    // The v3 empty-file fix (§16): an empty sender file must truncate a
    // non-empty receiver file (v0.2 errored with HashMismatch here).
    let sender = TempTree::new("empty_sender");
    let receiver = TempTree::new("empty_receiver");
    sender.write("was_big.bin", b"");
    receiver.write("was_big.bin", &vec![5u8; 4096]);

    let (s, _r) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s = s.expect("sender ok");
    assert_eq!(s.files, 1);
    assert_eq!(receiver.read("was_big.bin"), Some(Vec::new()));
}

#[test]
fn tree_kind_conflict_resolves_sender_authoritative() {
    // Sender has a FILE where the receiver has a DIR (with content), and a
    // DIR where the receiver has a FILE. The sender's kind must win.
    let sender = TempTree::new("kind_sender");
    let receiver = TempTree::new("kind_receiver");

    sender.write("x", b"now a file");
    sender.write("y/readme.txt", b"now a dir");
    fs::create_dir_all(receiver.path().join("x/sub")).unwrap();
    receiver.write("x/sub/junk.bin", b"junk");
    receiver.write("y", b"now becomes a dir");

    let (_s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    let _r = r.expect("receiver ok");
    assert_eq!(receiver.read("x"), Some(b"now a file".to_vec()));
    assert_eq!(receiver.read("y/readme.txt"), Some(b"now a dir".to_vec()));
    assert!(
        !receiver.exists("x/sub"),
        "receiver's conflicting dir content is gone"
    );
}

#[test]
fn tree_v2_flow_rejects_tree_frames() {
    // A peer without tree support (v2-style single-file flow, protocol v3
    // on the wire so the HELLO version check passes) must reject the
    // unexpected TREE_BEGIN frame loudly instead of corrupting state.
    let sender = TempTree::new("v2sim_sender");
    let receiver = TempTree::new("v2sim_receiver");
    sender.write("a.bin", b"data");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let recv_root = receiver.path().to_path_buf();
    let server = std::thread::spawn(move || {
        let (tcp, _peer) = listener.accept().unwrap();
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
        let req = ffsync::protocol::sync::ReceiveRequest {
            path: recv_root.join("a.bin"),
            local_source: recv_root.join("a.bin"),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
        };
        ffsync::protocol::sync::receive(&mut &tcp, &req, &PipelineConfig::default(), &mut |_p| {})
    });

    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let send_res = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender.path().to_path_buf(),
            force_checksum: false,
            no_delete: false,
            excludes: Vec::new(),
            parallel: 1,
            connect_worker: std::sync::Arc::new(|_| {
                Err(std::io::Error::other("no workers in sequential sessions"))
            }),
            dry_run: false,
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let recv_res = server.join().unwrap();
    // The single-file receiver must reject (BadFrame on TREE_BEGIN); the
    // sender sees the connection die with an error.
    assert!(
        send_res.is_err() || recv_res.is_err(),
        "tree frames against a non-tree peer must fail loudly: send={send_res:?} recv={recv_res:?}"
    );
}

#[test]
fn tree_paths_reject_traversal() {
    // The wire decoder must refuse traversal paths (§16.2).
    use ffsync::protocol::message::{TreeBegin, TreeEntryWire};
    use std::io::Cursor;

    let frame = TreeBegin {
        flags: 0,
        parallel: 1,
        excludes: Vec::new(),
        entries: vec![TreeEntryWire {
            rel_path: "../escape.bin".to_string(),
            kind: 0,
            size: 1,
            mtime_secs: 0,
            mtime_nanos: 0,
            mode: 0,
        }],
    }
    .into_frame()
    .unwrap();
    let mut buf = Vec::new();
    frame.encode(&mut buf).unwrap();
    let decoded = TreeBegin::from_frame(
        ffsync::protocol::framing::read_frame(&mut Cursor::new(buf)).unwrap(),
    );
    // from_frame succeeds (syntax), but manifest_from_wire must reject.
    let err = ffsync::protocol::tree_sync::manifest_from_wire(&decoded.unwrap().entries);
    assert!(err.is_err(), "traversal path must be rejected");
}

#[test]
fn sender_vanished_file_fails_loudly() {
    // The sender's triage lists a file that disappears before its
    // conversation: tree_send must fail loudly, and the receiver's tree
    // must be untouched (fail-closed: no deletions on a failed session).
    let sender = TempTree::new("vanish_sender");
    let receiver = TempTree::new("vanish_receiver");
    sender.write("a.bin", &vec![1u8; 32 * 1024]);
    sender.write("b.bin", &vec![2u8; 32 * 1024]);
    receiver.write("stale.bin", b"stale");

    // A background racer removes b.bin right after the session's TREE_BEGIN
    // exchange (before its conversation starts — tree_send re-stats each
    // file and must fail on the vanished file).
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let recv_root = receiver.path().to_path_buf();
    let del_target = sender.path().join("b.bin");
    let server = std::thread::spawn(move || {
        let (tcp, _peer) = listener.accept().unwrap();
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
        tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
        tree_receive(
            &mut &tcp,
            &TreeReceiveRequest {
                root: recv_root,
                force_checksum: false,
                no_delete: false,
                excludes: Vec::new(),
                accept_worker: std::sync::Arc::new(|| {
                    Err(std::io::Error::other("no workers in sequential sessions"))
                }),
                dry_run: false,
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        )
    });

    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();

    // Racer: delete b.bin while the session is starting. Timing is loose —
    // the point is that whenever it lands (before or during b.bin's
    // conversation), the session fails and the receiver stays intact.
    let racer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(2));
        let _ = fs::remove_file(&del_target);
    });

    let send_res = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender.path().to_path_buf(),
            force_checksum: false,
            no_delete: false,
            excludes: Vec::new(),
            parallel: 1,
            connect_worker: std::sync::Arc::new(|_| {
                Err(std::io::Error::other("no workers in sequential sessions"))
            }),
            dry_run: false,
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let _ = racer.join();
    let recv_res = server.join().unwrap();

    // If the racer landed before b.bin's conversation, the session failed
    // loudly; if it landed after (file already synced), the session
    // succeeded with both files. Either way the receiver must NOT have
    // deleted anything (deletes only run on full success, and a vanished
    // sender file never triggers them).
    match (&send_res, &recv_res) {
        (Ok(_), Ok(_)) => {}   // racer landed late; both files synced
        (Err(_), Err(_)) => {} // both saw the failure
        (Err(e), Ok(_)) => {
            // Sender failed, receiver thinks all is well: the receiver's
            // reply raced the sender's death. Acceptable, but the tree must
            // be intact.
            eprintln!("sender failed ({e}), receiver raced OK");
        }
        (Ok(_), Err(e)) => panic!("sender succeeded but receiver failed: {e}"),
    }
    assert!(
        receiver.exists("stale.bin"),
        "fail-closed: no deletions on a failed session"
    );
}

// ---- Excludes (§16.7) ----

#[test]
fn tree_excludes_protect_receiver_files_from_delete() {
    // The sender does not have secrets/ (excluded by pattern); the receiver
    // does. Without excludes the delete pass would wipe it; with them it
    // must survive, and the sync reports 0 deletions.
    let sender = TempTree::new("excl_protect_s");
    sender.write("code.rs", b"fn main() {}");
    let receiver = TempTree::new("excl_protect_r");
    receiver.write("code.rs", b"fn main() {}");
    receiver.write("secrets/api.key", b"hunter2");
    receiver.write("secrets/notes.txt", b"private");

    let (s, r) = run_tree_session_ex(
        sender.path(),
        receiver.path(),
        false,
        false,
        &["secrets/"],
        &[],
    );
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.deleted, 0);
    assert_eq!(r.deleted, 0);
    assert!(
        receiver.exists("secrets/api.key"),
        "excluded receiver-only file must survive the delete pass"
    );
    assert!(receiver.exists("secrets/notes.txt"));
    assert_eq!(receiver.read("code.rs").unwrap(), b"fn main() {}");
    // Second session with NO excludes: the sender's tree is authoritative,
    // so now the secrets are deleted (excludes are per-session, not state).
    // Three deletions: the two files plus the emptied secrets dir itself.
    let (s2, r2) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s2 = s2.expect("sender ok");
    let r2 = r2.expect("receiver ok");
    assert_eq!(s2.deleted, 3);
    assert_eq!(r2.deleted, 3);
    assert!(!receiver.exists("secrets/api.key"));
}

#[test]
fn tree_sender_excludes_filter_its_own_tree() {
    // Sender has junk it does not want to sync; with the pattern in place
    // the receiver never sees those files (and does not keep local copies
    // of them either — they are invisible to the whole session).
    let sender = TempTree::new("excl_filter_s");
    sender.write("app.bin", b"app");
    sender.write("cache/tmp1.bin", b"junk1");
    sender.write("cache/deep/tmp2.bin", b"junk2");
    let receiver = TempTree::new("excl_filter_r");
    receiver.write("cache/old.bin", b"old"); // receiver-only, but cache/ excluded

    let (s, r) = run_tree_session_ex(
        sender.path(),
        receiver.path(),
        false,
        false,
        &["cache"],
        &[],
    );
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    // Only app.bin was in the filtered conversation set.
    assert_eq!(s.files, 1);
    assert_eq!(r.files, 1);
    assert!(receiver.exists("app.bin"));
    // The excluded dir was pruned on the sender; the receiver's excluded
    // content was never in any manifest, so nothing was deleted either.
    assert!(receiver.exists("cache/old.bin"));
    assert!(!receiver.exists("cache/tmp1.bin"));
    assert_eq!(s.deleted, 0);
    assert_eq!(r.deleted, 0);
}

#[test]
fn tree_excludes_deterministic_across_both_peers() {
    // Both peers must derive the SAME filtered manifests: triage on both
    // sides then produces the identical conversation sequence (§16.4). A
    // receiver that filtered differently would fail loudly on the first
    // unexpected HELLO; this test pins the happy path with several files.
    let sender = TempTree::new("excl_det_s");
    sender.write("a.bin", b"a");
    sender.write("b.bin", b"b");
    sender.write("skip1.bin", b"s1");
    sender.write("sub/skip2.bin", b"s2");
    sender.write("sub/keep.bin", b"k");
    let receiver = TempTree::new("excl_det_r");

    let (s, r) = run_tree_session_ex(
        sender.path(),
        receiver.path(),
        false,
        false,
        &["skip*.bin"],
        &[],
    );
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    // Conversations: a.bin, b.bin, sub/keep.bin (skip*.bin filtered at any
    // depth; sub/ itself is not excluded).
    assert_eq!(s.files, 3);
    assert_eq!(r.files, 3);
    assert!(receiver.exists("a.bin"));
    assert!(receiver.exists("sub/keep.bin"));
    assert!(!receiver.exists("skip1.bin"));
    assert!(!receiver.exists("sub/skip2.bin"));
    assert_eq!(r.transferred_bytes, 3); // 1 + 1 + 1 bytes
}

#[test]
fn tree_excludes_v3_peer_without_flag_interops() {
    // A peer that never sets the excludes flag parses the §16.2 layout —
    // verified by a session whose sender sends no patterns: the receiver
    // (same code) treats it as a plain session.
    let sender = TempTree::new("excl_noflag_s");
    sender.write("only.bin", b"x");
    let receiver = TempTree::new("excl_noflag_r");

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    s.expect("sender ok");
    r.expect("receiver ok");
    assert!(receiver.exists("only.bin"));
}

// ---- Parallel per-file conversations (§16.8) ----

#[test]
fn tree_parallel_converges_and_reports_all_files() {
    // K=4 connections over a mixed tree: conversations are partitioned
    // round-robin, run concurrently, and every file converges. The outcome
    // aggregates across all connections.
    let sender = TempTree::new("par_conv_s");
    for i in 0..12 {
        sender.write(&format!("f{i:02}.bin"), &vec![i as u8; 4096]);
    }
    sender.write("sub/nested.bin", b"nested");
    let receiver = TempTree::new("par_conv_r");

    let (s, r) = run_tree_session_full(sender.path(), receiver.path(), false, false, &[], &[], 4);
    let s = s.unwrap_or_else(|e| panic!("sender failed: {e}"));
    let r = r.unwrap_or_else(|e| panic!("receiver failed: {e}"));
    assert_eq!(s.files, 13);
    assert_eq!(r.files, 13);
    assert_eq!(s.in_sync, 0);
    assert_eq!(r.in_sync, 0);
    // Every file present with the right content.
    for i in 0..12 {
        let rel = format!("f{i:02}.bin");
        assert_eq!(
            receiver.read(&rel).unwrap(),
            vec![i as u8; 4096],
            "file {rel} did not converge"
        );
    }
    assert_eq!(receiver.read("sub/nested.bin").unwrap(), b"nested");
    // Second session: nothing changed → everything in sync (warm path).
    let (s2, r2) = run_tree_session_full(sender.path(), receiver.path(), false, false, &[], &[], 4);
    let s2 = s2.expect("sender ok");
    let r2 = r2.expect("receiver ok");
    assert_eq!(s2.files, 0, "second session has no conversations");
    assert_eq!(r2.files, 0);
}

#[test]
fn tree_parallel_sequential_fallback_interops() {
    // A parallel sender against a sequential receiver (parallel=1): the
    // wire layout carries the flag, but the session still converges — the
    // receiver mirrors K from TREE_BEGIN and runs the same flow.
    let sender = TempTree::new("par_fb_s");
    for i in 0..6 {
        sender.write(&format!("g{i}.bin"), &vec![7u8; 2048]);
    }
    let receiver = TempTree::new("par_fb_r");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let recv_root = receiver.path().to_path_buf();
    let server = std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
        tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
        tree_receive(
            &mut &tcp,
            &TreeReceiveRequest {
                root: recv_root,
                force_checksum: false,
                no_delete: false,
                excludes: Vec::new(),
                accept_worker: std::sync::Arc::new(move || {
                    // Sequential receiver: refuses extra connections.
                    Err(std::io::Error::other("no worker connections accepted"))
                }),
                dry_run: false,
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        )
    });
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
    let send_addr = addr;
    let send_res = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender.path().to_path_buf(),
            force_checksum: false,
            no_delete: false,
            excludes: Vec::new(),
            parallel: 4,
            connect_worker: std::sync::Arc::new(move |_index: u16| {
                std::net::TcpStream::connect(send_addr)
                    .map(|t| Box::new(t) as Box<dyn ffsync::protocol::tree_sync::ReadWrite>)
                    .map_err(std::io::Error::other)
            }),
            dry_run: false,
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let recv_res = server.join().unwrap();
    // The receiver read K=4 from TREE_BEGIN, tried to accept workers, and
    // failed loudly — the session MUST fail on both sides, not desync.
    assert!(
        send_res.is_err(),
        "sender must see the receiver's rejection"
    );
    assert!(recv_res.is_err(), "receiver must fail on missing workers");
}

#[test]
fn tree_parallel_fail_closed_on_worker_error() {
    // One worker hits a vanished sender file mid-session: the whole session
    // fails, and — critically — the receiver deletes nothing (fail-closed,
    // §16.8 failure semantics).
    let sender = TempTree::new("par_fail_s");
    sender.write("a.bin", b"aaa");
    sender.write("b.bin", b"bbb");
    let receiver = TempTree::new("par_fail_r");
    receiver.write("stale.bin", b"old");

    // Vanish a.bin after the session starts (racer), forcing an error on
    // whichever connection picks it up.
    let vanish_target = sender.path().join("a.bin");
    let vanish = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(5));
        let _ = std::fs::remove_file(&vanish_target);
    });

    let (s, r) = run_tree_session_full(sender.path(), receiver.path(), false, false, &[], &[], 2);
    let _ = vanish.join();
    // Either the session failed on the vanished file, or the racer landed
    // late and everything synced — but if it failed, NOTHING was deleted.
    match (&s, &r) {
        (Ok(_), Ok(_)) => {}
        (Err(_), Err(_)) | (Err(_), Ok(_)) => {
            assert!(
                receiver.exists("stale.bin"),
                "fail-closed: a failed parallel session must not delete"
            );
        }
        (Ok(_), Err(e)) => panic!("sender ok but receiver failed: {e}"),
    }
}

#[test]
fn tree_converges_posix_mode_bits() {
    // §16.9: the sender's mode converges to the receiver (unix: exact bits,
    // including the exec bit; Windows: no-write bits → read-only attribute).
    let sender = TempTree::new("mode_sender");
    let receiver = TempTree::new("mode_receiver");
    sender.write("script.sh", b"#!/bin/sh\necho hi\n");
    sender.write("data.bin", &vec![5u8; 64 * 1024]);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = sender.path().join("script.sh");
        std::fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
        let p = sender.path().join("data.bin");
        std::fs::set_permissions(&p, fs::Permissions::from_mode(0o640)).unwrap();
    }
    #[cfg(windows)]
    {
        // Sender: data.bin read-only (the no-write → read-only direction).
        let p = sender.path().join("data.bin");
        let mut perms = fs::metadata(&p).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&p, perms).unwrap();
    }

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    s.expect("sender ok");
    r.expect("receiver ok");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let got = fs::metadata(receiver.path().join("script.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(got, 0o755, "exec bits must converge");
        let got = fs::metadata(receiver.path().join("data.bin"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(got, 0o640, "group-read/no-other must converge");
    }
    #[cfg(windows)]
    {
        // A Windows sender emits no metadata section (its entries would
        // carry mode 0 — synthesized modes would corrupt unix exec bits in
        // mixed syncs), so the receiver applies nothing: the converged
        // file's attribute is unset even though the sender's copy is
        // read-only. Documented asymmetry of §16.9.
        let got = fs::metadata(receiver.path().join("data.bin"))
            .unwrap()
            .permissions()
            .readonly();
        assert!(
            !got,
            "windows sender sends no mode section; receiver attribute untouched"
        );
    }

    // Second session still skips everything (mode apply did not disturb the
    // mtime order; the metadata fast path holds).
    let (s2, r2) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s2 = s2.expect("sender2 ok");
    assert_eq!(s2.files, 0, "mode apply must not break the fast path");
    let _r2 = r2.expect("receiver2 ok");
}

#[cfg(windows)]
#[test]
fn tree_windows_readonly_receiver_roundtrip() {
    // Receiver-side read-only file: a changed sender file must still
    // converge (the read-only attribute must not block the rename/apply).
    let sender = TempTree::new("ro_sender");
    let receiver = TempTree::new("ro_receiver");
    sender.write("doc.bin", &vec![1u8; 64 * 1024]);
    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false);
    s.expect("sender ok");
    r.expect("receiver ok");

    // Make the receiver's copy read-only, then change the sender's copy.
    let rp = receiver.path().join("doc.bin");
    let mut perms = fs::metadata(&rp).unwrap().permissions();
    perms.set_readonly(true);
    fs::set_permissions(&rp, perms).unwrap();
    sender.write("doc.bin", &vec![2u8; 64 * 1024]);

    let (s2, r2) = run_tree_session(sender.path(), receiver.path(), false, false);
    let s2 = s2.expect("sender2 ok");
    let _r2 = r2.expect("receiver2 ok");
    assert_eq!(s2.files, 1);
    assert_eq!(receiver.read("doc.bin"), sender.read("doc.bin"));
}

#[test]
fn scan_skips_dir_symlinks_and_dangling_links() {
    // D18: directory symlinks/junctions are never recursed into; dangling
    // links are skipped with a warning. The scan must terminate (the old
    // code recursed on metadata().is_dir(), so an ancestor junction was a
    // plausible infinite loop on Windows) and must not contain the linked
    // paths.
    let tree = TempTree::new("scan_sym");
    tree.write("real.txt", b"content");
    fs::create_dir_all(tree.path().join("sub")).unwrap();
    tree.write("sub/inner.txt", b"inner");

    // Directory junction pointing at the tree root itself (ancestor — the
    // recursion hazard). Windows: junction via mklink /J (no privilege);
    // unix: a real dir symlink.
    #[cfg(windows)]
    {
        let out = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(tree.path().join("loop"))
            .arg(tree.path())
            .output()
            .expect("mklink /J");
        assert!(out.status.success(), "mklink /J failed: {:?}", out.stderr);
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(tree.path(), tree.path().join("loop")).unwrap();

    // Dangling file symlink.
    #[cfg(windows)]
    {
        // Creating a file symlink needs developer mode or privilege; a
        // junction to a nonexistent path is the privilege-free dangling case.
        let out = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(tree.path().join("dangling"))
            .arg(tree.path().join("nope"))
            .output()
            .expect("mklink /J dangling");
        assert!(out.status.success());
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(tree.path().join("nope"), tree.path().join("dangling")).unwrap();

    let manifest = TreeManifest::scan(tree.path()).expect("scan must terminate");
    assert!(manifest.get("real.txt").is_some());
    assert!(manifest.get("sub/inner.txt").is_some());
    assert!(
        manifest.get("loop").is_none(),
        "dir symlink/junction must not be recursed or listed"
    );
    assert!(
        manifest.get("dangling").is_none(),
        "dangling link must be skipped"
    );
}

#[test]
fn scan_follows_file_symlinks() {
    // D18: a symlink to a regular file is followed and synced as the
    // target's content (documented behavior since v0.3, now exercised).
    let tree = TempTree::new("scan_follow");
    tree.write("target.bin", &vec![9u8; 128 * 1024]);
    #[cfg(windows)]
    {
        // File symlinks need privilege/developer mode on Windows; use a
        // hardlink as the "resolves to a file" proxy only if symlink fails.
        let out = std::process::Command::new("cmd")
            .args(["/C", "mklink"])
            .arg(tree.path().join("link.bin"))
            .arg(tree.path().join("target.bin"))
            .output()
            .expect("mklink");
        if !out.status.success() {
            // No symlink privilege: fall back to a hardlink (not a symlink,
            // but it still exercises the "file that resolves to a file" path).
            fs::hard_link(tree.path().join("target.bin"), tree.path().join("link.bin"))
                .expect("hard_link fallback");
        }
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(tree.path().join("target.bin"), tree.path().join("link.bin"))
        .unwrap();

    let manifest = TreeManifest::scan(tree.path()).expect("scan ok");
    let e = manifest.get("link.bin").expect("file link must be listed");
    assert_eq!(e.size, 128 * 1024);
    assert_eq!(e.kind, EntryKind::File);
}

#[test]
fn tree_exclude_negation_converges() {
    // §16.7 v0.7.1 end-to-end: sender's negation pattern set filters BOTH
    // scans identically; the re-included file syncs, the excluded one does
    // not exist for the session.
    let sender = TempTree::new("neg_sender");
    let receiver = TempTree::new("neg_receiver");

    sender.write("logs/a.log", &vec![1u8; 32 * 1024]);
    sender.write("logs/keep.log", &vec![2u8; 32 * 1024]);
    sender.write("main.bin", &vec![3u8; 32 * 1024]);

    // Negation FIRST (first-match-wins): keep.log is re-included, every
    // other *.log excluded; main.bin unaffected.
    let excludes = ["!keep.log", "*.log"];
    let (s, r) = run_tree_session_full(
        sender.path(),
        receiver.path(),
        false,
        false,
        &excludes, // sender patterns (rule the session)
        &[],       // receiver patterns (delete guard only)
        1,
    );
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, 2, "keep.log + main.bin sync; a.log is invisible");
    assert_eq!(receiver.read("logs/keep.log"), sender.read("logs/keep.log"));
    assert_eq!(receiver.read("main.bin"), sender.read("main.bin"));
    assert!(
        !receiver.exists("logs/a.log"),
        "excluded file never enters the session"
    );
    assert_eq!(r.deleted, 0);

    // Second session with a receiver-only file matching the NEGATED pattern:
    // the sender's re-include does NOT protect it from deletion (protection
    // comes from the receiver's OWN patterns), but a.log (excluded) never
    // reaches the delete pass at all. keep.log exists on both sides now, so
    // nothing is receiver-only — add one on the receiver.
    receiver.write("logs/stray.log", b"stray");
    let excludes2 = ["!keep.log", "*.log"];
    let (s2, r2) = run_tree_session_full(
        sender.path(),
        receiver.path(),
        false,
        false,
        &excludes2,
        &["*.log"], // receiver protects all its *.log paths
        1,
    );
    let _s2 = s2.expect("sender2 ok");
    let r2 = r2.expect("receiver2 ok");
    assert_eq!(
        r2.deleted, 0,
        "receiver's own *.log pattern protects stray.log"
    );
    assert!(receiver.exists("logs/stray.log"));
}

#[test]
fn tree_negation_free_session_interoperates() {
    // The negation flag must NOT ride when no pattern negates (byte-identical
    // v4 behavior — the compatibility promise of §16.7 v0.7.1).
    let sender = TempTree::new("noneg_sender");
    let receiver = TempTree::new("noneg_receiver");
    sender.write("a.bin", &vec![1u8; 16 * 1024]);
    let excludes = ["*.log"];
    let (s, r) = run_tree_session_full(
        sender.path(),
        receiver.path(),
        false,
        false,
        &excludes,
        &[],
        1,
    );
    let s = s.expect("sender ok");
    let _r = r.expect("receiver ok");
    assert_eq!(s.files, 1);
}
