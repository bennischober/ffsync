//! Tree sync tests (D17, protocol v3 §16): full tree sessions over real TCP
//! loopback — the same code path the CLI uses.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::tree_sync::{
    TreeOutcome, TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send,
};

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
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let recv_root = receiver_root.to_path_buf();
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
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        )
    });

    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
    let send_res = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender_root.to_path_buf(),
            force_checksum: checksum,
            no_delete,
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
        entries: vec![TreeEntryWire {
            rel_path: "../escape.bin".to_string(),
            kind: 0,
            size: 1,
            mtime_secs: 0,
            mtime_nanos: 0,
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
