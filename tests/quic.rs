//! QUIC transport tests (D13 step 2, §17): full tree + single-file sessions
//! over real UDP loopback — the same code paths the CLI uses, with the
//! same D16 pinning model mapped onto quinn's TLS stack.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::sync::{ReceiveRequest, SendRequest, SyncOutcome, receive, send};
use ffsync::protocol::tree_sync::{
    TreeOutcome, TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send,
};
use ffsync::transport::quic::QuicStream;

/// Generates a fresh D16 identity (no state-dir interaction).
fn generate_identity(name: &str) -> ffsync::transport::Identity {
    let certified = rcgen::generate_simple_self_signed(vec![format!("ffsync-{name}")]).unwrap();
    ffsync::transport::Identity {
        cert_der: certified.cert.der().as_ref().to_vec(),
        key_der: certified.signing_key.serialize_der(),
    }
}

fn fingerprint(id: &ffsync::transport::Identity) -> Vec<u8> {
    ffsync::transport::sha256(&id.cert_der)
}

/// A temporary tree with a guard that removes it on drop.
struct TempTree(PathBuf);

impl TempTree {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("ffsync_quic_test_{name}_{}", std::process::id()));
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

/// One side's result of a QUIC sync case.
type SyncResult = Result<ffsync::protocol::sync::SyncOutcome, ffsync::protocol::sync::SyncError>;

/// One full single-file sync over QUIC loopback. `client_pin`/`server_pin`
/// select each side's PinPolicy (None = FirstUse). Returns both outcomes
/// and the fingerprints each side saw.
fn quic_sync_case(
    name: &str,
    client_id: ffsync::transport::Identity,
    server_id: ffsync::transport::Identity,
    client_pin: Option<Vec<u8>>,
    server_pin: Option<Vec<u8>>,
) -> (SyncResult, SyncResult, Vec<u8>, Vec<u8>) {
    let server_fp = fingerprint(&server_id);
    let client_fp = fingerprint(&client_id);
    let _ = (&server_fp, &client_fp);

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = ffsync::transport::quic::listen(
        bind,
        &server_id,
        &match server_pin {
            Some(fp) => ffsync::transport::PinPolicy::Pinned(vec![fp]),
            None => ffsync::transport::PinPolicy::FirstUse,
        },
    )
    .unwrap();
    let server_addr = endpoint.local_addr().unwrap();

    let name_owned = name.to_string();
    let server_handle = std::thread::spawn(move || {
        let (session, mut stream, saw) = ffsync::transport::quic::server_session(endpoint)?;
        // Hold the session guard past the sync (the connection lives as
        // long as any handle; the stream alone is not enough to keep the
        // endpoint's driver from being the last ref).
        let result = run_receive_over(&mut stream, &name_owned);
        session.wait_for_peer_close(Duration::from_secs(5));
        Ok::<_, std::io::Error>((result, saw))
    });

    let client_policy = match client_pin {
        Some(fp) => ffsync::transport::PinPolicy::Pinned(vec![fp]),
        None => ffsync::transport::PinPolicy::FirstUse,
    };
    let server_name = rustls::pki_types::ServerName::try_from("ffsync-peer".to_string()).unwrap();
    let (client_out, client_saw) = match ffsync::transport::quic::client_session(
        &client_id,
        &client_policy,
        server_addr,
        server_name,
    ) {
        Ok((session, mut stream, saw)) => {
            let out = run_send_over(&mut stream, name);
            session.wait_for_peer_close(Duration::from_secs(5));
            (out, saw)
        }
        Err(_) => {
            // Handshake refused (e.g. MITM pin mismatch): make the same
            // shape as the TLS tests.
            let err = Err(ffsync::protocol::sync::SyncError::Eof);
            (err, Vec::new())
        }
    };

    let (server_out, server_saw) = match server_handle.join().unwrap() {
        Ok((out, saw)) => (out, saw),
        Err(_) => (Err(ffsync::protocol::sync::SyncError::Eof), Vec::new()),
    };
    // Name the temp files for cleanup via the caller (files are created in
    // run_send_over/run_receive_over under the test's own names).
    let _ = name;
    (client_out, server_out, client_saw, server_saw)
}

/// Sends one file over a QUIC stream (the sync state machine, unchanged).
fn run_send_over(
    stream: &mut QuicStream,
    name: &str,
) -> Result<SyncOutcome, ffsync::protocol::sync::SyncError> {
    let dir = std::env::temp_dir().join(format!(
        "ffsync_quic_files_{name}_{}_sender",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.bin");
    fs::write(&path, vec![1u8; 256 * 1024]).unwrap();

    let req = SendRequest {
        path: path.clone(),
        display_path: "data.bin".into(),
        mtime: None,
        force_checksum: true,
        tree: false,
    };
    let pipeline = PipelineConfig::default();
    let out = send(stream, &req, &pipeline, &mut |_p| {});
    let _ = fs::remove_dir_all(&dir);
    out
}

/// Receives one file over a QUIC stream.
fn run_receive_over(
    stream: &mut QuicStream,
    name: &str,
) -> Result<SyncOutcome, ffsync::protocol::sync::SyncError> {
    let dir = std::env::temp_dir().join(format!(
        "ffsync_quic_files_{name}_{}_receiver",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.bin");
    let req = ReceiveRequest {
        path: path.clone(),
        local_source: path.clone(),
        force_checksum: true,
        tree: false,
        apply_mode: None,
        apply_mtime: None,
    };
    let pipeline = PipelineConfig::default();
    let out = receive(stream, &req, &pipeline, &mut |_p| {});
    let _ = fs::remove_dir_all(&dir);
    out
}

/// One full tree session over QUIC loopback with a parallel width
/// (§16.8; 1 = sequential). All conversations ride ONE connection as
/// streams — the QUIC mapping of the K-connection shape.
fn run_tree_session(
    sender_root: &Path,
    receiver_root: &Path,
    checksum: bool,
    no_delete: bool,
    parallel: u16,
) -> (
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
    Result<TreeOutcome, ffsync::protocol::sync::SyncError>,
) {
    let server_id = generate_identity("tree_server");
    let client_id = generate_identity("tree_client");
    let server_fp = fingerprint(&server_id);
    let client_fp = fingerprint(&client_id);

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = ffsync::transport::quic::listen(
        bind,
        &server_id,
        &ffsync::transport::PinPolicy::Pinned(vec![client_fp]),
    )
    .unwrap();
    let server_addr = endpoint.local_addr().unwrap();

    let recv_root = receiver_root.to_path_buf();
    let server = std::thread::spawn(move || {
        let (session, mut control, _saw) =
            ffsync::transport::quic::server_session(endpoint).unwrap();
        let res = tree_receive(
            &mut control,
            &TreeReceiveRequest {
                root: recv_root,
                force_checksum: checksum,
                no_delete,
                excludes: Vec::new(),
                accept_worker: std::sync::Arc::new({
                    let session = session.clone();
                    move || {
                        let s = session.accept_stream().map_err(|e| {
                            std::io::Error::other(format!("cannot accept worker: {e}"))
                        })?;
                        Ok(Box::new(s))
                    }
                }),
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        );
        session.wait_for_peer_close(Duration::from_secs(5));
        res
    });

    let server_name = rustls::pki_types::ServerName::try_from("ffsync-peer".to_string()).unwrap();
    let (session, mut control, _saw) = ffsync::transport::quic::client_session(
        &client_id,
        &ffsync::transport::PinPolicy::Pinned(vec![server_fp]),
        server_addr,
        server_name,
    )
    .unwrap();
    let send_res = tree_send(
        &mut control,
        &TreeSendRequest {
            root: sender_root.to_path_buf(),
            force_checksum: checksum,
            no_delete,
            excludes: Vec::new(),
            parallel,
            connect_worker: std::sync::Arc::new({
                let session = session.clone();
                move |_index: u16| {
                    let s = session
                        .open_stream()
                        .map_err(|e| std::io::Error::other(format!("cannot open worker: {e}")))?;
                    Ok(Box::new(s))
                }
            }),
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let recv_res = server.join().unwrap();
    (send_res, recv_res)
}

#[test]
fn quic_single_file_sync_converges_and_pins_match() {
    let client = generate_identity("q_basic_c");
    let server = generate_identity("q_basic_s");
    let (s_out, r_out, client_saw, server_saw) =
        quic_sync_case("basic", client.clone(), server.clone(), None, None);
    assert!(s_out.is_ok(), "sender: {s_out:?}");
    assert!(r_out.is_ok(), "receiver: {r_out:?}");
    // Mutual pinning: each side saw the OTHER's certificate.
    assert_eq!(client_saw, fingerprint(&server), "client saw server cert");
    assert_eq!(server_saw, fingerprint(&client), "server saw client cert");
}

#[test]
fn quic_pinned_peers_sync_and_mitm_is_rejected() {
    // Case 1: correct pins on both sides — sync works.
    let client = generate_identity("q_pin_c");
    let server = generate_identity("q_pin_s");
    let (s_out, r_out, client_saw, server_saw) = quic_sync_case(
        "pinmatch",
        client.clone(),
        server.clone(),
        Some(fingerprint(&server)),
        Some(fingerprint(&client)),
    );
    assert!(s_out.is_ok(), "sender: {s_out:?}");
    assert!(r_out.is_ok(), "receiver: {r_out:?}");
    assert_eq!(client_saw, fingerprint(&server));
    assert_eq!(server_saw, fingerprint(&client));

    // Case 2: the client pins a THIRD-party identity (simulated MITM):
    // the server presents its own cert, which does not match the pin —
    // the client must refuse.
    let attacker = generate_identity("q_mitm");
    let (s_out, _r_out, _cs, _ss) =
        quic_sync_case("mitm", client, server, Some(fingerprint(&attacker)), None);
    assert!(s_out.is_err(), "client must reject the wrong cert");
}

#[test]
fn quic_unpinned_client_is_rejected_by_pinned_receiver() {
    // Receiver pins a legit client; an unknown third party connects. The
    // RECEIVER must refuse the handshake (server-side pin check) — nothing
    // about the sync may start.
    let legit = generate_identity("q_up_legit");
    let server = generate_identity("q_up_server");
    let attacker = generate_identity("q_up_attacker");

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = ffsync::transport::quic::listen(
        bind,
        &server,
        &ffsync::transport::PinPolicy::Pinned(vec![fingerprint(&legit)]),
    )
    .unwrap();
    let server_addr = endpoint.local_addr().unwrap();

    let server_handle = std::thread::spawn(move || {
        // Map the Ok tuple to its fingerprint so the Err path is Debug.
        match ffsync::transport::quic::server_session(endpoint) {
            Ok((_session, _stream, saw)) => Ok(saw),
            Err(e) => Err(e),
        }
    });

    let server_name = rustls::pki_types::ServerName::try_from("ffsync-peer".to_string()).unwrap();
    let client_res = ffsync::transport::quic::client_session(
        &attacker,
        &ffsync::transport::PinPolicy::FirstUse,
        server_addr,
        server_name,
    );
    let server_res = server_handle.join().unwrap();
    // The receiver must refuse the attacker, loudly and specifically.
    let server_err = server_res.expect_err("receiver must refuse the attacker");
    let msg = server_err.to_string();
    assert!(
        msg.contains("not pinned"),
        "receiver error must name the pin failure, got: {msg}"
    );
    assert!(
        msg.contains(&ffsync::transport::format_fingerprint(&fingerprint(
            &attacker
        ))),
        "receiver error must show the attacker's fingerprint, got: {msg}"
    );
    // The attacker learns at most that the receiver refused.
    let _ = client_res;
}

#[test]
fn quic_tree_converges_mixed_content() {
    let sender = TempTree::new("q_conv_sender");
    let receiver = TempTree::new("q_conv_receiver");

    sender.write("big.bin", &vec![7u8; 3 * 1024 * 1024]);
    sender.write("sub/medium.bin", &vec![1u8; 300 * 1024]);
    sender.write("sub/deep/tiny.txt", b"hello tree");
    sender.write("empty.txt", b"");
    fs::create_dir_all(sender.path().join("empty_dir")).unwrap();

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false, 1);
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, r.files);
    assert!(s.transferred_bytes > 0);
    assert_eq!(s.deleted, 0);

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
    assert!(receiver.exists("empty_dir"));
}

#[test]
fn quic_tree_parallel_sessions_converge() {
    // §16.8 over QUIC: K streams replace K connections; membership
    // validation (TREE_FILE/TREE_WORKER) is unchanged. 100 small files so
    // several conversations actually race.
    let sender = TempTree::new("q_par_sender");
    let receiver = TempTree::new("q_par_receiver");

    for i in 0..100 {
        sender.write(&format!("f{i:03}.bin"), &vec![(i % 251) as u8; 4096]);
    }

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false, 4);
    let s = s.expect("sender ok");
    let r = r.expect("receiver ok");
    assert_eq!(s.files, r.files);
    assert_eq!(s.files, 100);
    for i in 0..100 {
        let rel = format!("f{i:03}.bin");
        assert_eq!(
            receiver.read(&rel),
            sender.read(&rel),
            "file {rel} did not converge"
        );
    }
}

#[test]
fn quic_tree_deletes_after_success() {
    // Receiver starts with an extra file; a successful session deletes it
    // (the sender's tree is authoritative).
    let sender = TempTree::new("q_del_sender");
    let receiver = TempTree::new("q_del_receiver");
    sender.write("keep.txt", b"keep me");
    receiver.write("keep.txt", b"keep me");
    receiver.write("stale.txt", b"delete me");

    let (s, r) = run_tree_session(sender.path(), receiver.path(), false, false, 1);
    s.expect("sender ok");
    r.expect("receiver ok");
    assert!(receiver.exists("keep.txt"));
    assert!(!receiver.exists("stale.txt"), "stale file must be deleted");
}
