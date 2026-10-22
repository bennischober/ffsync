//! Loopback integration tests: two sync state machines connected by an
//! in-memory duplex pair, driven concurrently on two threads. This exercises
//! the full protocol flow (HELLO, manifest, plan, DATA, VERIFY, COMMIT_ACK)
//! without a network.

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};

    use ffsync::chunking::PipelineConfig;
    use ffsync::protocol::sync::SyncError;
    use ffsync::protocol::sync::{ReceiveRequest, SendRequest, SyncOutcome, receive, send};

    /// A bidirectional in-memory pipe: what one side writes, the other reads.
    /// Each direction is a shared ring of bytes with blocking semantics via
    /// condvars (a bounded channel of bytes).
    struct DuplexPair {
        a_to_b: std::sync::mpsc::SyncSender<Vec<u8>>,
        b_to_a: std::sync::mpsc::SyncSender<Vec<u8>>,
    }

    /// One end of the duplex pair. Reads block until data is available (or
    /// the peer dropped its sender); writes enqueue to the peer.
    struct DuplexEnd {
        recv: std::sync::mpsc::Receiver<Vec<u8>>,
        send: std::sync::mpsc::SyncSender<Vec<u8>>,
        /// Partial message left over from a previous read.
        pending: Vec<u8>,
    }

    impl DuplexPair {
        fn new(bound: usize) -> (DuplexEnd, DuplexEnd) {
            let (a_tx, b_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(bound);
            let (b_tx, a_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(bound);
            (
                DuplexEnd {
                    recv: a_rx,
                    send: a_tx,
                    pending: Vec::new(),
                },
                DuplexEnd {
                    recv: b_rx,
                    send: b_tx,
                    pending: Vec::new(),
                },
            )
        }
    }

    impl Read for DuplexEnd {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            // First drain any pending bytes from a previous receive.
            if self.pending.is_empty() {
                match self.recv.recv() {
                    Ok(chunk) => self.pending = chunk,
                    Err(_) => return Ok(0), // peer dropped: clean EOF
                }
            }
            let n = buf.len().min(self.pending.len());
            buf[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            Ok(n)
        }
    }

    impl Write for DuplexEnd {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // SyncSender::send blocks when the peer's queue is full, which
            // gives us the backpressure the protocol relies on. Send whole
            // buffers; a peer that died makes send fail -> we report a
            // broken pipe.
            self.send
                .send(buf.to_vec())
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn create_file(path: &Path, data: &[u8]) {
        let mut f = File::create(path).unwrap();
        f.write_all(data).unwrap();
    }

    fn read_file(path: &Path) -> Vec<u8> {
        let mut v = Vec::new();
        File::open(path).unwrap().read_to_end(&mut v).unwrap();
        v
    }

    struct Fixture {
        dir: PathBuf,
        source: PathBuf,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ffsync_loopback_{}", name));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let source = dir.join("source.bin");
            Self { dir, source }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }

        fn source(&self) -> &Path {
            &self.source
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Runs sender and receiver concurrently over a duplex pair and asserts
    /// both sides finish without error.
    fn run_sync(
        send_req: SendRequest,
        recv_req: ReceiveRequest,
    ) -> (
        Result<SyncOutcome, SyncError>,
        Result<SyncOutcome, SyncError>,
    ) {
        let (mut sender_end, mut receiver_end) = DuplexPair::new(64);
        let pipeline = PipelineConfig::default();
        let pipeline_recv = PipelineConfig::default();

        let s_handle = std::thread::spawn(move || {
            let mut progress = |_p: ffsync::protocol::sync::Progress| {};
            send(&mut sender_end, &send_req, &pipeline, &mut progress)
        });
        let r_handle = std::thread::spawn(move || {
            let mut progress = |_p: ffsync::protocol::sync::Progress| {};
            receive(&mut receiver_end, &recv_req, &pipeline_recv, &mut progress)
        });

        let s = s_handle.join().unwrap();
        let r = r_handle.join().unwrap();
        (s, r)
    }

    #[test]
    fn sync_modified_file_transfers_delta_and_verifies() {
        let fx = Fixture::new("modified");
        // Source: 4 MiB of random-ish data (deterministic pattern).
        let mut src_data = vec![0u8; 4 * 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add((i / 65536) as u8);
        }
        create_file(fx.source(), &src_data);

        // Target (receiver's old file): same but middle 256 KiB modified.
        let mut old_data = src_data.clone();
        let mid = 2 * 1024 * 1024;
        for b in &mut old_data[mid..mid + 256 * 1024] {
            *b = *b ^ 0xFF;
        }
        let target = fx.path("target.bin");
        create_file(&target, &old_data);

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true, // keep the test deterministic
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
            },
        );

        let s = s.expect("sender ok");
        let r = r.expect("receiver ok");
        // The modified region must have been transferred (some delta > 0),
        // and the receiver's file must now equal the source.
        match (s, r) {
            (SyncOutcome::Transferred { bytes, .. }, SyncOutcome::Transferred { .. }) => {
                assert!(bytes > 0);
            }
            (other_s, other_r) => panic!("unexpected outcomes: s={:?} r={:?}", other_s, other_r),
        }
        assert_eq!(read_file(&target), src_data);
    }

    #[test]
    fn sync_identical_files_is_in_sync() {
        let fx = Fixture::new("identical");
        let data = vec![42u8; 2 * 1024 * 1024];
        create_file(fx.source(), &data);
        let target = fx.path("target.bin");
        create_file(&target, &data);

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
            },
        );

        assert!(matches!(s.unwrap(), SyncOutcome::InSync));
        assert!(matches!(r.unwrap(), SyncOutcome::InSync));
        assert_eq!(read_file(&target), data);
    }

    #[test]
    fn sync_fresh_file_transfers_everything() {
        let fx = Fixture::new("fresh");
        let data = vec![7u8; 2 * 1024 * 1024];
        create_file(fx.source(), &data);
        let target = fx.path("target.bin");

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(), // does not exist yet
                force_checksum: true,
            },
        );

        let s = s.unwrap();
        let r = r.unwrap();
        match (s, r) {
            (SyncOutcome::Transferred { bytes, .. }, SyncOutcome::Transferred { .. }) => {
                assert_eq!(bytes as usize, data.len());
            }
            (other_s, other_r) => panic!("unexpected outcomes: s={:?} r={:?}", other_s, other_r),
        }
        assert_eq!(read_file(&target), data);
    }

    #[test]
    fn sync_trust_times_fast_path_skips_transfer() {
        let fx = Fixture::new("fastpath");
        let data = vec![9u8; 1024 * 1024];
        create_file(fx.source(), &data);
        let target = fx.path("target.bin");
        create_file(&target, &data);
        // Make mtimes explicitly equal so the fast path triggers.
        let t = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        filetime::set_file_mtime(&fx.source(), filetime::FileTime::from_system_time(t)).unwrap();
        filetime::set_file_mtime(&target, filetime::FileTime::from_system_time(t)).unwrap();

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: Some(t),
                force_checksum: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
            },
        );

        assert!(matches!(s.unwrap(), SyncOutcome::InSync));
        assert!(matches!(r.unwrap(), SyncOutcome::InSync));
        assert_eq!(read_file(&target), data);
    }
}
