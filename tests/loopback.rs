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
    use ffsync::protocol::message::ErrorCode;
    use ffsync::protocol::sync::SyncError;
    use ffsync::protocol::sync::{
        Progress, ReceiveRequest, SendRequest, SyncOutcome, receive, send,
    };

    /// A bidirectional in-memory pipe: what one side writes, the other reads.
    /// Each direction is a bounded byte channel; `new` returns both ends.
    struct DuplexPair;

    /// One end of the duplex pair. Reads block until data is available (or
    /// the peer dropped its sender); writes enqueue to the peer.
    struct DuplexEnd {
        recv: std::sync::mpsc::Receiver<Vec<u8>>,
        send: std::sync::mpsc::SyncSender<Vec<u8>>,
        /// Partial message left over from a previous read.
        pending: Vec<u8>,
    }

    impl DuplexPair {
        #[allow(clippy::new_ret_no_self)] // returns the (sender, receiver) end pair
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

    /// Like `run_sync`, but records each side's progress events so tests can
    /// assert on phases (e.g. that a warm start happened).
    fn run_sync_recording(
        send_req: SendRequest,
        recv_req: ReceiveRequest,
    ) -> (Vec<Progress>, Vec<Progress>) {
        let (mut sender_end, mut receiver_end) = DuplexPair::new(64);
        let pipeline = PipelineConfig::default();
        let pipeline_recv = PipelineConfig::default();

        let s_handle = std::thread::spawn(move || {
            let mut events = Vec::new();
            let mut progress = |p: ffsync::protocol::sync::Progress| events.push(p);
            let r = send(&mut sender_end, &send_req, &pipeline, &mut progress);
            assert!(r.is_ok(), "sender failed: {:?}", r);
            events
        });
        let r_handle = std::thread::spawn(move || {
            let mut events = Vec::new();
            let mut progress = |p: ffsync::protocol::sync::Progress| events.push(p);
            let r = receive(&mut receiver_end, &recv_req, &pipeline_recv, &mut progress);
            assert!(r.is_ok(), "receiver failed: {:?}", r);
            events
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
            *b ^= 0xFF;
        }
        let target = fx.path("target.bin");
        create_file(&target, &old_data);

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true, // keep the test deterministic
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
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
    fn warm_start_uses_index_on_second_sync() {
        let fx = Fixture::new("warmstart");
        let mut src_data = vec![0u8; 2 * 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add((i / 65536) as u8);
        }
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data[..1024 * 1024]);

        let mk_send = || SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "source.bin".into(),
            mtime: None,
            force_checksum: false, // warm start needs the default mode
            tree: false,
            dry_run: false,
            eager: false,
        };
        let mk_recv = || ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
            eager: false,
        };

        // Sync 1 (cold): full scan both sides, index files written at commit.
        let (s1, r1) = run_sync(mk_send(), mk_recv());
        s1.expect("sync 1 sender ok");
        r1.expect("sync 1 receiver ok");
        let src_index = ffsync::index::index_path_for(fx.source());
        let tgt_index = ffsync::index::index_path_for(&target);
        assert!(src_index.exists(), "sender index written at commit");
        assert!(tgt_index.exists(), "receiver index written at commit");

        // Sync 2 (warm, unchanged data): both sides must load their chunk
        // lists from the index — the sender emits WarmStart before Diffing,
        // and the receiver's WarmStart precedes the manifest.
        let (s2_events, r2_events) = run_sync_recording(mk_send(), mk_recv());
        assert!(
            s2_events
                .iter()
                .any(|p| matches!(p, Progress::WarmStart(_))),
            "sender must warm-start: {:?}",
            s2_events
        );
        assert!(
            r2_events
                .iter()
                .any(|p| matches!(p, Progress::WarmStart(_))),
            "receiver must warm-start: {:?}",
            r2_events
        );
        // The receiver must still be InSync (nothing to transfer).
        let (_s2, r2) = run_sync(mk_send(), mk_recv());
        assert!(matches!(r2.expect("sync 3 ok"), SyncOutcome::InSync));

        // Now change the source: warm start must NOT be used (mtime changes
        // invalidate the sender's index; the receiver's index still validates
        // but the plan will differ).
        let mut changed = src_data.clone();
        changed[0] = changed[0].wrapping_add(1);
        create_file(fx.source(), &changed);
        let (s3, r3) = run_sync(mk_send(), mk_recv());
        s3.expect("sync after change ok");
        r3.expect("sync after change ok");
        assert_eq!(read_file(&target), changed, "convergence after change");
        // Sender's index must have been invalidated (mtime mismatch) and
        // refreshed at commit: loading it now must succeed for new content.
        assert!(
            ffsync::index::load_index(fx.source()).is_some(),
            "sender index refreshed at commit"
        );
    }

    #[test]
    fn corrupt_or_stale_index_falls_back_to_cold_scan() {
        let fx = Fixture::new("staleindex");
        let mut src_data = vec![0u8; 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(11).wrapping_add((i / 16384) as u8);
        }
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data);

        // Write a valid *stale* index for the source: claims different
        // chunks than the file really has. This is the poison case: only
        // plan-read hash verification + VERIFY can recover; stat validation
        // alone cannot refuse it (D15's accepted caveat).

        // Corrupt the target's index on disk (truncated): receiver must
        // treat it as missing and cold-scan.
        let tgt_index = ffsync::index::index_path_for(&target);
        std::fs::write(&tgt_index, b"FFSYIX02corrupt").unwrap();

        // Backdate both files' mtimes to DISTINCT values so the wire fast
        // path (size+mtime, D11) cannot mask the scenario — with natural
        // same-second mtimes the sender exits at NOT_MODIFIED before any
        // index is consulted (bit us once already).
        let t_src = std::time::SystemTime::now() - std::time::Duration::from_secs(120);
        let t_tgt = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        filetime::set_file_mtime(fx.source(), filetime::FileTime::from_system_time(t_src)).unwrap();
        filetime::set_file_mtime(&target, filetime::FileTime::from_system_time(t_tgt)).unwrap();
        // Re-stamp the bogus index with the backdated source mtime so its
        // validation still passes — the poison scenario must survive.
        let stale = ffsync::index::FileIndex::from_scan(
            vec![ffsync::chunking::Chunk {
                offset: 0,
                length: 512,
                hash: blake3::hash(b"bogus"),
            }],
            src_data.len() as u64,
            ffsync::index::stat_mtime(fx.source()).unwrap(),
            {
                let (a, b, c) = ffsync::chunking::cdc_params_for_size(src_data.len() as u64);
                (a as u32, b as u32, c as u32)
            },
        );
        ffsync::index::write_index(fx.source(), &stale).unwrap();

        // The bogus sender index passes size+mtime validation (we crafted
        // it) — its wrong chunk list makes the sender promise content it
        // cannot back up. Safety property (D15): the sync must FAIL CLEANLY
        // at VERIFY (HashMismatch) — never converge to wrong content — and
        // the sender's lying index must be invalidated so the retry
        // cold-scans and succeeds (no poison loop).
        let (s1, r1) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        assert!(
            matches!(&s1, Err(SyncError::Peer(code, _)) if *code == ErrorCode::HashMismatch),
            "attempt 1 must fail loudly at VERIFY: {s1:?}"
        );
        assert!(r1.is_err(), "attempt 1 receiver must also fail");
        assert!(
            ffsync::index::load_index(fx.source()).is_none(),
            "sender index must be invalidated after the warm-start lie"
        );

        // Attempt 2: cold scan on both sides; must converge.
        let (s2, r2) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        s2.expect("attempt 2 sender converges");
        r2.expect("attempt 2 receiver converges");
        assert_eq!(read_file(&target), src_data);
        // Post-commit indexes must now be valid for the real content.
        assert!(ffsync::index::load_index(fx.source()).is_some());
        assert!(ffsync::index::load_index(&target).is_some());
    }

    #[test]
    fn orphan_index_is_cleaned_up_on_receive() {
        let fx = Fixture::new("orphan");
        let data = vec![9u8; 256 * 1024];
        create_file(fx.source(), &data);
        let target = fx.path("target.bin");
        create_file(&target, &data);

        // Orphan: index without a target file.
        let orphan_target = fx.path("gone.bin");
        let orphan_index = ffsync::index::index_path_for(&orphan_target);
        std::fs::write(&orphan_index, vec![0u8; 64]).unwrap();

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        s.expect("sender ok");
        r.expect("receiver ok");
        // Orphan cleanup: the index whose target no longer exists is gone.
        assert!(
            !orphan_index.exists(),
            "orphan index must be removed by receive"
        );
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
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );

        assert!(matches!(s.unwrap(), SyncOutcome::InSync));
        assert!(matches!(r.unwrap(), SyncOutcome::InSync));
        assert_eq!(read_file(&target), data);
    }

    #[test]
    fn sync_all_local_plan_repositions_chunks() {
        // Found by the static-binary smoke test: the receiver's file contains
        // every chunk hash of the sender's file (a prefix of it), so the plan
        // is all-Local — but the chunks sit at DIFFERENT positions. The old
        // "no Remote ⇒ in sync" shortcut skipped the writes and errored
        // HashMismatch at VERIFY; the receiver must execute the plan (locals
        // are dedup'd by hash, not position) and the sender must report the
        // executed plan (Transferred, 0 wire bytes).
        let fx = Fixture::new("all_local_reposition");
        // Periodic content: chunks hit the 1 MiB cap, so the receiver's
        // single 1 MiB chunk hash-equals the sender's first 1 MiB chunk.
        let sender_data: Vec<u8> = (0..=255u8).cycle().take(2 * 1024 * 1024).collect();
        let receiver_data: Vec<u8> = (0..=255u8).cycle().take(1024 * 1024).collect();
        create_file(fx.source(), &sender_data);
        let target = fx.path("target.bin");
        create_file(&target, &receiver_data);

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );

        let s = s.expect("sender ok");
        let r = r.expect("receiver ok");
        // D19: 2 MiB < 16 MiB, so the conversation chunks with the
        // small-file config — the sender's 2 MiB of periodic content is 8
        // capped 256 KiB chunks (was 2 × 1 MiB under the shipped defaults).
        assert!(
            matches!(r, SyncOutcome::Transferred { chunks: 8, .. }),
            "receiver must execute the all-Local plan: {r:?}"
        );
        assert!(
            matches!(s, SyncOutcome::Transferred { bytes, .. } if bytes == 0),
            "sender transferred nothing over the wire: {s:?}"
        );
        assert_eq!(read_file(&target), sender_data);
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
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(), // does not exist yet
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
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

    /// A duplex end that can be dropped mid-session to simulate a crash.
    /// The peer then observes EOF/BrokenPipe. The kill is thread-scoped:
    /// the sender thread panics once `kill_after_bytes` of payload have
    /// been sent, which drops its duplex end — the in-memory equivalent of
    /// a process/socket dying (a real `abort()` would take down the whole
    /// test process, which the first version of this test did).
    fn run_sync_resumable(
        send_req: SendRequest,
        recv_req: ReceiveRequest,
        kill_after_bytes: u64,
    ) -> (
        Result<SyncOutcome, SyncError>,
        Result<SyncOutcome, SyncError>,
    ) {
        let (mut sender_end, mut receiver_end) = DuplexPair::new(64);
        let pipeline = PipelineConfig::default();
        let pipeline_recv = PipelineConfig::default();

        let s_handle = std::thread::spawn(move || {
            let kill = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let kill_writer = std::sync::Arc::clone(&kill);
            // Guard wrapping the duplex end: once the flag is set, the next
            // write panics the thread (dropping the end -> receiver EOF).
            struct KillGuard<'a> {
                inner: &'a mut DuplexEnd,
                kill: std::sync::Arc<std::sync::atomic::AtomicBool>,
            }
            impl Write for KillGuard<'_> {
                fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                    if self.kill.load(std::sync::atomic::Ordering::SeqCst) {
                        panic!("simulated sender crash");
                    }
                    self.inner.write(buf)
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    self.inner.flush()
                }
            }
            impl Read for KillGuard<'_> {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    self.inner.read(buf)
                }
            }
            let mut progress = |p: ffsync::protocol::sync::Progress| {
                if let ffsync::protocol::sync::Progress::Transferring(sent, _) = p
                    && sent >= kill_after_bytes
                {
                    kill_writer.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            };
            let mut guard = KillGuard {
                inner: &mut sender_end,
                kill,
            };
            send(&mut guard, &send_req, &pipeline, &mut progress)
        });
        let r_handle = std::thread::spawn(move || {
            let mut progress = |_p: ffsync::protocol::sync::Progress| {};
            receive(&mut receiver_end, &recv_req, &pipeline_recv, &mut progress)
        });

        let s = s_handle
            .join()
            .unwrap_or_else(|_| Err(SyncError::Protocol("sender crashed (simulated)".into())));
        let r = r_handle.join().unwrap_or(Err(SyncError::Eof));
        (s, r)
    }

    #[test]
    fn resume_after_mid_transfer_disconnect() {
        let fx = Fixture::new("resume");
        let mut src_data = vec![0u8; 4 * 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add((i / 65536) as u8);
        }
        create_file(fx.source(), &src_data);

        let mut old_data = src_data.clone();
        let mid = 2 * 1024 * 1024;
        for b in &mut old_data[mid..mid + 256 * 1024] {
            *b ^= 0xFF;
        }
        let target = fx.path("target.bin");
        create_file(&target, &old_data);

        // Attempt 1: crash the sender after ~64 KiB of DATA.
        let (s1, r1) = run_sync_resumable(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
            64 * 1024,
        );
        // Both sides must have observed the crash (no false success).
        assert!(s1.is_err(), "sender should have crashed");
        assert!(r1.is_err(), "receiver should have seen the disconnect");
        // The receiver must have left the temp file behind (resume state).
        let temp = ffsync::reconstruction::Reconstructor::temp_path_for(&target);
        assert!(temp.exists(), "temp file must survive the disconnect");

        // Attempt 2: reconnect. The receiver now unions temp + old file.
        let (s2, r2) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        let _ = s2.expect("resumed sender ok");
        let _ = r2.expect("resumed receiver ok");
        // Convergence is the oracle.
        assert_eq!(read_file(&target), src_data);
        // The temp file must be gone (committed).
        assert!(!temp.exists(), "temp file must be committed away");
    }

    #[test]
    fn eager_resume_after_mid_transfer_disconnect() {
        // D10 x §9.2: an eager session that dies mid-transfer must leave the
        // same resume state as a lazy one, and the resumed session (eager
        // again) must converge. The pre-plan DATA the sender streamed before
        // the crash is content-addressed garbage to the next session: the
        // re-scan unions temp + old file by hash, so nothing can poison the
        // reconstruction.
        let fx = Fixture::new("eager_resume");
        let mut src_data = vec![0u8; 4 * 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(57).wrapping_add((i / 65536) as u8);
        }
        create_file(fx.source(), &src_data);

        let mut old_data = src_data.clone();
        let mid = 2 * 1024 * 1024;
        for b in &mut old_data[mid..mid + 256 * 1024] {
            *b ^= 0xFF;
        }
        let target = fx.path("target.bin");
        create_file(&target, &old_data);

        // Attempt 1: eager session, crash the sender after ~64 KiB of DATA.
        // The kill counter counts eager DATA too (it rides the Transferring
        // progress), so the crash lands mid-manifest or mid-data — both fine.
        let (s1, r1) = run_sync_resumable(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: true,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: true,
            },
            64 * 1024,
        );
        assert!(s1.is_err(), "sender should have crashed");
        assert!(r1.is_err(), "receiver should have seen the disconnect");
        // §9.2 x D10 timing note: eager DATA flows DURING the manifest
        // exchange, so a 64 KiB kill can land before the receiver ever
        // reaches plan execution — no temp yet (resume state is created
        // with the plan, not with the stream). Both cases are legal; the
        // oracle is attempt 2's convergence either way.
        let temp = ffsync::reconstruction::Reconstructor::temp_path_for(&target);
        let _ = temp.exists();

        // Attempt 2: reconnect, eager again.
        let (s2, r2) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: true,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: true,
            },
        );
        let _ = s2.expect("resumed eager sender ok");
        let _ = r2.expect("resumed eager receiver ok");
        assert_eq!(read_file(&target), src_data);
        assert!(!temp.exists(), "temp file must be committed away");
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
        filetime::set_file_mtime(fx.source(), filetime::FileTime::from_system_time(t)).unwrap();
        filetime::set_file_mtime(&target, filetime::FileTime::from_system_time(t)).unwrap();

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: Some(t),
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );

        assert!(matches!(s.unwrap(), SyncOutcome::InSync));
        assert!(matches!(r.unwrap(), SyncOutcome::InSync));
        assert_eq!(read_file(&target), data);
    }

    #[test]
    fn version_mismatch_is_rejected_loudly() {
        // Protocol compatibility contract (§4): a peer speaking a different
        // protocol version is rejected with ERROR(Version) by BOTH sides —
        // never silently mis-interpreted. Pinned: the v0.2 wire change
        // (HELLO file_mtime_nanos) bumped VERSION 1 -> 2; a v1 peer (e.g.
        // ffsync 0.1.0) must fail the handshake, not corrupt a sync by
        // reading a v2 frame with v1 expectations.
        let fx = Fixture::new("versionmismatch");
        let data = vec![7u8; 512 * 1024];
        create_file(fx.source(), &data);
        let target = fx.path("target.bin");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // A fake "old peer" that speaks protocol version 1. Returns Err on
        // purpose: it asserts the rejection inline and reports "done" as an
        // error (join() propagates assertion panics).
        let old_peer: std::thread::JoinHandle<std::io::Result<()>> =
            std::thread::spawn(move || {
                use ffsync::protocol::message::Message as _;
                let mut sock = std::net::TcpStream::connect(addr).unwrap();
                // Send a v1 HELLO as a sender would.
                let hello = ffsync::protocol::message::Hello {
                    version: 1,
                    flags: ffsync::protocol::message::FLAG_TRUST_TIMES,
                    role: ffsync::protocol::message::Role::Sender,
                    cdc_min: 64 * 1024,
                    cdc_avg: 256 * 1024,
                    cdc_max: 1024 * 1024,
                    file_size: data.len() as u64,
                    file_mtime: 1_760_000_000,
                    file_mtime_nanos: 0,
                    path: "source.bin".into(),
                };
                hello.into_frame().unwrap().encode(&mut sock).unwrap();
                sock.flush().unwrap();
                // Read the receiver's answer. The receiver sends its own HELLO
                // first (both sides greet immediately), so frame 1 is HELLO
                // (0x01); the next readable frame must be ERROR(Version) — or
                // the receiver hangs up (also acceptable, but a code is
                // required if anything beyond the HELLO arrives).
                loop {
                    let frame = match ffsync::protocol::framing::read_frame(&mut sock) {
                        Ok(f) => f,
                        Err(_) => return Err(std::io::Error::other("connection closed (correct)")),
                    };
                    if frame.ftype == ffsync::protocol::framing::T_HELLO {
                        continue; // expected: the receiver's own greeting
                    }
                    assert_eq!(
                        frame.ftype,
                        ffsync::protocol::framing::T_ERROR,
                        "expected ERROR frame, got 0x{:02X}",
                        frame.ftype
                    );
                    let (code, _msg) =
                        ffsync::protocol::message::read_error_payload(&frame).unwrap();
                    assert_eq!(code, ffsync::protocol::message::ErrorCode::Version);
                    return Err(std::io::Error::other("peer rejected (correct)"));
                }
            });

        let (sock, _) = listener.accept().unwrap();
        let mut sock = sock;
        let request = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
            eager: false,
        };
        let pipeline = PipelineConfig::default();
        let mut progress = |_p: Progress| {};
        let r = receive(&mut sock, &request, &pipeline, &mut progress);
        // The old-peer closure returns Err deliberately once it has asserted
        // the rejection; join() panics only if its assertions failed.
        let _ = old_peer.join().expect("old peer thread panicked");

        // The receiver must fail — and with the version error specifically.
        match r {
            Err(SyncError::Peer(code, msg)) => {
                assert_eq!(code, ErrorCode::Version, "message: {msg}");
            }
            other => panic!("receiver must fail with Peer(Version, _), got {other:?}"),
        }
    }

    #[test]
    fn receive_with_from_does_not_write_wrong_index() {
        // D15 ownership rule: the index describes THE FILE IT WAS WRITTEN
        // FROM. With --from <old-file>, an InSync outcome must not write an
        // index for the target claiming the old file's content — otherwise
        // the next warm start serves a chunk list the target does not have.
        let fx = Fixture::new("fromindex");
        let old_data = vec![3u8; 512 * 1024];
        let new_data = vec![5u8; 512 * 1024];
        let from_file = fx.path("from.bin");
        create_file(&from_file, &old_data);
        let target = fx.path("target.bin");
        create_file(&target, &new_data);

        // Sync where the sender's content equals the OLD file (so the plan
        // is empty relative to the from-file's manifest), receiver converges
        // to... nothing (already InSync relative to from.bin).
        let (s, r) = run_sync(
            SendRequest {
                path: from_file.clone(),
                display_path: "from.bin".into(),
                mtime: None,
                force_checksum: true,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: from_file.clone(),
                force_checksum: true,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        let _s = s.expect("sender ok");
        let _r = r.expect("receiver ok");

        // The from-file's index is fine (it describes from.bin), but the
        // TARGET must not have an index written from another file's content.
        // (The InSync path must skip the index write when local_source !=
        // target.)
        let target_index = ffsync::index::index_path_for(&target);
        if target_index.exists() {
            // If an index exists for the target, it must VALIDATE against
            // the target's actual content — it must not be a copy of the
            // from-file's state.
            let target_root = ffsync::chunking::chunk_root(
                &ffsync::chunking::scan_file_ordered(
                    &target,
                    &PipelineConfig::default(),
                    ffsync::chunking::cdc_params_for_size(new_data.len() as u64),
                )
                .expect("target scan")
                .1,
            );
            let idx =
                ffsync::index::load_index(&target).expect("target index (if present) must load");
            assert_eq!(
                idx.root, target_root,
                "target index must describe the target's own content"
            );
        }
    }

    #[test]
    fn cdc_policy_boundary_and_sanity() {
        // D19: params are a pure function of the file size.
        use ffsync::chunking::{
            CHUNK_AVG, CHUNK_MAX, CHUNK_MIN, SMALL_CHUNK_AVG, SMALL_CHUNK_MAX, SMALL_CHUNK_MIN,
            SMALL_FILE_BOUNDARY, cdc_params_for_size, sane_cdc_params,
        };
        assert_eq!(
            cdc_params_for_size(SMALL_FILE_BOUNDARY - 1),
            (SMALL_CHUNK_MIN, SMALL_CHUNK_AVG, SMALL_CHUNK_MAX),
            "below the boundary: small-file config"
        );
        assert_eq!(
            cdc_params_for_size(SMALL_FILE_BOUNDARY),
            (CHUNK_MIN, CHUNK_AVG, CHUNK_MAX),
            "at the boundary (inclusive): shipped defaults"
        );
        assert_eq!(
            cdc_params_for_size(u64::MAX),
            (CHUNK_MIN, CHUNK_AVG, CHUNK_MAX)
        );
        // Sane-parameter gate (the receiver's hostile-peer guard).
        assert!(sane_cdc_params((16, 64, 256)));
        assert!(sane_cdc_params((64, 256, 1024)));
        assert!(!sane_cdc_params((0, 64, 256)), "zero min is degenerate");
        assert!(!sane_cdc_params((256, 64, 256)), "min > avg is nonsense");
        assert!(!sane_cdc_params((64, 256, 32)), "avg > max is nonsense");
        assert!(
            !sane_cdc_params((1, 1, 128 * 1024 * 1024)),
            "max beyond the 64 MiB cap is refused"
        );
    }

    #[test]
    fn small_file_session_chunks_with_small_config_and_warm_restarts() {
        // D19 end-to-end: a sub-boundary file converges with the small-file
        // parameters, and the indexes both sides write record those
        // parameters — a second sync warm-starts (the served layout matches
        // the session's).
        use ffsync::chunking::{SMALL_CHUNK_AVG, SMALL_CHUNK_MAX, SMALL_CHUNK_MIN};
        use ffsync::index::index_path_for;

        let fx = Fixture::new("adaptive_small");
        let mut src_data = vec![0u8; 2 * 1024 * 1024];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(29).wrapping_add((i / 4096) as u8);
        }
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data[..1024 * 1024]);

        let mk_send = || SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "source.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
            dry_run: false,
            eager: false,
        };
        let mk_recv = || ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
            eager: false,
        };

        // Sync 1: converges; both indexes record the small-file parameters.
        let (s1, r1) = run_sync(mk_send(), mk_recv());
        s1.expect("sync 1 sender ok");
        r1.expect("sync 1 receiver ok");
        assert_eq!(read_file(&target), src_data);
        let expected_cdc = (
            SMALL_CHUNK_MIN as u32,
            SMALL_CHUNK_AVG as u32,
            SMALL_CHUNK_MAX as u32,
        );
        for (what, p) in [("sender", fx.source()), ("receiver", &target)] {
            let idx = ffsync::index::load_index(p).expect("index written at commit");
            assert_eq!(
                idx.cdc, expected_cdc,
                "{what} index records the session's CDC parameters"
            );
        }

        // Sync 2 (unchanged): fast path on the wire (size+mtime) — but force
        // a scan-less warm check by changing nothing; InSync either way.
        let (_s2, r2) = run_sync(mk_send(), mk_recv());
        assert!(matches!(r2.expect("sync 2 ok"), SyncOutcome::InSync));

        // The written indexes validate against the session's parameters and
        // refuse a foreign layout — the D19 warm-start contract.
        for (what, p) in [("sender", fx.source()), ("receiver", &target)] {
            let idx = ffsync::index::load_index(p).unwrap();
            assert!(
                idx.validate(p, expected_cdc).is_some(),
                "{what} index validates with the session's parameters"
            );
            assert!(
                idx.validate(p, (64, 256, 1024)).is_none(),
                "{what} index refuses a foreign layout"
            );
        }
        // The index path naming convention is untouched by the codec bump.
        assert!(
            index_path_for(&target)
                .to_string_lossy()
                .ends_with(".ffsync-index"),
            "suffix appended, not substituted (D6/D15)"
        );
    }

    #[test]
    fn large_file_session_keeps_default_layout_and_warm_restarts() {
        // The compatibility promise of D19: a file at/above the boundary
        // chunks byte-identically to the shipped defaults — its index
        // validates against the DEFAULT parameters.
        use ffsync::chunking::{CHUNK_AVG, CHUNK_MAX, CHUNK_MIN};
        let fx = Fixture::new("adaptive_large");
        // Just above the boundary is enough (16 MiB + 1); the pipeline caps
        // chunk max at 1 MiB so this scans fast.
        let mut src_data = vec![0u8; ffsync::chunking::SMALL_FILE_BOUNDARY as usize + 1];
        for (i, b) in src_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(13).wrapping_add((i / 8192) as u8);
        }
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, Vec::new().as_slice());

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        s.expect("sender ok");
        r.expect("receiver ok");
        assert_eq!(read_file(&target), src_data);

        let expected_cdc = (CHUNK_MIN as u32, CHUNK_AVG as u32, CHUNK_MAX as u32);
        for (what, p) in [("sender", fx.source()), ("receiver", &target)] {
            let idx = ffsync::index::load_index(p).expect("index written at commit");
            assert_eq!(idx.cdc, expected_cdc, "{what} index records the defaults");
            assert!(
                idx.validate(p, expected_cdc).is_some(),
                "{what} index validates with default parameters (large-file layout unchanged)"
            );
        }
    }

    #[test]
    fn stale_v1_index_is_treated_as_missing() {
        // A pre-D19 (FFSYIX01) index must not be served: the magic differs,
        // so the warm start cold-scans and the commit rewrites it as v2.
        let fx = Fixture::new("v1index");
        let src_data = vec![7u8; 1024 * 1024];
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data);

        // Force the slow path: identical size + natural (same-second)
        // mtimes would take the wire fast path (size+mtime, D11) and never
        // reach a commit (bit us once already — see the stale-index test).
        // Backdate the target so the sender scans for real.
        let t_tgt = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        filetime::set_file_mtime(&target, filetime::FileTime::from_system_time(t_tgt)).unwrap();

        // Forge a v1 index: the old header layout (68 bytes) with plausible
        // content. Whatever its claims, a v2 reader must refuse it.
        let mut v1 = Vec::new();
        v1.extend_from_slice(b"FFSYIX01");
        v1.extend_from_slice(&(src_data.len() as u64).to_le_bytes());
        v1.extend_from_slice(&1_700_000_000u64.to_le_bytes());
        v1.extend_from_slice(&0u32.to_le_bytes());
        v1.extend_from_slice(&[0u8; 32]); // root: garbage
        v1.extend_from_slice(&0u64.to_le_bytes()); // 0 chunks
        let src_index = ffsync::index::index_path_for(fx.source());
        std::fs::write(&src_index, &v1).unwrap();
        assert!(
            ffsync::index::load_index(fx.source()).is_none(),
            "v1 magic is not an index to a v2 reader"
        );

        // The sync still works (cold scan) and rewrites the index as v2.
        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        s.expect("sender ok");
        r.expect("receiver ok");
        let idx = ffsync::index::load_index(fx.source()).expect("index rewritten at commit");
        // 1 MiB of constant bytes: FastCDC cuts the mandatory minimum chunk
        // (min = 64 KiB) plus the tail — the exact count is content-defined,
        // so assert the layout identity instead: the recorded root equals a
        // fresh small-config scan of the same file.
        let (fresh_root, fresh_chunks) = {
            use ffsync::chunking::scan_file_ordered;
            let scanned = scan_file_ordered(
                fx.source(),
                &PipelineConfig::default(),
                ffsync::chunking::cdc_params_for_size(src_data.len() as u64),
            )
            .expect("fresh scan");
            (ffsync::chunking::chunk_root(&scanned.1), scanned.1)
        };
        assert_eq!(
            idx.root, fresh_root,
            "index describes the small-config layout"
        );
        assert_eq!(idx.chunks.len(), fresh_chunks.len());
        assert!(idx.cdc.0 == 16 * 1024 && idx.cdc.1 == 64 * 1024 && idx.cdc.2 == 256 * 1024);
    }

    #[test]
    fn resume_commit_writes_nonempty_index() {
        // Found while landing D19: the resume commit path re-scanned the
        // temp path AFTER renaming it away, feeding `unwrap_or_default` an
        // empty chunk list — every successful resume commit wrote an EMPTY
        // index, so the next sync never warmed. Regression test: the index
        // written by a resumed session must describe the target.
        use ffsync::protocol::sync::SyncOutcome;
        let fx = Fixture::new("resumeindex");
        let src_data: Vec<u8> = (0..2 * 1024 * 1024u32)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add((i / 8192) as u8))
            .collect();
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data[..1024 * 1024]);

        // Seed a temp file: the receiver will see FLAG_RESUMING and union
        // the temp chunks into its manifest.
        let temp_path = ffsync::reconstruction::Reconstructor::temp_path_for(&target);
        std::fs::copy(&target, &temp_path).unwrap();

        let (s, r) = run_sync(
            SendRequest {
                path: fx.source().to_path_buf(),
                display_path: "source.bin".into(),
                mtime: None,
                force_checksum: false,
                tree: false,
                dry_run: false,
                eager: false,
            },
            ReceiveRequest {
                path: target.clone(),
                local_source: target.clone(),
                force_checksum: false,
                tree: false,
                apply_mode: None,
                apply_mtime: None,
                eager: false,
            },
        );
        let s = s.expect("sender ok");
        let r = r.expect("receiver ok");
        assert!(
            matches!(s, SyncOutcome::Transferred { .. })
                || matches!(r, SyncOutcome::Transferred { .. }),
            "the resumed session must have executed a plan: {s:?} / {r:?}"
        );
        assert_eq!(read_file(&target), src_data, "resume converges");
        let idx = ffsync::index::load_index(&target).expect("index written at resume commit");
        assert!(
            !idx.chunks.is_empty(),
            "the resume commit's index must describe the target (was empty before the fix)"
        );
        assert!(
            idx.validate(&target, idx.cdc).is_some(),
            "and it validates against the target"
        );
    }

    #[test]
    fn dry_run_reports_would_change_without_writing() {
        let fx = Fixture::new("dry_run_change");
        // Different SIZES from the receiver's old file: the size+mtime fast
        // path is content-blind by design (D11), so same-size files with
        // same-tick NTFS timestamps (100 ns resolution) would legitimately
        // fast-path to InSync and make this test flaky. Size differences
        // make the would-change path deterministic.
        let src_data = vec![7u8; 2 * 1024 * 1024];
        create_file(fx.source(), &src_data);
        // Receiver's old file differs in size and content.
        let old_data = vec![9u8; 1024 * 1024 + 4096];
        let target = fx.path("target.bin");
        create_file(&target, &old_data);
        let before = std::fs::metadata(&target).unwrap().modified().unwrap();

        let s = SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "src.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
            dry_run: true,
            eager: false,
        };
        let r = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mtime: None,
            apply_mode: None,
            eager: false,
        };
        let (s_out, r_out) = run_sync(s, r);
        assert!(
            matches!(s_out, Ok(SyncOutcome::WouldChange)),
            "sender: {:?}",
            s_out
        );
        assert!(
            matches!(r_out, Ok(SyncOutcome::WouldChange)),
            "receiver: {:?}",
            r_out
        );
        // Nothing was written: content identical, no temp file, mtime intact.
        assert_eq!(std::fs::read(&target).unwrap(), old_data);
        assert!(!target.with_extension("ffsync-tmp").exists());
        assert_eq!(
            std::fs::metadata(&target).unwrap().modified().unwrap(),
            before,
            "mtime must be untouched by a dry run"
        );
        // And no index sidecar was created.
        assert!(!target.with_extension("ffsync-index").exists());
    }

    #[test]
    fn dry_run_reports_nothing_to_do_when_identical() {
        let fx = Fixture::new("dry_run_same");
        let src_data = vec![3u8; 512 * 1024];
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data.clone());

        let s = SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "src.bin".into(),
            mtime: None,
            force_checksum: true, // defeat the fast path so a plan is built
            tree: false,
            dry_run: true,
            eager: false,
        };
        let r = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: true,
            tree: false,
            apply_mtime: None,
            apply_mode: None,
            eager: false,
        };
        let (s_out, r_out) = run_sync(s, r);
        assert!(
            matches!(s_out, Ok(SyncOutcome::InSync)),
            "sender: {:?}",
            s_out
        );
        assert!(
            matches!(r_out, Ok(SyncOutcome::InSync)),
            "receiver: {:?}",
            r_out
        );
    }

    #[test]
    fn hello_with_unknown_flag_bit_is_rejected() {
        // Strict-codec doctrine: a HELLO with bits this build does not know
        // must fail at decode (TREE_BEGIN already did this; HELLO did not).
        use ffsync::protocol::message::{Hello, Message, Role};
        let mut hello = Hello {
            version: ffsync::protocol::VERSION,
            flags: 0x8000, // not a known bit
            role: Role::Sender,
            cdc_min: 16384,
            cdc_avg: 65536,
            cdc_max: 262144,
            file_size: 1,
            file_mtime: 0,
            file_mtime_nanos: 0,
            path: "x".into(),
        };
        let frame = hello.clone().into_frame().unwrap();
        let err = Hello::from_frame(frame).unwrap_err();
        assert!(
            matches!(err, ffsync::protocol::message::MessageError::Malformed(_)),
            "unknown bit must be Malformed, got {err:?}"
        );
        hello.flags = 0;
        let frame = hello.into_frame().unwrap();
        assert!(Hello::from_frame(frame).is_ok());
    }
    #[test]
    fn eager_mode_converges_and_starts_early() {
        let fx = Fixture::new("eager_converge");
        let src_data = vec![7u8; 2 * 1024 * 1024];
        create_file(fx.source(), &src_data);
        // Receiver differs in size AND content: real Remote entries.
        let old_data = vec![9u8; 1024 * 1024];
        let target = fx.path("target.bin");
        create_file(&target, &old_data);

        let s = SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "src.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
            eager: true,
            dry_run: false,
        };
        let r = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            eager: true,
            apply_mtime: None,
            apply_mode: None,
        };
        let (s_out, r_out) = run_sync(s, r);
        assert!(
            matches!(s_out, Ok(SyncOutcome::Transferred { .. })),
            "sender: {:?}",
            s_out
        );
        assert!(
            matches!(r_out, Ok(SyncOutcome::Transferred { .. })),
            "receiver: {:?}",
            r_out
        );
        assert_eq!(std::fs::read(&target).unwrap(), src_data);
    }

    #[test]
    fn eager_sender_fails_loudly_when_receiver_declines() {
        // The receiver did NOT opt in: its HELLO lacks the bit. The eager
        // sender must fail loudly (session-level Version error), never
        // deadlock or silently degrade.
        let fx = Fixture::new("eager_decline");
        let src_data = vec![7u8; 512 * 1024];
        create_file(fx.source(), &src_data);
        let target = fx.path("target.bin");
        create_file(&target, &src_data.clone());

        let s = SendRequest {
            path: fx.source().to_path_buf(),
            display_path: "src.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
            eager: true, // sender offers
            dry_run: false,
        };
        let r = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            eager: false, // receiver declines
            apply_mtime: None,
            apply_mode: None,
        };
        let (s_out, r_out) = run_sync(s, r);
        match (&s_out, &r_out) {
            (Err(SyncError::Peer(ErrorCode::Version, _)), _) => {}
            (s, r) => panic!("expected loud decline, got sender={s:?} receiver={r:?}"),
        }
    }
}
