//! In-process soak test: 100 sequential sync sessions in ONE process.
//!
//! Ignored by default (run with `cargo test --test soak -- --ignored`):
//! each session runs the full protocol (scan, manifest, plan, transfer,
//! verify, commit) over real TCP loopback between two threads of this
//! process. A counting global allocator tracks net live heap bytes, so the
//! assertion is about *accumulation across sessions* — the leak class that
//! matters for library embedders (one-shot CLI processes exit and reclaim
//! everything anyway). Complements `evaluation/memory.md`, which measures
//! per-process peaks.

use std::fs::File;
use std::io::Write as _;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::sync::{Progress, ReceiveRequest, SendRequest, receive, send};

/// Net live heap bytes (alloc - dealloc), maintained by `CountingAllocator`.
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        // Safety: caller upholds the GlobalAlloc contract.
        unsafe {
            let ptr = std::alloc::System.alloc(layout);
            if !ptr.is_null() {
                LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            }
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // Safety: caller upholds the GlobalAlloc contract.
        unsafe {
            std::alloc::System.dealloc(ptr, layout);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        // Safety: caller upholds the GlobalAlloc contract.
        unsafe {
            let new_ptr = std::alloc::System.realloc(ptr, layout, new_size);
            if !new_ptr.is_null() {
                if new_ptr == ptr {
                    // In-place growth/shrink: adjust by the delta only.
                    if new_size > layout.size() {
                        LIVE_BYTES.fetch_add(new_size - layout.size(), Ordering::Relaxed);
                    } else {
                        LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
                    }
                } else {
                    LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
                    LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
                }
            }
            new_ptr
        }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn create_file(path: &Path, data: &[u8]) {
    let mut f = File::create(path).unwrap();
    f.write_all(data).unwrap();
}

fn read_file(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

/// One full sync session over real TCP loopback, both peers in this process.
/// The listener binds an OS-assigned (ephemeral) port so concurrent tests
/// never fight over a fixed range.
fn run_session(send_req: SendRequest, recv_req: ReceiveRequest) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();

    let recv_handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut progress = |_p: Progress| {};
        receive(
            &mut stream,
            &recv_req,
            &PipelineConfig::default(),
            &mut progress,
        )
    });
    let send_handle = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut progress = |_p: Progress| {};
        send(
            &mut stream,
            &send_req,
            &PipelineConfig::default(),
            &mut progress,
        )
    });

    let s = send_handle.join().unwrap();
    let r = recv_handle.join().unwrap();
    s.expect("sender ok");
    r.expect("receiver ok");
}

#[test]
#[ignore = "soak: run explicitly with `cargo test --test soak -- --ignored`"]
fn hundred_sessions_do_not_accumulate_memory() {
    let dir = std::env::temp_dir().join("ffsync_soak");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 2 MiB patterned source; the receiver starts with a diverged copy.
    let mut src_data = vec![0u8; 2 * 1024 * 1024];
    for (i, b) in src_data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(17).wrapping_add((i / 65536) as u8);
    }
    let source = dir.join("source.bin");
    create_file(&source, &src_data);
    let target = dir.join("target.bin");
    create_file(&target, &src_data[..1024 * 1024]);

    // Live bytes BEFORE any session work (fixtures exist, but are on disk).
    let start_live = LIVE_BYTES.load(Ordering::Relaxed);

    let sessions = 100;
    for i in 0..sessions {
        // Change the source every session: flip a few bytes at a roaming
        // offset AND vary the length (appended tail), so neither peer can
        // take the mtime/size fast path — every session does real scan,
        // plan, transfer, and reconstruction work with fresh allocations.
        let tail_len = i % 257;
        let mut data = src_data.clone();
        data.extend(std::iter::repeat_n(0xA5u8, tail_len));
        let flip_at = (i * 977) % (data.len() - 4096);
        for b in &mut data[flip_at..flip_at + 4096] {
            *b ^= 0x5A;
        }
        create_file(&source, &data);

        let send_req = SendRequest {
            path: source.clone(),
            display_path: "source.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
        };
        let recv_req = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
        };
        run_session(send_req, recv_req);

        assert_eq!(
            read_file(&target),
            data,
            "session {i}: target did not converge"
        );
    }

    let end_live = LIVE_BYTES.load(Ordering::Relaxed);
    let grown = end_live as i64 - start_live as i64;
    println!(
        "live heap: start {start_live} B, end {end_live} B, delta {grown} B over {sessions} sessions"
    );

    // Tolerance: 1 MiB over 100 sessions (10 KiB/session) would already be a
    // leak worth hunting; a healthy run should land within a few KiB.
    assert!(
        grown <= 1024 * 1024,
        "net live heap grew {grown} B across {sessions} sessions — accumulating state"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The same loop, but asserts the *outcomes* stay correct when sessions
/// alternate between delta syncs and full transfers — protocol-state
/// coverage for the allocator test above, and a check that no session
/// inherits state from the previous one (e.g. stale resume temps).
#[test]
#[ignore = "soak: run explicitly with `cargo test --test soak -- --ignored`"]
fn hundred_sessions_alternating_deltas_and_full_transfers() {
    let dir = std::env::temp_dir().join("ffsync_soak_alt");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut src_data = vec![0u8; 512 * 1024];
    for (i, b) in src_data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add((i / 4096) as u8);
    }
    let source = dir.join("source.bin");
    let target = dir.join("target.bin");

    let sessions = 100;
    for i in 0..sessions {
        let full = i % 2 == 0;
        let mut data = if full {
            // Full transfer: an entirely different payload each even session.
            let mut fresh = vec![0u8; 512 * 1024];
            for (j, b) in fresh.iter_mut().enumerate() {
                *b = (j as u8)
                    .wrapping_mul(7)
                    .wrapping_add((j / 8192) as u8)
                    .wrapping_add(i as u8);
            }
            fresh
        } else {
            // Delta: same base, one flipped region.
            let mut d = src_data.clone();
            let at = (i * 4099) % (d.len() - 8192);
            for b in &mut d[at..at + 8192] {
                *b ^= 0xFF;
            }
            d
        };
        // Vary length so the fast path never engages.
        data.extend(std::iter::repeat_n(i as u8, i % 33));
        create_file(&source, &data);
        if i == 0 {
            create_file(&target, &src_data[..256 * 1024]);
        }

        let send_req = SendRequest {
            path: source.clone(),
            display_path: "source.bin".into(),
            mtime: None,
            force_checksum: false,
            tree: false,
        };
        let recv_req = ReceiveRequest {
            path: target.clone(),
            local_source: target.clone(),
            force_checksum: false,
            tree: false,
            apply_mode: None,
            apply_mtime: None,
        };
        run_session(send_req, recv_req);
        assert_eq!(read_file(&target), data, "session {i}: no convergence");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
