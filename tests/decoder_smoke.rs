use ffsync::protocol::framing::{
    Frame, T_ERROR, T_HELLO, T_MANIFEST_CHUNK, T_PLAN_LOCAL, T_PLAN_REMOTE, T_TREE_BEGIN,
    T_TREE_END, T_TREE_FILE, T_TREE_WORKER,
};
use ffsync::protocol::message::*;

#[test]
fn decoders_survive_adversarial_payloads() {
    // Truncations of valid payloads at every length (the classic decoder bug class).
    let h = Hello {
        version: ffsync::protocol::VERSION,
        flags: FLAG_TRUST_TIMES,
        role: Role::Sender,
        cdc_min: 16384,
        cdc_avg: 65536,
        cdc_max: 262144,
        file_size: 1024,
        file_mtime: 1_700_000_000,
        file_mtime_nanos: 5,
        path: "a.bin".into(),
    };
    let hbytes = h.into_frame().unwrap().payload;
    for n in 0..hbytes.len() {
        let f = Frame::new(T_HELLO, hbytes[..n].to_vec());
        let _ = Hello::from_frame(f); // must return Err, never panic
    }
    // TreeBegin with a large claimed entry count (allocation-bomb check).
    let mut bomb = vec![0u8; 10];
    bomb[0..2].copy_from_slice(&0xFFFFu16.to_le_bytes()); // flags: many bits
    bomb[2..4].copy_from_slice(&4u16.to_le_bytes()); // parallel
    bomb[4..6].copy_from_slice(&1u16.to_le_bytes()); // exclude count
    bomb[6..8].copy_from_slice(&4u16.to_le_bytes()); // pattern len 4 (no bytes follow: truncation)
    bomb[8..10].copy_from_slice(&0u16.to_le_bytes());
    let f = Frame::new(T_TREE_BEGIN, bomb);
    let _ = TreeBegin::from_frame(f);
    // Entry-count bomb: header says 2^40 entries but payload is empty.
    let mut bomb2 = vec![0u8; 8];
    bomb2[0..2].copy_from_slice(&0u16.to_le_bytes()); // no flags
    bomb2[2..6].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // entry count high bits... entry count is u64 at the end
    let f2 = Frame::new(T_TREE_BEGIN, bomb2);
    let _ = TreeBegin::from_frame(f2);
    // Plan entries truncated
    let pl = PlanEntry::Local {
        target_offset: 0,
        length: 65536,
        source_offset: 8192,
        hash: blake3::Hash::from_bytes([1u8; 32]),
    };
    let plb = pl.into_frame().unwrap().payload;
    for n in 0..plb.len() {
        let _ = PlanEntry::from_frame(Frame::new(T_PLAN_LOCAL, plb[..n].to_vec()));
    }
    // Random garbage payloads for every type.
    let garbage: Vec<Vec<u8>> = (0..64).map(|i| vec![i as u8; i * 7]).collect();
    for g in &garbage {
        for t in [
            T_HELLO,
            T_MANIFEST_CHUNK,
            T_PLAN_LOCAL,
            T_PLAN_REMOTE,
            T_TREE_BEGIN,
            T_TREE_END,
            T_TREE_FILE,
            T_TREE_WORKER,
            T_ERROR,
        ] {
            let f = Frame::new(t, g.clone());
            let _ = Hello::from_frame(f.clone());
            let _ = ManifestChunk::from_frame(f.clone());
            let _ = PlanEntry::from_frame(f.clone());
            let _ = TreeBegin::from_frame(f.clone());
            let _ = TreeEnd::from_frame(f.clone());
            let _ = TreeFile::from_frame(f.clone());
            let _ = TreeWorker::from_frame(f.clone());
            let _ = read_error_payload(&f);
        }
        let _ = decode_commit_ack(g); // D24 outcome byte
    }
}

// ---- Frame-header allocation bomb (§12) ----
//
// A header claiming the 64 MiB maximum must not make the reader allocate
// 64 MiB before a single payload byte arrived: that is a pre-auth
// (`--plaintext`) or per-worker (`--parallel 16`) memory lever costing the
// attacker five bytes. A thread-local switch keeps the other test in this
// binary out of the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

struct PeakTracking;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
}
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards every call to `System` unchanged; only counts sizes.
unsafe impl GlobalAlloc for PeakTracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && TRACK.with(|t| t.get()) {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACK.with(|t| t.get()) {
            // Saturating: a tracked dealloc may free an untracked block.
            // (Hand-rolled CAS: `fetch_update` is deprecated on current
            // stable and its replacement is newer than the MSRV.)
            let mut cur = LIVE.load(Ordering::Relaxed);
            while let Err(seen) = LIVE.compare_exchange_weak(
                cur,
                cur.saturating_sub(layout.size()),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                cur = seen;
            }
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: PeakTracking = PeakTracking;

#[test]
fn frame_header_does_not_preallocate_claimed_length() {
    use ffsync::protocol::framing::{FrameError, MAX_PAYLOAD, T_DATA, read_frame};
    let mut wire = vec![T_DATA];
    wire.extend_from_slice(&MAX_PAYLOAD.to_le_bytes());
    wire.extend_from_slice(&[0u8; 16]); // then the peer stops sending
    let mut cursor = std::io::Cursor::new(wire);
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    TRACK.with(|t| t.set(true));
    let r = read_frame(&mut cursor);
    TRACK.with(|t| t.set(false));
    assert!(matches!(r, Err(FrameError::Truncated)), "{r:?}");
    let peak = PEAK.load(Ordering::Relaxed);
    assert!(
        peak < 4 * 1024 * 1024,
        "reading 16 payload bytes peaked at {peak} bytes of heap"
    );
}
