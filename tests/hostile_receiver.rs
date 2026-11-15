//! Hostile-receiver property tests: the REAL sender (`tree_send`, and
//! through it `sync::send`; plus single-file `sync::send`) against a
//! receiver whose frames are well-framed but malicious or inconsistent —
//! the mirror image of `tests/hostile_sender.rs`.
//!
//! The receiver is the real `tree_receive` (or `sync::receive`); every frame
//! it writes passes through [`MutatingStream`], which applies ONE scripted
//! mutation per case to the nth frame of one receiver frame type: flip a
//! byte, drop, duplicate, truncate the connection, inject an arbitrary
//! frame, replace it with a well-formed ERROR (the receiver gives up), or
//! decode the frame, lie in one field (or flood copies of it),
//! and re-encode it. The sender sees only the result. Everything the sender
//! writes passes through [`TapStream`], which checks every frame it puts on
//! the wire.
//!
//! Most of what a receiver says is a claim about its own disk (sizes,
//! mtimes, chunk hashes, "committed", "deleted N"). The sender cannot verify
//! those claims, and a receiver lying about its own state only hurts
//! itself. The properties are therefore about what a hostile receiver must
//! NOT be able to make the sender do (`evaluation/verification-pilot.md`,
//! "Hostile-receiver properties"):
//!
//! 1. the sender never panics and never hangs (a watchdog cuts every
//!    socket once the session goes idle; the sender must then return);
//! 2. the sender's tree never changes — no content, size or mtime change,
//!    nothing removed, nothing created except its own D15 index sidecars —
//!    and nothing outside either root changes;
//! 3. everything the sender puts on the wire comes from its own tree: every
//!    path it names is one of its entries, every DATA payload decompresses
//!    to bytes of one of its files that hash to the announced hash, and a
//!    dry-run session sends no DATA at all (a lying receiver steers WHICH
//!    chunks move, never WHAT bytes leave);
//! 4. the sender's heap stays bounded whatever the receiver announces
//!    (floods, absurd offsets and lengths): live-heap growth per session
//!    stays under [`HEAP_BUDGET`];
//! 5. no unearned success: when the sender reports success, the receiver
//!    did too — unless the mutation forged a whole frame in the receiver's
//!    name, which is indistinguishable from a receiver that lies — and,
//!    unless the mutation touched the receiver's per-file HELLO (its
//!    in-sync claim, trusted by design: §4 fast path), every sender file
//!    reached the receiver; a dry-run session changes nothing there; and a
//!    sender that was refused (an ERROR in place of a receiver frame) never
//!    reports success;
//! 6. the honest receiver stays safe while the sender reacts to a lie told
//!    in its name: its files end as old content or the sender's, and a file
//!    disappears only in a successful delete pass or a kind-conflict
//!    removal;
//! 7. when both peers succeed, the sender's report agrees with the
//!    receiver's, except in the labels the mutated frame carries
//!    (COMMIT_ACK's outcome byte, TREE_END's delete report);
//! 8. with the mutation never applied, the session converges.
//!
//! A failing property is a finding, not a property to weaken — record it
//! in the pilot document.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::framing::{self, Frame};
use ffsync::protocol::message::{
    ErrorCode, Hello, ManifestChunk, Message, Role, TREE_FLAG_METADATA, TreeBegin, TreeEnd,
    TreeEntryWire, TreeFile, TreeWorker, encode_error, split_data_payload,
};
use ffsync::protocol::sync::{ReceiveRequest, SendRequest, SyncOutcome, receive, send};
use ffsync::protocol::tree_sync::{
    ReadWrite, TreeOutcome, TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send,
};

// ===========================================================================
// Heap accounting (property 4)
// ===========================================================================

/// Live-heap growth one session may cause (both peers run in this process,
/// so the honest receiver's share is included). An honest session of this
/// fixture stays in the low MiB; a decoder that trusted a lying count or
/// length would reach for GiB.
const HEAP_BUDGET: usize = 256 << 20;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(n: usize) {
    let now = LIVE.fetch_add(n, Ordering::Relaxed) + n;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

/// Counts live heap bytes and the peak since the last [`heap_mark`].
struct PeakAllocator;

unsafe impl GlobalAlloc for PeakAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Safety: caller upholds the GlobalAlloc contract.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // Safety: caller upholds the GlobalAlloc contract.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: caller upholds the GlobalAlloc contract.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Safety: caller upholds the GlobalAlloc contract.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size >= layout.size() {
                grew(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: PeakAllocator = PeakAllocator;

/// Resets the peak to the current live size and returns that baseline.
/// Tests in this binary run concurrently: a mark from another test can
/// only LOWER the peak this session sees (a missed spike, never a false
/// alarm), and the budget is far above two honest sessions together.
fn heap_mark() -> usize {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    base
}

// ===========================================================================
// Shared helpers (as in tests/hostile_sender.rs)
// ===========================================================================

/// Proptest config without failure persistence (see `tests/properties.rs`).
/// `PROPTEST_CASES` overrides the count for longer local runs.
fn cases(n: u32) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(n);
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// A unique scratch directory per case, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "ffsync_hostile_recv_{tag}_{}_{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        make_accessible(&self.0);
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn native(root: &Path, rel: &str) -> PathBuf {
    root.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR))
}

/// Undo modes the honest receiver applied from the sender's manifest
/// (§16.9), so the oracles can read every file and the scratch dir can be
/// removed.
fn make_accessible(dir: &Path) {
    let Ok(meta) = fs::symlink_metadata(dir) else {
        return;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if meta.is_dir() { 0o755 } else { 0o644 };
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(mode));
    }
    #[cfg(not(unix))]
    {
        let mut perm = meta.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perm.set_readonly(false);
        let _ = fs::set_permissions(dir, perm);
    }
    if meta.is_dir()
        && let Ok(rd) = fs::read_dir(dir)
    {
        for e in rd.flatten() {
            make_accessible(&e.path());
        }
    }
}

fn is_sidecar(name: &str) -> bool {
    name.ends_with(".ffsync-tmp")
        || name.ends_with(".ffsync-index")
        || name.ends_with(".ffsync-index-tmp")
}

/// One node of [`walk_all`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Dir,
    File {
        content: Vec<u8>,
        mtime: Option<SystemTime>,
    },
}

/// Every node under `root` (wire paths), sidecars and directories
/// included, with file content and mtime.
fn walk_all(root: &Path) -> BTreeMap<String, Node> {
    fn go(root: &Path, prefix: &str, out: &mut BTreeMap<String, Node>) {
        let Ok(rd) = fs::read_dir(root) else { return };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let Ok(ft) = e.file_type() else { continue };
            if ft.is_dir() {
                out.insert(rel.clone(), Node::Dir);
                go(&e.path(), &rel, out);
            } else {
                let mtime = e.metadata().ok().and_then(|m| m.modified().ok());
                let content = fs::read(e.path()).unwrap_or_default();
                out.insert(rel, Node::File { content, mtime });
            }
        }
    }
    let mut out = BTreeMap::new();
    go(root, "", &mut out);
    out
}

/// Every regular file under `root` (wire paths), sidecars excluded.
fn walk(root: &Path) -> BTreeMap<String, Vec<u8>> {
    walk_all(root)
        .into_iter()
        .filter_map(|(rel, node)| match node {
            Node::File { content, .. } if !is_sidecar(&rel) => Some((rel, content)),
            _ => None,
        })
        .collect()
}

fn xorshift_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn set_mtime(path: &Path, secs: u64, nanos: u32) {
    let t = UNIX_EPOCH + Duration::new(secs, nanos);
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(t)
        .unwrap();
}

fn write_file(root: &Path, rel: &str, content: &[u8], secs: u64, nanos: u32) {
    let p = native(root, rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(&p, content).unwrap();
    set_mtime(&p, secs, nanos);
}

fn list_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

/// Proper ancestors of a wire path (`a/b/c` → `a`, `a/b`).
fn ancestors(rel: &str) -> impl Iterator<Item = &str> {
    rel.match_indices('/').map(|(i, _)| &rel[..i])
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && needle.len() <= haystack.len()
        && haystack.windows(needle.len()).any(|w| w == needle)
}

// ===========================================================================
// Fixture (the tree fixture of tests/hostile_sender.rs)
// ===========================================================================

/// Lowercase only: case-only collisions are a separate concern (D23).
const POOL: [&str; 6] = ["a", "b", "c", "d/x", "d/y", "e/f/z"];
/// Path targets for lies: the pool, a path neither peer has, and paths no
/// honest manifest can carry (the sender must refuse them, never open them).
const LIE_PATHS: [&str; 12] = [
    "a",
    "b",
    "c",
    "d/x",
    "d/y",
    "e/f/z",
    "zz",
    "../escape",
    "/abs",
    "C:/x",
    "a/../../x",
    "",
];

#[derive(Debug, Clone)]
enum RecvVariant {
    Absent,
    /// Same content and mtime as the sender (triage skips it).
    Same,
    /// One byte flipped, different mtime (a delta conversation).
    Edited,
    /// Unrelated content (receiver-only when the sender lacks the path).
    Other(u64),
}

#[derive(Debug, Clone)]
struct Slot {
    on_sender: bool,
    /// 0 = empty, 1 = small, 2 = multi-chunk.
    len_class: u8,
    seed: u64,
    recv: RecvVariant,
}

#[derive(Debug, Clone)]
struct Fixture {
    slots: Vec<Slot>,
    /// The receiver has a FILE `d` (when it has nothing under `d/`): a
    /// kind conflict when the sender has `d/…`, a receiver-only file else.
    recv_d_file: bool,
}

fn recv_strategy() -> impl Strategy<Value = RecvVariant> {
    prop_oneof![
        Just(RecvVariant::Absent),
        Just(RecvVariant::Same),
        Just(RecvVariant::Edited),
        any::<u64>().prop_map(RecvVariant::Other),
    ]
}

fn slot_strategy() -> impl Strategy<Value = Slot> {
    (any::<bool>(), 0u8..3, any::<u64>(), recv_strategy()).prop_map(
        |(on_sender, len_class, seed, recv)| Slot {
            on_sender,
            len_class,
            seed,
            recv,
        },
    )
}

fn fixture_strategy() -> impl Strategy<Value = Fixture> {
    (prop::collection::vec(slot_strategy(), 6), any::<bool>())
        .prop_map(|(slots, recv_d_file)| Fixture { slots, recv_d_file })
}

fn content_len(len_class: u8, seed: u64) -> usize {
    match len_class {
        0 => 0,
        1 => 1 + (seed % 4000) as usize,
        _ => 40_000 + (seed % 80_000) as usize,
    }
}

/// The receiver's copy of one sender file (`None`: absent).
fn receiver_copy(
    variant: &RecvVariant,
    content: Option<&[u8]>,
    seed: u64,
    i: usize,
    sender_time: (u64, u32),
) -> Option<(Vec<u8>, u64, u32)> {
    let other = |seed: u64| xorshift_bytes((seed % 5000) as usize + 1, seed ^ 0x9e37);
    let old_time = 1_500_000_000 + i as u64;
    match (variant, content) {
        (RecvVariant::Absent, _) => None,
        (RecvVariant::Same, Some(c)) => Some((c.to_vec(), sender_time.0, sender_time.1)),
        (RecvVariant::Edited, Some(c)) => {
            let mut c = c.to_vec();
            if c.is_empty() {
                c.push(b'x');
            } else {
                let at = (seed as usize / 7) % c.len();
                c[at] ^= 0x55;
            }
            Some((c, old_time, 0))
        }
        (RecvVariant::Same | RecvVariant::Edited, None) => Some((other(seed), old_time, 0)),
        (RecvVariant::Other(seed), _) => Some((other(*seed), old_time, 0)),
    }
}

/// Writes both trees; returns (sender files, receiver files).
fn materialize(
    fx: &Fixture,
    s: &Path,
    r: &Path,
) -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>) {
    let mut sf = BTreeMap::new();
    let mut rf = BTreeMap::new();
    for (i, (rel, slot)) in POOL.iter().zip(&fx.slots).enumerate() {
        let time = (1_600_000_000 + i as u64, 123_456_700);
        let content = xorshift_bytes(content_len(slot.len_class, slot.seed), slot.seed);
        if slot.on_sender {
            write_file(s, rel, &content, time.0, time.1);
            sf.insert(rel.to_string(), content.clone());
        }
        let theirs = slot.on_sender.then_some(content.as_slice());
        if let Some((c, secs, nanos)) = receiver_copy(&slot.recv, theirs, slot.seed, i, time) {
            write_file(r, rel, &c, secs, nanos);
            rf.insert(rel.to_string(), c);
        }
    }
    if fx.recv_d_file && !rf.keys().any(|k| k.starts_with("d/")) {
        write_file(r, "d", b"receiver file d", 1_500_000_100, 0);
        rf.insert("d".into(), b"receiver file d".to_vec());
    }
    (sf, rf)
}

// ===========================================================================
// Mutations of receiver frames
// ===========================================================================

#[derive(Debug, Clone)]
enum Lie {
    // The receiver's TREE_BEGIN reply (its manifest, §16.2).
    BeginMtime {
        idx: usize,
        secs: u64,
        nanos: u32,
    },
    BeginSize {
        idx: usize,
        size: u64,
    },
    BeginKind {
        idx: usize,
        kind: u8,
    },
    BeginMode {
        idx: usize,
        mode: u32,
    },
    BeginRename {
        idx: usize,
        to: usize,
    },
    BeginDrop {
        idx: usize,
    },
    BeginChild {
        idx: usize,
    },
    BeginAdd {
        to: usize,
        kind: u8,
    },
    BeginFlags {
        flags: u16,
    },
    BeginParallel {
        parallel: u16,
    },
    // The TREE_WORKER ack (§16.8).
    WorkerIndex(u16),
    // The receiver's per-file HELLO (§4).
    HelloSize(u64),
    HelloMtime(u64, u32),
    HelloCdc(u32, u32, u32),
    HelloFlags(u16),
    HelloRole,
    HelloVersion(u16),
    HelloPath {
        to: usize,
    },
    // One MANIFEST_CHUNK (§5).
    ManifestOffset(u64),
    ManifestLength(u32),
    ManifestHash {
        pos: usize,
        xor: u8,
    },
    /// Claim a chunk the sender has (the hash of one sender file — a whole
    /// small file is one chunk) that the receiver does not.
    ManifestClaim {
        file: usize,
    },
    /// Send this manifest chunk `1 + n` times.
    ManifestFlood(u32),
    // COMMIT_ACK's payload (§8, D24).
    CommitPayload(Vec<u8>),
    // The receiver's TREE_END delete report (§16.5).
    EndReport {
        count: u32,
        bytes: u64,
    },
}

impl Lie {
    fn ftype(&self) -> u8 {
        use Lie::*;
        match self {
            BeginMtime { .. }
            | BeginSize { .. }
            | BeginKind { .. }
            | BeginMode { .. }
            | BeginRename { .. }
            | BeginDrop { .. }
            | BeginChild { .. }
            | BeginAdd { .. }
            | BeginFlags { .. }
            | BeginParallel { .. } => framing::T_TREE_BEGIN,
            WorkerIndex(_) => framing::T_TREE_WORKER,
            HelloSize(_)
            | HelloMtime(..)
            | HelloCdc(..)
            | HelloFlags(_)
            | HelloRole
            | HelloVersion(_)
            | HelloPath { .. } => framing::T_HELLO,
            ManifestOffset(_)
            | ManifestLength(_)
            | ManifestHash { .. }
            | ManifestClaim { .. }
            | ManifestFlood(_) => framing::T_MANIFEST_CHUNK,
            CommitPayload(_) => framing::T_COMMIT_ACK,
            EndReport { .. } => framing::T_TREE_END,
        }
    }
}

#[derive(Debug, Clone)]
enum Action {
    Flip {
        pos: usize,
        xor: u8,
    },
    Drop,
    Duplicate,
    /// Forward `keep` bytes of the frame (mod its length), then cut the
    /// connection.
    Truncate {
        keep: usize,
    },
    /// Send an arbitrary frame just before the target frame.
    Inject {
        ftype: u8,
        payload: Vec<u8>,
    },
    /// Replace the target frame with a well-formed ERROR: the receiver
    /// gives up at that point (its later frames still flow).
    Refuse,
    Lie(Lie),
}

#[derive(Debug, Clone)]
struct Mutation {
    ftype: u8,
    /// Which frame of that type (counted across all connections).
    nth: u32,
    action: Action,
}

/// Frames a tree receiver writes (ERROR only follows a sender fault, which
/// a single mutation of receiver frames never causes first).
const TREE_RECEIVER_TYPES: [u8; 7] = [
    framing::T_TREE_BEGIN,
    framing::T_TREE_WORKER,
    framing::T_HELLO,
    framing::T_MANIFEST_CHUNK,
    framing::T_MANIFEST_DONE,
    framing::T_COMMIT_ACK,
    framing::T_TREE_END,
];

/// Frames a single-file receiver writes (dry-run adds the §7.1 answers).
const FILE_RECEIVER_TYPES: [u8; 6] = [
    framing::T_HELLO,
    framing::T_DRY_RUN_ACK,
    framing::T_MANIFEST_CHUNK,
    framing::T_MANIFEST_DONE,
    framing::T_COMMIT_ACK,
    framing::T_NOT_MODIFIED,
];

/// Values that break arithmetic and bounds checks.
fn interesting_u64() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(1),
        Just(u64::MAX),
        Just(u64::MAX / 2),
        Just(1u64 << 62),
        0u64..200_000,
    ]
}

fn interesting_u32() -> impl Strategy<Value = u32> {
    prop_oneof![
        Just(0u32),
        Just(1),
        Just(u32::MAX),
        Just(1u32 << 31),
        0u32..300_000,
    ]
}

/// Lies in frames both session kinds carry: HELLO, manifest, COMMIT_ACK.
fn conversation_lie() -> BoxedStrategy<Lie> {
    let to = 0usize..LIE_PATHS.len();
    prop_oneof![
        interesting_u64().prop_map(Lie::HelloSize),
        (
            prop_oneof![interesting_u64(), Just(1_600_000_000u64)],
            prop_oneof![Just(0u32), Just(123_456_700), any::<u32>()]
        )
            .prop_map(|(s, n)| Lie::HelloMtime(s, n)),
        (interesting_u32(), interesting_u32(), interesting_u32())
            .prop_map(|(a, b, c)| Lie::HelloCdc(a, b, c)),
        prop_oneof![Just(0u16), Just(1), Just(2), Just(4), Just(8), any::<u16>()]
            .prop_map(Lie::HelloFlags),
        Just(Lie::HelloRole),
        prop_oneof![Just(3u16), Just(5), any::<u16>()].prop_map(Lie::HelloVersion),
        to.prop_map(|to| Lie::HelloPath { to }),
        interesting_u64().prop_map(Lie::ManifestOffset),
        interesting_u32().prop_map(Lie::ManifestLength),
        (0usize..32, 1u8..=255).prop_map(|(pos, xor)| Lie::ManifestHash { pos, xor }),
        (0usize..8).prop_map(|file| Lie::ManifestClaim { file }),
        prop_oneof![Just(1u32), Just(1_000), 1u32..50_000].prop_map(Lie::ManifestFlood),
        prop_oneof![
            Just(vec![]),
            Just(vec![0u8]),
            Just(vec![1u8]),
            Just(vec![2u8]),
            Just(vec![0xFFu8]),
            Just(vec![1u8, 1]),
        ]
        .prop_map(Lie::CommitPayload),
    ]
    .boxed()
}

/// Lies in the tree envelope: TREE_BEGIN reply, TREE_WORKER ack, TREE_END.
fn envelope_lie() -> BoxedStrategy<Lie> {
    let idx = 0usize..16;
    let to = 0usize..LIE_PATHS.len();
    prop_oneof![
        (
            idx.clone(),
            prop_oneof![interesting_u64(), 1_500_000_000u64..1_700_000_000],
            prop_oneof![
                Just(0u32),
                Just(123_456_700),
                Just(1_000_000_000),
                any::<u32>()
            ]
        )
            .prop_map(|(idx, secs, nanos)| Lie::BeginMtime { idx, secs, nanos }),
        (idx.clone(), interesting_u64()).prop_map(|(idx, size)| Lie::BeginSize { idx, size }),
        (idx.clone(), 0u8..4).prop_map(|(idx, kind)| Lie::BeginKind { idx, kind }),
        (
            idx.clone(),
            prop_oneof![Just(0u32), Just(0o444), Just(0o777), any::<u32>()]
        )
            .prop_map(|(idx, mode)| Lie::BeginMode { idx, mode }),
        (idx.clone(), to.clone()).prop_map(|(idx, to)| Lie::BeginRename { idx, to }),
        idx.clone().prop_map(|idx| Lie::BeginDrop { idx }),
        idx.prop_map(|idx| Lie::BeginChild { idx }),
        (to, 0u8..3).prop_map(|(to, kind)| Lie::BeginAdd { to, kind }),
        any::<u16>().prop_map(|flags| Lie::BeginFlags { flags }),
        prop_oneof![
            Just(0u16),
            Just(1),
            Just(2),
            Just(3),
            Just(17),
            any::<u16>()
        ]
        .prop_map(|parallel| Lie::BeginParallel { parallel }),
        prop_oneof![Just(0u16), Just(1), Just(2), Just(16), any::<u16>()]
            .prop_map(Lie::WorkerIndex),
        (any::<u32>(), interesting_u64())
            .prop_map(|(count, bytes)| Lie::EndReport { count, bytes }),
    ]
    .boxed()
}

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        (any::<usize>(), 1u8..=255).prop_map(|(pos, xor)| Action::Flip { pos, xor }),
        Just(Action::Drop),
        Just(Action::Duplicate),
        any::<usize>().prop_map(|keep| Action::Truncate { keep }),
        (0u8..=0x12, prop::collection::vec(any::<u8>(), 0..16))
            .prop_map(|(ftype, payload)| Action::Inject { ftype, payload }),
    ]
}

fn mutation_strategy(tree: bool) -> impl Strategy<Value = Option<Mutation>> {
    let types = if tree {
        TREE_RECEIVER_TYPES.to_vec()
    } else {
        FILE_RECEIVER_TYPES.to_vec()
    };
    let types_for_refuse = types.clone();
    let generic = (prop::sample::select(types), 0u32..3, action_strategy())
        .prop_map(|(ftype, nth, action)| Mutation { ftype, nth, action });
    let lies = if tree {
        prop_oneof![conversation_lie(), envelope_lie()].boxed()
    } else {
        conversation_lie()
    };
    let lie = (0u32..3, lies).prop_map(|(nth, lie)| {
        let ftype = lie.ftype();
        // One TREE_BEGIN reply per session: only the first can be a lie.
        let nth = if ftype == framing::T_TREE_BEGIN {
            0
        } else {
            nth
        };
        Mutation {
            ftype,
            nth,
            action: Action::Lie(lie),
        }
    });
    let refuse =
        (prop::sample::select(types_for_refuse), 0u32..3).prop_map(|(ftype, nth)| Mutation {
            ftype,
            nth,
            action: Action::Refuse,
        });
    prop_oneof![
        1 => Just(None),
        4 => generic.prop_map(Some),
        2 => refuse.prop_map(Some),
        7 => lie.prop_map(Some),
    ]
}

/// Applies a lie to a decoded frame: the frames to forward instead, or
/// `None` when the frame does not decode as the lie expects (it is then
/// forwarded unchanged).
fn apply_lie(lie: &Lie, frame: &Frame, claims: &[blake3::Hash]) -> Option<Vec<Frame>> {
    let path_to = |to: usize| LIE_PATHS[to % LIE_PATHS.len()].to_string();
    let one = |f: io::Result<Frame>| f.ok().map(|f| vec![f]);
    match frame.ftype {
        framing::T_TREE_BEGIN => {
            let mut b = TreeBegin::from_frame(frame.clone()).ok()?;
            let n = b.entries.len();
            let pick = |idx: usize| if n == 0 { None } else { Some(idx % n) };
            match lie {
                Lie::BeginMtime { idx, secs, nanos } => {
                    let e = &mut b.entries[pick(*idx)?];
                    e.mtime_secs = *secs;
                    e.mtime_nanos = *nanos;
                }
                Lie::BeginSize { idx, size } => b.entries[pick(*idx)?].size = *size,
                Lie::BeginKind { idx, kind } => b.entries[pick(*idx)?].kind = *kind,
                Lie::BeginMode { idx, mode } => {
                    b.flags |= TREE_FLAG_METADATA;
                    b.entries[pick(*idx)?].mode = *mode;
                }
                Lie::BeginRename { idx, to } => b.entries[pick(*idx)?].rel_path = path_to(*to),
                Lie::BeginDrop { idx } => {
                    b.entries.remove(pick(*idx)?);
                }
                Lie::BeginChild { idx } => {
                    let i = pick(*idx)?;
                    let child = TreeEntryWire {
                        rel_path: format!("{}/q", b.entries[i].rel_path),
                        kind: 0,
                        size: 1,
                        mtime_secs: 1_600_000_000,
                        mtime_nanos: 0,
                        mode: 0o644,
                    };
                    b.entries.insert(i + 1, child);
                }
                Lie::BeginAdd { to, kind } => b.entries.push(TreeEntryWire {
                    rel_path: path_to(*to),
                    kind: *kind,
                    size: 7,
                    mtime_secs: 1_600_000_000,
                    mtime_nanos: 123_456_700,
                    mode: 0o644,
                }),
                Lie::BeginFlags { flags } => b.flags = *flags,
                Lie::BeginParallel { parallel } => b.parallel = *parallel,
                _ => return None,
            }
            one(b.into_frame())
        }
        framing::T_TREE_WORKER => match lie {
            Lie::WorkerIndex(v) => one(TreeWorker { worker_index: *v }.into_frame()),
            _ => None,
        },
        framing::T_HELLO => {
            let mut h = Hello::from_frame(frame.clone()).ok()?;
            match lie {
                Lie::HelloSize(v) => h.file_size = *v,
                Lie::HelloMtime(s, n) => {
                    h.file_mtime = *s;
                    h.file_mtime_nanos = *n;
                }
                Lie::HelloCdc(a, b, c) => {
                    h.cdc_min = *a;
                    h.cdc_avg = *b;
                    h.cdc_max = *c;
                }
                Lie::HelloFlags(f) => h.flags = *f,
                Lie::HelloRole => h.role = Role::Sender,
                Lie::HelloVersion(v) => h.version = *v,
                Lie::HelloPath { to } => h.path = path_to(*to),
                _ => return None,
            }
            one(h.into_frame())
        }
        framing::T_MANIFEST_CHUNK => {
            let mut m = ManifestChunk::from_frame(frame.clone()).ok()?;
            match lie {
                Lie::ManifestOffset(v) => m.offset = *v,
                Lie::ManifestLength(v) => m.length = *v,
                Lie::ManifestHash { pos, xor } => {
                    let mut b = *m.hash.as_bytes();
                    b[pos % 32] ^= xor;
                    m.hash = blake3::Hash::from_bytes(b);
                }
                Lie::ManifestClaim { file } => {
                    if claims.is_empty() {
                        return None;
                    }
                    m.hash = claims[file % claims.len()];
                }
                Lie::ManifestFlood(n) => {
                    let f = m.into_frame().ok()?;
                    return Some(vec![f; 1 + *n as usize]);
                }
                _ => return None,
            }
            one(m.into_frame())
        }
        framing::T_COMMIT_ACK => match lie {
            Lie::CommitPayload(p) => Some(vec![Frame::new(framing::T_COMMIT_ACK, p.clone())]),
            _ => None,
        },
        framing::T_TREE_END => match lie {
            Lie::EndReport { count, bytes } => {
                TreeEnd::from_frame(frame.clone()).ok()?;
                one(TreeEnd {
                    deleted_count: *count,
                    deleted_bytes: *bytes,
                }
                .into_frame())
            }
            _ => None,
        },
        _ => None,
    }
}

// ===========================================================================
// What the sender may put on the wire (property 3)
// ===========================================================================

/// The sender's tree as it was before the session.
struct Truth {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
    dry_run: bool,
}

/// Checks one frame the sender wrote; `Some(why)` on a violation.
fn check_sender_frame(truth: &Truth, frame: &Frame) -> Option<String> {
    match frame.ftype {
        framing::T_TREE_BEGIN => match TreeBegin::from_frame(frame.clone()) {
            Err(e) => Some(format!("sender wrote an undecodable TREE_BEGIN: {e}")),
            Ok(b) => b
                .entries
                .iter()
                .find(|e| match e.kind {
                    0 => !truth.files.contains_key(&e.rel_path),
                    1 => !truth.dirs.contains(&e.rel_path),
                    _ => true,
                })
                .map(|e| {
                    format!(
                        "sender's TREE_BEGIN names {:?} (kind {}), not an entry of its tree",
                        e.rel_path, e.kind
                    )
                }),
        },
        framing::T_HELLO => match Hello::from_frame(frame.clone()) {
            Err(e) => Some(format!("sender wrote an undecodable HELLO: {e}")),
            Ok(h) if !truth.files.contains_key(&h.path) => Some(format!(
                "sender's HELLO names {:?}, not one of its files",
                h.path
            )),
            Ok(_) => None,
        },
        framing::T_TREE_FILE => match TreeFile::from_frame(frame.clone()) {
            Err(e) => Some(format!("sender wrote an undecodable TREE_FILE: {e}")),
            Ok(f) if !truth.files.contains_key(&f.rel_path) => Some(format!(
                "sender's TREE_FILE names {:?}, not one of its files",
                f.rel_path
            )),
            Ok(_) => None,
        },
        framing::T_DATA => {
            if truth.dry_run {
                return Some("sender wrote DATA in a dry-run session".into());
            }
            let (hash, compressed) = match split_data_payload(frame.payload.clone()) {
                Ok(v) => v,
                Err(e) => return Some(format!("sender wrote an undecodable DATA frame: {e}")),
            };
            let raw = match zstd::bulk::decompress(&compressed, 4 << 20) {
                Ok(v) => v,
                Err(e) => return Some(format!("sender's DATA does not decompress: {e}")),
            };
            if blake3::hash(&raw) != hash {
                return Some("sender's DATA bytes do not hash to the announced hash".into());
            }
            if !truth.files.values().any(|f| contains(f, &raw)) {
                return Some(format!(
                    "sender's DATA ({} bytes) is not a span of any of its files",
                    raw.len()
                ));
            }
            None
        }
        _ => None,
    }
}

// ===========================================================================
// The two proxies
// ===========================================================================

/// State shared by every connection of one session.
struct Shared {
    mutation: Option<Mutation>,
    /// Hashes the sender has (each sender file's), for forged claims.
    claims: Vec<blake3::Hash>,
    truth: Truth,
    counts: Mutex<HashMap<u8, u32>>,
    /// The mutation changed at least one forwarded byte.
    applied: AtomicBool,
    violations: Mutex<Vec<String>>,
    last_activity: Mutex<Instant>,
    /// Clones of every socket of the session (both ends), for the watchdog.
    sockets: Mutex<Vec<TcpStream>>,
    killed: AtomicBool,
}

impl Shared {
    fn new(mutation: Option<Mutation>, truth: Truth) -> Arc<Self> {
        let claims = truth.files.values().map(|c| blake3::hash(c)).collect();
        Arc::new(Shared {
            mutation,
            claims,
            truth,
            counts: Mutex::new(HashMap::new()),
            applied: AtomicBool::new(false),
            violations: Mutex::new(Vec::new()),
            last_activity: Mutex::new(Instant::now()),
            sockets: Mutex::new(Vec::new()),
            killed: AtomicBool::new(false),
        })
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    fn register(&self, s: &TcpStream) {
        let clone = s.try_clone().unwrap();
        // Check `killed` under the lock `kill` holds, so a socket registered
        // while the watchdog fires is cut too.
        let mut sockets = self.sockets.lock().unwrap();
        if self.killed.load(Ordering::SeqCst) {
            let _ = clone.shutdown(Shutdown::Both);
        }
        sockets.push(clone);
    }

    fn kill(&self) {
        let sockets = self.sockets.lock().unwrap();
        self.killed.store(true, Ordering::SeqCst);
        for s in sockets.iter() {
            let _ = s.shutdown(Shutdown::Both);
        }
    }
}

/// Splits complete frames off the front of `pending`.
fn take_frames(pending: &mut Vec<u8>) -> Vec<Frame> {
    let mut out = Vec::new();
    while pending.len() >= 5 {
        let len = u32::from_le_bytes(pending[1..5].try_into().unwrap()) as usize;
        if pending.len() < 5 + len {
            break;
        }
        let rest = pending.split_off(5 + len);
        let raw = std::mem::replace(pending, rest);
        out.push(Frame::new(raw[0], raw[5..].to_vec()));
    }
    out
}

fn encode(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::new();
    frame.encode(&mut out).unwrap();
    out
}

/// The receiver's side of one connection: parses every frame the real
/// receiver writes and forwards it (possibly mutated). Reads pass through.
struct MutatingStream {
    inner: TcpStream,
    pending: Vec<u8>,
    shared: Arc<Shared>,
    dead: bool,
}

impl MutatingStream {
    fn new(inner: TcpStream, shared: Arc<Shared>) -> Self {
        shared.register(&inner);
        MutatingStream {
            inner,
            pending: Vec::new(),
            shared,
            dead: false,
        }
    }

    fn send(&mut self, frame: &Frame) -> io::Result<()> {
        self.inner.write_all(&encode(frame))
    }

    fn forward(&mut self, frame: Frame) -> io::Result<()> {
        let n = {
            let mut counts = self.shared.counts.lock().unwrap();
            let c = counts.entry(frame.ftype).or_insert(0);
            *c += 1;
            *c - 1
        };
        let Some(m) = self
            .shared
            .mutation
            .clone()
            .filter(|m| m.ftype == frame.ftype && m.nth == n)
        else {
            return self.send(&frame);
        };
        let applied = || self.shared.applied.store(true, Ordering::SeqCst);
        match m.action {
            Action::Flip { pos, xor } => {
                let mut f = frame.clone();
                if f.payload.is_empty() {
                    f.payload.push(xor);
                } else {
                    let at = pos % f.payload.len();
                    f.payload[at] ^= xor;
                }
                applied();
                self.send(&f)
            }
            Action::Drop => {
                applied();
                Ok(())
            }
            Action::Duplicate => {
                applied();
                self.send(&frame)?;
                self.send(&frame)
            }
            Action::Truncate { keep } => {
                applied();
                let bytes = encode(&frame);
                let keep = keep % bytes.len();
                let _ = self.inner.write_all(&bytes[..keep]);
                let _ = self.inner.flush();
                let _ = self.inner.shutdown(Shutdown::Both);
                self.dead = true;
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "truncated by test",
                ))
            }
            Action::Inject { ftype, ref payload } => {
                applied();
                self.send(&Frame::new(ftype, payload.clone()))?;
                self.send(&frame)
            }
            Action::Refuse => {
                applied();
                self.send(&encode_error(ErrorCode::BadFrame, "refused by test").unwrap())
            }
            Action::Lie(ref lie) => match apply_lie(lie, &frame, &self.shared.claims) {
                Some(frames) if frames.len() != 1 || encode(&frames[0]) != encode(&frame) => {
                    applied();
                    let bytes: Vec<u8> = frames.iter().flat_map(encode).collect();
                    self.inner.write_all(&bytes)
                }
                _ => self.send(&frame),
            },
        }
    }
}

// The watchdog holds a clone of every socket, which keeps the connection
// open past its owner's drop: close explicitly, as a plain drop would.
impl Drop for MutatingStream {
    fn drop(&mut self) {
        let _ = self.inner.shutdown(Shutdown::Both);
    }
}

impl Write for MutatingStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.dead {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "truncated by test",
            ));
        }
        self.shared.touch();
        self.pending.extend_from_slice(buf);
        for frame in take_frames(&mut self.pending) {
            self.forward(frame)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Read for MutatingStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.shared.touch();
        Ok(n)
    }
}

/// The sender's side of one connection: forwards every byte unchanged and
/// checks every complete frame against [`Truth`].
struct TapStream {
    inner: TcpStream,
    pending: Vec<u8>,
    shared: Arc<Shared>,
}

impl TapStream {
    fn new(inner: TcpStream, shared: Arc<Shared>) -> Self {
        shared.register(&inner);
        TapStream {
            inner,
            pending: Vec::new(),
            shared,
        }
    }
}

impl Drop for TapStream {
    fn drop(&mut self) {
        let _ = self.inner.shutdown(Shutdown::Both);
    }
}

impl Write for TapStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write_all(buf)?;
        self.shared.touch();
        self.pending.extend_from_slice(buf);
        for frame in take_frames(&mut self.pending) {
            if let Some(why) = check_sender_frame(&self.shared.truth, &frame) {
                self.shared.violations.lock().unwrap().push(why);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Read for TapStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.shared.touch();
        Ok(n)
    }
}

// ===========================================================================
// One hostile session
// ===========================================================================

/// How long the session may sit without I/O before the watchdog cuts every
/// socket (a dropped frame deadlocks both peers by design: each waits for
/// the other; real deployments rely on transport timeouts).
const IDLE: Duration = Duration::from_millis(1500);
/// How long a peer may take to return once its sockets are cut.
const RETURN_BUDGET: Duration = Duration::from_secs(20);

/// A peer's result; read through `Debug` in failure reports.
#[derive(Debug)]
#[allow(dead_code)]
enum Outcome<T> {
    Ok(T),
    Err(String),
    Panic(String),
}

impl<T> Outcome<T> {
    fn is_ok(&self) -> bool {
        matches!(self, Outcome::Ok(_))
    }

    fn ok(&self) -> Option<&T> {
        match self {
            Outcome::Ok(t) => Some(t),
            _ => None,
        }
    }
}

struct Report<A, B> {
    sender: Outcome<A>,
    receiver: Outcome<B>,
    applied: bool,
    killed: bool,
    violations: Vec<String>,
    heap_growth: usize,
}

fn run_guarded<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> mpsc::Receiver<Outcome<T>> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(Ok(t)) => Outcome::Ok(t),
            Ok(Err(e)) => Outcome::Err(e),
            Err(p) => Outcome::Panic(
                p.downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default(),
            ),
        };
        let _ = tx.send(out);
    });
    rx
}

fn timeouts(tcp: &TcpStream) {
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(Duration::from_secs(30))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(30))).ok();
}

/// Accepts one connection, giving up once the watchdog fired.
fn accept_one(listener: &TcpListener, shared: &Arc<Shared>) -> io::Result<MutatingStream> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match listener.accept() {
            Ok((tcp, _)) => {
                // Windows hands out accepted sockets in the listener's
                // nonblocking mode.
                tcp.set_nonblocking(false)?;
                timeouts(&tcp);
                return Ok(MutatingStream::new(tcp, shared.clone()));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if shared.killed.load(Ordering::SeqCst) || Instant::now() > deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "no connection"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e),
        }
    }
}

type Accept = Arc<dyn Fn() -> io::Result<MutatingStream> + Send + Sync>;
type Connect = Arc<dyn Fn() -> io::Result<TapStream> + Send + Sync>;

/// Runs one session: `receiver` gets an accept function (its connections
/// pass through [`MutatingStream`]), `sender` a connect function (through
/// [`TapStream`]). A watchdog cuts every socket once the session is idle.
fn run_hostile<A, B>(
    shared: Arc<Shared>,
    receiver: impl FnOnce(Accept) -> Result<B, String> + Send + 'static,
    sender: impl FnOnce(Connect) -> Result<A, String> + Send + 'static,
) -> Result<Report<A, B>, String>
where
    A: std::fmt::Debug + Send + 'static,
    B: std::fmt::Debug + Send + 'static,
{
    let heap_base = heap_mark();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    // The receiver thread owns the listener: it closes when the receiver
    // returns, like the CLI's (see tests/hostile_sender.rs for why).
    let recv_rx = {
        let shared = shared.clone();
        run_guarded(move || {
            let listener = Arc::new(listener);
            let accept: Accept = Arc::new(move || accept_one(&listener, &shared));
            receiver(accept)
        })
    };
    let send_rx = {
        let shared = shared.clone();
        run_guarded(move || {
            let connect: Connect = Arc::new(move || {
                let tcp = TcpStream::connect(addr)?;
                timeouts(&tcp);
                Ok(TapStream::new(tcp, shared.clone()))
            });
            sender(connect)
        })
    };

    let mut sender = None;
    let mut receiver = None;
    let mut killed_at: Option<Instant> = None;
    while sender.is_none() || receiver.is_none() {
        std::thread::sleep(Duration::from_millis(10));
        if sender.is_none()
            && let Ok(o) = send_rx.try_recv()
        {
            sender = Some(o);
        }
        if receiver.is_none()
            && let Ok(o) = recv_rx.try_recv()
        {
            receiver = Some(o);
        }
        match killed_at {
            None => {
                if shared.last_activity.lock().unwrap().elapsed() > IDLE {
                    shared.kill();
                    killed_at = Some(Instant::now());
                }
            }
            Some(t) if t.elapsed() > RETURN_BUDGET => {
                return Err(format!(
                    "HANG: {} did not return {}s after its sockets were cut \
                     (sender {:?}, receiver {:?})",
                    match (&sender, &receiver) {
                        (None, None) => "neither peer",
                        (None, _) => "the sender",
                        _ => "the receiver",
                    },
                    RETURN_BUDGET.as_secs(),
                    sender,
                    receiver
                ));
            }
            Some(_) => {}
        }
    }
    let heap_growth = PEAK.load(Ordering::Relaxed).saturating_sub(heap_base);
    let violations = shared.violations.lock().unwrap().clone();
    Ok(Report {
        sender: sender.unwrap(),
        receiver: receiver.unwrap(),
        applied: shared.applied.load(Ordering::SeqCst),
        killed: killed_at.is_some(),
        violations,
        heap_growth,
    })
}

/// Session knobs shared by sender and receiver.
#[derive(Debug, Clone, Copy)]
struct Knobs {
    no_delete: bool,
    parallel: u16,
    eager: bool,
    dry_run: bool,
    checksum: bool,
}

fn run_tree_session(
    s: &Path,
    r: &Path,
    shared: Arc<Shared>,
    k: Knobs,
) -> Result<Report<TreeOutcome, TreeOutcome>, String> {
    let (s, r) = (s.to_path_buf(), r.to_path_buf());
    run_hostile(
        shared,
        move |accept| {
            let mut control = accept().map_err(|e| format!("accept: {e}"))?;
            tree_receive(
                &mut control,
                &{
                    let mut rq = TreeReceiveRequest::new(r);
                    rq.force_checksum = k.checksum;
                    rq.no_delete = k.no_delete;
                    rq.accept_worker =
                        Arc::new(move || Ok(Box::new(accept()?) as Box<dyn ReadWrite>));
                    rq.eager = k.eager;
                    rq
                },
                &PipelineConfig::default(),
                &mut |_| {},
            )
            .map_err(|e| e.to_string())
        },
        move |connect| {
            let mut control = connect().map_err(|e| format!("connect: {e}"))?;
            tree_send(
                &mut control,
                &{
                    let mut rq = TreeSendRequest::new(s);
                    rq.force_checksum = k.checksum;
                    rq.no_delete = k.no_delete;
                    rq.parallel = k.parallel;
                    rq.dry_run = k.dry_run;
                    rq.eager = k.eager;
                    rq.connect_worker = Arc::new(move |_| Ok(Box::new(connect()?)));
                    rq
                },
                &PipelineConfig::default(),
                &mut |_| {},
            )
            .map_err(|e| e.to_string())
        },
    )
}

// ===========================================================================
// Oracles shared by both properties
// ===========================================================================

/// Property 2 (sender side): every node the sender had is unchanged, and
/// the only new nodes are D15 indexes of its own files.
fn sender_tree_unchanged(
    pre: &BTreeMap<String, Node>,
    post: &BTreeMap<String, Node>,
) -> Result<(), String> {
    for (p, node) in pre {
        if post.get(p) != Some(node) {
            return Err(format!("sender node {p:?} changed or vanished"));
        }
    }
    for (p, node) in post {
        if pre.contains_key(p) {
            continue;
        }
        let own_index = matches!(node, Node::File { .. })
            && p.strip_suffix(".ffsync-index")
                .is_some_and(|f| matches!(pre.get(f), Some(Node::File { .. })));
        if !own_index {
            return Err(format!("sender tree gained {p:?}"));
        }
    }
    Ok(())
}

/// Context for failure messages.
fn describe<A: std::fmt::Debug, B: std::fmt::Debug>(
    mutation: &Option<Mutation>,
    report: &Report<A, B>,
) -> String {
    format!(
        "mutation {mutation:?} applied={} killed={} sender={:?} receiver={:?} violations={:?}",
        report.applied, report.killed, report.sender, report.receiver, report.violations
    )
}

/// Which parts of the mutation actually reached the sender.
struct Hit<'a>(Option<&'a Mutation>);

impl Hit<'_> {
    fn new<'m, A, B>(mutation: &'m Option<Mutation>, report: &Report<A, B>) -> Hit<'m> {
        Hit(mutation.as_ref().filter(|_| report.applied))
    }

    fn frame(&self, ftype: u8) -> bool {
        self.0.is_some_and(|m| m.ftype == ftype)
    }

    /// A whole frame was made up in the receiver's name.
    fn forged(&self) -> bool {
        self.0
            .is_some_and(|m| matches!(m.action, Action::Inject { .. }))
    }

    /// The receiver gave up: a well-formed ERROR replaced one of its frames.
    fn refused(&self) -> bool {
        self.0.is_some_and(|m| matches!(m.action, Action::Refuse))
    }

    /// The receiver's connection was cut under its own write.
    fn cut(&self) -> bool {
        self.0
            .is_some_and(|m| matches!(m.action, Action::Truncate { .. }))
    }

    fn any(&self) -> bool {
        self.0.is_some()
    }
}

// ===========================================================================
// The properties
// ===========================================================================

proptest! {
    #![proptest_config(cases(64))]

    #[test]
    fn hostile_receiver_cannot_subvert_tree_sender(
        fx in fixture_strategy(),
        mutation in mutation_strategy(true),
        no_delete in any::<bool>(),
        parallel in 1u16..=3,
        eager in any::<bool>(),
        dry_run in prop::bool::weighted(0.2),
        checksum in prop::bool::weighted(0.2),
    ) {
        let scratch = Scratch::new("tree");
        let (s, r, outside) = (
            scratch.path().join("s"),
            scratch.path().join("r"),
            scratch.path().join("outside"),
        );
        for d in [&s, &r, &outside] {
            fs::create_dir_all(d).unwrap();
        }
        fs::write(outside.join("canary"), b"canary").unwrap();
        let (sender_files, pre_r) = materialize(&fx, &s, &r);
        let pre_s_all = walk_all(&s);
        let pre_r_all = walk_all(&r);
        let dirs = pre_s_all
            .iter()
            .filter(|(_, n)| **n == Node::Dir)
            .map(|(p, _)| p.clone())
            .collect();
        let truth = Truth { files: sender_files.clone(), dirs, dry_run };
        let shared = Shared::new(mutation.clone(), truth);
        let knobs = Knobs { no_delete, parallel, eager, dry_run, checksum };

        let report = run_tree_session(&s, &r, shared, knobs).map_err(TestCaseError::fail)?;
        make_accessible(scratch.path());
        let post_r = walk(&r);
        let ctx = describe(&mutation, &report);
        let hit = Hit::new(&mutation, &report);

        // (1) No panics (hangs fail inside run_tree_session).
        prop_assert!(!matches!(report.sender, Outcome::Panic(_)), "sender panicked: {}", ctx);
        prop_assert!(
            !matches!(report.receiver, Outcome::Panic(_)),
            "receiver panicked: {}", ctx
        );

        // (2) Containment: the sender's tree and everything outside the
        // roots are untouched.
        sender_tree_unchanged(&pre_s_all, &walk_all(&s))
            .map_err(|e| TestCaseError::fail(format!("{e}: {ctx}")))?;
        prop_assert_eq!(list_names(scratch.path()), vec!["outside", "r", "s"], "{}", ctx);
        prop_assert_eq!(list_names(&outside), vec!["canary"], "{}", ctx);
        prop_assert_eq!(fs::read(outside.join("canary")).unwrap(), b"canary".to_vec(), "{}", ctx);

        // (3) Everything on the wire came from the sender's tree.
        prop_assert!(report.violations.is_empty(), "sender wire violation: {}", ctx);

        // (4) Bounded heap.
        prop_assert!(
            report.heap_growth < HEAP_BUDGET,
            "session grew the heap by {} MiB: {}", report.heap_growth >> 20, ctx
        );

        // (5) No unearned success.
        if dry_run {
            prop_assert!(walk_all(&r) == pre_r_all, "dry-run changed the receiver: {}", ctx);
        }
        // A receiver's ERROR is final: a sender that was refused never
        // reports success.
        prop_assert!(
            !(hit.refused() && report.sender.is_ok()),
            "sender reported success past the receiver's ERROR: {}", ctx
        );
        if report.sender.is_ok() && !hit.forged() {
            prop_assert!(
                report.receiver.is_ok(),
                "sender reported success but the receiver failed: {}", ctx
            );
            if !dry_run && !hit.frame(framing::T_HELLO) {
                for (p, c) in &sender_files {
                    prop_assert!(
                        post_r.get(p) == Some(c),
                        "sender reported success but {:?} did not reach the receiver: {}",
                        p, ctx
                    );
                }
            }
        }

        // (6) The honest receiver stays safe.
        for (p, c) in &post_r {
            let ok = pre_r.get(p) == Some(c) || sender_files.get(p) == Some(c);
            prop_assert!(
                ok,
                "receiver file {:?} ({} bytes) is neither its old content nor the sender's: {}",
                p, c.len(), ctx
            );
        }
        let sender_dir = |p: &str| sender_files.keys().any(|q| q.starts_with(&format!("{p}/")));
        for p in pre_r.keys().filter(|p| !post_r.contains_key(*p)) {
            // The receiver deletes before it writes its TREE_END reply
            // (§16.5): a mutation that reached that reply came after an
            // earned delete pass, even if it then failed the receiver.
            let delete_pass = (report.receiver.is_ok() || hit.frame(framing::T_TREE_END))
                && !no_delete
                && !dry_run
                && !sender_files.contains_key(p)
                && !sender_dir(p);
            let conflict = sender_dir(p) || ancestors(p).any(|a| sender_files.contains_key(a));
            prop_assert!(
                delete_pass || conflict,
                "receiver file {:?} vanished outside a delete pass or conflict removal: {}",
                p, ctx
            );
        }

        // (7) Reports agree, except in the labels the mutated frame carries.
        if let (Some(so), Some(ro)) = (report.sender.ok(), report.receiver.ok())
            && !hit.forged()
        {
            let labels_lied = hit.frame(framing::T_COMMIT_ACK);
            let report_lied = hit.frame(framing::T_TREE_END);
            let triage_lied = dry_run && hit.frame(framing::T_TREE_BEGIN);
            if !labels_lied && !triage_lied {
                prop_assert_eq!(so.in_sync, ro.in_sync, "in_sync: {}", ctx);
                prop_assert_eq!(so.files, ro.files, "files: {}", ctx);
            }
            if !report_lied {
                prop_assert_eq!(so.deleted, ro.deleted, "deleted: {}", ctx);
                prop_assert_eq!(so.deleted_bytes, ro.deleted_bytes, "deleted_bytes: {}", ctx);
            }
            let total: u64 = sender_files.values().map(|c| c.len() as u64).sum();
            prop_assert!(so.transferred_bytes <= total, "transferred: {}", ctx);
        }

        // (8) Control: an unapplied mutation is an honest session.
        if !hit.any() {
            prop_assert!(
                report.sender.is_ok() && report.receiver.is_ok() && !report.killed,
                "honest session failed: {}", ctx
            );
            if !dry_run {
                for (p, c) in &sender_files {
                    prop_assert_eq!(post_r.get(p), Some(c), "honest session: {} differs", p);
                }
                for p in pre_r.keys().filter(|p| !sender_files.contains_key(*p)) {
                    // A receiver file where the sender has a dir is a kind
                    // conflict: removed even under --no-delete.
                    prop_assert_eq!(
                        post_r.contains_key(p), no_delete && !sender_dir(p),
                        "honest session: receiver-only {} (no_delete={})", p, no_delete
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Single-file sessions: the §4 drain, §7.1 dry-run answers and D10 resume
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FileFixture {
    len_class: u8,
    seed: u64,
    recv: RecvVariant,
    /// The receiver holds a partial temp from a crashed attempt (D10): its
    /// HELLO says RESUMING and its manifest unions two files.
    resume_temp: bool,
}

fn file_fixture_strategy() -> impl Strategy<Value = FileFixture> {
    (0u8..3, any::<u64>(), recv_strategy(), any::<bool>()).prop_map(
        |(len_class, seed, recv, resume_temp)| FileFixture {
            len_class,
            seed,
            recv,
            resume_temp,
        },
    )
}

proptest! {
    #![proptest_config(cases(64))]

    #[test]
    fn hostile_receiver_cannot_subvert_single_file_sender(
        fx in file_fixture_strategy(),
        mutation in mutation_strategy(false),
        eager in any::<bool>(),
        dry_run in prop::bool::weighted(0.3),
        checksum in prop::bool::weighted(0.2),
    ) {
        let scratch = Scratch::new("file");
        let (s, r, outside) = (
            scratch.path().join("s"),
            scratch.path().join("r"),
            scratch.path().join("outside"),
        );
        for d in [&s, &r, &outside] {
            fs::create_dir_all(d).unwrap();
        }
        fs::write(outside.join("canary"), b"canary").unwrap();
        let time = (1_600_000_000, 123_456_700);
        let content = xorshift_bytes(content_len(fx.len_class, fx.seed), fx.seed);
        write_file(&s, "f", &content, time.0, time.1);
        let old = receiver_copy(&fx.recv, Some(&content), fx.seed, 0, time);
        if let Some((c, secs, nanos)) = &old {
            write_file(&r, "f", c, *secs, *nanos);
        }
        if fx.resume_temp {
            fs::write(r.join("f.ffsync-tmp"), &content[..content.len() / 2]).unwrap();
        }
        let old = old.map(|(c, ..)| c);
        let pre_s_all = walk_all(&s);
        let pre_r_all = walk_all(&r);
        let truth = Truth {
            files: BTreeMap::from([("f".to_string(), content.clone())]),
            dirs: BTreeSet::new(),
            dry_run,
        };
        let shared = Shared::new(mutation.clone(), truth);

        let (sp, rp) = (s.join("f"), r.join("f"));
        let report = run_hostile(
            shared,
            move |accept| {
                let mut conn = accept().map_err(|e| format!("accept: {e}"))?;
                let mut request = ReceiveRequest::new(rp.clone(), rp);
                request.force_checksum = checksum;
                request.eager = eager;
                receive(
                    &mut conn,
                    &request,
                    &PipelineConfig::default(),
                    &mut |_| {},
                )
                .map_err(|e| e.to_string())
            },
            move |connect| {
                let mut conn = connect().map_err(|e| format!("connect: {e}"))?;
                let mut request = SendRequest::new(sp, "f".into());
                request.force_checksum = checksum;
                request.eager = eager;
                request.dry_run = dry_run;
                send(
                    &mut conn,
                    &request,
                    &PipelineConfig::default(),
                    &mut |_| {},
                )
                .map_err(|e| e.to_string())
            },
        )
        .map_err(TestCaseError::fail)?;
        let post_f = fs::read(r.join("f")).ok();
        let ctx = describe(&mutation, &report);
        let hit = Hit::new(&mutation, &report);

        // (1)
        prop_assert!(!matches!(report.sender, Outcome::Panic(_)), "sender panicked: {}", ctx);
        prop_assert!(
            !matches!(report.receiver, Outcome::Panic(_)),
            "receiver panicked: {}", ctx
        );
        // (2)
        sender_tree_unchanged(&pre_s_all, &walk_all(&s))
            .map_err(|e| TestCaseError::fail(format!("{e}: {ctx}")))?;
        prop_assert_eq!(list_names(scratch.path()), vec!["outside", "r", "s"], "{}", ctx);
        prop_assert_eq!(list_names(&outside), vec!["canary"], "{}", ctx);
        // (3)
        prop_assert!(report.violations.is_empty(), "sender wire violation: {}", ctx);
        // (4)
        prop_assert!(
            report.heap_growth < HEAP_BUDGET,
            "session grew the heap by {} MiB: {}", report.heap_growth >> 20, ctx
        );
        // (5)
        if dry_run {
            prop_assert!(walk_all(&r) == pre_r_all, "dry-run changed the receiver: {}", ctx);
        }
        // A fast-path sender (§4) is done once it sent NOT_MODIFIED and
        // drains whatever the receiver still writes, unread: cutting the
        // receiver's own connection under that drain fails only the
        // receiver, an ERROR written into it is never seen, and the file is
        // in sync either way. Any other receiver failure (or ERROR, HELLO
        // included) behind a sender success is a finding.
        let done_before = matches!(report.sender, Outcome::Ok(SyncOutcome::InSync))
            && post_f.as_ref() == Some(&content);
        let cut_after_done = hit.cut() && done_before;
        let refused_after_done = done_before && !hit.frame(framing::T_HELLO);
        prop_assert!(
            !(hit.refused() && report.sender.is_ok() && !refused_after_done),
            "sender reported success past the receiver's ERROR: {}", ctx
        );
        if report.sender.is_ok() && !hit.forged() {
            prop_assert!(
                report.receiver.is_ok() || cut_after_done,
                "sender reported success but the receiver failed: {}", ctx
            );
            if !dry_run && !hit.frame(framing::T_HELLO) {
                prop_assert!(
                    post_f.as_ref() == Some(&content),
                    "sender reported success but the receiver's file differs: {}", ctx
                );
            }
        }
        // (6)
        prop_assert!(
            post_f == old || post_f.as_ref() == Some(&content),
            "receiver file is neither its old content nor the sender's: {}", ctx
        );
        // (8)
        if !hit.any() {
            prop_assert!(
                report.sender.is_ok() && report.receiver.is_ok() && !report.killed,
                "honest session failed: {}", ctx
            );
            if !dry_run {
                prop_assert!(post_f.as_ref() == Some(&content), "honest session: {}", ctx);
            }
        }
    }
}

#[test]
fn proxies_forward_honest_frames_unchanged() {
    // The proxies themselves: with no mutation, a session is an ordinary
    // one and the tap finds nothing to object to.
    let scratch = Scratch::new("honest");
    let (s, r) = (scratch.path().join("s"), scratch.path().join("r"));
    fs::create_dir_all(&s).unwrap();
    fs::create_dir_all(&r).unwrap();
    write_file(&s, "a", &xorshift_bytes(70_000, 7), 1_600_000_000, 0);
    write_file(&s, "d/x", b"x", 1_600_000_001, 0);
    write_file(&r, "gone", b"receiver only", 1_500_000_000, 0);
    let truth = Truth {
        files: walk(&s),
        dirs: BTreeSet::from(["d".to_string()]),
        dry_run: false,
    };
    let knobs = Knobs {
        no_delete: false,
        parallel: 2,
        eager: false,
        dry_run: false,
        checksum: false,
    };
    let report = run_tree_session(&s, &r, Shared::new(None, truth), knobs).unwrap();
    let (so, ro) = (report.sender.ok().unwrap(), report.receiver.ok().unwrap());
    assert!(!report.applied && !report.killed);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert_eq!(walk(&r), walk(&s));
    assert_eq!(so, ro);
    assert_eq!((so.files, so.deleted), (2, 1));
}
