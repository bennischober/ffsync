//! Hostile-sender property tests: the REAL tree receiver (`tree_receive`,
//! and through it `sync::receive`) against a sender whose frames are
//! well-framed but malicious or inconsistent.
//!
//! The sender is the real `tree_send`; every frame it writes passes through
//! [`MutatingStream`], which applies ONE scripted mutation per case to the
//! nth frame of one sender frame type: flip a byte, drop, duplicate,
//! truncate the connection, inject an arbitrary frame, or decode the frame,
//! lie in one field, and re-encode it. The receiver sees only the result.
//!
//! What must hold for every case (`evaluation/verification-pilot.md`,
//! "Hostile-sender properties"):
//!
//! 1. the receiver never panics and never hangs (a watchdog cuts every
//!    socket once the session goes idle; the receiver must then return);
//! 2. nothing outside the receiver's root changes;
//! 3. every receiver file ends as its old content or the sender's real
//!    content at that same path;
//! 4. a file disappears only through the delete pass of a session the
//!    receiver reported as successful, or through the documented
//!    kind-conflict removal the (possibly lying) TREE_BEGIN asked for;
//! 5. a receiver that reports success has converged on every file the
//!    TREE_BEGIN it accepted names (lies must fail loudly, not silently);
//! 6. with the mutation never applied, the session converges.
//!
//! A failing property is a finding, not a property to weaken — record it
//! in the pilot document.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, UNIX_EPOCH};

use proptest::prelude::*;

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::framing::{self, Frame};
use ffsync::protocol::message::{
    Hello, Message, PlanEntry, Role, TREE_FLAG_METADATA, TreeBegin, TreeEntryWire, TreeFile,
    TreeWorker, encode_data, split_data_payload,
};
use ffsync::protocol::tree_sync::{TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send};
use ffsync::tree::{TREE_FLAG_DRY_RUN, TREE_FLAG_NO_DELETE};

// ===========================================================================
// Shared helpers
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
        let dir =
            std::env::temp_dir().join(format!("ffsync_hostile_{tag}_{}_{n}", std::process::id()));
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

/// Undo whatever mode a lying sender applied (§16.9), so the oracles can
/// read every file and the scratch dir can be removed.
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

/// Every regular file under `root` (wire paths), sidecars excluded.
fn walk(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn go(root: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<u8>>) {
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
                go(&e.path(), &rel, out);
            } else if !is_sidecar(&name) {
                out.insert(rel, fs::read(e.path()).unwrap_or_default());
            }
        }
    }
    let mut out = BTreeMap::new();
    go(root, "", &mut out);
    out
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

// ===========================================================================
// Fixture: a small sender tree and a receiver tree related to it
// ===========================================================================

/// Lowercase only: case-only collisions are a separate concern (D23).
const POOL: [&str; 6] = ["a", "b", "c", "d/x", "d/y", "e/f/z"];
/// Rename targets for lies: the pool plus a path neither peer has.
const RENAME_TARGETS: [&str; 7] = ["a", "b", "c", "d/x", "d/y", "e/f/z", "zz"];

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

fn slot_strategy() -> impl Strategy<Value = Slot> {
    let recv = prop_oneof![
        Just(RecvVariant::Absent),
        Just(RecvVariant::Same),
        Just(RecvVariant::Edited),
        any::<u64>().prop_map(RecvVariant::Other),
    ];
    (any::<bool>(), 0u8..3, any::<u64>(), recv).prop_map(|(on_sender, len_class, seed, recv)| {
        Slot {
            on_sender,
            len_class,
            seed,
            recv,
        }
    })
}

fn fixture_strategy() -> impl Strategy<Value = Fixture> {
    (prop::collection::vec(slot_strategy(), 6), any::<bool>())
        .prop_map(|(slots, recv_d_file)| Fixture { slots, recv_d_file })
}

fn sender_len(slot: &Slot) -> usize {
    match slot.len_class {
        0 => 0,
        1 => 1 + (slot.seed % 4000) as usize,
        _ => 40_000 + (slot.seed % 80_000) as usize,
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
        let (ss, sn) = (1_600_000_000 + i as u64, 123_456_700);
        let content = xorshift_bytes(sender_len(slot), slot.seed);
        if slot.on_sender {
            write_file(s, rel, &content, ss, sn);
            sf.insert(rel.to_string(), content.clone());
        }
        let other = |seed: u64| xorshift_bytes((seed % 5000) as usize + 1, seed ^ 0x9e37);
        let recv: Option<(Vec<u8>, u64, u32)> = match (&slot.recv, slot.on_sender) {
            (RecvVariant::Absent, _) => None,
            (RecvVariant::Same, true) => Some((content.clone(), ss, sn)),
            (RecvVariant::Edited, true) => {
                let mut c = content.clone();
                if c.is_empty() {
                    c.push(b'x');
                } else {
                    let at = (slot.seed as usize / 7) % c.len();
                    c[at] ^= 0x55;
                }
                Some((c, 1_500_000_000 + i as u64, 0))
            }
            (RecvVariant::Same | RecvVariant::Edited, false) => {
                Some((other(slot.seed), 1_500_000_000 + i as u64, 0))
            }
            (RecvVariant::Other(seed), _) => Some((other(*seed), 1_500_000_000 + i as u64, 0)),
        };
        if let Some((c, secs, nanos)) = recv {
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
// Mutations
// ===========================================================================

#[derive(Debug, Clone)]
enum Lie {
    BeginMtime { idx: usize, secs: u64, nanos: u32 },
    BeginSize { idx: usize, size: u64 },
    BeginKind { idx: usize, kind: u8 },
    BeginMode { idx: usize, mode: u32 },
    BeginRename { idx: usize, to: usize },
    BeginDrop { idx: usize },
    BeginChild { idx: usize },
    BeginFlags { flags: u16 },
    BeginParallel { parallel: u16 },
    HelloSize(u64),
    HelloMtime(u64, u32),
    HelloCdc(u32, u32, u32),
    HelloFlags(u16),
    HelloRole,
    HelloVersion(u16),
    HelloPath { to: usize },
    PlanTarget { local: bool, v: u64 },
    PlanLength { local: bool, v: u32 },
    PlanSource(u64),
    PlanHash { local: bool, pos: usize, xor: u8 },
    PlanSwap { local: bool },
    DataHash { pos: usize, xor: u8 },
    DataBomb,
    DataOther { seed: u64, len: usize },
    FileRename { to: usize },
    WorkerIndex(u16),
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
            | BeginFlags { .. }
            | BeginParallel { .. } => framing::T_TREE_BEGIN,
            HelloSize(_)
            | HelloMtime(..)
            | HelloCdc(..)
            | HelloFlags(_)
            | HelloRole
            | HelloVersion(_)
            | HelloPath { .. } => framing::T_HELLO,
            PlanTarget { local, .. }
            | PlanLength { local, .. }
            | PlanHash { local, .. }
            | PlanSwap { local } => {
                if *local {
                    framing::T_PLAN_LOCAL
                } else {
                    framing::T_PLAN_REMOTE
                }
            }
            PlanSource(_) => framing::T_PLAN_LOCAL,
            DataHash { .. } | DataBomb | DataOther { .. } => framing::T_DATA,
            FileRename { .. } => framing::T_TREE_FILE,
            WorkerIndex(_) => framing::T_TREE_WORKER,
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
    Lie(Lie),
}

#[derive(Debug, Clone)]
struct Mutation {
    ftype: u8,
    /// Which frame of that type (counted across all connections).
    nth: u32,
    action: Action,
}

const SENDER_TYPES: [u8; 12] = [
    framing::T_TREE_BEGIN,
    framing::T_HELLO,
    framing::T_PLAN_LOCAL,
    framing::T_PLAN_REMOTE,
    framing::T_PLAN_DONE,
    framing::T_DATA,
    framing::T_VERIFY,
    framing::T_NOT_MODIFIED,
    framing::T_FILE_SYNC,
    framing::T_TREE_END,
    framing::T_TREE_WORKER,
    framing::T_TREE_FILE,
];

/// Values that break arithmetic and bounds checks. Mid-range sizes and
/// offsets (GiB..TiB) are deliberately absent: the receiver pre-allocates
/// the size the sender announces (File Reconstruction.md), and a test
/// must not allocate hundreds of GiB on a developer's disk. The huge ones
/// fail fast instead.
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

fn lie_strategy() -> impl Strategy<Value = Lie> {
    let idx = 0usize..16;
    let to = 0usize..RENAME_TARGETS.len();
    prop_oneof![
        (
            idx.clone(),
            prop_oneof![interesting_u64(), 1_500_000_000u64..1_700_000_000],
            prop_oneof![
                Just(0u32),
                Just(123_456_700),
                Just(999_999_999),
                Just(1_000_000_000),
                any::<u32>()
            ]
        )
            .prop_map(|(idx, secs, nanos)| Lie::BeginMtime { idx, secs, nanos }),
        (idx.clone(), interesting_u64()).prop_map(|(idx, size)| Lie::BeginSize { idx, size }),
        (idx.clone(), 0u8..4).prop_map(|(idx, kind)| Lie::BeginKind { idx, kind }),
        (
            idx.clone(),
            prop_oneof![
                Just(0u32),
                Just(0o444),
                Just(0o4755),
                Just(0o777),
                any::<u32>()
            ]
        )
            .prop_map(|(idx, mode)| Lie::BeginMode { idx, mode }),
        (idx.clone(), to.clone()).prop_map(|(idx, to)| Lie::BeginRename { idx, to }),
        idx.clone().prop_map(|idx| Lie::BeginDrop { idx }),
        idx.clone().prop_map(|idx| Lie::BeginChild { idx }),
        any::<u16>().prop_map(|flags| Lie::BeginFlags { flags }),
        prop_oneof![Just(0u16), Just(2), Just(3), Just(17), any::<u16>()]
            .prop_map(|parallel| Lie::BeginParallel { parallel }),
        interesting_u64().prop_map(Lie::HelloSize),
        (interesting_u64(), any::<u32>()).prop_map(|(s, n)| Lie::HelloMtime(s, n)),
        (interesting_u32(), interesting_u32(), interesting_u32())
            .prop_map(|(a, b, c)| Lie::HelloCdc(a, b, c)),
        any::<u16>().prop_map(Lie::HelloFlags),
        Just(Lie::HelloRole),
        prop_oneof![Just(3u16), Just(5), any::<u16>()].prop_map(Lie::HelloVersion),
        to.clone().prop_map(|to| Lie::HelloPath { to }),
        (any::<bool>(), interesting_u64()).prop_map(|(local, v)| Lie::PlanTarget { local, v }),
        (any::<bool>(), interesting_u32()).prop_map(|(local, v)| Lie::PlanLength { local, v }),
        interesting_u64().prop_map(Lie::PlanSource),
        (any::<bool>(), 0usize..32, 1u8..=255).prop_map(|(local, pos, xor)| Lie::PlanHash {
            local,
            pos,
            xor
        }),
        any::<bool>().prop_map(|local| Lie::PlanSwap { local }),
        (0usize..32, 1u8..=255).prop_map(|(pos, xor)| Lie::DataHash { pos, xor }),
        Just(Lie::DataBomb),
        (any::<u64>(), 1usize..70_000).prop_map(|(seed, len)| Lie::DataOther { seed, len }),
        to.prop_map(|to| Lie::FileRename { to }),
        prop_oneof![Just(0u16), Just(1), Just(2), Just(16), any::<u16>()]
            .prop_map(Lie::WorkerIndex),
    ]
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

fn mutation_strategy() -> impl Strategy<Value = Option<Mutation>> {
    let generic = (
        prop::sample::select(SENDER_TYPES.to_vec()),
        0u32..3,
        action_strategy(),
    )
        .prop_map(|(ftype, nth, action)| Mutation { ftype, nth, action });
    let lie = (0u32..3, lie_strategy()).prop_map(|(nth, lie)| {
        let ftype = lie.ftype();
        // One TREE_BEGIN per session: only the first can be a lie.
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
    prop_oneof![
        1 => Just(None),
        4 => generic.prop_map(Some),
        7 => lie.prop_map(Some),
    ]
}

/// Applies a lie to a decoded frame; `None` when the frame does not decode
/// as the lie expects (it is then forwarded unchanged).
fn apply_lie(lie: &Lie, frame: &Frame) -> Option<Frame> {
    let path_to = |to: usize| RENAME_TARGETS[to % RENAME_TARGETS.len()].to_string();
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
                Lie::BeginFlags { flags } => b.flags = *flags,
                Lie::BeginParallel { parallel } => b.parallel = *parallel,
                _ => return None,
            }
            b.into_frame().ok()
        }
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
                Lie::HelloRole => h.role = Role::Receiver,
                Lie::HelloVersion(v) => h.version = *v,
                Lie::HelloPath { to } => h.path = path_to(*to),
                _ => return None,
            }
            h.into_frame().ok()
        }
        framing::T_PLAN_LOCAL | framing::T_PLAN_REMOTE => {
            let entry = PlanEntry::from_frame(frame.clone()).ok()?;
            let xor_hash = |h: &blake3::Hash, pos: usize, xor: u8| {
                let mut b = *h.as_bytes();
                b[pos % 32] ^= xor;
                blake3::Hash::from_bytes(b)
            };
            let out = match (entry, lie) {
                (
                    PlanEntry::Local {
                        length,
                        source_offset,
                        hash,
                        ..
                    },
                    Lie::PlanTarget { v, .. },
                ) => PlanEntry::Local {
                    target_offset: *v,
                    length,
                    source_offset,
                    hash,
                },
                (PlanEntry::Remote { length, hash, .. }, Lie::PlanTarget { v, .. }) => {
                    PlanEntry::Remote {
                        target_offset: *v,
                        length,
                        hash,
                    }
                }
                (
                    PlanEntry::Local {
                        target_offset,
                        source_offset,
                        hash,
                        ..
                    },
                    Lie::PlanLength { v, .. },
                ) => PlanEntry::Local {
                    target_offset,
                    length: *v,
                    source_offset,
                    hash,
                },
                (
                    PlanEntry::Remote {
                        target_offset,
                        hash,
                        ..
                    },
                    Lie::PlanLength { v, .. },
                ) => PlanEntry::Remote {
                    target_offset,
                    length: *v,
                    hash,
                },
                (
                    PlanEntry::Local {
                        target_offset,
                        length,
                        hash,
                        ..
                    },
                    Lie::PlanSource(v),
                ) => PlanEntry::Local {
                    target_offset,
                    length,
                    source_offset: *v,
                    hash,
                },
                (
                    PlanEntry::Local {
                        target_offset,
                        length,
                        source_offset,
                        hash,
                    },
                    Lie::PlanHash { pos, xor, .. },
                ) => PlanEntry::Local {
                    target_offset,
                    length,
                    source_offset,
                    hash: xor_hash(&hash, *pos, *xor),
                },
                (
                    PlanEntry::Remote {
                        target_offset,
                        length,
                        hash,
                    },
                    Lie::PlanHash { pos, xor, .. },
                ) => PlanEntry::Remote {
                    target_offset,
                    length,
                    hash: xor_hash(&hash, *pos, *xor),
                },
                (
                    PlanEntry::Local {
                        target_offset,
                        length,
                        hash,
                        ..
                    },
                    Lie::PlanSwap { .. },
                ) => PlanEntry::Remote {
                    target_offset,
                    length,
                    hash,
                },
                (
                    PlanEntry::Remote {
                        target_offset,
                        length,
                        hash,
                    },
                    Lie::PlanSwap { .. },
                ) => PlanEntry::Local {
                    target_offset,
                    length,
                    source_offset: 0,
                    hash,
                },
                _ => return None,
            };
            out.into_frame().ok()
        }
        framing::T_DATA => {
            let (hash, compressed) = split_data_payload(frame.payload.clone()).ok()?;
            match lie {
                Lie::DataHash { pos, xor } => {
                    let mut b = *hash.as_bytes();
                    b[pos % 32] ^= xor;
                    encode_data(blake3::Hash::from_bytes(b), &compressed).ok()
                }
                Lie::DataBomb => {
                    let bomb = zstd::bulk::compress(&vec![0u8; 8 << 20], 3).ok()?;
                    encode_data(hash, &bomb).ok()
                }
                Lie::DataOther { seed, len } => {
                    let other = zstd::bulk::compress(&xorshift_bytes(*len, *seed), 1).ok()?;
                    encode_data(hash, &other).ok()
                }
                _ => None,
            }
        }
        framing::T_TREE_FILE => match lie {
            Lie::FileRename { to } => TreeFile {
                rel_path: path_to(*to),
            }
            .into_frame()
            .ok(),
            _ => None,
        },
        framing::T_TREE_WORKER => match lie {
            Lie::WorkerIndex(v) => TreeWorker { worker_index: *v }.into_frame().ok(),
            _ => None,
        },
        _ => None,
    }
}

// ===========================================================================
// The mutating proxy
// ===========================================================================

/// State shared by every connection of one session.
struct Shared {
    mutation: Option<Mutation>,
    counts: Mutex<HashMap<u8, u32>>,
    /// The mutation changed at least one forwarded byte.
    applied: AtomicBool,
    /// The first TREE_BEGIN frame the receiver was given, decoded
    /// (`Some(None)`: it did not decode).
    begin: Mutex<Option<Option<TreeBegin>>>,
    last_activity: Mutex<Instant>,
    /// Clones of every socket of the session (both ends), for the watchdog.
    sockets: Mutex<Vec<TcpStream>>,
    killed: AtomicBool,
}

impl Shared {
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

/// The sender's side of one connection: parses every frame the real sender
/// writes and forwards it (possibly mutated). Reads pass through.
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

    fn encode(frame: &Frame) -> Vec<u8> {
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        out
    }

    fn send(&mut self, frame: &Frame) -> io::Result<()> {
        if frame.ftype == framing::T_TREE_BEGIN {
            let mut begin = self.shared.begin.lock().unwrap();
            if begin.is_none() {
                *begin = Some(TreeBegin::from_frame(frame.clone()).ok());
            }
        }
        self.inner.write_all(&Self::encode(frame))
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
                let bytes = Self::encode(&frame);
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
            Action::Lie(ref lie) => match apply_lie(lie, &frame) {
                Some(f) if Self::encode(&f) != Self::encode(&frame) => {
                    applied();
                    self.send(&f)
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

/// A receiver-side socket that closes on drop despite the watchdog's clone.
struct Conn(TcpStream);

impl Drop for Conn {
    fn drop(&mut self) {
        let _ = self.0.shutdown(Shutdown::Both);
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
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
        loop {
            if self.pending.len() < 5 {
                break;
            }
            let len = u32::from_le_bytes(self.pending[1..5].try_into().unwrap()) as usize;
            if self.pending.len() < 5 + len {
                break;
            }
            let rest = self.pending.split_off(5 + len);
            let raw = std::mem::replace(&mut self.pending, rest);
            let frame = Frame::new(raw[0], raw[5..].to_vec());
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

// ===========================================================================
// One hostile session
// ===========================================================================

/// How long the session may sit without sender I/O before the watchdog
/// cuts every socket (dropped frames deadlock both peers by design: each
/// waits for the other; real deployments rely on transport timeouts).
const IDLE: Duration = Duration::from_millis(1500);
/// How long a peer may take to return once its sockets are cut.
const RETURN_BUDGET: Duration = Duration::from_secs(20);

/// A peer's result; the messages are read through `Debug` in failure
/// reports.
#[derive(Debug)]
#[allow(dead_code)]
enum Outcome {
    Ok,
    Err(String),
    Panic(String),
}

impl Outcome {
    fn is_ok(&self) -> bool {
        matches!(self, Outcome::Ok)
    }
}

struct SessionReport {
    sender: Outcome,
    receiver: Outcome,
    applied: bool,
    killed: bool,
    begin: Option<TreeBegin>,
}

fn run_guarded<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> mpsc::Receiver<Outcome> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(Ok(_)) => Outcome::Ok,
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
fn accept_one(listener: &TcpListener, shared: &Shared) -> io::Result<Conn> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match listener.accept() {
            Ok((tcp, _)) => {
                // Windows hands out accepted sockets in the listener's
                // nonblocking mode.
                tcp.set_nonblocking(false)?;
                timeouts(&tcp);
                shared.register(&tcp);
                return Ok(Conn(tcp));
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

fn run_hostile_session(
    s: &Path,
    r: &Path,
    mutation: Option<Mutation>,
    no_delete: bool,
    parallel: u16,
    eager: bool,
) -> Result<SessionReport, String> {
    let shared = Arc::new(Shared {
        mutation,
        counts: Mutex::new(HashMap::new()),
        applied: AtomicBool::new(false),
        begin: Mutex::new(None),
        last_activity: Mutex::new(Instant::now()),
        sockets: Mutex::new(Vec::new()),
        killed: AtomicBool::new(false),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();

    // The receiver thread owns the listener: it closes when `tree_receive`
    // returns, like the CLI's. That resets worker connections the receiver
    // never accepted (a refused TREE_WORKER ends the session early); kept
    // open, they sit in the backlog and the sender's ACK read blocks until
    // its read timeout, because on Windows a local shutdown does not wake
    // a read on a connection no peer socket ever owned.
    let recv_rx = {
        let (shared, root) = (shared.clone(), r.to_path_buf());
        run_guarded(move || {
            let listener = Arc::new(listener);
            let mut tcp = accept_one(&listener, &shared).map_err(|e| format!("accept: {e}"))?;
            let worker_shared = shared.clone();
            tree_receive(
                &mut tcp,
                &TreeReceiveRequest {
                    root,
                    force_checksum: false,
                    no_delete,
                    excludes: Vec::new(),
                    accept_worker: Arc::new(move || {
                        Ok(Box::new(accept_one(&listener, &worker_shared)?))
                    }),
                    dry_run: false,
                    eager,
                },
                &PipelineConfig::default(),
                &mut |_| {},
            )
            .map_err(|e| e.to_string())
        })
    };
    let send_rx = {
        let (shared, root) = (shared.clone(), s.to_path_buf());
        run_guarded(move || {
            let connect = move |shared: &Arc<Shared>| -> io::Result<MutatingStream> {
                let tcp = TcpStream::connect(addr)?;
                timeouts(&tcp);
                Ok(MutatingStream::new(tcp, shared.clone()))
            };
            let mut control = connect(&shared).map_err(|e| format!("connect: {e}"))?;
            let worker_shared = shared.clone();
            tree_send(
                &mut control,
                &TreeSendRequest {
                    root,
                    force_checksum: false,
                    no_delete,
                    excludes: Vec::new(),
                    parallel,
                    dry_run: false,
                    eager,
                    connect_worker: Arc::new(move |_| Ok(Box::new(connect(&worker_shared)?))),
                },
                &PipelineConfig::default(),
                &mut |_| {},
            )
            .map_err(|e| e.to_string())
        })
    };

    // Watchdog: wait for both peers; cut every socket once idle.
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
    let begin = shared.begin.lock().unwrap().clone().flatten();
    Ok(SessionReport {
        sender: sender.unwrap(),
        receiver: receiver.unwrap(),
        applied: shared.applied.load(Ordering::SeqCst),
        killed: killed_at.is_some(),
        begin,
    })
}

// ===========================================================================
// The property
// ===========================================================================

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

proptest! {
    #![proptest_config(cases(64))]

    #[test]
    fn hostile_sender_cannot_corrupt_receiver(
        fx in fixture_strategy(),
        mutation in mutation_strategy(),
        no_delete in any::<bool>(),
        parallel in 1u16..=3,
        eager in any::<bool>(),
    ) {
        let scratch = Scratch::new("case");
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

        let report = run_hostile_session(&s, &r, mutation.clone(), no_delete, parallel, eager)
            .map_err(TestCaseError::fail)?;
        make_accessible(scratch.path());
        let post_r = walk(&r);
        let ctx = format!(
            "mutation {mutation:?} applied={} killed={} sender={:?} receiver={:?}",
            report.applied, report.killed, report.sender, report.receiver
        );

        // (1) No panics.
        prop_assert!(
            !matches!(report.receiver, Outcome::Panic(_)),
            "receiver panicked: {}", ctx
        );
        prop_assert!(!matches!(report.sender, Outcome::Panic(_)), "sender panicked: {}", ctx);

        // (2) Containment: nothing outside the receiver root changed.
        prop_assert_eq!(list_names(scratch.path()), vec!["outside", "r", "s"], "{}", ctx);
        prop_assert_eq!(list_names(&outside), vec!["canary"], "{}", ctx);
        prop_assert_eq!(fs::read(outside.join("canary")).unwrap(), b"canary".to_vec(), "{}", ctx);
        prop_assert_eq!(&walk(&s), &sender_files, "sender tree changed: {}", ctx);

        // (3) Content: old content or the sender's real content, same path.
        for (p, c) in &post_r {
            let ok = pre_r.get(p) == Some(c) || sender_files.get(p) == Some(c);
            prop_assert!(
                ok,
                "receiver file {:?} ({} bytes) is neither its old content nor the sender's: {}",
                p, c.len(), ctx
            );
        }

        // (4) Deletions: only a successful delete pass or a documented
        // kind-conflict removal (against the TREE_BEGIN the receiver saw).
        let begin = report.begin.as_ref();
        let r_entry = |p: &str| begin.and_then(|b| b.entries.iter().find(|e| e.rel_path == p));
        let r_flags = begin.map(|b| b.flags).unwrap_or(0);
        for p in pre_r.keys() {
            if post_r.contains_key(p) {
                continue;
            }
            let delete_pass = report.receiver.is_ok()
                && !no_delete
                && r_flags & TREE_FLAG_NO_DELETE == 0
                && r_entry(p).is_none();
            let conflict = r_entry(p).is_some_and(|e| e.kind == 1)
                || ancestors(p).any(|a| r_entry(a).is_some_and(|e| e.kind == 0));
            prop_assert!(
                delete_pass || conflict,
                "receiver file {:?} vanished outside a delete pass or conflict removal: {}",
                p, ctx
            );
        }

        // (5) Loud failure: a receiver that reports success has converged
        // on every file the accepted TREE_BEGIN names.
        if report.receiver.is_ok() && r_flags & TREE_FLAG_DRY_RUN == 0 {
            for e in begin.map(|b| b.entries.as_slice()).unwrap_or(&[]) {
                if e.kind != 0 {
                    continue;
                }
                prop_assert!(
                    post_r.contains_key(&e.rel_path),
                    "receiver reported success but {:?} (in its TREE_BEGIN) is missing: {}",
                    e.rel_path, ctx
                );
                if let Some(want) = sender_files.get(&e.rel_path) {
                    prop_assert!(
                        post_r.get(&e.rel_path) == Some(want),
                        "receiver reported success but {:?} differs from the sender's: {}",
                        e.rel_path, ctx
                    );
                }
            }
        }

        // (6) Control: an unapplied mutation is an honest session.
        if !report.applied {
            prop_assert!(
                report.sender.is_ok() && report.receiver.is_ok() && !report.killed,
                "honest session failed: {}", ctx
            );
            for (p, c) in &sender_files {
                prop_assert_eq!(post_r.get(p), Some(c), "honest session: {} differs", p);
            }
            for p in pre_r.keys().filter(|p| !sender_files.contains_key(*p)) {
                // A receiver file where the sender has a dir is a kind
                // conflict: removed even under --no-delete.
                let conflict = sender_files.keys().any(|q| q.starts_with(&format!("{p}/")));
                prop_assert_eq!(
                    post_r.contains_key(p), no_delete && !conflict,
                    "honest session: receiver-only {} (no_delete={})", p, no_delete
                );
            }
        }
    }
}

#[test]
fn mutating_stream_forwards_honest_frames_unchanged() {
    // The proxy itself: with no mutation, a session is an ordinary one.
    let scratch = Scratch::new("honest");
    let (s, r) = (scratch.path().join("s"), scratch.path().join("r"));
    fs::create_dir_all(&s).unwrap();
    fs::create_dir_all(&r).unwrap();
    write_file(&s, "a", &xorshift_bytes(70_000, 7), 1_600_000_000, 0);
    write_file(&s, "d/x", b"x", 1_600_000_001, 0);
    let report = run_hostile_session(&s, &r, None, false, 2, false).unwrap();
    assert!(
        report.sender.is_ok() && report.receiver.is_ok(),
        "{:?} {:?}",
        report.sender,
        report.receiver
    );
    assert!(!report.applied && !report.killed);
    assert_eq!(walk(&r), walk(&s));
    assert_eq!(report.begin.unwrap().entries.len(), 3);
}
