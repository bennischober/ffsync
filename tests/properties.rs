//! Property tests (verification pilot, `evaluation/verification-pilot.md`).
//!
//! Each group states an invariant the design documents promise and lets
//! proptest search for a counterexample:
//!
//! - tree triage (§16.3/§16.4): determinism, the post-sync all-Skip model,
//!   delete gating under `--no-delete`, kind-conflict classification, and
//!   peer agreement on the conversation set;
//! - real tree sessions over TCP loopback converge to the model above;
//! - exclude patterns (§16.7) against an independent reference matcher,
//!   and the scan's pruning rule against `matches_path_or_ancestor`;
//! - path safety (§16.2): `validate_rel_path`, `check_local_name`,
//!   `link_ancestor`;
//! - the 100 ns mtime equivalence (`mtimes_match`, D23);
//! - chunk coverage and the plan model (§5/§6): tiling of `[0, size)`,
//!   local sources inside the old file, reconstruction equals the source;
//! - wire codecs (§3/§4/§8/§16): round trips, canonical encodings, strict
//!   flag decoding, and no panics on arbitrary payloads.
//!
//! A failing property is a finding, not a property to weaken — record it
//! in the pilot document.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use proptest::prelude::*;

use ffsync::chunking::{
    Blake3Hasher, Chunk, ChunkEvent, FixedChunker, PipelineConfig, cdc_params_for_size,
    process_file, sane_cdc_params, scan_file_ordered,
};
use ffsync::protocol::framing::{self, Frame, FrameError, MAX_PAYLOAD, read_frame};
use ffsync::protocol::message::{
    ErrorCode, FLAG_DRY_RUN, FLAG_EAGER, FLAG_RESUMING, FLAG_TRUST_TIMES, Hello, ManifestChunk,
    Message, PlanEntry, Role, TREE_FLAG_EXCLUDES, TREE_FLAG_METADATA, TREE_FLAG_NEGATION,
    TREE_FLAG_PARALLEL, TreeBegin, TreeEnd, TreeEntryWire, TreeFile, TreeWorker, decode_commit_ack,
    encode_commit_ack, encode_data, encode_error, read_error_payload, split_data_payload,
};
use ffsync::protocol::tree_sync::{
    TreeReceiveRequest, TreeSendRequest, manifest_from_wire, tree_receive, tree_send,
};
use ffsync::tree::{
    self, EntryKind, ExcludePattern, ExcludeSet, MTIME_GRANULARITY_NANOS, TREE_FLAG_CHECKSUM,
    TREE_FLAG_DRY_RUN, TREE_FLAG_EAGER, TREE_FLAG_NO_DELETE, TreeEntry, TreeManifest, Triage,
};

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Proptest config without failure persistence: integration tests have no
/// `lib.rs` beside them, and regression files would be generated artifacts.
/// A failing case prints its minimal input; record it in the pilot doc.
fn cases(n: u32) -> ProptestConfig {
    ProptestConfig {
        cases: n,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// A unique scratch directory per proptest case, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("ffsync_prop_{tag}_{}_{n}", std::process::id()));
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
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn native(root: &Path, rel: &str) -> PathBuf {
    root.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR))
}

/// Proper ancestors of a wire path, shallowest first (`a/b/c` → `a`, `a/b`).
fn ancestors(rel: &str) -> Vec<&str> {
    rel.match_indices('/').map(|(i, _)| &rel[..i]).collect()
}

// ===========================================================================
// Tree model: a consistent tree (every ancestor of an entry is a dir, no
// file has children), generated from small name alphabets so that two
// independently generated trees overlap and conflict often.
// ===========================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
enum Node {
    Dir,
    File {
        content: Vec<u8>,
        secs: u64,
        nanos: u32,
    },
}

type Model = BTreeMap<String, Node>;

/// (name indices, is_dir, content, nanos) — normalized into a [`Model`].
type Spec = (Vec<usize>, bool, Vec<u8>, u32);

const NAMES: [&str; 3] = ["a", "b", "c"];

fn content_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        4 => prop::collection::vec(any::<u8>(), 0..48),
        1 => (any::<u8>(), 20_000usize..40_000).prop_map(|(b, n)| {
            // Low-entropy but not constant: several CDC chunks.
            (0..n).map(|i| b.wrapping_add((i / 997) as u8)).collect()
        }),
    ]
}

fn specs_strategy() -> impl Strategy<Value = Vec<Spec>> {
    prop::collection::vec(
        (
            prop::collection::vec(0usize..NAMES.len(), 1..=3),
            any::<bool>(),
            content_strategy(),
            0u32..1_000_000_000,
        ),
        0..8,
    )
}

/// Normalizes specs into a consistent tree. Earlier specs win: a spec whose
/// ancestor is already a file, or whose path is already taken, is dropped.
/// File i gets `secs_base + i` — two models built with different bases
/// never share an mtime by accident.
fn build_model(specs: &[Spec], secs_base: u64) -> Model {
    let mut m = Model::new();
    'spec: for (i, (idx, is_dir, content, nanos)) in specs.iter().enumerate() {
        let segs: Vec<&str> = idx.iter().map(|&k| NAMES[k]).collect();
        for d in 1..segs.len() {
            let prefix = segs[..d].join("/");
            if matches!(m.get(&prefix), Some(Node::File { .. })) {
                continue 'spec;
            }
        }
        let path = segs.join("/");
        if m.contains_key(&path) {
            continue;
        }
        for d in 1..segs.len() {
            m.entry(segs[..d].join("/")).or_insert(Node::Dir);
        }
        let node = if *is_dir {
            Node::Dir
        } else {
            Node::File {
                content: content.clone(),
                secs: secs_base + i as u64,
                nanos: *nanos,
            }
        };
        m.insert(path, node);
    }
    m
}

/// The receiver tree: built independently, then every path where both
/// trees hold a file is optionally made an identical copy (content AND
/// mtime) of the sender's — the fast-path Skip case.
fn receiver_model(sender: &Model, specs: &[Spec], copies: &[bool]) -> Model {
    let mut r = build_model(specs, 1_500_000_000);
    let mut k = 0usize;
    for (path, node) in r.iter_mut() {
        if let (Node::File { .. }, Some(s @ Node::File { .. })) = (&*node, sender.get(path)) {
            if copies.get(k).copied().unwrap_or(false) {
                *node = s.clone();
            }
            k += 1;
        }
    }
    r
}

fn to_manifest(m: &Model) -> TreeManifest {
    let mut out = TreeManifest::new();
    for (p, n) in m {
        out.insert(match n {
            Node::Dir => TreeEntry {
                rel_path: p.clone(),
                kind: EntryKind::Dir,
                size: 0,
                mtime_secs: 0,
                mtime_nanos: 0,
                mode: 0,
            },
            Node::File {
                content,
                secs,
                nanos,
            } => TreeEntry {
                rel_path: p.clone(),
                kind: EntryKind::File,
                size: content.len() as u64,
                mtime_secs: *secs,
                mtime_nanos: *nanos,
                mode: 0,
            },
        });
    }
    out
}

fn union_paths(a: &TreeManifest, b: &TreeManifest) -> BTreeSet<String> {
    a.entries()
        .chain(b.entries())
        .map(|e| e.rel_path.clone())
        .collect()
}

/// The sender's conversation set, as `tree_send` derives it: its own
/// entries whose triage is Sync/Transfer/Conflict{File}.
fn sender_conversations(s: &TreeManifest, r: &TreeManifest, checksum: bool) -> Vec<String> {
    let mut v: Vec<String> = s
        .entries()
        .filter(|e| {
            matches!(
                tree::triage(&e.rel_path, Some(e), r.get(&e.rel_path), checksum),
                Triage::Sync
                    | Triage::Transfer
                    | Triage::Conflict {
                        sender_kind: EntryKind::File,
                        ..
                    }
            )
        })
        .map(|e| e.rel_path.clone())
        .collect();
    v.sort();
    v
}

/// The receiver's conversation set, as `tree_receive` derives it: the
/// union of both manifests, in canonical order.
fn receiver_conversations(s: &TreeManifest, r: &TreeManifest, checksum: bool) -> Vec<String> {
    union_paths(s, r)
        .into_iter()
        .filter(|p| {
            matches!(
                tree::triage(p, s.get(p), r.get(p), checksum),
                Triage::Sync
                    | Triage::Transfer
                    | Triage::Conflict {
                        sender_kind: EntryKind::File,
                        ..
                    }
            )
        })
        .collect()
}

/// Applies one session's triage to the receiver model (§16.3/§16.5/§16.6):
/// conversations install the sender's file with its mtime (stored at the
/// receiver filesystem's 100 ns tick when `ntfs`), conflicts remove the
/// receiver's entry and its subtree (never gated), deletes run unless
/// `no_delete`.
fn apply_session(s: &Model, r: &Model, checksum: bool, no_delete: bool, ntfs: bool) -> Model {
    let sm = to_manifest(s);
    let rm = to_manifest(r);
    let mut out = r.clone();
    let install = |out: &mut Model, p: &str| {
        let mut n = s[p].clone();
        if ntfs && let Node::File { nanos, .. } = &mut n {
            *nanos -= *nanos % MTIME_GRANULARITY_NANOS;
        }
        out.insert(p.to_string(), n);
    };
    for p in union_paths(&sm, &rm) {
        match tree::triage(&p, sm.get(&p), rm.get(&p), checksum) {
            Triage::Skip => {}
            Triage::Sync | Triage::Transfer | Triage::CreateDir => install(&mut out, &p),
            Triage::Delete => {
                if !no_delete {
                    out.remove(&p);
                }
            }
            Triage::Conflict { .. } => {
                let under = format!("{p}/");
                out.retain(|q, _| !q.starts_with(&under));
                install(&mut out, &p);
            }
        }
    }
    out
}

// ===========================================================================
// 1. Tree triage (pure)
// ===========================================================================

proptest! {
    #![proptest_config(cases(512))]

    /// Triage is a pure function of the two entries and the flag, and the
    /// manifest's canonical order does not depend on insertion order —
    /// both peers derive the same sequence (§16.4).
    #[test]
    fn triage_is_deterministic_and_order_independent(
        s in specs_strategy(), r in specs_strategy(), copies in prop::collection::vec(any::<bool>(), 8),
        checksum in any::<bool>(), seed in any::<u64>(),
    ) {
        let sm = build_model(&s, 1_600_000_000);
        let rm = receiver_model(&sm, &r, &copies);
        let (a, b) = (to_manifest(&sm), to_manifest(&rm));
        // Rebuild the receiver manifest in a scrambled insertion order.
        let mut entries: Vec<TreeEntry> = b.entries().cloned().collect();
        let mut x = seed | 1;
        for i in (1..entries.len()).rev() {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            entries.swap(i, (x % (i as u64 + 1)) as usize);
        }
        let mut b2 = TreeManifest::new();
        for e in entries { b2.insert(e); }
        prop_assert_eq!(&b, &b2);
        let ord1: Vec<&str> = b.entries().map(|e| e.rel_path.as_str()).collect();
        let ord2: Vec<&str> = b2.entries().map(|e| e.rel_path.as_str()).collect();
        prop_assert_eq!(ord1, ord2);
        for p in union_paths(&a, &b) {
            prop_assert_eq!(
                tree::triage(&p, a.get(&p), b.get(&p), checksum),
                tree::triage(&p, a.get(&p), b2.get(&p), checksum)
            );
        }
        // The two peers' conversation sets agree (the sender iterates its
        // own entries, the receiver the union).
        prop_assert_eq!(sender_conversations(&a, &b, checksum), receiver_conversations(&a, &b, checksum));
    }

    /// Classification: Delete only for receiver-only paths (never for a
    /// path the sender has), Conflict exactly when kinds differ and always
    /// carrying the sender's kind, checksum never skips a file pair, and a
    /// file pair skips iff size and mtime (100 ns) agree.
    #[test]
    fn triage_classification(
        s in specs_strategy(), r in specs_strategy(), copies in prop::collection::vec(any::<bool>(), 8),
        checksum in any::<bool>(),
    ) {
        let sm = build_model(&s, 1_600_000_000);
        let rm = receiver_model(&sm, &r, &copies);
        let (a, b) = (to_manifest(&sm), to_manifest(&rm));
        for p in union_paths(&a, &b) {
            let (se, re) = (a.get(&p), b.get(&p));
            let t = tree::triage(&p, se, re, checksum);
            if se.is_some() {
                prop_assert_ne!(&t, &Triage::Delete, "{} is on the sender", p);
            }
            prop_assert_eq!(t == Triage::Delete, se.is_none() && re.is_some());
            match (se, re) {
                (Some(x), Some(y)) if x.kind != y.kind => prop_assert_eq!(
                    &t,
                    &Triage::Conflict { sender_kind: x.kind, remove_receiver_entry: true }
                ),
                _ => {
                    let conflict = matches!(t, Triage::Conflict { .. });
                    prop_assert!(!conflict, "{} classified as a conflict", p);
                }
            }
            if let (Some(x), Some(y)) = (se, re) && x.kind == EntryKind::File && y.kind == EntryKind::File {
                let meta_eq = x.size == y.size
                    && tree::mtimes_match((x.mtime_secs, x.mtime_nanos), (y.mtime_secs, y.mtime_nanos));
                prop_assert_eq!(t == Triage::Skip, !checksum && meta_eq);
                prop_assert_eq!(t == Triage::Sync, checksum || !meta_eq);
            }
            if se.is_some_and(|e| e.kind == EntryKind::File) && re.is_none() {
                prop_assert_eq!(&t, &Triage::Transfer);
            }
            if se.is_some_and(|e| e.kind == EntryKind::Dir) && re.is_none() {
                prop_assert_eq!(&t, &Triage::CreateDir);
            }
        }
    }

    /// After one session (mtimes preserved, receiver storing at NTFS's
    /// 100 ns tick or exactly), a second triage finds nothing to do:
    /// all Skip — or, under --no-delete, Skip for the sender's paths and
    /// Delete (gated) for the receiver-only leftovers.
    #[test]
    fn second_triage_after_sync_is_all_skip(
        s in specs_strategy(), r in specs_strategy(), copies in prop::collection::vec(any::<bool>(), 8),
        checksum in any::<bool>(), no_delete in any::<bool>(), ntfs in any::<bool>(),
    ) {
        let sm = build_model(&s, 1_600_000_000);
        let rm = receiver_model(&sm, &r, &copies);
        let after = apply_session(&sm, &rm, checksum, no_delete, ntfs);
        let (a, b) = (to_manifest(&sm), to_manifest(&after));
        for p in union_paths(&a, &b) {
            let t = tree::triage(&p, a.get(&p), b.get(&p), false);
            if a.get(&p).is_some() {
                prop_assert_eq!(&t, &Triage::Skip, "{} after one session", p);
            } else {
                prop_assert!(no_delete, "{} survived a deleting session", p);
                prop_assert_eq!(&t, &Triage::Delete);
            }
        }
    }

    /// --no-delete keeps every receiver path except those under a kind
    /// conflict the sender resolved (§16.3: "Conflict resolution is not
    /// gated" — the sender's file replaces the receiver's directory and
    /// its subtree), and keeps their content.
    #[test]
    fn no_delete_removes_only_conflict_subtrees(
        s in specs_strategy(), r in specs_strategy(), copies in prop::collection::vec(any::<bool>(), 8),
        checksum in any::<bool>(),
    ) {
        let sm = build_model(&s, 1_600_000_000);
        let rm = receiver_model(&sm, &r, &copies);
        let after = apply_session(&sm, &rm, checksum, true, false);
        for (p, node) in &rm {
            if sm.contains_key(p) {
                continue;
            }
            let under_conflict = ancestors(p)
                .into_iter()
                .any(|q| matches!(sm.get(q), Some(Node::File { .. })));
            if under_conflict {
                prop_assert!(!after.contains_key(p));
            } else {
                prop_assert_eq!(after.get(p), Some(node), "{} lost under --no-delete", p);
            }
        }
    }

    /// The receiver's delete pass sorts deletions in reverse lexicographic
    /// order ("deepest first"): every descendant precedes its ancestor.
    #[test]
    fn reverse_sort_is_children_first(r in specs_strategy()) {
        let m = build_model(&r, 1);
        let mut v: Vec<&String> = m.keys().collect();
        v.sort_by(|a, b| b.cmp(a));
        for (i, p) in v.iter().enumerate() {
            for q in ancestors(p) {
                let j = v.iter().position(|x| x.as_str() == q).unwrap();
                prop_assert!(i < j, "{} must precede its ancestor {}", p, q);
            }
        }
    }
}

// ===========================================================================
// 2. Real tree sessions over TCP loopback converge to the model
// ===========================================================================

fn materialize(root: &Path, m: &Model) {
    for (p, n) in m {
        let path = native(root, p);
        match n {
            Node::Dir => fs::create_dir_all(&path).unwrap(),
            Node::File {
                content,
                secs,
                nanos,
            } => {
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(&path, content).unwrap();
                let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
                f.set_modified(SystemTime::UNIX_EPOCH + Duration::new(*secs, *nanos))
                    .unwrap();
            }
        }
    }
}

/// The tree as the scan sees it, with file contents.
fn read_tree(root: &Path) -> (TreeManifest, BTreeMap<String, Option<Vec<u8>>>) {
    let m = TreeManifest::scan(root).unwrap();
    let contents = m
        .entries()
        .map(|e| {
            let c =
                (e.kind == EntryKind::File).then(|| fs::read(native(root, &e.rel_path)).unwrap());
            (e.rel_path.clone(), c)
        })
        .collect();
    (m, contents)
}

fn expected_contents(m: &Model) -> BTreeMap<String, Option<Vec<u8>>> {
    m.iter()
        .map(|(p, n)| {
            (
                p.clone(),
                match n {
                    Node::Dir => None,
                    Node::File { content, .. } => Some(content.clone()),
                },
            )
        })
        .collect()
}

/// One sequential or parallel tree session over TCP loopback (the
/// `tests/tree.rs` harness, trimmed).
fn run_session(
    sender_root: &Path,
    receiver_root: &Path,
    no_delete: bool,
    parallel: u16,
    eager: bool,
) -> Result<(), String> {
    let timeouts = |tcp: &std::net::TcpStream| {
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
        tcp.set_write_timeout(Some(Duration::from_secs(20))).ok();
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let listener2 = listener.try_clone().unwrap();
    let recv_root = receiver_root.to_path_buf();
    let server = std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        timeouts(&tcp);
        tree_receive(
            &mut &tcp,
            &TreeReceiveRequest {
                root: recv_root,
                force_checksum: false,
                no_delete,
                excludes: Vec::new(),
                accept_worker: std::sync::Arc::new(move || {
                    let (tcp, _) = listener2.accept()?;
                    timeouts(&tcp);
                    Ok(Box::new(tcp))
                }),
                dry_run: false,
                eager,
            },
            &PipelineConfig::default(),
            &mut |_| {},
        )
    });
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    timeouts(&tcp);
    let send = tree_send(
        &mut &tcp,
        &TreeSendRequest {
            root: sender_root.to_path_buf(),
            force_checksum: false,
            no_delete,
            excludes: Vec::new(),
            parallel,
            dry_run: false,
            eager,
            connect_worker: std::sync::Arc::new(move |_| {
                let tcp = std::net::TcpStream::connect(addr)?;
                timeouts(&tcp);
                Ok(Box::new(tcp))
            }),
        },
        &PipelineConfig::default(),
        &mut |_| {},
    );
    drop(tcp);
    let recv = server.join().unwrap();
    match (send, recv) {
        (Ok(s), Ok(r)) if s.files == r.files => Ok(()),
        (Ok(s), Ok(r)) => Err(format!("file counts differ: {} vs {}", s.files, r.files)),
        (s, r) => Err(format!("sender {:?} / receiver {:?}", s.err(), r.err())),
    }
}

proptest! {
    #![proptest_config(cases(24))]

    /// A real session (sequential or parallel, lazy or eager) leaves the
    /// receiver exactly where the triage model says, and a second triage
    /// over the two real scans finds nothing to do.
    #[test]
    fn tree_session_converges(
        s in specs_strategy(), r in specs_strategy(), copies in prop::collection::vec(any::<bool>(), 8),
        no_delete in any::<bool>(), parallel in 1u16..=3, eager in any::<bool>(),
    ) {
        let sm = build_model(&s, 1_600_000_000);
        let rm = receiver_model(&sm, &r, &copies);
        let (sd, rd) = (Scratch::new("sess_s"), Scratch::new("sess_r"));
        materialize(sd.path(), &sm);
        materialize(rd.path(), &rm);

        let res = run_session(sd.path(), rd.path(), no_delete, parallel, eager);
        prop_assert!(res.is_ok(), "session failed: {:?}", res);

        let expected = apply_session(&sm, &rm, false, no_delete, false);
        let (scan_r, got) = read_tree(rd.path());
        prop_assert_eq!(&got, &expected_contents(&expected));

        let (scan_s, _) = read_tree(sd.path());
        for p in union_paths(&scan_s, &scan_r) {
            let t = tree::triage(&p, scan_s.get(&p), scan_r.get(&p), false);
            if scan_s.get(&p).is_some() {
                prop_assert_eq!(&t, &Triage::Skip, "{} re-syncs", p);
            } else {
                prop_assert!(no_delete);
            }
        }
    }
}

// ===========================================================================
// 3. Exclude patterns vs an independent reference matcher (§16.7)
// ===========================================================================

/// A direct, recursive reading of the §16.7 grammar — deliberately not the
/// DP / single-backtrack structure of `src/tree.rs`.
mod reference {
    #[derive(Debug)]
    pub struct Pat {
        pub negated: bool,
        dir_only: bool,
        anchored: bool,
        segs: Vec<Seg>,
    }

    #[derive(Debug)]
    enum Seg {
        AnySegs,
        Glob(Vec<Tok>),
    }

    #[derive(Debug)]
    enum Tok {
        Star,
        One,
        Lit(char),
        Class(bool, Vec<(char, char)>),
    }

    /// `None` = the pattern is invalid.
    pub fn parse(raw: &str) -> Option<Pat> {
        let (negated, rest) = match raw.strip_prefix("!!") {
            Some(r) => (false, format!("!{r}")),
            None => match raw.strip_prefix('!') {
                Some(r) => (true, r.to_string()),
                None => (false, raw.to_string()),
            },
        };
        if rest.is_empty() || rest.contains('\\') {
            return None;
        }
        let dir_only = rest.ends_with('/');
        let core = rest.trim_end_matches('/');
        if core.is_empty() {
            return None;
        }
        let anchored = core.contains('/');
        let core = core.strip_prefix('/').unwrap_or(core);
        let mut segs = Vec::new();
        for s in core.split('/') {
            if s.is_empty() {
                return None;
            }
            segs.push(if s == "**" {
                Seg::AnySegs
            } else {
                Seg::Glob(tokens(s))
            });
        }
        Some(Pat {
            negated,
            dir_only,
            anchored,
            segs,
        })
    }

    fn tokens(seg: &str) -> Vec<Tok> {
        let c: Vec<char> = seg.chars().collect();
        let mut out = Vec::new();
        let mut i = 0;
        while i < c.len() {
            match c[i] {
                '*' => out.push(Tok::Star),
                '?' => out.push(Tok::One),
                '[' => {
                    let mut j = i + 1;
                    let neg = c.get(j) == Some(&'!');
                    if neg {
                        j += 1;
                    }
                    // The class closes at the first ']' after at least one
                    // member (a leading ']' is a member).
                    if let Some(k) = (j + 1..c.len()).find(|&k| c[k] == ']') {
                        let members = &c[j..k];
                        let mut items = Vec::new();
                        let mut m = 0;
                        while m < members.len() {
                            if m + 2 < members.len() && members[m + 1] == '-' {
                                items.push((members[m], members[m + 2]));
                                m += 3;
                            } else {
                                items.push((members[m], members[m]));
                                m += 1;
                            }
                        }
                        out.push(Tok::Class(neg, items));
                        i = k + 1;
                        continue;
                    }
                    out.push(Tok::Lit('['));
                }
                ch => out.push(Tok::Lit(ch)),
            }
            i += 1;
        }
        out
    }

    fn glob(t: &[Tok], s: &[char]) -> bool {
        match t.split_first() {
            None => s.is_empty(),
            Some((Tok::Star, rest)) => (0..=s.len()).any(|k| glob(rest, &s[k..])),
            Some((tok, rest)) => {
                let Some((&ch, tail)) = s.split_first() else {
                    return false;
                };
                let ok = match tok {
                    Tok::One => true,
                    Tok::Lit(l) => *l == ch,
                    Tok::Class(neg, items) => {
                        items.iter().any(|&(lo, hi)| lo <= ch && ch <= hi) != *neg
                    }
                    Tok::Star => unreachable!(),
                };
                ok && glob(rest, tail)
            }
        }
    }

    fn segs_match(p: &[Seg], path: &[&str]) -> bool {
        match p.split_first() {
            None => path.is_empty(),
            Some((Seg::AnySegs, rest)) => (0..=path.len()).any(|k| segs_match(rest, &path[k..])),
            Some((Seg::Glob(t), rest)) => match path.split_first() {
                Some((first, tail)) => {
                    glob(t, &first.chars().collect::<Vec<_>>()) && segs_match(rest, tail)
                }
                None => false,
            },
        }
    }

    impl Pat {
        pub fn matches(&self, rel: &str, is_dir: bool) -> bool {
            if self.dir_only && !is_dir {
                return false;
            }
            let path: Vec<&str> = rel.split('/').collect();
            if self.anchored {
                segs_match(&self.segs, &path)
            } else {
                (0..path.len()).any(|k| segs_match(&self.segs, &path[k..]))
            }
        }
    }

    /// First match wins; no match = included.
    pub fn excluded(set: &[Pat], rel: &str, is_dir: bool) -> bool {
        set.iter()
            .find(|p| p.matches(rel, is_dir))
            .is_some_and(|p| !p.negated)
    }
}

fn pattern_strategy() -> impl Strategy<Value = String> {
    let tok = prop::sample::select(vec![
        "a", "b", "*", "?", "[ab]", "[!a]", "[a-b]", "[", "]", "!", "**", "[]a]", "[!]",
    ]);
    let seg = prop::collection::vec(tok, 1..=3).prop_map(|v| v.concat());
    (
        prop::sample::select(vec!["", "", "!", "!!"]),
        prop::sample::select(vec!["", "", "/"]),
        prop::collection::vec(seg, 1..=3),
        prop::sample::select(vec!["", "", "/"]),
    )
        .prop_map(|(neg, lead, segs, trail)| format!("{neg}{lead}{}{trail}", segs.join("/")))
}

fn wire_path_strategy() -> impl Strategy<Value = String> {
    let seg = prop::sample::select(vec!["a", "b", "ab", "ba", "aa", "[a]", "!a", "a]", "-"]);
    prop::collection::vec(seg, 1..=4).prop_map(|v| v.join("/"))
}

proptest! {
    #![proptest_config(cases(2048))]

    /// One pattern: same validity verdict and same match verdict as the
    /// reference, for files and dirs.
    #[test]
    fn exclude_pattern_matches_reference(raw in pattern_strategy(), path in wire_path_strategy()) {
        let ours = ExcludePattern::new(&raw);
        let theirs = reference::parse(&raw);
        prop_assert_eq!(ours.is_ok(), theirs.is_some(), "validity of {:?}", raw);
        if let (Ok(p), Some(q)) = (ours, theirs) {
            prop_assert_eq!(p.is_negated(), q.negated);
            for (kind, is_dir) in [(EntryKind::File, false), (EntryKind::Dir, true)] {
                prop_assert_eq!(
                    p.matches(&path, kind), q.matches(&path, is_dir),
                    "{:?} on {:?} ({:?})", raw, path, kind
                );
            }
        }
    }

    /// A pattern list: first-match-wins verdict and the delete-pass guard
    /// (own verdict, or any proper ancestor's verdict as a dir).
    #[test]
    fn exclude_set_matches_reference(
        raws in prop::collection::vec(pattern_strategy(), 0..4),
        path in wire_path_strategy(), is_dir in any::<bool>(),
    ) {
        let Ok(set) = ExcludeSet::from_patterns(&raws) else {
            prop_assert!(raws.iter().any(|r| reference::parse(r).is_none()));
            return Ok(());
        };
        let refs: Vec<reference::Pat> = raws.iter().map(|r| reference::parse(r).unwrap()).collect();
        let kind = if is_dir { EntryKind::Dir } else { EntryKind::File };
        prop_assert_eq!(set.matches(&path, kind), reference::excluded(&refs, &path, is_dir));
        let guarded = reference::excluded(&refs, &path, is_dir)
            || ancestors(&path).into_iter().any(|a| reference::excluded(&refs, a, true));
        prop_assert_eq!(set.matches_path_or_ancestor(&path, kind), guarded);
    }
}

proptest! {
    #![proptest_config(cases(32))]

    /// The scan's pruning rule (§16.7) on a real tree: an entry appears iff
    /// neither it nor any ancestor directory is excluded — exactly the
    /// complement of `matches_path_or_ancestor`.
    #[test]
    fn scan_with_excludes_prunes_like_the_guard(
        specs in specs_strategy(),
        raws in prop::collection::vec(
            prop::sample::select(vec!["a", "b/", "!a", "!b", "*/c", "/a/**", "c", "!c/", "**/b/c", "[ab]/*"]),
            0..4,
        ),
    ) {
        let m = build_model(&specs, 1_600_000_000);
        let dir = Scratch::new("scan_ex");
        materialize(dir.path(), &m);
        let set = ExcludeSet::from_patterns(&raws).unwrap();
        let scanned: BTreeSet<String> = TreeManifest::scan_with_excludes(dir.path(), &set)
            .unwrap()
            .entries()
            .map(|e| e.rel_path.clone())
            .collect();
        let expected: BTreeSet<String> = m
            .iter()
            .filter(|(p, n)| {
                let kind = if **n == Node::Dir { EntryKind::Dir } else { EntryKind::File };
                !set.matches_path_or_ancestor(p, kind)
            })
            .map(|(p, _)| p.clone())
            .collect();
        prop_assert_eq!(scanned, expected);
    }
}

// ===========================================================================
// 4. Path safety (§16.2)
// ===========================================================================

fn hostile_path_strategy() -> impl Strategy<Value = String> {
    let seg = prop::sample::select(vec![
        "a",
        "b",
        "..",
        ".",
        "",
        "C:",
        "c:x",
        "a.",
        "a ",
        "nul",
        "CON.txt",
        "com1",
        "lpt¹",
        "é",
        "a:b",
        "x\\y",
        "...",
        "a?",
        "<",
        "\u{1}",
        "\\\\?\\C:",
        "/",
    ]);
    prop_oneof![
        prop::collection::vec(seg, 1..=4).prop_map(|v| v.join("/")),
        prop::collection::vec(
            prop::sample::select(vec![
                'a', '.', '/', '\\', ':', 'C', ' ', '?', 'n', 'u', 'l', '\u{0}'
            ]),
            0..10,
        )
        .prop_map(|v| v.into_iter().collect()),
    ]
}

proptest! {
    #![proptest_config(cases(4096))]

    /// `validate_rel_path` accepts exactly the §16.2 language.
    #[test]
    fn validate_rel_path_is_the_spec(p in hostile_path_strategy()) {
        let spec = !p.is_empty()
            && !p.contains('\\')
            && p.split('/').all(|c| !c.is_empty() && c != "." && c != "..");
        prop_assert_eq!(tree::validate_rel_path(&p).is_ok(), spec, "{:?}", p);
    }

    /// A path both receiver checks accept names exactly one object under
    /// the root: joining it adds one Normal component per wire segment,
    /// verbatim — no escape (`..`, a drive prefix, a root), no collapse.
    #[test]
    fn accepted_paths_stay_under_root(p in hostile_path_strategy()) {
        // An implication, not prop_assume: most hostile strings are
        // (rightly) refused, and the refusals are covered above.
        if tree::validate_rel_path(&p).is_err() || tree::check_local_name(&p).is_err() {
            return Ok(());
        }
        let root = std::env::temp_dir().join("ffsync_root");
        let joined = native(&root, &p);
        prop_assert!(joined.starts_with(&root), "{:?} escapes to {:?}", p, joined);
        let tail: Vec<Component> = joined.components().skip(root.components().count()).collect();
        let segs: Vec<&str> = p.split('/').collect();
        prop_assert_eq!(tail.len(), segs.len(), "{:?} → {:?}", p, joined);
        for (c, s) in tail.iter().zip(&segs) {
            prop_assert_eq!(c, &Component::Normal(std::ffi::OsStr::new(s)));
        }
    }

    /// `manifest_from_wire` admits only validated, non-sidecar, unique
    /// paths with a known kind and size-0 dirs, none of them under a file
    /// entry (§16.2).
    #[test]
    fn manifest_from_wire_admits_only_safe_entries(
        entries in prop::collection::vec(
            // Valid paths too, so that `a` + `a/b` pairs of otherwise
            // admissible entries actually occur.
            (
                prop_oneof![hostile_path_strategy(), wire_path_strategy()],
                0u8..3,
                prop_oneof![Just(0u64), any::<u64>()],
            ),
            0..5,
        ),
        sidecar in any::<bool>(),
    ) {
        let mut wire: Vec<TreeEntryWire> = entries
            .into_iter()
            .map(|(rel_path, kind, size)| TreeEntryWire { rel_path, kind, size, mtime_secs: 0, mtime_nanos: 0, mode: 0 })
            .collect();
        if sidecar && let Some(e) = wire.first_mut() {
            e.rel_path.push_str(".ffsync-tmp");
        }
        let ok = wire.iter().all(|e| {
            tree::validate_rel_path(&e.rel_path).is_ok()
                && !tree::is_sidecar(&e.rel_path)
                && e.kind <= 1
                && (e.kind == 0 || e.size == 0)
        }) && wire.iter().map(|e| &e.rel_path).collect::<BTreeSet<_>>().len() == wire.len()
            && !wire.iter().any(|e| {
                wire.iter().any(|f| {
                    f.kind == 0
                        && e.rel_path.len() > f.rel_path.len()
                        && e.rel_path.starts_with(f.rel_path.as_str())
                        && e.rel_path.as_bytes()[f.rel_path.len()] == b'/'
                })
            });
        prop_assert_eq!(manifest_from_wire(&wire).is_ok(), ok);
    }
}

/// Creates a directory link `link` → `target` (unix symlink; Windows
/// junction, which needs no privilege — the same hazard the scan skips).
fn make_dir_link(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).unwrap();
    #[cfg(windows)]
    {
        let out = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()
            .expect("mklink /J");
        assert!(out.status.success(), "mklink /J failed: {:?}", out.stderr);
    }
}

proptest! {
    #![proptest_config(cases(16))]

    /// `link_ancestor` reports the shallowest proper ancestor that is a
    /// directory link, and its per-prefix cache never changes an answer.
    #[test]
    fn link_ancestor_finds_the_first_link(
        dirs in prop::collection::vec((prop::collection::vec(0usize..2, 1..=3), any::<bool>()), 1..6),
        queries in prop::collection::vec(prop::collection::vec(0usize..3, 1..=4), 1..8),
    ) {
        let base = Scratch::new("links");
        let root = base.path().join("root");
        let outside = base.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        // Model: each dir is real or a link; nothing is created under a link.
        let mut links: BTreeSet<String> = BTreeSet::new();
        let mut real: BTreeSet<String> = BTreeSet::new();
        for (idx, is_link) in &dirs {
            let segs: Vec<&str> = idx.iter().map(|&k| NAMES[k]).collect();
            let path = segs.join("/");
            if ancestors(&path).iter().any(|a| links.contains(*a))
                || links.contains(&path) || real.contains(&path)
            {
                continue;
            }
            for a in ancestors(&path) {
                if real.insert(a.to_string()) {
                    fs::create_dir_all(native(&root, a)).unwrap();
                }
            }
            if *is_link {
                make_dir_link(&outside, &native(&root, &path));
                links.insert(path);
            } else {
                fs::create_dir_all(native(&root, &path)).unwrap();
                real.insert(path);
            }
        }
        let mut shared = HashMap::new();
        for q in &queries {
            let rel = q.iter().map(|&k| NAMES[k]).collect::<Vec<_>>().join("/");
            let expected = ancestors(&rel).into_iter().find(|a| links.contains(*a)).map(str::to_string);
            let fresh = tree::link_ancestor(&root, &rel, &mut HashMap::new());
            let cached = tree::link_ancestor(&root, &rel, &mut shared);
            prop_assert_eq!(&fresh, &expected, "{}", rel);
            prop_assert_eq!(&cached, &expected, "{} (shared cache)", rel);
        }
    }
}

// ===========================================================================
// 5. mtimes_match: the 100 ns equivalence (D23)
// ===========================================================================

fn near_mtime() -> impl Strategy<Value = (u64, u32)> {
    prop_oneof![
        (0u64..3, 0u32..450),
        (0u64..3, 999_999_500u32..1_000_000_000),
        (any::<u64>(), 0u32..1_000_000_000),
    ]
}

proptest! {
    #![proptest_config(cases(4096))]

    /// Same 100 ns bucket — an equivalence relation, never coarser than
    /// one tick, and invariant under the receiver's NTFS truncation.
    #[test]
    fn mtimes_match_is_the_100ns_bucket(a in near_mtime(), b in near_mtime(), c in near_mtime()) {
        let m = tree::mtimes_match;
        let g = MTIME_GRANULARITY_NANOS;
        prop_assert!(m(a, a));
        prop_assert_eq!(m(a, b), m(b, a));
        if m(a, b) && m(b, c) {
            prop_assert!(m(a, c));
        }
        prop_assert_eq!(m(a, b), a.0 == b.0 && a.1 / g == b.1 / g);
        if m(a, b) {
            prop_assert!(a.1.abs_diff(b.1) < g, "{:?} {:?}", a, b);
        }
        // What an NTFS receiver stores (§16.6) still matches the original.
        prop_assert!(m(a, (a.0, a.1 - a.1 % g)));
        // Whole seconds never decide: one tick apart never matches.
        if a.1 + g < 1_000_000_000 {
            prop_assert!(!m(a, (a.0, a.1 + g)));
        }
    }
}

// ===========================================================================
// 6. Chunk coverage and the plan model (§5/§6)
// ===========================================================================

fn file_bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop_oneof![
            prop::collection::vec(any::<u8>(), 0..4096),
            (any::<u8>(), 0usize..20_000).prop_map(|(b, n)| vec![b; n]),
        ],
        0..24,
    )
    .prop_map(|parts| parts.concat())
}

fn cdc_strategy() -> impl Strategy<Value = (usize, usize, usize)> {
    prop_oneof![
        Just(cdc_params_for_size(0)),
        (32usize..=1024, 0usize..=2048, 0usize..=8192).prop_map(|(a, b, c)| {
            let min = 2 * a;
            let avg = (min.max(256) + 2 * b) & !1;
            let max = (avg.max(1024) + 2 * c) & !1;
            (min, avg, max)
        }),
    ]
    .prop_filter("sane", |&(a, b, c)| {
        sane_cdc_params((a as u32, b as u32, c as u32))
    })
}

fn scan_bytes(
    dir: &Path,
    name: &str,
    bytes: &[u8],
    cdc: (usize, usize, usize),
    workers: usize,
) -> Vec<Chunk> {
    let path = dir.join(name);
    fs::write(&path, bytes).unwrap();
    let config = PipelineConfig {
        hash_workers: workers,
        ..PipelineConfig::default()
    };
    let (hashed, chunks) = scan_file_ordered(&path, &config, cdc).unwrap();
    assert_eq!(hashed, bytes.len() as u64);
    chunks
}

/// Chunks tile `[0, size)` with no gap or overlap, respect the CDC bounds
/// (min for all but the last chunk), and hash their own bytes.
fn check_tiling(
    chunks: &[Chunk],
    bytes: &[u8],
    min: usize,
    max: usize,
) -> Result<(), TestCaseError> {
    let mut at = 0u64;
    for (i, c) in chunks.iter().enumerate() {
        prop_assert_eq!(c.offset, at, "gap or overlap at chunk {}", i);
        prop_assert!(c.length > 0 && c.length <= max);
        if i + 1 < chunks.len() {
            prop_assert!(c.length >= min, "chunk {} shorter than min", i);
        }
        let slice = &bytes[c.offset as usize..c.offset as usize + c.length];
        prop_assert_eq!(c.hash, blake3::hash(slice));
        at += c.length as u64;
    }
    prop_assert_eq!(at, bytes.len() as u64);
    // The receiver's plan-entry cap (`2*size/min + 2`) admits every honest plan.
    prop_assert!(chunks.len() as u64 <= 2 * bytes.len() as u64 / min as u64 + 2);
    Ok(())
}

proptest! {
    #![proptest_config(cases(48))]

    /// CDC scans tile the file, and the layout does not depend on the
    /// number of hash workers (thread scheduling must not move a boundary).
    #[test]
    fn cdc_chunks_tile_the_file(bytes in file_bytes_strategy(), cdc in cdc_strategy(), workers in 1usize..6) {
        let dir = Scratch::new("tile");
        let chunks = scan_bytes(dir.path(), "f", &bytes, cdc, workers);
        check_tiling(&chunks, &bytes, cdc.0, cdc.2)?;
        let again = scan_bytes(dir.path(), "g", &bytes, cdc, 1);
        let a: Vec<_> = chunks.iter().map(|c| (c.offset, c.length, c.hash)).collect();
        let b: Vec<_> = again.iter().map(|c| (c.offset, c.length, c.hash)).collect();
        prop_assert_eq!(a, b);
    }

    /// The fixed-size chunker through the same pipeline tiles the file too.
    #[test]
    fn fixed_chunks_tile_the_file(bytes in file_bytes_strategy(), size in 1usize..5000) {
        let dir = Scratch::new("fixed");
        let path = dir.path().join("f");
        fs::write(&path, &bytes).unwrap();
        let rx = process_file(&path, move || FixedChunker::new(size), Blake3Hasher, &PipelineConfig::default()).unwrap();
        let mut chunks = Vec::new();
        while let Ok(ChunkEvent::Chunk(c)) = rx.recv() {
            chunks.push(c);
        }
        chunks.sort_by_key(|c| c.offset);
        check_tiling(&chunks, &bytes, size, size)?;
    }

    /// The §6 plan model: every source chunk becomes PLAN_LOCAL (hash in
    /// the receiver's manifest) or PLAN_REMOTE. Local sources lie inside
    /// the old file and hold the target's bytes, and executing the plan
    /// reproduces the source exactly.
    #[test]
    fn plan_reconstructs_the_source(
        old in file_bytes_strategy(), cdc in cdc_strategy(),
        edits in prop::collection::vec((any::<prop::sample::Index>(), 0usize..3000, prop::collection::vec(any::<u8>(), 0..3000)), 0..4),
    ) {
        let mut new = old.clone();
        for (at, del, ins) in &edits {
            let i = if new.is_empty() { 0 } else { at.index(new.len()) };
            let end = (i + del).min(new.len());
            new.splice(i..end, ins.iter().copied());
        }
        let dir = Scratch::new("plan");
        let old_chunks = scan_bytes(dir.path(), "old", &old, cdc, 2);
        let new_chunks = scan_bytes(dir.path(), "new", &new, cdc, 2);
        let manifest: HashMap<blake3::Hash, (u64, usize)> =
            old_chunks.iter().map(|c| (c.hash, (c.offset, c.length))).collect();
        let mut out = vec![0u8; new.len()];
        for c in &new_chunks {
            let t = c.offset as usize;
            match manifest.get(&c.hash) {
                Some(&(src, len)) => {
                    prop_assert_eq!(len, c.length);
                    prop_assert!(src + len as u64 <= old.len() as u64, "local source outside the old file");
                    out[t..t + len].copy_from_slice(&old[src as usize..src as usize + len]);
                }
                None => out[t..t + c.length].copy_from_slice(&new[t..t + c.length]),
            }
        }
        prop_assert!(out == new, "reconstruction differs from the source");
    }
}

// ===========================================================================
// 7. Wire codecs (§3/§4/§8/§16)
// ===========================================================================

const HELLO_KNOWN: u16 = FLAG_TRUST_TIMES | FLAG_EAGER | FLAG_RESUMING | FLAG_DRY_RUN;
const TREE_KNOWN: u16 = TREE_FLAG_CHECKSUM
    | TREE_FLAG_NO_DELETE
    | TREE_FLAG_EXCLUDES
    | TREE_FLAG_PARALLEL
    | TREE_FLAG_METADATA
    | TREE_FLAG_NEGATION
    | TREE_FLAG_DRY_RUN
    | TREE_FLAG_EAGER;

fn hash_strategy() -> impl Strategy<Value = blake3::Hash> {
    any::<[u8; 32]>().prop_map(blake3::Hash::from_bytes)
}

fn hello_strategy() -> impl Strategy<Value = Hello> {
    (
        any::<u16>(),
        any::<u16>().prop_map(|f| f & HELLO_KNOWN),
        any::<bool>(),
        any::<(u32, u32, u32)>(),
        any::<(u64, u64, u32)>(),
        ".{0,40}",
    )
        .prop_map(
            |(version, flags, sender, cdc, (size, mtime, nanos), path)| Hello {
                version,
                flags,
                role: if sender { Role::Sender } else { Role::Receiver },
                cdc_min: cdc.0,
                cdc_avg: cdc.1,
                cdc_max: cdc.2,
                file_size: size,
                file_mtime: mtime,
                file_mtime_nanos: nanos,
                path,
            },
        )
}

fn tree_begin_strategy() -> impl Strategy<Value = TreeBegin> {
    (
        any::<u16>().prop_map(|f| f & TREE_KNOWN),
        any::<u16>(),
        prop::collection::vec(".{1,12}", 0..4),
        prop::collection::vec(
            (
                ".{0,16}",
                any::<u8>(),
                any::<u64>(),
                any::<u64>(),
                any::<u32>(),
                any::<u32>(),
            ),
            0..5,
        ),
    )
        .prop_map(|(flags, parallel, excludes, entries)| TreeBegin {
            flags,
            parallel,
            excludes,
            entries: entries
                .into_iter()
                .map(
                    |(rel_path, kind, size, mtime_secs, mtime_nanos, mode)| TreeEntryWire {
                        rel_path,
                        kind,
                        size,
                        mtime_secs,
                        mtime_nanos,
                        mode,
                    },
                )
                .collect(),
        })
}

/// What a TREE_BEGIN decodes back to: flag-gated fields take their
/// defaults when their flag is clear (parallel 1, no excludes, mode 0).
fn tree_begin_normalized(mut tb: TreeBegin) -> TreeBegin {
    if tb.flags & TREE_FLAG_PARALLEL == 0 {
        tb.parallel = 1;
    }
    if tb.flags & TREE_FLAG_EXCLUDES == 0 {
        tb.excludes.clear();
    }
    if tb.flags & TREE_FLAG_METADATA == 0 {
        for e in &mut tb.entries {
            e.mode = 0;
        }
    }
    tb
}

fn plan_strategy() -> impl Strategy<Value = PlanEntry> {
    prop_oneof![
        (any::<u64>(), any::<u32>(), any::<u64>(), hash_strategy()).prop_map(|(t, l, s, h)| {
            PlanEntry::Local {
                target_offset: t,
                length: l,
                source_offset: s,
                hash: h,
            }
        }),
        (any::<u64>(), any::<u32>(), hash_strategy()).prop_map(|(t, l, h)| PlanEntry::Remote {
            target_offset: t,
            length: l,
            hash: h,
        }),
    ]
}

/// Flips one byte of `payload` (if any) — "near-valid" input for the
/// canonical-encoding properties.
fn mutate(mut payload: Vec<u8>, at: prop::sample::Index, xor: u8) -> Vec<u8> {
    if !payload.is_empty() {
        let i = at.index(payload.len());
        payload[i] ^= xor;
    }
    payload
}

proptest! {
    #![proptest_config(cases(1024))]

    #[test]
    fn frame_round_trip_and_truncation(ftype in any::<u8>(), payload in prop::collection::vec(any::<u8>(), 0..300), cut in any::<prop::sample::Index>()) {
        let f = Frame::new(ftype, payload);
        let mut buf = Vec::new();
        f.encode(&mut buf).unwrap();
        prop_assert_eq!(buf.len(), f.encoded_len());
        prop_assert_eq!(&read_frame(&mut &buf[..]).unwrap(), &f);
        // Any strict prefix: Eof when nothing arrived, Truncated otherwise.
        let n = cut.index(buf.len());
        match read_frame(&mut &buf[..n]) {
            Err(FrameError::Eof) => prop_assert_eq!(n, 0),
            Err(FrameError::Truncated) => prop_assert!(n > 0),
            other => prop_assert!(false, "prefix {} of {} decoded: {:?}", n, buf.len(), other),
        }
    }

    #[test]
    fn read_frame_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        if let Ok(f) = read_frame(&mut &bytes[..]) {
            prop_assert!(f.payload.len() <= MAX_PAYLOAD as usize);
            prop_assert!(5 + f.payload.len() <= bytes.len());
            prop_assert_eq!(&bytes[5..5 + f.payload.len()], &f.payload[..]);
        }
    }

    #[test]
    fn hello_round_trip(h in hello_strategy()) {
        let f = h.clone().into_frame().unwrap();
        prop_assert_eq!(Hello::from_frame(f).unwrap(), h);
    }

    /// Strict codec: any HELLO with a flag bit this build does not know is
    /// refused at decode.
    #[test]
    fn hello_rejects_unknown_flag_bits(h in hello_strategy(), extra in any::<u16>()) {
        let extra = extra & !HELLO_KNOWN;
        prop_assume!(extra != 0);
        let mut h = h;
        h.flags |= extra;
        prop_assert!(Hello::from_frame(h.into_frame().unwrap()).is_err());
    }

    /// Canonical encoding: whatever a decoder accepts re-encodes to the
    /// same bytes (no two wire forms for one message).
    #[test]
    fn decoders_are_canonical(
        h in hello_strategy(), tb in tree_begin_strategy(), pe in plan_strategy(),
        at in any::<prop::sample::Index>(), xor in 1u8..=255,
    ) {
        let p = mutate(h.into_frame().unwrap().payload, at, xor);
        if let Ok(d) = Hello::from_frame(Frame::new(framing::T_HELLO, p.clone())) {
            prop_assert_eq!(d.into_frame().unwrap().payload, p);
        }
        let p = mutate(tb.into_frame().unwrap().payload, at, xor);
        if let Ok(d) = TreeBegin::from_frame(Frame::new(framing::T_TREE_BEGIN, p.clone())) {
            prop_assert_eq!(d.into_frame().unwrap().payload, p);
        }
        let f = pe.into_frame().unwrap();
        let p = mutate(f.payload, at, xor);
        if let Ok(d) = PlanEntry::from_frame(Frame::new(f.ftype, p.clone())) {
            prop_assert_eq!(d.into_frame().unwrap().payload, p);
        }
    }

    #[test]
    fn tree_begin_round_trip(tb in tree_begin_strategy()) {
        let f = tb.clone().into_frame().unwrap();
        prop_assert_eq!(TreeBegin::from_frame(f).unwrap(), tree_begin_normalized(tb));
    }

    #[test]
    fn tree_begin_rejects_unknown_flag_bits(tb in tree_begin_strategy(), extra in any::<u16>()) {
        let extra = extra & !TREE_KNOWN;
        prop_assume!(extra != 0);
        let mut tb = tb;
        tb.flags |= extra;
        prop_assert!(TreeBegin::from_frame(tb.into_frame().unwrap()).is_err());
    }

    #[test]
    fn small_messages_round_trip(
        hash in hash_strategy(), offset in any::<u64>(), length in any::<u32>(), pe in plan_strategy(),
        worker in any::<u16>(), rel in ".{0,40}", count in any::<u32>(), bytes in any::<u64>(),
        code in 1u16..=6, msg in ".{0,40}", data in prop::collection::vec(any::<u8>(), 0..100),
    ) {
        let mc = ManifestChunk { hash, offset, length };
        prop_assert_eq!(ManifestChunk::from_frame(mc.into_frame().unwrap()).unwrap(), mc);
        prop_assert_eq!(PlanEntry::from_frame(pe.into_frame().unwrap()).unwrap(), pe);
        let tw = TreeWorker { worker_index: worker };
        prop_assert_eq!(TreeWorker::from_frame(tw.clone().into_frame().unwrap()).unwrap(), tw);
        let tf = TreeFile { rel_path: rel };
        prop_assert_eq!(TreeFile::from_frame(tf.clone().into_frame().unwrap()).unwrap(), tf);
        let te = TreeEnd { deleted_count: count, deleted_bytes: bytes };
        prop_assert_eq!(TreeEnd::from_frame(te.clone().into_frame().unwrap()).unwrap(), te);
        let code = [ErrorCode::Version, ErrorCode::RoleConflict, ErrorCode::BadFrame,
            ErrorCode::HashMismatch, ErrorCode::Io, ErrorCode::Aborted][code as usize - 1];
        let (c, m) = read_error_payload(&encode_error(code, &msg).unwrap()).unwrap();
        prop_assert_eq!((c, m), (code, msg));
        let (h, d) = split_data_payload(encode_data(hash, &data).unwrap().payload).unwrap();
        prop_assert_eq!((h, d), (hash, data));
    }

    /// COMMIT_ACK (D24): the outcome byte round-trips; empty = pre-D24
    /// receiver; anything else is malformed.
    #[test]
    fn commit_ack_outcome(written in any::<bool>(), payload in prop::collection::vec(any::<u8>(), 0..3)) {
        let f = encode_commit_ack(written).unwrap();
        prop_assert_eq!(f.ftype, framing::T_COMMIT_ACK);
        prop_assert_eq!(decode_commit_ack(&f.payload).unwrap(), Some(written));
        let expected = match payload.as_slice() {
            [] => Some(None),
            [0] => Some(Some(false)),
            [1] => Some(Some(true)),
            _ => None,
        };
        prop_assert_eq!(decode_commit_ack(&payload).ok(), expected);
    }

    /// No decoder panics on arbitrary payloads of its own frame type.
    #[test]
    fn decoders_never_panic(payload in prop::collection::vec(any::<u8>(), 0..96), local in any::<bool>()) {
        let _ = Hello::from_frame(Frame::new(framing::T_HELLO, payload.clone()));
        let _ = TreeBegin::from_frame(Frame::new(framing::T_TREE_BEGIN, payload.clone()));
        let _ = ManifestChunk::from_frame(Frame::new(framing::T_MANIFEST_CHUNK, payload.clone()));
        let t = if local { framing::T_PLAN_LOCAL } else { framing::T_PLAN_REMOTE };
        let _ = PlanEntry::from_frame(Frame::new(t, payload.clone()));
        let _ = TreeFile::from_frame(Frame::new(framing::T_TREE_FILE, payload.clone()));
        let _ = TreeEnd::from_frame(Frame::new(framing::T_TREE_END, payload.clone()));
        let _ = TreeWorker::from_frame(Frame::new(framing::T_TREE_WORKER, payload.clone()));
        let _ = read_error_payload(&Frame::new(framing::T_ERROR, payload.clone()));
        let _ = decode_commit_ack(&payload);
        let _ = split_data_payload(payload);
    }
}
