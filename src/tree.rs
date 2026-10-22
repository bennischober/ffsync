//! Directory-tree sync (D17): tree scan, wire manifest codec, and triage.
//!
//! A tree session (protocol v3, `concept/Protocol v0.1.md` §16) exchanges
//! both peers' tree manifests, triages locally and deterministically, then
//! runs the existing per-file state machines sequentially inside a
//! TREE_BEGIN/TREE_END envelope.
//!
//! Everything here is pure computation over metadata — no I/O beyond the
//! initial scan, no protocol frames — so triage rules are exhaustively
//! unit-testable.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Kind of a tree entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File = 0,
    Dir = 1,
}

impl EntryKind {
    pub(crate) fn from_u8(v: u8) -> Option<EntryKind> {
        match v {
            0 => Some(EntryKind::File),
            1 => Some(EntryKind::Dir),
            _ => None,
        }
    }
}

/// One entry of the tree manifest: relative path + the metadata the triage
/// decides on. Directories exist in the manifest only to create empty dirs
/// on the receiver (a dir with files is implied by its files).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// `/`-separated path relative to the synced root, no `..` components.
    pub rel_path: String,
    pub kind: EntryKind,
    pub size: u64,
    /// Unix seconds (0 if absent) — same convention as the per-file HELLO.
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
}

impl TreeEntry {
    /// Scans one filesystem path into an entry (absolute or root-relative
    /// input; the caller supplies the relative path).
    pub fn from_metadata(rel_path: String, kind: EntryKind, meta: &std::fs::Metadata) -> Self {
        let mtime = meta.modified().ok();
        let (mtime_secs, mtime_nanos) = mtime
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| (d.as_secs(), d.subsec_nanos()))
            .unwrap_or((0, 0));
        Self {
            rel_path,
            kind,
            size: if kind == EntryKind::File {
                meta.len()
            } else {
                0
            },
            mtime_secs,
            mtime_nanos,
        }
    }
}

/// A whole-tree manifest, sorted by rel_path (byte order — the wire format's
/// canonical order, §16.2). The BTreeMap keyed by rel_path is the in-memory
/// form; the wire form is the sorted entry list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TreeManifest {
    entries: BTreeMap<String, TreeEntry>,
}

impl TreeManifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an entry, replacing any previous entry for the same path.
    pub fn insert(&mut self, entry: TreeEntry) {
        self.entries.insert(entry.rel_path.clone(), entry);
    }

    pub fn get(&self, rel_path: &str) -> Option<&TreeEntry> {
        self.entries.get(rel_path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Entries in canonical (sorted) order.
    pub fn entries(&self) -> impl Iterator<Item = &TreeEntry> {
        self.entries.values()
    }

    /// Total file bytes (for progress and the delete report).
    pub fn total_file_bytes(&self) -> u64 {
        self.entries
            .values()
            .filter(|e| e.kind == EntryKind::File)
            .map(|e| e.size)
            .sum()
    }

    /// Scans a directory tree rooted at `root` into a manifest. Follows
    /// symlinks (a symlink is recorded as its target type; a dangling
    /// symlink is skipped with a warning — D17, recorded not silent).
    /// The returned error is only for fatal I/O on the root itself.
    pub fn scan(root: &Path) -> io::Result<TreeManifest> {
        Self::scan_with_excludes(root, &ExcludeSet::new())
    }

    /// [`TreeManifest::scan`] with exclude patterns (§16.7): an excluded
    /// file is skipped; an excluded directory is pruned with its whole
    /// subtree (contents cannot re-include — there is no negation syntax).
    /// The sidecar filter (`*.ffsync-index` / `*.ffsync-tmp`) applies in
    /// addition and is not user-visible.
    pub fn scan_with_excludes(root: &Path, excludes: &ExcludeSet) -> io::Result<TreeManifest> {
        let mut manifest = TreeManifest::new();
        let root_meta = std::fs::metadata(root)?;
        if !root_meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tree root is not a directory: {}", root.display()),
            ));
        }
        Self::scan_dir(root, "", excludes, &mut manifest)?;
        Ok(manifest)
    }

    fn scan_dir(
        dir: &Path,
        prefix: &str,
        excludes: &ExcludeSet,
        out: &mut TreeManifest,
    ) -> io::Result<()> {
        let mut entries: Vec<(String, PathBuf, std::fs::Metadata)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let meta = match entry.metadata() {
                Ok(m) => m,
                // A file deleted between read_dir and metadata: skip, the
                // next sync sees a consistent state. Anything else is fatal.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            entries.push((rel, entry.path(), meta));
        }
        // Deterministic order regardless of filesystem enumeration order.
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (rel, path, meta) in entries {
            if meta.is_dir() {
                // Excluded directory: prune the whole subtree (§16.7).
                if excludes.matches(&rel, EntryKind::Dir) {
                    continue;
                }
                out.insert(TreeEntry::from_metadata(rel.clone(), EntryKind::Dir, &meta));
                Self::scan_dir(&path, &rel, excludes, out)?;
            } else if meta.is_file() {
                // D15/D17: the sidecar index and the temp file are ffsync's
                // own bookkeeping, never tree content. The index filename is
                // `<target>.ffsync-index` (suffix APPENDED — see index.rs);
                // the temp is `<target-with-ext>.ffsync-tmp`. Both are
                // excluded from tree manifests: syncing them would (a) sync
                // our bookkeeping to the peer and (b) desync on every run
                // (an index written at commit changes mtime → re-transfer
                // loop).
                if rel.ends_with(".ffsync-index") || rel.ends_with(".ffsync-tmp") {
                    continue;
                }
                if excludes.matches(&rel, EntryKind::File) {
                    continue;
                }
                out.insert(TreeEntry::from_metadata(rel, EntryKind::File, &meta));
            } else {
                // Symlink to something else (fifo, socket, ...): skip with a
                // warning (D17: recorded, not silent).
                eprintln!("warning: skipping non-regular file: {}", path.display());
            }
        }
        Ok(())
    }
}

/// Tree-level flags carried by TREE_BEGIN (§16.2). The `excludes` bit
/// (§16.7) lives with the TREE_BEGIN codec in `protocol::message`, next to
/// the layout it gates.
pub const TREE_FLAG_CHECKSUM: u16 = 1 << 0; // D11 tree-wide: no fast path
pub const TREE_FLAG_NO_DELETE: u16 = 1 << 1; // §16.5 opt-out

// ---- Exclude patterns (§16.7) ----
//
// Hand-rolled glob matcher (same rationale as the hand-rolled codecs: the
// grammar is small, the semantics are ours to pin, and no dependency).

/// One parsed segment of a pattern between `/` separators.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternSeg {
    /// `**` as a complete segment: any number of path segments (incl. zero).
    DoubleStar,
    /// A within-segment sub-pattern (no `/` inside).
    Parts(Vec<Part>),
}

/// One element of a within-segment sub-pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Lit(char),
    /// `*`: any run of characters (within the segment).
    Star,
    /// `?`: exactly one character.
    One,
    /// `[...]` character class, matching exactly one character.
    Class {
        negated: bool,
        items: Vec<ClassItem>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClassItem {
    Char(char),
    Range(char, char),
}

/// One exclude pattern (§16.7 grammar): anchored vs basename, `*`/`?`/`[]`
/// classes, `**` segments, optional dir-only trailing `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludePattern {
    raw: String,
    anchored: bool,
    dir_only: bool,
    segs: Vec<PatternSeg>,
}

impl ExcludePattern {
    /// Parses and validates one pattern. A `\` anywhere is a parse error
    /// (§16.7: no backslash escapes; wire paths are `/`-separated).
    pub fn new(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("empty exclude pattern".to_string());
        }
        if raw.contains('\\') {
            return Err(format!(
                "backslash in exclude pattern (no escapes, paths are /-separated): {raw:?}"
            ));
        }
        let dir_only = raw.ends_with('/');
        let core = raw.trim_end_matches('/');
        if core.is_empty() {
            return Err(format!("exclude pattern is only a slash: {raw:?}"));
        }
        // Anchor test BEFORE stripping the leading '/': a leading slash (or
        // any internal one) anchors the pattern to the tree root (§16.7).
        let anchored = core.contains('/');
        let core = core.strip_prefix('/').unwrap_or(core);
        let mut segs = Vec::new();
        for seg in core.split('/') {
            if seg == "**" {
                segs.push(PatternSeg::DoubleStar);
            } else {
                segs.push(PatternSeg::Parts(parse_parts(seg, raw)?));
            }
        }
        Ok(Self {
            raw: raw.to_string(),
            anchored,
            dir_only,
            segs,
        })
    }

    /// The pattern as given (wire form).
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Does this pattern match one tree entry?
    pub fn matches(&self, rel_path: &str, kind: EntryKind) -> bool {
        if self.dir_only && kind != EntryKind::Dir {
            return false;
        }
        let path_segs: Vec<&str> = rel_path.split('/').collect();
        if self.anchored {
            match_segs(&self.segs, &path_segs)
        } else {
            // Unanchored: match the basename at any depth.
            (0..path_segs.len()).any(|start| match_segs(&self.segs, &path_segs[start..]))
        }
    }
}

/// Parses one within-segment sub-pattern. An unterminated `[` is literal
/// (§16.7); consecutive `*` collapse into one `Star`.
fn parse_parts(seg: &str, whole: &str) -> Result<Vec<Part>, String> {
    let chars: Vec<char> = seg.chars().collect();
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                while i < chars.len() && chars[i] == '*' {
                    i += 1;
                }
                parts.push(Part::Star);
            }
            '?' => {
                parts.push(Part::One);
                i += 1;
            }
            '[' => {
                match parse_class(&chars, i) {
                    Some((class, next)) => {
                        parts.push(class);
                        i = next;
                    }
                    // Unterminated: '[' is literal (§16.7).
                    None => {
                        parts.push(Part::Lit('['));
                        i += 1;
                    }
                }
            }
            c => {
                parts.push(Part::Lit(c));
                i += 1;
            }
        }
    }
    if parts.is_empty() {
        // An empty segment cannot occur in a valid wire path; a pattern
        // with one (e.g. "a//b") is a user error worth naming.
        return Err(format!("empty segment in exclude pattern: {whole:?}"));
    }
    Ok(parts)
}

/// Parses `[...]` starting at `chars[start] == '['`. Returns the class and
/// the index just past `]`, or `None` if unterminated.
fn parse_class(chars: &[char], start: usize) -> Option<(Part, usize)> {
    let mut i = start + 1; // past '['
    let negated = i < chars.len() && chars[i] == '!';
    if negated {
        i += 1;
    }
    let mut items = Vec::new();
    // A ']' immediately after '[' (or '!') is a literal member (POSIX rule).
    let mut first = true;
    while i < chars.len() {
        if chars[i] == ']' && !first {
            return Some((Part::Class { negated, items }, i + 1));
        }
        first = false;
        let lo = chars[i];
        // Range "a-z" (the '-' is literal at the end of the class).
        if i + 2 < chars.len() && chars[i + 1] == '-' && chars[i + 2] != ']' {
            items.push(ClassItem::Range(lo, chars[i + 2]));
            i += 3;
        } else {
            items.push(ClassItem::Char(lo));
            i += 1;
        }
    }
    None // unterminated
}

/// Matches pattern segments against path segments (no `**` backtracking
/// beyond its own: it tries every consumption width).
fn match_segs(segs: &[PatternSeg], path: &[&str]) -> bool {
    match segs.split_first() {
        None => path.is_empty(),
        Some((PatternSeg::DoubleStar, rest)) => {
            // Zero segments, then every possible consumption.
            (0..=path.len()).any(|skip| match_segs(rest, &path[skip..]))
        }
        Some((PatternSeg::Parts(parts), rest)) => {
            let Some((head, tail)) = path.split_first() else {
                return false;
            };
            match_parts(parts, head) && match_segs(rest, tail)
        }
    }
}

/// Classic glob match of one sub-pattern against one path segment
/// (characters only — `*` never crosses `/` because segments are split).
/// Iterative with single-star backtracking (consecutive Stars collapse at
/// parse time, so one backtrack point suffices).
fn match_parts(parts: &[Part], text: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    // Backtrack point: (part index OF the Star, chars it has swallowed so far).
    let mut star: Option<(usize, usize)> = None;
    while ti < text.len() {
        let advanced = if pi < parts.len() {
            match &parts[pi] {
                Part::Star => {
                    // The Star matches the empty run first; backtracking
                    // grows its match if the rest fails.
                    star = Some((pi, ti));
                    pi += 1;
                    true
                }
                Part::One => {
                    pi += 1;
                    ti += 1;
                    true
                }
                Part::Lit(c) => {
                    if text[ti] == *c {
                        pi += 1;
                        ti += 1;
                        true
                    } else {
                        false
                    }
                }
                Part::Class { negated, items } => {
                    let c = text[ti];
                    let hit = items.iter().any(|it| match it {
                        ClassItem::Char(x) => *x == c,
                        ClassItem::Range(lo, hi) => c >= *lo && c <= *hi,
                    });
                    if hit != *negated {
                        pi += 1;
                        ti += 1;
                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        };
        if advanced {
            continue;
        }
        match star {
            // Retry: let the last seen Star swallow one more character.
            Some((sp, st)) => {
                let st = st + 1;
                if st > text.len() {
                    return false;
                }
                star = Some((sp, st));
                pi = sp + 1;
                ti = st;
            }
            None => return false,
        }
    }
    // Trailing Stars match the empty remainder.
    while pi < parts.len() && parts[pi] == Part::Star {
        pi += 1;
    }
    pi == parts.len()
}

/// A validated set of exclude patterns (§16.7). Matching is pure; the set
/// travels the wire as its raw pattern strings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExcludeSet {
    patterns: Vec<ExcludePattern>,
}

impl ExcludeSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a set, validating every pattern (first error wins).
    pub fn from_patterns<I, S>(raws: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut set = Self::new();
        for raw in raws {
            set.push(raw.as_ref())?;
        }
        Ok(set)
    }

    /// Adds one pattern.
    pub fn push(&mut self, raw: &str) -> Result<(), String> {
        self.patterns.push(ExcludePattern::new(raw)?);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    /// The raw pattern strings (wire form, §16.7).
    pub fn raws(&self) -> Vec<String> {
        self.patterns.iter().map(|p| p.raw().to_string()).collect()
    }

    /// Does any pattern match this entry?
    pub fn matches(&self, rel_path: &str, kind: EntryKind) -> bool {
        self.patterns.iter().any(|p| p.matches(rel_path, kind))
    }

    /// Delete-pass protection (§16.7): the path itself matches, or any of
    /// its ancestor directories matches (an excluded directory protects its
    /// whole subtree from deletion).
    pub fn matches_path_or_ancestor(&self, rel_path: &str, kind: EntryKind) -> bool {
        if self.matches(rel_path, kind) {
            return true;
        }
        let mut prefix = String::new();
        for seg in rel_path.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            if self.matches(&prefix, EntryKind::Dir) {
                return true;
            }
        }
        false
    }
}

/// What the receiver should do with one path (§16.3). Computed identically
/// on both sides from the same two manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Triage {
    /// Metadata matches (and no `checksum` flag): nothing to do.
    Skip,
    /// Exists on both sides as files, metadata differs: per-file conversation.
    Sync,
    /// Sender-only file: per-file conversation; receiver starts from nothing.
    Transfer,
    /// Sender-only directory: the receiver creates it locally (no
    /// conversation — a directory carries no content).
    CreateDir,
    /// Receiver-only file: deferred deletion (§16.5), gated on `no_delete`.
    Delete,
    /// File on one side, dir on the other. The sender's kind wins: the
    /// receiver removes its own entry of the other kind and applies the
    /// sender's. Carries the sender's kind for the receiver to enforce.
    Conflict {
        sender_kind: EntryKind,
        /// Also delete files/dirs under the receiver's conflicting entry.
        remove_receiver_entry: bool,
    },
}

impl fmt::Display for Triage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Triage::Skip => write!(f, "skip"),
            Triage::Sync => write!(f, "sync"),
            Triage::Transfer => write!(f, "transfer"),
            Triage::CreateDir => write!(f, "create-dir"),
            Triage::Delete => write!(f, "delete"),
            Triage::Conflict { .. } => write!(f, "conflict"),
        }
    }
}

/// The triage of one path, given both sides' entries (`None` = absent).
/// Pure function — identical on both peers (§16.4's determinism contract).
pub fn triage(
    _rel_path: &str,
    sender: Option<&TreeEntry>,
    receiver: Option<&TreeEntry>,
    checksum: bool,
) -> Triage {
    match (sender, receiver) {
        (None, None) => Triage::Skip, // not in either tree; unreachable via manifests
        (Some(s), None) => match s.kind {
            EntryKind::File => Triage::Transfer,
            // Sender-only dir: the receiver creates it (empty dirs are
            // first-class tree members, §16.2/§16.3).
            EntryKind::Dir => Triage::CreateDir,
        },
        (None, Some(_r)) => {
            // Receiver-only content: file deleted; a receiver-only empty dir
            // is also removed (the sender's tree is authoritative), but an
            // empty dir carries no data — its removal is part of the same
            // delete pass.
            Triage::Delete
        }
        (Some(s), Some(r)) => {
            match (s.kind, r.kind) {
                (EntryKind::Dir, EntryKind::Dir) => Triage::Skip,
                (EntryKind::File, EntryKind::File) => {
                    if !checksum
                        && s.size == r.size
                        && s.mtime_secs == r.mtime_secs
                        && s.mtime_nanos == r.mtime_nanos
                    {
                        Triage::Skip
                    } else {
                        Triage::Sync
                    }
                }
                // Kind conflict: the sender's kind wins (§16.3).
                _ => Triage::Conflict {
                    sender_kind: s.kind,
                    remove_receiver_entry: true,
                },
            }
        }
    }
}

/// Validates a relative path per §16.2: no empty components, no `..`, no
/// leading/trailing slash, no backslash (Windows is source-side too; the
/// wire form is always `/`-separated).
pub fn validate_rel_path(p: &str) -> Result<(), String> {
    if p.is_empty() {
        return Err("empty path".to_string());
    }
    if p.starts_with('/') || p.ends_with('/') {
        return Err(format!("leading or trailing slash: {p:?}"));
    }
    if p.contains('\\') {
        return Err(format!(
            "backslash in path (wire form is /-separated): {p:?}"
        ));
    }
    if p.split('/').any(|c| c.is_empty() || c == "." || c == "..") {
        return Err(format!("empty, '.', or '..' path component: {p:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, kind: EntryKind, size: u64, secs: u64, nanos: u32) -> TreeEntry {
        TreeEntry {
            rel_path: path.to_string(),
            kind,
            size,
            mtime_secs: secs,
            mtime_nanos: nanos,
        }
    }

    #[test]
    fn triage_metadata_match_skips() {
        let s = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        let r = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r), false), Triage::Skip);
        // checksum mode forces Sync despite metadata equality (D11).
        assert_eq!(triage("a.bin", Some(&s), Some(&r), true), Triage::Sync);
    }

    #[test]
    fn triage_size_or_mtime_difference_syncs() {
        let s = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        // Size differs.
        let r1 = entry("a.bin", EntryKind::File, 101, 1600000000, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r1), false), Triage::Sync);
        // Only sub-second mtime differs (the v0.2 lesson, now in triage).
        let r2 = entry("a.bin", EntryKind::File, 100, 1600000000, 501);
        assert_eq!(triage("a.bin", Some(&s), Some(&r2), false), Triage::Sync);
        // Whole-second equal, nanos equal, but secs differ.
        let r3 = entry("a.bin", EntryKind::File, 100, 1600000001, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r3), false), Triage::Sync);
    }

    #[test]
    fn triage_one_sided_and_kind_conflicts() {
        let f = entry("x", EntryKind::File, 5, 1, 0);
        let d = entry("x", EntryKind::Dir, 0, 1, 0);
        // Sender-only file → transfer; sender-only dir → create on receiver.
        assert_eq!(triage("x", Some(&f), None, false), Triage::Transfer);
        assert_eq!(triage("x", Some(&d), None, false), Triage::CreateDir);
        // Receiver-only anything → delete (sender authoritative).
        assert_eq!(triage("x", None, Some(&f), false), Triage::Delete);
        assert_eq!(triage("x", None, Some(&d), false), Triage::Delete);
        // Kind conflict: sender's kind wins.
        assert_eq!(
            triage("x", Some(&f), Some(&d), false),
            Triage::Conflict {
                sender_kind: EntryKind::File,
                remove_receiver_entry: true
            }
        );
        assert_eq!(
            triage("x", Some(&d), Some(&f), false),
            Triage::Conflict {
                sender_kind: EntryKind::Dir,
                remove_receiver_entry: true
            }
        );
    }

    #[test]
    fn rel_path_validation() {
        assert!(validate_rel_path("a").is_ok());
        assert!(validate_rel_path("a/b/c.txt").is_ok());
        assert!(validate_rel_path("").is_err());
        assert!(validate_rel_path("/abs").is_err());
        assert!(validate_rel_path("a/").is_err());
        assert!(validate_rel_path("a//b").is_err());
        assert!(validate_rel_path("a/../b").is_err());
        assert!(validate_rel_path("a/./b").is_err());
        assert!(validate_rel_path("..").is_err());
        assert!(validate_rel_path("a\\b").is_err());
    }

    #[test]
    fn manifest_scan_and_canonical_order() {
        let dir = std::env::temp_dir().join(format!("ffsync_tree_scan_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("sub/deep")).unwrap();
        std::fs::create_dir_all(dir.join("empty_dir")).unwrap();
        std::fs::write(dir.join("b.txt"), b"bbb").unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        std::fs::write(dir.join("sub/one.bin"), b"1111").unwrap();
        std::fs::write(dir.join("sub/deep/two.bin"), b"22").unwrap();

        let m = TreeManifest::scan(&dir).unwrap();
        let paths: Vec<&str> = m.entries().map(|e| e.rel_path.as_str()).collect();
        // Sorted, with dirs and empty_dir present.
        assert_eq!(
            paths,
            vec![
                "a.txt",
                "b.txt",
                "empty_dir",
                "sub",
                "sub/deep",
                "sub/deep/two.bin",
                "sub/one.bin"
            ]
        );
        // File sizes recorded.
        assert_eq!(m.get("a.txt").unwrap().size, 1);
        assert_eq!(m.get("sub/one.bin").unwrap().size, 4);
        assert_eq!(m.get("empty_dir").unwrap().kind, EntryKind::Dir);
        // Total bytes: 1 + 3 + 4 + 2.
        assert_eq!(m.total_file_bytes(), 10);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_rejects_non_directory_root() {
        let f = std::env::temp_dir().join(format!("ffsync_tree_notadir_{}", std::process::id()));
        std::fs::write(&f, b"x").unwrap();
        assert!(TreeManifest::scan(&f).is_err());
        let _ = std::fs::remove_file(&f);
    }

    // ---- Exclude pattern grammar (§16.7) ----

    fn pat(raw: &str) -> ExcludePattern {
        ExcludePattern::new(raw).unwrap()
    }

    fn m(raw: &str, path: &str, kind: EntryKind) -> bool {
        pat(raw).matches(path, kind)
    }

    #[test]
    fn exclude_basename_patterns_match_at_any_depth() {
        assert!(m("*.tmp", "a.tmp", EntryKind::File));
        assert!(m("*.tmp", "deep/dir/a.tmp", EntryKind::File));
        assert!(!m("*.tmp", "dir.tmp/x", EntryKind::File)); // matches basename only
        assert!(m("node_modules", "node_modules", EntryKind::Dir));
        assert!(m("node_modules", "a/node_modules", EntryKind::Dir));
        assert!(!m("node_modules", "node_modules/x", EntryKind::File)); // dir itself, not contents
    }

    #[test]
    fn exclude_anchored_patterns_match_from_root_only() {
        assert!(m("/build", "build", EntryKind::Dir));
        assert!(!m("/build", "sub/build", EntryKind::Dir));
        assert!(m("doc/*.md", "doc/readme.md", EntryKind::File));
        assert!(!m("doc/*.md", "doc/sub/readme.md", EntryKind::File));
        assert!(m("a/b/c", "a/b/c", EntryKind::File));
    }

    #[test]
    fn exclude_star_never_crosses_slash() {
        assert!(m("*.c", "main.c", EntryKind::File));
        // Unanchored: basename matching at any depth.
        assert!(m("*.c", "a/main.c", EntryKind::File));
        assert!(m("/*.c", "main.c", EntryKind::File));
        assert!(!m("/*.c", "a/main.c", EntryKind::File)); // anchored: no depth
        assert!(!m("/a/*/f", "a/b/c/f", EntryKind::File)); // '*' is one segment
        assert!(m("/a/*/f", "a/b/f", EntryKind::File));
    }

    #[test]
    fn exclude_question_mark_is_exactly_one_char() {
        assert!(m("file?.txt", "file1.txt", EntryKind::File));
        assert!(!m("file?.txt", "file12.txt", EntryKind::File));
        assert!(!m("file?.txt", "file.txt", EntryKind::File));
        assert!(m("a?c", "abc", EntryKind::File));
    }

    #[test]
    fn exclude_character_classes() {
        assert!(m("[abc].txt", "a.txt", EntryKind::File));
        assert!(m("[a-c].txt", "b.txt", EntryKind::File));
        assert!(!m("[a-c].txt", "d.txt", EntryKind::File));
        assert!(m("[!a-c].txt", "d.txt", EntryKind::File));
        assert!(!m("[!a-c].txt", "b.txt", EntryKind::File));
        // Unterminated '[' is literal.
        assert!(m("a[.txt", "a[.txt", EntryKind::File));
        assert!(!m("a[.txt", "ab.txt", EntryKind::File));
    }

    #[test]
    fn exclude_double_star_spans_directories() {
        // Zero segments or many.
        assert!(m("logs/**/*.log", "logs/a.log", EntryKind::File));
        assert!(m("logs/**/*.log", "logs/a/b/a.log", EntryKind::File));
        assert!(!m("logs/**/*.log", "other/a.log", EntryKind::File));
        assert!(m("a/**/b", "a/b", EntryKind::File));
        assert!(m("a/**/b", "a/x/y/b", EntryKind::File));
        // '**' inside a segment collapses to '*': one segment, no '/'.
        assert!(m("a**b", "axxb", EntryKind::File));
        assert!(!m("a**b", "a/b", EntryKind::File));
    }

    #[test]
    fn exclude_dir_only_trailing_slash() {
        assert!(m("build/", "build", EntryKind::Dir));
        assert!(!m("build/", "build", EntryKind::File));
        assert!(m("cache/", "deep/cache", EntryKind::Dir));
        assert!(!m("cache/", "deep/cache", EntryKind::File));
    }

    #[test]
    fn exclude_parse_errors() {
        assert!(ExcludePattern::new("").is_err());
        assert!(ExcludePattern::new("a\\b").is_err()); // no escapes
        assert!(ExcludePattern::new("/").is_err());
        assert!(ExcludePattern::new("a//b").is_err()); // empty segment
    }

    #[test]
    fn exclude_set_path_or_ancestor_protection() {
        let set = ExcludeSet::from_patterns(["node_modules", "*.lock"]).unwrap();
        // Direct match.
        assert!(set.matches_path_or_ancestor("node_modules", EntryKind::Dir));
        // Ancestor protection: a file under an excluded dir is protected.
        assert!(set.matches_path_or_ancestor("node_modules/pkg/index.js", EntryKind::File));
        // Unrelated path: no protection.
        assert!(!set.matches_path_or_ancestor("src/main.rs", EntryKind::File));
        // Bare-name pattern anchored to an ancestor segment.
        let set2 = ExcludeSet::from_patterns(["target"]).unwrap();
        assert!(set2.matches_path_or_ancestor("target/debug/deps/a", EntryKind::File));
    }

    #[test]
    fn scan_with_excludes_prunes_dirs_and_files() {
        let dir = std::env::temp_dir().join(format!("ffsync_tree_excl_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("node_modules/pkg")).unwrap();
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("node_modules/pkg/index.js"), b"js").unwrap();
        std::fs::write(dir.join("src/main.rs"), b"rs").unwrap();
        std::fs::write(dir.join("debug.log"), b"log").unwrap();
        std::fs::write(dir.join("keep.log"), b"keep").unwrap();

        let ex = ExcludeSet::from_patterns(["node_modules", "*.log"]).unwrap();
        let manifest = TreeManifest::scan_with_excludes(&dir, &ex).unwrap();
        let paths: Vec<&str> = manifest.entries().map(|e| e.rel_path.as_str()).collect();
        // node_modules (dir + contents) and both *.log files are gone; the
        // 'src' dir itself remains (needed to create it on the receiver).
        assert_eq!(paths, vec!["src", "src/main.rs"]);

        // No excludes = plain scan.
        let full = TreeManifest::scan_with_excludes(&dir, &ExcludeSet::new()).unwrap();
        assert_eq!(full.len(), 7); // 3 dirs + 4 files

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn exclude_round_trip_through_wire_form() {
        let raws = ["node_modules", "/build/", "logs/**/*.log", "[abc]?.tmp"];
        let set = ExcludeSet::from_patterns(raws).unwrap();
        assert_eq!(
            set.raws(),
            raws.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        );
        // Re-parsing the raws yields the same matching behavior.
        let set2 = ExcludeSet::from_patterns(set.raws()).unwrap();
        assert_eq!(set, set2);
    }
}
