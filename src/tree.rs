//! Directory-tree sync (D17): tree scan, wire manifest codec, and triage.
//!
//! A tree session (protocol v3, `concept/Protocol.md` §16) exchanges
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
    /// POSIX mode low bits (`0o7777` mask: rwxrwxrwx + setuid/setgid/sticky,
    /// §16.9 / D18). 0 where the platform has no such concept. Never a
    /// triage input — mode rides conversations, it does not trigger them.
    pub mode: u32,
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
            mode: metadata_mode(meta),
        }
    }
}

/// The POSIX mode low bits of a metadata value (§16.9): the raw st_mode
/// masked to `0o7777` on unix; 0 on platforms without the concept (Windows
/// has no POSIX mode bits — the receiver maps the no-write bits to the
/// read-only attribute instead, the only direction an NTFS attribute can
/// express).
#[cfg(unix)]
fn metadata_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o7777
}

#[cfg(not(unix))]
fn metadata_mode(_meta: &std::fs::Metadata) -> u32 {
    0
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
    /// subtree (a `!` negation cannot re-include anything below a pruned
    /// directory — the scan never descends into it).
    /// The sidecar filter ([`is_sidecar`]) applies in addition and is not
    /// user-visible.
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
        let mut entries: Vec<(String, PathBuf, std::fs::Metadata, bool)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            // Symlink-aware metadata (D18): the classification below must see
            // the LINK, not the target — a directory symlink/junction would
            // otherwise be recursed into (on Windows a junction needs no
            // privileges and may point at an ancestor: infinite recursion).
            let meta = match entry.metadata() {
                Ok(m) => m,
                // A file deleted between read_dir and metadata: skip, the
                // next sync sees a consistent state. Anything else is fatal.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            // `DirEntry::metadata` never traverses a symlink (lstat on unix,
            // the directory record on Windows), so its flag is authoritative
            // without a second stat per entry.
            let is_symlink = meta.is_symlink();
            // Wire paths are UTF-8 (§16.2). A lossy conversion would map the
            // name to one that does not exist (the sender's open then fails
            // the whole session) and could merge two distinct names into
            // one. Skip it, recorded not silent (D17).
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                eprintln!(
                    "warning: skipping non-UTF-8 file name: {}",
                    entry.path().display()
                );
                continue;
            };
            let rel = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            entries.push((rel, entry.path(), meta, is_symlink));
        }
        // Deterministic order regardless of filesystem enumeration order.
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (rel, path, meta, is_symlink) in entries {
            if is_symlink {
                // D18 symlink policy (§16.9): file links are followed and
                // synced as their target's content (the metadata() call
                // above already resolved the target — a broken link errors
                // there and is skipped below); directory links/junctions are
                // NEVER recursed into. Sync-as-entry is out of scope (D9):
                // Windows link creation needs privilege, and link targets
                // would add a traversal surface.
                let target_meta = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        // Dangling link: recorded, not silent.
                        eprintln!("warning: skipping dangling symlink: {}", path.display());
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if target_meta.is_file() {
                    if is_sidecar(&rel) {
                        continue;
                    }
                    if excludes.matches(&rel, EntryKind::File) {
                        continue;
                    }
                    out.insert(TreeEntry::from_metadata(rel, EntryKind::File, &target_meta));
                } else {
                    eprintln!(
                        "warning: skipping directory symlink (not recursed): {}",
                        path.display()
                    );
                }
            } else if meta.is_dir() {
                // Excluded directory: prune the whole subtree (§16.7).
                if excludes.matches(&rel, EntryKind::Dir) {
                    continue;
                }
                out.insert(TreeEntry::from_metadata(rel.clone(), EntryKind::Dir, &meta));
                Self::scan_dir(&path, &rel, excludes, out)?;
            } else if meta.is_file() {
                // D15/D17: the sidecar index and the temp file are ffsync's
                // own bookkeeping, never tree content. The index filename is
                // `<target>.ffsync-index` and the temp `<target>.ffsync-tmp`
                // (suffixes APPENDED — index.rs, D6 amendment). Both are
                // excluded from tree manifests: syncing them would (a) sync
                // our bookkeeping to the peer and (b) desync on every run
                // (an index written at commit changes mtime → re-transfer
                // loop). See `is_sidecar` for the index temp.
                if is_sidecar(&rel) {
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
/// §16.8-adjacent eager opt-in (D8/§9.2 extended to trees): the sender's
/// TREE_BEGIN announces the offer; the receiver mirrors it iff IT opted in
/// with its own --eager — then every per-file conversation runs the §9.2
/// eager machinery (position gate, hash-keyed pre-plan DATA buffer). A
/// receiver that declined still converges (lazy), same as single-file.
pub const TREE_FLAG_EAGER: u16 = 1 << 7;
/// §16.10 dry-run: preview session — triage runs, conversations and
/// deletions do not; the receiver reports what WOULD happen.
pub const TREE_FLAG_DRY_RUN: u16 = 1 << 6;

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
/// classes, `**` segments, optional dir-only trailing `/`, and (v0.7.1)
/// the negation prefix `!` — a negated pattern *re-includes* the paths it
/// matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludePattern {
    raw: String,
    anchored: bool,
    dir_only: bool,
    /// `!` prefix (stripped from `raw`'s interpretation): first-match-wins
    /// evaluation turns this pattern from an exclusion into a re-inclusion.
    negated: bool,
    segs: Vec<PatternSeg>,
}

impl ExcludePattern {
    /// Parses and validates one pattern. A `\` anywhere is a parse error
    /// (§16.7: no backslash escapes; wire paths are `/`-separated). A
    /// leading `!` negates (re-includes); `!!foo` is the literal `!foo`.
    pub fn new(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("empty exclude pattern".to_string());
        }
        // Negation prefix (§16.7, v0.7.1): one '!' re-includes; a leading
        // '!!' is the literal-'!' escape (strip exactly one '!' — the
        // remaining one is pattern text, and the pattern is NOT a
        // negation). A deeper '!' is ordinary pattern text.
        let (negated, rest) = if raw.starts_with("!!") {
            (false, &raw[1..])
        } else if let Some(r) = raw.strip_prefix('!') {
            (true, r)
        } else {
            (false, raw)
        };
        if rest.is_empty() {
            return Err(format!("exclude pattern is only a negation: {raw:?}"));
        }
        if rest.contains('\\') {
            return Err(format!(
                "backslash in exclude pattern (no escapes, paths are /-separated): {raw:?}"
            ));
        }
        let dir_only = rest.ends_with('/');
        let core = rest.trim_end_matches('/');
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
            negated,
            segs,
        })
    }

    /// The pattern as given (wire form).
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Whether this pattern re-includes (leading `!`, §16.7 v0.7.1).
    pub fn is_negated(&self) -> bool {
        self.negated
    }

    /// Does this pattern match one tree entry?
    pub fn matches(&self, rel_path: &str, kind: EntryKind) -> bool {
        let path_segs: Vec<&str> = rel_path.split('/').collect();
        self.matches_segs(&path_segs, kind)
    }

    /// [`matches`](Self::matches) on a pre-split path: an [`ExcludeSet`]
    /// splits once per entry and evaluates every pattern on the same
    /// slice (one allocation per entry, not one per pattern).
    fn matches_segs(&self, path_segs: &[&str], kind: EntryKind) -> bool {
        if self.dir_only && kind != EntryKind::Dir {
            return false;
        }
        let from = match_segs_from(&self.segs, path_segs);
        if self.anchored {
            from[0]
        } else {
            // Unanchored: match the basename at any depth.
            from[..path_segs.len()].iter().any(|&m| m)
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

/// Matches pattern segments against every suffix of `path` at once:
/// `result[j]` says whether `segs` matches `path[j..]` (`j` in `0..=len`).
/// Dynamic programming over (segment, path position), one row per pattern
/// segment, in O(segments × depth) time — the recursive form tried every
/// consumption width for every `**`, which a hostile sender's pattern
/// (`**/**/…/x` — excludes come from the PEER, §16.7) turned exponential.
fn match_segs_from(segs: &[PatternSeg], path: &[&str]) -> Vec<bool> {
    let n = path.len();
    // Row for the empty pattern: matches only the empty suffix.
    let mut next = vec![false; n + 1];
    next[n] = true;
    for seg in segs.iter().rev() {
        let mut cur = vec![false; n + 1];
        match seg {
            PatternSeg::DoubleStar => {
                // Zero or more segments: any later position the rest matches.
                let mut any_later = false;
                for j in (0..=n).rev() {
                    any_later |= next[j];
                    cur[j] = any_later;
                }
            }
            PatternSeg::Parts(parts) => {
                for j in 0..n {
                    cur[j] = next[j + 1] && match_parts(parts, path[j]);
                }
            }
        }
        next = cur;
    }
    next
}

/// Classic glob match of one sub-pattern against one path segment
/// (characters only — `*` never crosses `/` because segments are split).
/// Iterative with single-star backtracking (consecutive Stars collapse at
/// parse time, so one backtrack point suffices). Walks `text` by byte
/// offset (always on a char boundary) — no per-call `Vec<char>`.
fn match_parts(parts: &[Part], text: &str) -> bool {
    // Width of the char starting at byte offset `at` (callers keep `at` on
    // a boundary and below `text.len()`).
    let width = |at: usize| text[at..].chars().next().map_or(1, char::len_utf8);
    let (mut pi, mut ti) = (0usize, 0usize);
    // Backtrack point: (part index OF the Star, byte offset where the rest
    // of the pattern restarts — everything before it is swallowed).
    let mut star: Option<(usize, usize)> = None;
    while ti < text.len() {
        let c = text[ti..]
            .chars()
            .next()
            .expect("ti is below len, on a boundary");
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
                    ti += c.len_utf8();
                    true
                }
                Part::Lit(l) => {
                    if c == *l {
                        pi += 1;
                        ti += c.len_utf8();
                        true
                    } else {
                        false
                    }
                }
                Part::Class { negated, items } => {
                    let hit = items.iter().any(|it| match it {
                        ClassItem::Char(x) => *x == c,
                        ClassItem::Range(lo, hi) => c >= *lo && c <= *hi,
                    });
                    if hit != *negated {
                        pi += 1;
                        ti += c.len_utf8();
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
                if st >= text.len() {
                    return false;
                }
                let st = st + width(st);
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

    /// The raw pattern strings (wire form, §16.7, order preserved —
    /// first-match-wins evaluation (v0.7.1) is order-sensitive).
    pub fn raws(&self) -> Vec<String> {
        self.patterns.iter().map(|p| p.raw().to_string()).collect()
    }

    /// Whether any pattern in the set negates (v0.7.1): the sender sets the
    /// TREE_BEGIN `negation` flag iff this is true, so peers that cannot
    /// evaluate negations reject the session instead of misreading `!foo`
    /// as a literal pattern.
    pub fn has_negations(&self) -> bool {
        self.patterns.iter().any(|p| p.is_negated())
    }

    /// First-match-wins verdict (§16.7 v0.7.1): the first pattern that
    /// matches decides — exclude, or (negated) re-include. No match ⇒
    /// included. For negation-free sets this is exactly the old any-match
    /// behavior (every pattern excludes, so the first match excludes).
    fn excluded(&self, rel_path: &str, kind: EntryKind) -> bool {
        if self.patterns.is_empty() {
            return false;
        }
        let path_segs: Vec<&str> = rel_path.split('/').collect();
        for p in &self.patterns {
            if p.matches_segs(&path_segs, kind) {
                return !p.is_negated();
            }
        }
        false
    }

    /// Is this path excluded? (the scan filter's question)
    pub fn matches(&self, rel_path: &str, kind: EntryKind) -> bool {
        self.excluded(rel_path, kind)
    }

    /// Delete-pass protection (§16.7): the path is protected when its own
    /// first-match verdict is *excluded*, or when any ancestor directory's
    /// verdict is *excluded* (an excluded directory protects its whole
    /// subtree; a negated (re-included) ancestor protects nothing).
    pub fn matches_path_or_ancestor(&self, rel_path: &str, kind: EntryKind) -> bool {
        if self.excluded(rel_path, kind) {
            return true;
        }
        // Proper ancestors only: the path itself was judged above with its
        // REAL kind. Re-judging the final segment as a Dir made a dir-only
        // pattern (`build/`) protect a *file* named `build`.
        rel_path
            .match_indices('/')
            .any(|(i, _)| self.excluded(&rel_path[..i], EntryKind::Dir))
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
                        && mtimes_match(
                            (s.mtime_secs, s.mtime_nanos),
                            (r.mtime_secs, r.mtime_nanos),
                        )
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

/// Whether a tree-relative path is ffsync's own bookkeeping, never tree
/// content (D15/D17): the D15 index `<target>.ffsync-index`, its write
/// temp `<target>.ffsync-index-tmp` (left behind when a writer is killed
/// mid-index — the sender writes its own warm-start indexes too, so the
/// remnant sits in the SOURCE tree; scanned as content it vanishes on the
/// next index write and aborts the session "sender file vanished"), and
/// the reconstruction temp `<target-with-ext>.ffsync-tmp`. The single
/// place this filter lives.
pub fn is_sidecar(rel_path: &str) -> bool {
    rel_path.ends_with(".ffsync-index")
        || rel_path.ends_with(".ffsync-index-tmp")
        || rel_path.ends_with(".ffsync-tmp")
}

/// The mtime granularity of the metadata fast path, in nanoseconds: 100 ns,
/// NTFS's resolution (the coarsest timestamp of the supported targets that
/// still separates real rewrites).
pub const MTIME_GRANULARITY_NANOS: u32 = 100;

/// Whether two `(secs, nanos)` mtimes match for the metadata fast path
/// (tree triage §16.4, single-file HELLO compare §4).
///
/// Compared at [`MTIME_GRANULARITY_NANOS`]: a Linux sender's nanosecond
/// mtime set on a Windows receiver at commit (§16.6) is stored truncated to
/// 100 ns, so an exact compare would re-check that file on every session.
/// Never coarser: whole-second compares let a same-second same-size rewrite
/// pass as unchanged (the v0.2 lesson), while 100 ns is below any real
/// rewrite window (kernel file timestamps themselves tick far coarser).
/// Targets coarser than 100 ns (HFS+ 1 s, FAT 2 s) still re-check every
/// session — correct, just not fast.
pub fn mtimes_match(a: (u64, u32), b: (u64, u32)) -> bool {
    a.0 == b.0 && a.1 / MTIME_GRANULARITY_NANOS == b.1 / MTIME_GRANULARITY_NANOS
}

/// The case-fold key used to find paths that a case-insensitive filesystem
/// (NTFS, APFS default) maps to the same object. A candidate filter only:
/// every decision made on it is confirmed with [`same_file`], so an
/// imperfect fold can miss a pair but never conflate two distinct files.
pub fn case_fold(rel_path: &str) -> String {
    rel_path.to_lowercase()
}

/// Whether two existing paths name the same filesystem object (false if
/// either is missing). Directory entries are compared, not symlink targets.
///
/// This is the case-insensitive-receiver guard (§16.5): on NTFS/APFS
/// `README.md` and `Readme.md` are one file, so deleting the receiver-only
/// spelling after the sender's spelling was committed would destroy the
/// file just synced.
pub fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::symlink_metadata(a), std::fs::symlink_metadata(b)) {
            (Ok(x), Ok(y)) => x.dev() == y.dev() && x.ino() == y.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        // std exposes no stable file id on Windows; the canonical path
        // (GetFinalPathNameByHandleW) carries the on-disk spelling, so two
        // spellings of one file canonicalize identically.
        match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
            (Ok(x), Ok(y)) => x == y,
            _ => false,
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

/// Checks that a (validated, §16.2) wire path names exactly one object
/// under the root on THIS platform's filesystem — the receiver-side
/// representability check of §16.2.
///
/// On Windows a name the sender's filesystem stores verbatim can resolve
/// elsewhere: a drive prefix (`C:x`) makes `Path::join` discard the root
/// (writes escape the sync root), any other `:` opens an NTFS alternate
/// data stream, trailing dots/spaces are stripped (`a.` IS `a` — the next
/// session's delete pass then removes the file it just wrote), reserved
/// device names (`nul`, `COM1.txt`) are not files at all, and `<>"|?*` and
/// control characters are invalid. Unix receivers store every validated
/// name verbatim, so the check is a no-op there (`a:b` stays legal between
/// unix peers).
pub fn check_local_name(p: &str) -> Result<(), String> {
    #[cfg(windows)]
    for c in p.split('/') {
        if let Some(bad) = c
            .chars()
            .find(|&ch| ch < ' ' || matches!(ch, ':' | '<' | '>' | '"' | '|' | '?' | '*'))
        {
            return Err(format!(
                "{p:?}: character {bad:?} is not representable on a Windows receiver"
            ));
        }
        if c.ends_with('.') || c.ends_with(' ') {
            return Err(format!(
                "{p:?}: Windows strips trailing dots and spaces from {c:?}"
            ));
        }
        let stem = c.split('.').next().unwrap_or(c).trim_end_matches(' ');
        if is_reserved_device_name(stem) {
            return Err(format!("{p:?}: {c:?} is a reserved device name on Windows"));
        }
    }
    let _ = p;
    Ok(())
}

/// Windows reserved device names (matched case-insensitively against the
/// part of a component before its first dot; older Windows versions treat
/// `nul.txt` as the device too, so the extension does not save it).
#[cfg(windows)]
fn is_reserved_device_name(stem: &str) -> bool {
    let upper = stem.to_ascii_uppercase();
    match upper.as_str() {
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" => true,
        _ => {
            // `get`, not `split_at`: a multi-byte name must not panic.
            let (Some(head), Some(tail)) = (upper.get(..3), upper.get(3..)) else {
                return false;
            };
            matches!(head, "COM" | "LPT")
                && matches!(
                    tail,
                    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
        }
    }
}

/// Returns the first proper ancestor of `rel` (as a wire path) that is a
/// symlink or junction under `root`, if any. The scan never recurses into
/// directory links (D18), so the receiver must not WRITE through one
/// either: the bytes would land outside the sync root, and because the
/// next scan cannot see them they would re-transfer every session.
/// `checked` caches per-prefix results (one lstat per distinct directory).
pub fn link_ancestor(
    root: &Path,
    rel: &str,
    checked: &mut std::collections::HashMap<String, bool>,
) -> Option<String> {
    let mut end = 0;
    while let Some(i) = rel[end..].find('/') {
        end += i;
        let prefix = &rel[..end];
        let is_link = *checked.entry(prefix.to_string()).or_insert_with(|| {
            std::fs::symlink_metadata(root.join(prefix.replace('/', std::path::MAIN_SEPARATOR_STR)))
                .is_ok_and(|m| m.is_symlink())
        });
        if is_link {
            return Some(prefix.to_string());
        }
        end += 1;
    }
    None
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
            mode: 0,
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
        // Only sub-second mtime differs (the v0.2 lesson, now in triage) —
        // by one full 100 ns tick, the fast path's granularity.
        let r2 = entry("a.bin", EntryKind::File, 100, 1600000000, 600);
        assert_eq!(triage("a.bin", Some(&s), Some(&r2), false), Triage::Sync);
        // Whole-second equal, nanos equal, but secs differ.
        let r3 = entry("a.bin", EntryKind::File, 100, 1600000001, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r3), false), Triage::Sync);
    }

    #[test]
    fn triage_mtime_compares_at_100ns() {
        // A Linux sender's ns mtime, stored truncated to 100 ns by an NTFS
        // receiver at commit (§16.6), must still match next session…
        let s = entry("a.bin", EntryKind::File, 100, 1600000000, 123_456_789);
        let ntfs = entry("a.bin", EntryKind::File, 100, 1600000000, 123_456_700);
        assert_eq!(triage("a.bin", Some(&s), Some(&ntfs), false), Triage::Skip);
        assert_eq!(triage("a.bin", Some(&ntfs), Some(&s), false), Triage::Skip);
        // …but anything a full 100 ns tick apart is a different mtime.
        let other = entry("a.bin", EntryKind::File, 100, 1600000000, 123_456_699);
        assert_eq!(triage("a.bin", Some(&s), Some(&other), false), Triage::Sync);
        assert!(mtimes_match((5, 0), (5, 99)));
        assert!(!mtimes_match((5, 99), (5, 100)));
        assert!(!mtimes_match((5, 0), (6, 0)));
    }

    #[test]
    fn sidecars_are_filtered_from_scans() {
        assert!(is_sidecar("a.bin.ffsync-index"));
        assert!(is_sidecar("d/a.bin.ffsync-index-tmp"));
        assert!(is_sidecar("d/a.ffsync-tmp"));
        assert!(!is_sidecar("a.bin"));
        assert!(!is_sidecar("ffsync-index.txt"));
        let dir = std::env::temp_dir().join(format!("ffsync_sidecar_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in [
            "a.bin",
            "a.bin.ffsync-index",
            "a.bin.ffsync-index-tmp",
            "a.ffsync-tmp",
        ] {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        let m = TreeManifest::scan(&dir).unwrap();
        let paths: Vec<&str> = m.entries().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(paths, vec!["a.bin"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_file_distinguishes_entries() {
        let dir = std::env::temp_dir().join(format!("ffsync_same_file_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        std::fs::write(dir.join("b.txt"), b"b").unwrap();
        assert!(same_file(&dir.join("a.txt"), &dir.join("a.txt")));
        assert!(!same_file(&dir.join("a.txt"), &dir.join("b.txt")));
        assert!(!same_file(&dir.join("a.txt"), &dir.join("missing.txt")));
        // A case variant is the same file exactly when the fs folds case.
        let folds = dir.join("A.TXT").exists();
        assert_eq!(same_file(&dir.join("a.txt"), &dir.join("A.TXT")), folds);
        let _ = std::fs::remove_dir_all(&dir);
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
    fn exclude_patterns_match_multibyte_chars_as_one() {
        // `?` and class items consume one CHAR, not one byte; `*` backtracks
        // by whole chars (the matcher walks byte offsets on boundaries).
        let f = EntryKind::File;
        assert!(m("caf?.txt", "café.txt", f));
        assert!(!m("caf??.txt", "café.txt", f));
        assert!(m("*é*", "résumé", f));
        assert!(m("[äöü]*", "über", f));
        assert!(!m("[!äöü]*", "über", f));
        assert!(m("*.日本", "x.日本", f));
        assert!(m("*本", "日本", f));
        assert!(!m("*本?", "日本", f));
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
    fn exclude_double_star_matching_is_polynomial() {
        // Excludes come from the PEER (§16.7): a pattern of 24 `**`
        // segments against a 24-deep path used to explore ~C(48,24)
        // splits per entry (minutes; a hostile sender's denial of
        // service). The DP matcher is O(segments × depth).
        let pattern = format!("{}x", "**/".repeat(24));
        let path = vec!["d"; 24].join("/");
        let t = std::time::Instant::now();
        assert!(!m(&pattern, &path, EntryKind::File));
        assert!(m(&pattern, &format!("{path}/x"), EntryKind::File));
        // Unanchored form too (every start depth shares one table).
        assert!(!m(
            "**/a/**/a/**/a/**/b",
            &vec!["a"; 24].join("/"),
            EntryKind::File
        ));
        assert!(
            t.elapsed() < std::time::Duration::from_secs(1),
            "took {:?}",
            t.elapsed()
        );
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

    #[test]
    fn negation_reincludes_in_first_match_wins_order() {
        // §16.7 v0.7.1: the first matching pattern decides. The negation
        // must come BEFORE the wildcard it exempts (rsync's rule).
        let set = ExcludeSet::from_patterns(["!keep.log", "*.log"]).unwrap();
        assert!(set.has_negations());
        assert!(
            !set.matches("keep.log", EntryKind::File),
            "the earlier negation re-includes"
        );
        assert!(
            !set.matches("sub/keep.log", EntryKind::File),
            "basename patterns match at any depth, so the negation does too"
        );
        assert!(set.matches("delete.log", EntryKind::File));

        // Order matters: reversed, the wildcard matches first and the
        // negation is never consulted.
        let set = ExcludeSet::from_patterns(["*.log", "!keep.log"]).unwrap();
        assert!(set.matches("keep.log", EntryKind::File));

        // No match at all ⇒ included.
        let set = ExcludeSet::from_patterns(["!a", "b"]).unwrap();
        assert!(!set.matches("c", EntryKind::File));

        // Negation-free sets behave exactly as before (any-match ≡
        // first-match when every pattern excludes).
        let set = ExcludeSet::from_patterns(["a", "b"]).unwrap();
        assert!(!set.has_negations());
        assert!(set.matches("a", EntryKind::File));
        assert!(set.matches("b", EntryKind::File));
        assert!(!set.matches("c", EntryKind::File));
    }

    #[test]
    fn negation_literal_bang_escape() {
        // `!!foo` is the literal `!foo` (strip one '!' — verified: not a
        // negation). The class spelling for a literal '!' member is
        // `[]!]` (POSIX: ']' first in the class is literal, so `[]!]` =
        // {']', '!'}). A lone '!' is a parse error. NOTE `[!]x` is NOT
        // valid for this: POSIX-wise the ']' after '!' is a literal member
        // and the class never closes (unterminated '[' → literal '[').
        let set = ExcludeSet::from_patterns(["!!weird!name"]).unwrap();
        assert!(set.matches("!weird!name", EntryKind::File));
        assert!(!set.matches("weird!name", EntryKind::File));
        let set = ExcludeSet::from_patterns(["[]!]x"]).unwrap();
        assert!(set.matches("!x", EntryKind::File));
        assert!(set.matches("]x", EntryKind::File));
        assert!(!set.matches("x", EntryKind::File));
        assert!(ExcludeSet::from_patterns(["!"]).is_err());
    }

    #[test]
    fn delete_guard_judges_the_path_itself_by_its_real_kind() {
        let set = ExcludeSet::from_patterns(["build/"]).unwrap();
        // A FILE named build is not matched by a dir-only pattern.
        assert!(!set.matches_path_or_ancestor("build", EntryKind::File));
        assert!(!set.matches_path_or_ancestor("src/build", EntryKind::File));
        // The directory and everything below it are protected.
        assert!(set.matches_path_or_ancestor("build", EntryKind::Dir));
        assert!(set.matches_path_or_ancestor("build/out.o", EntryKind::File));
        assert!(set.matches_path_or_ancestor("src/build/out.o", EntryKind::File));
    }

    #[test]
    fn negation_delete_pass_semantics() {
        // The delete guard asks "is this path or an ancestor excluded?" —
        // a negated verdict is NOT an exclusion, so it protects nothing.
        // keep/ excludes (first match) ⇒ protects the whole subtree; the
        // negation under it changes nothing (the dir verdict is what the
        // ancestor check reads).
        let set = ExcludeSet::from_patterns(["keep", "!keep/temp"]).unwrap();
        assert!(set.matches_path_or_ancestor("keep/temp", EntryKind::File));
        // The negation first: keep/temp is re-included ⇒ NOT protected by
        // its own verdict, and keep (the ancestor) is... not excluded
        // either (it matches no pattern except via temp? no — the ancestor
        // check evaluates "keep" itself against the set: `keep` matches
        // nothing (no bare `keep` pattern here), so not protected.
        let set = ExcludeSet::from_patterns(["!keep/temp", "keep/*"]).unwrap();
        assert!(!set.matches_path_or_ancestor("keep/temp", EntryKind::File));
        assert!(set.matches_path_or_ancestor("keep/other", EntryKind::File));
        // A plain exclude set still protects as before. NOTE the negation
        // is dead here (the wildcard matches first) — keep.log IS excluded;
        // first-match-wins means this pattern order cannot re-include it.
        let set = ExcludeSet::from_patterns(["*.log", "!keep.log"]).unwrap();
        assert!(set.matches_path_or_ancestor("keep.log", EntryKind::File));
        assert!(set.matches_path_or_ancestor("other.log", EntryKind::File));
        // The live order: negation first, wildcard after — keep.log is
        // re-included, other.log excluded.
        let set = ExcludeSet::from_patterns(["!keep.log", "*.log"]).unwrap();
        assert!(!set.matches_path_or_ancestor("keep.log", EntryKind::File));
        assert!(set.matches_path_or_ancestor("other.log", EntryKind::File));
    }

    #[test]
    fn negation_under_pruned_dir_cannot_reinclude() {
        // The documented caveat: a pruned directory never enters the scan,
        // so a negation under it is unreachable (the scan-sidecar filter
        // has the same shape). Pattern order: `target/` excludes the dir;
        // `!target/keep` never gets evaluated against anything under it —
        // the scan never descends.
        let dir = std::env::temp_dir().join(format!(
            "ffsync_neg_scan_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(dir.join("target/keep")).unwrap();
        std::fs::write(dir.join("target/keep/x.txt"), b"x").unwrap();
        std::fs::write(dir.join("target/y.txt"), b"y").unwrap();

        let ex = ExcludeSet::from_patterns(["target/", "!target/keep"]).unwrap();
        let manifest = TreeManifest::scan_with_excludes(&dir, &ex).unwrap();
        assert!(
            manifest.get("target/keep/x.txt").is_none(),
            "pruned subtree: negation cannot re-include (documented)"
        );

        // The workaround from the spec: exclude the children, not the dir —
        // and the negation must come BEFORE the wildcard it exempts
        // (first-match-wins; I had the order backwards in the draft test).
        let ex = ExcludeSet::from_patterns(["!target/keep", "target/*"]).unwrap();
        let manifest = TreeManifest::scan_with_excludes(&dir, &ex).unwrap();
        let paths: Vec<&str> = manifest.entries().map(|e| e.rel_path.as_str()).collect();
        assert!(
            paths.contains(&"target/keep/x.txt"),
            "negation-first re-includes the kept subtree: {paths:?}"
        );
        assert!(
            !paths.contains(&"target/y.txt"),
            "the sibling file stays excluded: {paths:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn check_local_name_accepts_ordinary_names() {
        for p in [
            "a",
            "a.txt",
            "dir/sub/file.tar.gz",
            ".hidden",
            "consul",
            "com10",
            "lpt",
            "日本語/ファイル",
            "a b",
        ] {
            assert_eq!(check_local_name(p), Ok(()), "{p}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn check_local_name_refuses_names_windows_would_alias() {
        for p in [
            "C:x",
            "d/a:b",
            "a.",
            "d./x",
            "b ",
            "nul",
            "NUL.txt",
            "d/con.tar.gz",
            "aux ",
            "COM1",
            "lpt9.log",
            "COM\u{b9}",
            "conin$",
            "x?y",
            "a*b",
            "a<b",
            "a>b",
            "a|b",
            "a\"b",
            "tab\there",
        ] {
            assert!(check_local_name(p).is_err(), "{p:?} accepted");
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn check_local_name_is_a_no_op_on_unix() {
        for p in ["a:b", "a.", "nul", "x?y"] {
            assert_eq!(check_local_name(p), Ok(()), "{p}");
        }
    }

    #[test]
    fn check_local_name_does_not_panic_on_multibyte_names() {
        for p in [
            "\u{e9}t\u{e9}",
            "CO\u{e9}",
            "\u{1F600}\u{1F600}",
            "LP\u{b2}",
        ] {
            let _ = check_local_name(p);
        }
    }

    #[test]
    fn link_ancestor_finds_dir_links_but_not_the_entry_itself() {
        let dir = std::env::temp_dir().join(format!("ffsync_link_anc_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("real/sub")).unwrap();
        std::fs::create_dir_all(dir.join("target")).unwrap();
        #[cfg(windows)]
        {
            let out = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(dir.join("real").join("link"))
                .arg(dir.join("target"))
                .output()
                .unwrap();
            assert!(out.status.success());
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.join("target"), dir.join("real/link")).unwrap();
        let mut cache = std::collections::HashMap::new();
        assert_eq!(link_ancestor(&dir, "real/sub/f", &mut cache), None);
        assert_eq!(link_ancestor(&dir, "real/link", &mut cache), None);
        assert_eq!(
            link_ancestor(&dir, "real/link/x/y", &mut cache).as_deref(),
            Some("real/link")
        );
        assert_eq!(link_ancestor(&dir, "missing/a/b", &mut cache), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
