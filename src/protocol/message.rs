//! Typed protocol frames (`concept/Protocol v0.1.md` §3–§4).
//!
//! Each frame type maps to a struct with an explicit binary encoding on top
//! of the [`framing`] envelope. Encoding is hand-rolled (D1: no protobuf).

use blake3::Hash;
use std::io::{self, Read};

use super::MAGIC;
use super::framing::{self, Frame};

/// Role of a peer, sent in HELLO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Sender = 1,
    Receiver = 2,
}

impl Role {
    fn from_u8(v: u8) -> Option<Role> {
        match v {
            1 => Some(Role::Sender),
            2 => Some(Role::Receiver),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// HELLO flags (`concept/Protocol v0.1.md` §4).
pub const FLAG_TRUST_TIMES: u16 = 1 << 0;
pub const FLAG_EAGER: u16 = 1 << 1; // §9.2 (designed, unimplemented): must be 0 on the wire; rejected loudly
pub const FLAG_RESUMING: u16 = 1 << 2;
/// Dry-run offer (§7.1): the sender asks for a preview session — the
/// receiver executes nothing, writes nothing, deletes nothing, and reports
/// what WOULD happen. Sender-side flag; the receiver mirrors it internally.
pub const FLAG_DRY_RUN: u16 = 1 << 3;

/// All HELLO flag bits this build knows. Unknown bits are rejected at
/// decode (strict-codec doctrine): two peers that interpret the same bit
/// differently would desync the session.
const HELLO_FLAG_KNOWN: u16 = FLAG_TRUST_TIMES | FLAG_EAGER | FLAG_RESUMING | FLAG_DRY_RUN;

/// Protocol errors carried by the ERROR frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Version = 1,
    RoleConflict = 2,
    BadFrame = 3,
    HashMismatch = 4,
    Io = 5,
    Aborted = 6,
}

impl ErrorCode {
    fn from_u16(v: u16) -> Option<ErrorCode> {
        match v {
            1 => Some(ErrorCode::Version),
            2 => Some(ErrorCode::RoleConflict),
            3 => Some(ErrorCode::BadFrame),
            4 => Some(ErrorCode::HashMismatch),
            5 => Some(ErrorCode::Io),
            6 => Some(ErrorCode::Aborted),
            _ => None,
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

/// The HELLO handshake frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    pub flags: u16,
    pub role: Role,
    pub cdc_min: u32,
    pub cdc_avg: u32,
    pub cdc_max: u32,
    pub file_size: u64,
    pub file_mtime: u64,       // unix seconds, 0 if absent
    pub file_mtime_nanos: u32, // sub-second part of file_mtime, 0 if absent
    pub path: String,
}

/// One manifest entry: "I can serve these bytes locally".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestChunk {
    pub hash: Hash,
    pub offset: u64,
    pub length: u32,
}

/// One plan entry: what the receiver should do with a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanEntry {
    /// Receiver already has these bytes at `source_offset` (its manifest).
    /// `hash` identifies the content so the receiver can (a) disambiguate
    /// which local file the offset refers to when resuming (temp vs. old
    /// file) and (b) verify the local read against the expected content.
    Local {
        target_offset: u64,
        length: u32,
        source_offset: u64,
        hash: Hash,
    },
    /// Sender will transmit the bytes for `hash`.
    Remote {
        target_offset: u64,
        length: u32,
        hash: Hash,
    },
}

/// Error returned by typed frame decoding.
#[derive(Debug)]
pub enum MessageError {
    /// Frame with the wrong type byte for the expected message.
    UnexpectedType(u8),
    /// The peer sent an ERROR frame where the expected message was due —
    /// carries the peer's classification and message. Decoding an ERROR
    /// *instead of* the expected type keeps abort announcements visible
    /// (a peer that dies mid-session says so with an ERROR frame; without
    /// this variant the receiver would report a bare "unexpected frame
    /// type" and lose the reason).
    PeerError(ErrorCode, String),
    /// Payload does not match the expected structure.
    Malformed(&'static str),
    /// Underlying frame-level error.
    Frame(framing::FrameError),
}

/// Reads one frame; on ERROR frames returns [`MessageError::PeerError`],
/// otherwise hands the frame to the caller (used by `read_typed` so an
/// aborting peer's ERROR is classified instead of swallowed).
fn read_frame_or_error(inp: &mut impl Read) -> Result<Frame, MessageError> {
    let frame = framing::read_frame(inp)?;
    if frame.ftype == framing::T_ERROR {
        let (code, msg) = read_error_payload(&frame)?;
        return Err(MessageError::PeerError(code, msg));
    }
    Ok(frame)
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageError::UnexpectedType(t) => write!(f, "unexpected frame type 0x{:02X}", t),
            MessageError::PeerError(code, msg) => {
                write!(f, "peer aborted ({code:?}): {msg}")
            }
            MessageError::Malformed(why) => write!(f, "malformed frame payload: {}", why),
            MessageError::Frame(e) => write!(f, "{}", e),
        }
    }
}

impl From<framing::FrameError> for MessageError {
    fn from(e: framing::FrameError) -> Self {
        MessageError::Frame(e)
    }
}

/// A typed message: anything that can be encoded to / decoded from a frame.
pub trait Message: Sized {
    const TYPE: u8;

    fn into_frame(self) -> io::Result<Frame>;
    fn from_frame(frame: Frame) -> Result<Self, MessageError>;
}

fn read_u16_le(payload: &[u8], pos: &mut usize) -> Result<u16, MessageError> {
    let end = pos.checked_add(2).filter(|e| *e <= payload.len());
    let end = end.ok_or(MessageError::Malformed("short u16"))?;
    let v = u16::from_le_bytes([payload[*pos], payload[*pos + 1]]);
    *pos = end;
    Ok(v)
}

fn read_u32_le(payload: &[u8], pos: &mut usize) -> Result<u32, MessageError> {
    let end = pos.checked_add(4).filter(|e| *e <= payload.len());
    let end = end.ok_or(MessageError::Malformed("short u32"))?;
    let mut b = [0u8; 4];
    b.copy_from_slice(&payload[*pos..end]);
    *pos = end;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_le(payload: &[u8], pos: &mut usize) -> Result<u64, MessageError> {
    let end = pos.checked_add(8).filter(|e| *e <= payload.len());
    let end = end.ok_or(MessageError::Malformed("short u64"))?;
    let mut b = [0u8; 8];
    b.copy_from_slice(&payload[*pos..end]);
    *pos = end;
    Ok(u64::from_le_bytes(b))
}

fn read_bytes<'a>(payload: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], MessageError> {
    let end = pos.checked_add(n).filter(|e| *e <= payload.len());
    let end = end.ok_or(MessageError::Malformed("short bytes"))?;
    let s = &payload[*pos..end];
    *pos = end;
    Ok(s)
}

impl Message for Hello {
    const TYPE: u8 = framing::T_HELLO;

    fn into_frame(self) -> io::Result<Frame> {
        let mut payload = Vec::with_capacity(64 + self.path.len());
        payload.extend_from_slice(&MAGIC);
        payload.extend_from_slice(&self.version.to_le_bytes());
        payload.extend_from_slice(&self.flags.to_le_bytes());
        payload.push(self.role.as_u8());
        payload.extend_from_slice(&self.cdc_min.to_le_bytes());
        payload.extend_from_slice(&self.cdc_avg.to_le_bytes());
        payload.extend_from_slice(&self.cdc_max.to_le_bytes());
        payload.extend_from_slice(&self.file_size.to_le_bytes());
        payload.extend_from_slice(&self.file_mtime.to_le_bytes());
        payload.extend_from_slice(&self.file_mtime_nanos.to_le_bytes());
        let path = self.path.as_bytes();
        if path.len() > u16::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "path too long"));
        }
        payload.extend_from_slice(&(path.len() as u16).to_le_bytes());
        payload.extend_from_slice(path);
        Ok(Frame::new(Self::TYPE, payload))
    }

    fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != Self::TYPE {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        let p = &frame.payload;
        let mut pos = 0usize;
        let magic = read_bytes(p, &mut pos, 4)?;
        if magic != MAGIC {
            return Err(MessageError::Malformed("bad magic"));
        }
        let version = read_u16_le(p, &mut pos)?;
        let flags = read_u16_le(p, &mut pos)?;
        if flags & !HELLO_FLAG_KNOWN != 0 {
            return Err(MessageError::Malformed("HELLO has unknown flag bits"));
        }
        let role_byte = read_bytes(p, &mut pos, 1)?[0];
        let role = Role::from_u8(role_byte).ok_or(MessageError::Malformed("invalid role"))?;
        let cdc_min = read_u32_le(p, &mut pos)?;
        let cdc_avg = read_u32_le(p, &mut pos)?;
        let cdc_max = read_u32_le(p, &mut pos)?;
        let file_size = read_u64_le(p, &mut pos)?;
        let file_mtime = read_u64_le(p, &mut pos)?;
        let file_mtime_nanos = read_u32_le(p, &mut pos)?;
        let path_len = read_u16_le(p, &mut pos)? as usize;
        let path_bytes = read_bytes(p, &mut pos, path_len)?;
        if pos != p.len() {
            return Err(MessageError::Malformed("trailing bytes after HELLO"));
        }
        let path = String::from_utf8(path_bytes.to_vec())
            .map_err(|_| MessageError::Malformed("path is not UTF-8"))?;
        Ok(Hello {
            version,
            flags,
            role,
            cdc_min,
            cdc_avg,
            cdc_max,
            file_size,
            file_mtime,
            file_mtime_nanos,
            path,
        })
    }
}

impl Message for ManifestChunk {
    const TYPE: u8 = framing::T_MANIFEST_CHUNK;

    fn into_frame(self) -> io::Result<Frame> {
        let mut payload = Vec::with_capacity(44);
        payload.extend_from_slice(self.hash.as_bytes());
        payload.extend_from_slice(&self.offset.to_le_bytes());
        payload.extend_from_slice(&self.length.to_le_bytes());
        Ok(Frame::new(Self::TYPE, payload))
    }

    fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != Self::TYPE {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        if frame.payload.len() != 44 {
            return Err(MessageError::Malformed("manifest chunk must be 44 bytes"));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&frame.payload[..32]);
        let mut off = [0u8; 8];
        off.copy_from_slice(&frame.payload[32..40]);
        let mut len = [0u8; 4];
        len.copy_from_slice(&frame.payload[40..44]);
        Ok(ManifestChunk {
            hash: Hash::from_bytes(hash),
            offset: u64::from_le_bytes(off),
            length: u32::from_le_bytes(len),
        })
    }
}

impl Message for PlanEntry {
    const TYPE: u8 = framing::T_PLAN_REMOTE;

    fn into_frame(self) -> io::Result<Frame> {
        match self {
            PlanEntry::Local {
                target_offset,
                length,
                source_offset,
                hash,
            } => {
                let mut payload = Vec::with_capacity(52);
                payload.extend_from_slice(&target_offset.to_le_bytes());
                payload.extend_from_slice(&length.to_le_bytes());
                payload.extend_from_slice(&source_offset.to_le_bytes());
                payload.extend_from_slice(hash.as_bytes());
                Ok(Frame::new(framing::T_PLAN_LOCAL, payload))
            }
            PlanEntry::Remote {
                target_offset,
                length,
                hash,
            } => {
                let mut payload = Vec::with_capacity(44);
                payload.extend_from_slice(&target_offset.to_le_bytes());
                payload.extend_from_slice(&length.to_le_bytes());
                payload.extend_from_slice(hash.as_bytes());
                Ok(Frame::new(framing::T_PLAN_REMOTE, payload))
            }
        }
    }

    fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        match frame.ftype {
            framing::T_PLAN_LOCAL => {
                if frame.payload.len() != 52 {
                    return Err(MessageError::Malformed("plan local must be 52 bytes"));
                }
                let mut off = [0u8; 8];
                off.copy_from_slice(&frame.payload[0..8]);
                let mut len = [0u8; 4];
                len.copy_from_slice(&frame.payload[8..12]);
                let mut src = [0u8; 8];
                src.copy_from_slice(&frame.payload[12..20]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&frame.payload[20..52]);
                Ok(PlanEntry::Local {
                    target_offset: u64::from_le_bytes(off),
                    length: u32::from_le_bytes(len),
                    source_offset: u64::from_le_bytes(src),
                    hash: Hash::from_bytes(hash),
                })
            }
            framing::T_PLAN_REMOTE => {
                if frame.payload.len() != 44 {
                    return Err(MessageError::Malformed("plan remote must be 44 bytes"));
                }
                let mut off = [0u8; 8];
                off.copy_from_slice(&frame.payload[0..8]);
                let mut len = [0u8; 4];
                len.copy_from_slice(&frame.payload[8..12]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&frame.payload[12..44]);
                Ok(PlanEntry::Remote {
                    target_offset: u64::from_le_bytes(off),
                    length: u32::from_le_bytes(len),
                    hash: Hash::from_bytes(hash),
                })
            }
            other => Err(MessageError::UnexpectedType(other)),
        }
    }
}

/// Frames with empty payloads: MANIFEST_DONE, PLAN_DONE, COMMIT_ACK,
/// NOT_MODIFIED.
pub fn encode_empty(ftype: u8) -> io::Result<Frame> {
    Ok(Frame::new(ftype, Vec::new()))
}

/// Reads and validates an empty-payload frame of the given type.
pub fn read_empty(inp: &mut impl Read, ftype: u8) -> Result<(), MessageError> {
    let frame = framing::read_frame(inp)?;
    if frame.ftype != ftype {
        return Err(MessageError::UnexpectedType(frame.ftype));
    }
    if !frame.payload.is_empty() {
        return Err(MessageError::Malformed("expected empty payload"));
    }
    Ok(())
}

/// Reads a typed message of exactly the expected type.
pub fn read_typed<M: Message>(inp: &mut impl Read) -> Result<M, MessageError> {
    let frame = read_frame_or_error(inp)?;
    M::from_frame(frame)
}

/// VERIFY frame payload: the sender's 32-byte root hash.
pub fn encode_verify(root: Hash) -> io::Result<Frame> {
    Ok(Frame::new(framing::T_VERIFY, root.as_bytes().to_vec()))
}

pub fn read_verify(inp: &mut impl Read) -> Result<Hash, MessageError> {
    let frame = framing::read_frame(inp)?;
    if frame.ftype != framing::T_VERIFY {
        return Err(MessageError::UnexpectedType(frame.ftype));
    }
    if frame.payload.len() != 32 {
        return Err(MessageError::Malformed("verify payload must be 32 bytes"));
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&frame.payload);
    Ok(Hash::from_bytes(h))
}

/// ERROR frame: code + UTF-8 message.
pub fn encode_error(code: ErrorCode, message: &str) -> io::Result<Frame> {
    let mut payload = Vec::with_capacity(4 + message.len());
    payload.extend_from_slice(&code.as_u16().to_le_bytes());
    let msg = message.as_bytes();
    if msg.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too long",
        ));
    }
    payload.extend_from_slice(&(msg.len() as u16).to_le_bytes());
    payload.extend_from_slice(msg);
    Ok(Frame::new(framing::T_ERROR, payload))
}

pub fn read_error(inp: &mut impl Read) -> Result<(ErrorCode, String), MessageError> {
    let frame = framing::read_frame(inp)?;
    if frame.ftype != framing::T_ERROR {
        return Err(MessageError::UnexpectedType(frame.ftype));
    }
    let p = &frame.payload;
    let mut pos = 0usize;
    let code = read_u16_le(p, &mut pos)?;
    let code = ErrorCode::from_u16(code).ok_or(MessageError::Malformed("unknown error code"))?;
    let msg_len = read_u16_le(p, &mut pos)? as usize;
    let msg = read_bytes(p, &mut pos, msg_len)?;
    let msg = String::from_utf8(msg.to_vec())
        .map_err(|_| MessageError::Malformed("error message is not UTF-8"))?;
    Ok((code, msg))
}

/// Reads an ERROR frame payload from an already-received frame.
pub fn read_error_payload(frame: &Frame) -> Result<(ErrorCode, String), MessageError> {
    if frame.ftype != framing::T_ERROR {
        return Err(MessageError::UnexpectedType(frame.ftype));
    }
    let p = &frame.payload;
    let mut pos = 0usize;
    let code = read_u16_le(p, &mut pos)?;
    let code = ErrorCode::from_u16(code).ok_or(MessageError::Malformed("unknown error code"))?;
    let msg_len = read_u16_le(p, &mut pos)? as usize;
    let msg = read_bytes(p, &mut pos, msg_len)?;
    let msg = String::from_utf8(msg.to_vec())
        .map_err(|_| MessageError::Malformed("error message is not UTF-8"))?;
    Ok((code, msg))
}

/// DATA frame: hash + zstd-compressed chunk bytes.
pub fn encode_data(hash: Hash, compressed: &[u8]) -> io::Result<Frame> {
    let mut payload = Vec::with_capacity(32 + compressed.len());
    payload.extend_from_slice(hash.as_bytes());
    payload.extend_from_slice(compressed);
    Ok(Frame::new(framing::T_DATA, payload))
}

/// Reads a DATA frame, returning (hash, compressed bytes).
pub fn read_data(inp: &mut impl Read) -> Result<(Hash, Vec<u8>), MessageError> {
    let frame = framing::read_frame(inp)?;
    if frame.ftype != framing::T_DATA {
        return Err(MessageError::UnexpectedType(frame.ftype));
    }
    if frame.payload.len() < 32 {
        return Err(MessageError::Malformed("data frame too short"));
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&frame.payload[..32]);
    Ok((Hash::from_bytes(h), frame.payload[32..].to_vec()))
}

// ---------------------------------------------------------------------------
// Tree sync frames (v0.3, §16) — TREE_BEGIN / TREE_END
// ---------------------------------------------------------------------------

/// One entry of the TREE_BEGIN manifest (§16.2). Mirrors
/// `crate::tree::TreeEntry`; kept wire-local to avoid the protocol module
/// depending on the tree module (the tree module depends on this one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntryWire {
    pub rel_path: String,
    pub kind: u8, // 0 = file, 1 = dir (crate::tree::EntryKind values)
    pub size: u64,
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
    /// POSIX mode low bits (§16.9). Only read when the TREE_BEGIN carries
    /// TREE_FLAG_METADATA; 0 otherwise (and on platforms without POSIX
    /// modes). Kept per-entry so the field survives manifest round trips.
    pub mode: u32,
}

/// TREE_BEGIN payload: flags u16, then — only when the `parallel` flag
/// (bit 3, §16.8) is set — `parallel u16`, then — only when the `excludes`
/// flag (bit 2, §16.7) is set — the pattern section, then `entry_count u64`
/// and the entries (§16.2 layout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeBegin {
    pub flags: u16,
    /// Total connection count (only meaningful with the parallel flag set;
    /// 1 = sequential session, §16.8).
    pub parallel: u16,
    /// Exclude patterns (present only when `TREE_FLAG_EXCLUDES` is set).
    pub excludes: Vec<String>,
    pub entries: Vec<TreeEntryWire>,
}

/// TREE_BEGIN flag bit 2: the payload carries the exclude-pattern section
/// (§16.7). Defined here next to the codec whose layout it gates.
pub const TREE_FLAG_EXCLUDES: u16 = 1 << 2;
/// TREE_BEGIN flag bit 3: the payload carries the `parallel u16` field
/// (§16.8). Flag-gated fields are only safe when unknown flag bits are
/// rejected — the decoder enforces that.
pub const TREE_FLAG_PARALLEL: u16 = 1 << 3;
/// TREE_BEGIN flag bit 4: each tree entry carries an additional `mode u32`
/// field (§16.9, D18 — POSIX mode bits, 0 where the platform has none).
pub const TREE_FLAG_METADATA: u16 = 1 << 4;
/// TREE_BEGIN flag bit 5 (v0.7.1): the pattern section contains negations
/// (`!`-prefixed re-includes) and must be evaluated first-match-wins. No
/// payload change — a peer that cannot evaluate negations must reject the
/// bit (it would parse `!foo` as a literal basename pattern and desync
/// triage). Sessions without negations never set it and interoperate with
/// any peer.
pub const TREE_FLAG_NEGATION: u16 = 1 << 5;
/// Tree flags a decoder must reject as unknown (§16.8): everything beyond
/// the bits this protocol version defines. CHECKSUM/NO_DELETE live in
/// `crate::tree` next to the triage that consumes them.
const TREE_FLAG_KNOWN: u16 = crate::tree::TREE_FLAG_CHECKSUM
    | crate::tree::TREE_FLAG_NO_DELETE
    | TREE_FLAG_EXCLUDES
    | TREE_FLAG_PARALLEL
    | TREE_FLAG_METADATA
    | TREE_FLAG_NEGATION
    | crate::tree::TREE_FLAG_DRY_RUN;

impl TreeBegin {
    pub fn into_frame(self) -> io::Result<Frame> {
        let mut payload = Vec::with_capacity(10 + self.entries.len() * 24);
        payload.extend_from_slice(&self.flags.to_le_bytes());
        if self.flags & TREE_FLAG_PARALLEL != 0 {
            payload.extend_from_slice(&self.parallel.to_le_bytes());
        }
        if self.flags & TREE_FLAG_EXCLUDES != 0 {
            payload.extend_from_slice(&(self.excludes.len() as u32).to_le_bytes());
            for pat in &self.excludes {
                let pat_bytes = pat.as_bytes();
                if pat_bytes.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("exclude pattern longer than {} bytes", u16::MAX),
                    ));
                }
                payload.extend_from_slice(&(pat_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(pat_bytes);
            }
        }
        payload.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
        let metadata = self.flags & TREE_FLAG_METADATA != 0;
        for e in &self.entries {
            let path_bytes = e.rel_path.as_bytes();
            if path_bytes.len() > u16::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("tree path longer than {} bytes", u16::MAX),
                ));
            }
            payload.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(path_bytes);
            payload.push(e.kind);
            payload.extend_from_slice(&e.size.to_le_bytes());
            payload.extend_from_slice(&e.mtime_secs.to_le_bytes());
            payload.extend_from_slice(&e.mtime_nanos.to_le_bytes());
            if metadata {
                payload.extend_from_slice(&e.mode.to_le_bytes());
            }
        }
        Ok(Frame::new(framing::T_TREE_BEGIN, payload))
    }

    pub fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != framing::T_TREE_BEGIN {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        let p = &frame.payload[..];
        let mut pos = 0usize;
        let flags = read_u16_le(p, &mut pos)?;
        if flags & !TREE_FLAG_KNOWN != 0 {
            // Flag-gated fields (§16.7/§16.8) are only safe when unknown
            // bits are rejected: a newer peer's extra bit would otherwise
            // silently change what we parse out of this payload.
            return Err(MessageError::Malformed("TREE_BEGIN has unknown flag bits"));
        }
        let parallel = if flags & TREE_FLAG_PARALLEL != 0 {
            read_u16_le(p, &mut pos)?
        } else {
            1
        };
        let mut excludes = Vec::new();
        if flags & TREE_FLAG_EXCLUDES != 0 {
            let pat_count = read_u32_le(p, &mut pos)? as usize;
            // Each pattern is at least 3 bytes on the wire (2 len + 1 char);
            // reject absurd counts before allocating (§12 spirit).
            if pat_count > (p.len().saturating_sub(pos)) / 3 + 1 {
                return Err(MessageError::Malformed(
                    "exclude pattern count exceeds payload",
                ));
            }
            for _ in 0..pat_count {
                let pat_len = read_u16_le(p, &mut pos)? as usize;
                let pat = read_bytes(p, &mut pos, pat_len)?;
                let raw = std::str::from_utf8(pat)
                    .map_err(|_| MessageError::Malformed("exclude pattern is not UTF-8"))?
                    .to_string();
                excludes.push(raw);
            }
        }
        let count = read_u64_le(p, &mut pos)? as usize;
        // Resource limit (§12 spirit): a manifest claiming absurd counts must
        // not allocate for them upfront. Each entry is at least 24 bytes on
        // the wire (2 + 1 + 8 + 8 + 4 + path), so the payload bounds the count.
        if count > (p.len().saturating_sub(pos)) / 24 + 1 {
            return Err(MessageError::Malformed("tree entry count exceeds payload"));
        }
        let mut entries = Vec::with_capacity(count);
        // §16.9: with the metadata flag, each entry carries 4 more bytes
        // (mode u32); the entry-size bound below accounts for it.
        let metadata = flags & TREE_FLAG_METADATA != 0;
        let min_entry = if metadata { 28 } else { 24 };
        if count > (p.len().saturating_sub(pos)) / min_entry + 1 {
            return Err(MessageError::Malformed("tree entry count exceeds payload"));
        }
        for _ in 0..count {
            let path_len = read_u16_le(p, &mut pos)? as usize;
            let path = read_bytes(p, &mut pos, path_len)?;
            let rel_path = std::str::from_utf8(path)
                .map_err(|_| MessageError::Malformed("tree path is not UTF-8"))?
                .to_string();
            if pos >= p.len() {
                return Err(MessageError::Malformed("short tree entry"));
            }
            let kind = p[pos];
            pos += 1;
            let size = read_u64_le(p, &mut pos)?;
            let mtime_secs = read_u64_le(p, &mut pos)?;
            let mtime_nanos = read_u32_le(p, &mut pos)?;
            let mode = if metadata {
                read_u32_le(p, &mut pos)?
            } else {
                0
            };
            entries.push(TreeEntryWire {
                rel_path,
                kind,
                size,
                mtime_secs,
                mtime_nanos,
                mode,
            });
        }
        if pos != p.len() {
            return Err(MessageError::Malformed("trailing bytes in TREE_BEGIN"));
        }
        Ok(Self {
            flags,
            parallel,
            excludes,
            entries,
        })
    }
}

/// TREE_WORKER (v4, §16.8): sender→receiver on a worker connection,
/// announcing the connection's index. Payload: `worker_index u16`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeWorker {
    pub worker_index: u16,
}

impl TreeWorker {
    pub fn into_frame(self) -> io::Result<Frame> {
        Ok(Frame::new(
            framing::T_TREE_WORKER,
            self.worker_index.to_le_bytes().to_vec(),
        ))
    }

    pub fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != framing::T_TREE_WORKER {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        let p = &frame.payload[..];
        if p.len() != 2 {
            return Err(MessageError::Malformed(
                "TREE_WORKER payload must be 2 bytes",
            ));
        }
        Ok(Self {
            worker_index: u16::from_le_bytes([p[0], p[1]]),
        })
    }
}

/// TREE_FILE (v4, §16.8): sender→receiver before each conversation,
/// naming the path about to be synced. In a parallel session the receiver
/// validates membership (expected set, exactly once) instead of order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeFile {
    pub rel_path: String,
}

impl TreeFile {
    pub fn into_frame(self) -> io::Result<Frame> {
        let path_bytes = self.rel_path.as_bytes();
        if path_bytes.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("tree path longer than {} bytes", u16::MAX),
            ));
        }
        let mut payload = Vec::with_capacity(2 + path_bytes.len());
        payload.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(path_bytes);
        Ok(Frame::new(framing::T_TREE_FILE, payload))
    }

    pub fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != framing::T_TREE_FILE {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        let p = &frame.payload[..];
        let mut pos = 0usize;
        let path_len = read_u16_le(p, &mut pos)? as usize;
        let path = read_bytes(p, &mut pos, path_len)?;
        let rel_path = std::str::from_utf8(path)
            .map_err(|_| MessageError::Malformed("tree path is not UTF-8"))?
            .to_string();
        if pos != p.len() {
            return Err(MessageError::Malformed("trailing bytes in TREE_FILE"));
        }
        Ok(Self { rel_path })
    }
}

/// TREE_END payload: on the receiver→sender leg, the delete report
/// (§16.5); on the sender→receiver leg, empty.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TreeEnd {
    pub deleted_count: u32,
    pub deleted_bytes: u64,
}

impl TreeEnd {
    pub fn into_frame(self) -> io::Result<Frame> {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&self.deleted_count.to_le_bytes());
        payload.extend_from_slice(&self.deleted_bytes.to_le_bytes());
        Ok(Frame::new(framing::T_TREE_END, payload))
    }

    pub fn from_frame(frame: Frame) -> Result<Self, MessageError> {
        if frame.ftype != framing::T_TREE_END {
            return Err(MessageError::UnexpectedType(frame.ftype));
        }
        if frame.payload.len() != 12 {
            return Err(MessageError::Malformed("TREE_END payload must be 12 bytes"));
        }
        let mut pos = 0usize;
        let p = &frame.payload[..];
        let deleted_count = read_u32_le(p, &mut pos)?;
        let deleted_bytes = read_u64_le(p, &mut pos)?;
        Ok(Self {
            deleted_count,
            deleted_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip<M: Message + PartialEq + std::fmt::Debug + Clone>(msg: M) {
        let expected = msg.clone();
        let frame = msg.into_frame().unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let frame = framing::read_frame(&mut cursor).unwrap();
        let decoded = M::from_frame(frame).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn hello_round_trip() {
        round_trip(Hello {
            version: super::super::VERSION,
            flags: FLAG_TRUST_TIMES | FLAG_RESUMING,
            role: Role::Sender,
            cdc_min: 64 * 1024,
            cdc_avg: 256 * 1024,
            cdc_max: 1024 * 1024,
            file_size: 12_345_678,
            file_mtime: 1_760_000_000,
            file_mtime_nanos: 123_456_700,
            path: "videos/movie file.mkv".to_string(),
        });
    }

    #[test]
    fn manifest_chunk_round_trip() {
        round_trip(ManifestChunk {
            hash: blake3::hash(b"chunk-data"),
            offset: 987_654_321,
            length: 262_144,
        });
    }

    #[test]
    fn plan_entries_round_trip() {
        round_trip(PlanEntry::Local {
            target_offset: 4096,
            length: 65536,
            source_offset: 262_144,
            hash: blake3::hash(b"local-chunk"),
        });
        round_trip(PlanEntry::Remote {
            target_offset: 4096,
            length: 65536,
            hash: blake3::hash(b"remote-chunk"),
        });
    }

    #[test]
    fn verify_and_error_round_trip() {
        let root = blake3::hash(b"root");
        let frame = encode_verify(root).unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_verify(&mut cursor).unwrap(), root);

        let frame = encode_error(ErrorCode::HashMismatch, "root hash differs").unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (code, msg) = read_error(&mut cursor).unwrap();
        assert_eq!(code, ErrorCode::HashMismatch);
        assert_eq!(msg, "root hash differs");
    }

    #[test]
    fn data_round_trip() {
        let hash = blake3::hash(b"payload");
        let frame = encode_data(hash, &[1, 2, 3, 4, 5]).unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (h, data) = read_data(&mut cursor).unwrap();
        assert_eq!(h, hash);
        assert_eq!(data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn malformed_frames_rejected() {
        // HELLO with bad magic.
        let frame = Frame::new(
            framing::T_HELLO,
            vec![0, 0, 0, 0, 1, 0, 2, 0, 1, 0, 0, 0, 0],
        );
        assert!(matches!(
            Hello::from_frame(frame),
            Err(MessageError::Malformed(_))
        ));

        // Manifest chunk with wrong size.
        let frame = Frame::new(framing::T_MANIFEST_CHUNK, vec![0u8; 43]);
        assert!(matches!(
            ManifestChunk::from_frame(frame),
            Err(MessageError::Malformed(_))
        ));

        // Wrong type for read_verify.
        let frame = Frame::new(framing::T_COMMIT_ACK, vec![]);
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        assert!(matches!(
            read_verify(&mut cursor),
            Err(MessageError::UnexpectedType(_))
        ));
    }

    #[test]
    fn empty_frames() {
        let frame = encode_empty(framing::T_MANIFEST_DONE).unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        read_empty(&mut cursor, framing::T_MANIFEST_DONE).unwrap();

        // Wrong type is an error.
        let frame = encode_empty(framing::T_PLAN_DONE).unwrap();
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        assert!(read_empty(&mut cursor, framing::T_MANIFEST_DONE).is_err());
    }

    #[test]
    fn tree_begin_metadata_flag_round_trip() {
        // §16.9: with the flag, each entry carries mode u32; without it,
        // the layout is byte-identical to v3 (mode never encoded).
        let entries = vec![TreeEntryWire {
            rel_path: "a/b.sh".to_string(),
            kind: 0,
            size: 42,
            mtime_secs: 1700000000,
            mtime_nanos: 7,
            mode: 0o755,
        }];
        let with = TreeBegin {
            flags: TREE_FLAG_METADATA,
            parallel: 1,
            excludes: Vec::new(),
            entries: entries.clone(),
        }
        .into_frame()
        .unwrap();
        let back = TreeBegin::from_frame(with).expect("round trip");
        assert_eq!(back.entries, entries, "mode survives the wire");

        // Without the flag: round trip must also succeed and yield mode 0
        // on the wire form (the field is never read).
        let without = TreeBegin {
            flags: 0,
            parallel: 1,
            excludes: Vec::new(),
            entries: entries.clone(),
        }
        .into_frame()
        .unwrap();
        let back = TreeBegin::from_frame(without).unwrap();
        assert_eq!(back.entries[0].rel_path, "a/b.sh");
        assert_eq!(back.entries[0].mode, 0);

        // Payload-size discipline: the metadata flag adds exactly 4 bytes
        // per entry.
        let with_len = {
            let mut b = Vec::new();
            TreeBegin {
                flags: TREE_FLAG_METADATA,
                parallel: 1,
                excludes: Vec::new(),
                entries: entries.clone(),
            }
            .into_frame()
            .unwrap()
            .encode(&mut b)
            .unwrap();
            b.len()
        };
        let without_len = {
            let mut b = Vec::new();
            TreeBegin {
                flags: 0,
                parallel: 1,
                excludes: Vec::new(),
                entries,
            }
            .into_frame()
            .unwrap()
            .encode(&mut b)
            .unwrap();
            b.len()
        };
        assert_eq!(with_len - without_len, 4);
    }

    #[test]
    fn tree_begin_unknown_flag_bit_rejected() {
        // The §16.7/§16.8/§16.9 extension mechanism's foundation: unknown
        // bits fail loudly so flag-gated fields can never silently change
        // what a peer parses.
        let frame = TreeBegin {
            flags: 1 << 15, // undefined bit
            parallel: 1,
            excludes: Vec::new(),
            entries: Vec::new(),
        }
        .into_frame()
        .unwrap();
        assert!(matches!(
            TreeBegin::from_frame(frame),
            Err(MessageError::Malformed("TREE_BEGIN has unknown flag bits"))
        ));
    }
}
