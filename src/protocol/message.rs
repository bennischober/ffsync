//! Typed protocol frames (`concept/Protocol v0.1.md` §3–§4).
//!
//! Each frame type maps to a struct with an explicit binary encoding on top
//! of the [`framing`] envelope. Encoding is hand-rolled (D1: no protobuf).

use blake3::Hash;
use std::io::{self, Read, Write};

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
pub const FLAG_EAGER: u16 = 1 << 1; // reserved, must be 0 in v0.1
pub const FLAG_RESUMING: u16 = 1 << 2;

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
    pub file_mtime: u64, // unix seconds, 0 if absent
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
    Local {
        target_offset: u64,
        length: u32,
        source_offset: u64,
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
    /// Payload does not match the expected structure.
    Malformed(&'static str),
    /// Underlying frame-level error.
    Frame(framing::FrameError),
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageError::UnexpectedType(t) => write!(f, "unexpected frame type 0x{:02X}", t),
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
        let role_byte = read_bytes(p, &mut pos, 1)?[0];
        let role = Role::from_u8(role_byte).ok_or(MessageError::Malformed("invalid role"))?;
        let cdc_min = read_u32_le(p, &mut pos)?;
        let cdc_avg = read_u32_le(p, &mut pos)?;
        let cdc_max = read_u32_le(p, &mut pos)?;
        let file_size = read_u64_le(p, &mut pos)?;
        let file_mtime = read_u64_le(p, &mut pos)?;
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
            } => {
                let mut payload = Vec::with_capacity(20);
                payload.extend_from_slice(&target_offset.to_le_bytes());
                payload.extend_from_slice(&length.to_le_bytes());
                payload.extend_from_slice(&source_offset.to_le_bytes());
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
                if frame.payload.len() != 20 {
                    return Err(MessageError::Malformed("plan local must be 20 bytes"));
                }
                let mut off = [0u8; 8];
                off.copy_from_slice(&frame.payload[0..8]);
                let mut len = [0u8; 4];
                len.copy_from_slice(&frame.payload[8..12]);
                let mut src = [0u8; 8];
                src.copy_from_slice(&frame.payload[12..20]);
                Ok(PlanEntry::Local {
                    target_offset: u64::from_le_bytes(off),
                    length: u32::from_le_bytes(len),
                    source_offset: u64::from_le_bytes(src),
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
    let frame = framing::read_frame(inp)?;
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

/// Writes a frame and returns the wire length (for progress accounting).
pub fn write_frame(out: &mut impl Write, frame: &Frame) -> io::Result<usize> {
    frame.encode(out)?;
    Ok(frame.encoded_len())
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
}
