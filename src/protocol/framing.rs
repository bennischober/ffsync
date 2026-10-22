//! Wire framing for the ffsync v0.1 protocol (`concept/Protocol v0.1.md` §3).
//!
//! Frame layout: `type: u8 | payload_len: u32 (LE) | payload`.
//! All integers are little-endian. Maximum payload is 64 MiB. Unknown frame
//! types are a protocol error (forward compatibility is by version
//! negotiation in HELLO, not by tolerating unknown frames).

use std::io::{self, Read, Write};

/// Maximum payload size of a single frame.
pub const MAX_PAYLOAD: u32 = 64 * 1024 * 1024;

/// Frame type byte for `HELLO`.
pub const T_HELLO: u8 = 0x01;
/// Frame type byte for `MANIFEST_CHUNK`.
pub const T_MANIFEST_CHUNK: u8 = 0x02;
/// Frame type byte for `MANIFEST_DONE`.
pub const T_MANIFEST_DONE: u8 = 0x03;
/// Frame type byte for `PLAN_LOCAL`.
pub const T_PLAN_LOCAL: u8 = 0x04;
/// Frame type byte for `PLAN_REMOTE`.
pub const T_PLAN_REMOTE: u8 = 0x05;
/// Frame type byte for `PLAN_DONE`.
pub const T_PLAN_DONE: u8 = 0x06;
/// Frame type byte for `DATA`.
pub const T_DATA: u8 = 0x07;
/// Frame type byte for `VERIFY`.
pub const T_VERIFY: u8 = 0x08;
/// Frame type byte for `COMMIT_ACK`.
pub const T_COMMIT_ACK: u8 = 0x09;
/// Frame type byte for `NOT_MODIFIED`.
pub const T_NOT_MODIFIED: u8 = 0x0A;
/// Frame type byte for `ERROR`.
pub const T_ERROR: u8 = 0x0B;
/// Frame type byte for `TREE_BEGIN` (v0.3, §16).
pub const T_TREE_BEGIN: u8 = 0x0C;
/// Frame type byte for `TREE_END` (v0.3, §16).
pub const T_TREE_END: u8 = 0x0D;
/// Frame type byte for `FILE_SYNC` (v0.3, §16.4): the sender's "this file
/// needs a full conversation" signal inside a tree session — the tree-mode
/// receiver reads exactly one frame after HELLO (NOT_MODIFIED or FILE_SYNC)
/// instead of racing manifest writes against a fast-path sender.
pub const T_FILE_SYNC: u8 = 0x0E;
/// Frame type byte for `TREE_WORKER` (v4, §16.8): announces a worker
/// connection (worker_index) and is ACKed before conversations start.
pub const T_TREE_WORKER: u8 = 0x0F;
/// Frame type byte for `TREE_FILE` (v4, §16.8): names the path of the next
/// conversation on a (worker) connection. Replaces the implicit sequential
/// order of a parallel session — the receiver validates membership, not
/// order.
pub const T_TREE_FILE: u8 = 0x10;

/// A raw frame: type byte plus payload. Typed frames are built on top of
/// this in [`super::message`]; the codec layer only knows the envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub ftype: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(ftype: u8, payload: Vec<u8>) -> Self {
        Self { ftype, payload }
    }

    /// Encodes the frame into `out`.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        if self.payload.len() as u64 > MAX_PAYLOAD as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "payload of {} bytes exceeds {}",
                    self.payload.len(),
                    MAX_PAYLOAD
                ),
            ));
        }
        out.write_all(&[self.ftype])?;
        out.write_all(&(self.payload.len() as u32).to_le_bytes())?;
        out.write_all(&self.payload)
    }

    /// Encoded size in bytes (for progress accounting).
    pub fn encoded_len(&self) -> usize {
        1 + 4 + self.payload.len()
    }
}

/// Errors from [`read_frame`].
#[derive(Debug)]
pub enum FrameError {
    /// Clean end of stream before any byte of a frame (normal EOF).
    Eof,
    /// Stream ended in the middle of a frame.
    Truncated,
    /// Protocol violation (oversized payload, unknown type if strict, io).
    Io(io::Error),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Eof => write!(f, "connection closed"),
            FrameError::Truncated => write!(f, "connection closed mid-frame"),
            FrameError::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        // UnexpectedEof while reading payload bytes means truncation, not
        // a clean EOF: the length header was already consumed.
        if e.kind() == io::ErrorKind::UnexpectedEof {
            FrameError::Truncated
        } else {
            FrameError::Io(e)
        }
    }
}

/// Reads one frame. Clean EOF before any byte of a frame is reported as
/// `Err(FrameError::Eof)` (callers match on it explicitly); a header or
/// payload that ends mid-frame is `FrameError::Truncated`.
pub fn read_frame(inp: &mut (impl Read + ?Sized)) -> Result<Frame, FrameError> {
    // Read the header byte-wise so we can distinguish "stream ended cleanly
    // between frames" (Eof) from "stream ended inside a frame" (Truncated):
    // read_exact consumes a partial header without reporting how many bytes
    // it got, which would make that distinction impossible.
    let mut first = [0u8; 1];
    match inp.read_exact(&mut first) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(FrameError::Eof),
        Err(e) => return Err(FrameError::Io(e)),
    }
    let mut rest = [0u8; 4];
    if let Err(e) = inp.read_exact(&mut rest) {
        return Err(FrameError::from(e));
    }
    let ftype = first[0];
    let len = u32::from_le_bytes(rest);
    if len > MAX_PAYLOAD {
        return Err(FrameError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame payload of {} bytes exceeds {}", len, MAX_PAYLOAD),
        )));
    }
    let mut payload = vec![0u8; len as usize];
    if let Err(e) = inp.read_exact(&mut payload) {
        return Err(FrameError::from(e));
    }
    Ok(Frame { ftype, payload })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip() {
        for payload_len in [0usize, 1, 5, 44, 1000, 3 * 1024 * 1024] {
            let frame = Frame::new(T_DATA, vec![0xABu8; payload_len]);
            let mut buf = Vec::new();
            frame.encode(&mut buf).unwrap();
            assert_eq!(buf.len(), frame.encoded_len());

            let mut cursor = Cursor::new(buf);
            let decoded = read_frame(&mut cursor).unwrap();
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn multiple_frames_sequential() {
        let frames = vec![
            Frame::new(T_HELLO, vec![1, 2, 3]),
            Frame::new(T_MANIFEST_DONE, vec![]),
            Frame::new(T_PLAN_REMOTE, vec![0x55; 44]),
            Frame::new(T_COMMIT_ACK, vec![]),
        ];
        let mut buf = Vec::new();
        for f in &frames {
            f.encode(&mut buf).unwrap();
        }
        let mut cursor = Cursor::new(buf);
        for expected in &frames {
            assert_eq!(read_frame(&mut cursor).unwrap(), *expected);
        }
        assert!(matches!(read_frame(&mut cursor), Err(FrameError::Eof)));
    }

    #[test]
    fn oversized_length_rejected() {
        // Length field claims 1 byte more than the maximum.
        let mut buf = Vec::new();
        buf.push(T_DATA);
        buf.extend_from_slice(&(MAX_PAYLOAD + 1).to_le_bytes());
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert!(matches!(err, FrameError::Io(_)));
    }

    #[test]
    fn truncated_payload_detected() {
        let frame = Frame::new(T_DATA, vec![0x11; 100]);
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        buf.truncate(5 + 40); // header + partial payload
        let mut cursor = Cursor::new(buf);
        assert!(matches!(
            read_frame(&mut cursor),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn empty_frame_round_trip() {
        let frame = Frame::new(T_NOT_MODIFIED, vec![]);
        let mut buf = Vec::new();
        frame.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 5);
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_frame(&mut cursor).unwrap(), frame);
    }

    #[test]
    fn truncated_header_detected() {
        // A header that starts but does not finish is Truncated, not Eof.
        let buf = [T_DATA, 0x01]; // type + 1 of 4 length bytes
        let mut cursor = Cursor::new(buf);
        assert!(matches!(
            read_frame(&mut cursor),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let payload = vec![0u8; (MAX_PAYLOAD + 1) as usize];
        let frame = Frame::new(T_DATA, payload);
        let mut buf = Vec::new();
        assert!(frame.encode(&mut buf).is_err());
    }
}
