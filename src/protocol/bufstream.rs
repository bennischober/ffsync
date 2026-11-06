//! Buffered duplex adapter for protocol streams.

use std::io::{self, Read, Write};

/// Default read and write buffer size.
const DEFAULT_CAPACITY: usize = 64 * 1024;

/// A read- and write-buffered duplex stream for the sync protocol.
///
/// The protocol writes many small frames (manifest and plan entries are
/// ~50 bytes each). Unbuffered, every frame becomes its own syscall, its own
/// TLS record and — with `TCP_NODELAY` — its own TCP segment; over QUIC
/// every write is a round trip into the async runtime. `BufStream`
/// coalesces writes until [`flush`](Write::flush), until the buffer fills,
/// or until a read needs the peer, and buffers reads so a frame header no
/// longer costs two syscalls. Writes of at least the buffer size (DATA
/// payloads) bypass the buffer after flushing what is pending, so large
/// transfers are not copied twice.
///
/// **Flush before blocking read.** A read that needs bytes from the inner
/// stream first flushes pending writes: a peer waiting for our frame can
/// never be starved by our buffer, so request/response steps stay
/// deadlock-free without relying on every call site's explicit flush. A
/// failing flush there does not fail the read — the peer may have closed
/// its read side after sending a final frame (the §4 fast path's
/// NOT_MODIFIED, an ERROR), and that frame must still be readable. The
/// pending bytes are dropped and every later write reports the failure.
///
/// **Lifetime.** Wrap a connection ONCE, for its whole lifetime: bytes read
/// ahead belong to the next frame, so a wrapper dropped mid-session would
/// lose them. Dropping flushes pending writes (best effort).
pub struct BufStream<S: Read + Write> {
    inner: S,
    rbuf: Box<[u8]>,
    rpos: usize,
    rlen: usize,
    wbuf: Vec<u8>,
    /// A flush on the read path failed: the write side is dead.
    write_failed: Option<io::ErrorKind>,
}

impl<S: Read + Write> BufStream<S> {
    /// Wraps `inner` with the default (64 KiB) read and write buffers.
    pub fn new(inner: S) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, inner)
    }

    /// Wraps `inner` with `capacity`-byte read and write buffers.
    pub fn with_capacity(capacity: usize, inner: S) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner,
            rbuf: vec![0u8; capacity].into_boxed_slice(),
            rpos: 0,
            rlen: 0,
            wbuf: Vec::with_capacity(capacity),
            write_failed: None,
        }
    }

    /// The wrapped stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// The wrapped stream, mutably. Writing to it directly bypasses (and
    /// reorders around) pending buffered writes — flush first.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    fn check_writable(&self) -> io::Result<()> {
        match self.write_failed {
            Some(kind) => Err(io::Error::new(
                kind,
                "stream write side failed during an earlier flush",
            )),
            None => Ok(()),
        }
    }

    /// Writes the whole write buffer to the inner stream. On error the
    /// unwritten bytes stay buffered (the caller decides what that means).
    fn flush_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        let mut result = Ok(());
        while written < self.wbuf.len() {
            match self.inner.write(&self.wbuf[written..]) {
                Ok(0) => {
                    result = Err(io::Error::from(io::ErrorKind::WriteZero));
                    break;
                }
                Ok(n) => written += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.wbuf.drain(..written);
        result
    }
}

impl<S: Read + Write> Read for BufStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.rpos == self.rlen {
            // About to wait for the peer: never leave our frames here.
            if !self.wbuf.is_empty()
                && self.write_failed.is_none()
                && let Err(e) = self.flush_buf().and_then(|()| self.inner.flush())
            {
                self.wbuf.clear();
                self.write_failed = Some(e.kind());
            }
            if buf.len() >= self.rbuf.len() {
                return self.inner.read(buf);
            }
            self.rlen = self.inner.read(&mut self.rbuf)?;
            self.rpos = 0;
        }
        let n = (self.rlen - self.rpos).min(buf.len());
        buf[..n].copy_from_slice(&self.rbuf[self.rpos..self.rpos + n]);
        self.rpos += n;
        Ok(n)
    }
}

impl<S: Read + Write> Write for BufStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.check_writable()?;
        if self.wbuf.len() + buf.len() > self.wbuf.capacity() {
            self.flush_buf()?;
        }
        if buf.len() >= self.wbuf.capacity() {
            return self.inner.write(buf);
        }
        self.wbuf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.check_writable()?;
        self.flush_buf()?;
        self.inner.flush()
    }
}

impl<S: Read + Write> Drop for BufStream<S> {
    fn drop(&mut self) {
        if self.write_failed.is_none() && !self.wbuf.is_empty() {
            let _ = self.flush_buf().and_then(|()| self.inner.flush());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A duplex double: reads from a script, records each inner write call.
    struct Recorder {
        input: Cursor<Vec<u8>>,
        writes: Vec<Vec<u8>>,
        fail_writes: bool,
    }

    impl Read for Recorder {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.fail_writes {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            self.writes.push(buf.to_vec());
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn recorder(input: &[u8]) -> Recorder {
        Recorder {
            input: Cursor::new(input.to_vec()),
            writes: Vec::new(),
            fail_writes: false,
        }
    }

    #[test]
    fn small_writes_coalesce_until_flush() {
        let mut s = BufStream::with_capacity(64, recorder(b""));
        for _ in 0..10 {
            s.write_all(b"abcde").unwrap();
        }
        assert!(s.get_ref().writes.is_empty());
        s.flush().unwrap();
        assert_eq!(s.get_ref().writes, vec![b"abcde".repeat(10)]);
    }

    #[test]
    fn large_writes_bypass_after_pending_bytes() {
        let mut s = BufStream::with_capacity(16, recorder(b""));
        s.write_all(b"head").unwrap();
        s.write_all(&[7u8; 40]).unwrap();
        s.flush().unwrap();
        // Order preserved: the pending header first, then the payload.
        assert_eq!(s.get_ref().writes, vec![b"head".to_vec(), vec![7u8; 40]]);
    }

    #[test]
    fn blocking_read_flushes_pending_writes_first() {
        let mut s = BufStream::with_capacity(64, recorder(b"reply"));
        s.write_all(b"request").unwrap();
        let mut buf = [0u8; 5];
        s.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"reply");
        assert_eq!(s.get_ref().writes, vec![b"request".to_vec()]);
    }

    #[test]
    fn reads_are_served_from_the_buffer() {
        let data: Vec<u8> = (0..200u8).collect();
        let mut s = BufStream::with_capacity(64, recorder(&data));
        let mut out = Vec::new();
        let mut byte = [0u8; 1];
        while s.read(&mut byte).unwrap() == 1 {
            out.push(byte[0]);
        }
        assert_eq!(out, data);
    }

    #[test]
    fn failed_flush_on_read_still_returns_peer_bytes() {
        // The peer sent a final frame and closed its read side: our
        // pending writes fail, but its frame must stay readable, and the
        // write failure must surface on the next write.
        let mut inner = recorder(b"final");
        inner.fail_writes = true;
        let mut s = BufStream::with_capacity(64, inner);
        s.write_all(b"manifest").unwrap();
        let mut buf = [0u8; 5];
        s.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"final");
        assert!(s.write_all(b"more").is_err());
        assert!(s.flush().is_err());
    }

    #[test]
    fn drop_flushes_pending_writes() {
        let mut inner = recorder(b"");
        {
            let mut s = BufStream::with_capacity(64, &mut inner);
            s.write_all(b"bye").unwrap();
        }
        assert_eq!(inner.writes, vec![b"bye".to_vec()]);
    }
}
