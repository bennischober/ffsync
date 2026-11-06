use std::fs::File;
use std::io;

/// Trait for positional I/O (reading/writing at an offset without moving cursor)
pub trait FileExt {
    /// One positional read; may return fewer bytes than `buf.len()`.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    /// One positional write; may write fewer bytes than `buf.len()`.
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;

    /// Reads until `buf` is full or end of file, retrying short reads and
    /// `Interrupted`; returns the bytes read (`< buf.len()` only at EOF).
    /// For callers that verify the content anyway (a short read then fails
    /// the hash check instead of surfacing as a separate error).
    fn read_full_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let mut done = 0usize;
        while done < buf.len() {
            match self.read_at(&mut buf[done..], offset + done as u64) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(done)
    }

    /// Fills `buf` completely from `offset` (`UnexpectedEof` if the file
    /// ends first). A single `read_at` may return short; this never does.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if self.read_full_at(buf, offset)? < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file ended before the requested range",
            ));
        }
        Ok(())
    }

    /// Writes all of `buf` at `offset`, retrying short writes and
    /// `Interrupted` (`WriteZero` if the OS stops accepting bytes).
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut done = 0usize;
        while done < buf.len() {
            match self.write_at(&buf[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "positional write accepted no bytes",
                    ));
                }
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

// Unix Implementation (uses pread/pwrite)
#[cfg(unix)]
impl FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(self, buf, offset)
    }
}

// Windows Implementation (uses Seek+Write atomically or Overlapped I/O internally)
#[cfg(windows)]
impl FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(self, buf, offset)
    }
}
