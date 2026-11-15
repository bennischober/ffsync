//! ffsync library crate: chunk-level file synchronisation primitives.
//!
//! The data plane lives in `chunking` and `reconstruction`, the wire
//! protocol in `protocol`, tree scanning/triage in `tree`, the persistent
//! chunk index in `index`, and transport security in `transport`. The
//! binary in `src/main.rs` is the `ffsync` CLI (send/receive/trust/
//! untrust/fingerprints); the benchmark harness is `src/bin/bench.rs`.
//!
//! # Example
//!
//! A single-file sync over plaintext TCP. The state machines run over any
//! `Read + Write` stream; the CLI adds TLS ([`transport`]) and wraps each
//! connection in a [`protocol::BufStream`]. Requests are built with a
//! constructor, and every option starts off; set the ones you need on the
//! public fields.
//!
//! ```no_run
//! use std::net::{TcpListener, TcpStream};
//! use std::path::PathBuf;
//! use std::thread;
//!
//! use ffsync::chunking::PipelineConfig;
//! use ffsync::protocol::BufStream;
//! use ffsync::protocol::sync::{ReceiveRequest, SendRequest, receive, send};
//!
//! let listener = TcpListener::bind("127.0.0.1:0")?;
//! let addr = listener.local_addr()?;
//!
//! let receiver = thread::spawn(move || {
//!     let (stream, _) = listener.accept().unwrap();
//!     let target = PathBuf::from("copy/video.mkv");
//!     // The existing target is the basis whose chunks are reused.
//!     let request = ReceiveRequest::new(target.clone(), target);
//!     let mut stream = BufStream::new(stream);
//!     receive(&mut stream, &request, &PipelineConfig::default(), &mut |_| {})
//! });
//!
//! let mut request = SendRequest::new(PathBuf::from("video.mkv"), "video.mkv".into());
//! request.force_checksum = true; // compare content even if size and mtime match
//! let mut stream = BufStream::new(TcpStream::connect(addr)?);
//! let outcome = send(&mut stream, &request, &PipelineConfig::default(), &mut |_| {});
//! println!("sender: {outcome:?}");
//! println!("receiver: {:?}", receiver.join().unwrap());
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod chunking;
pub mod index;
pub mod protocol;
pub mod reconstruction;
pub mod transport;
pub mod tree;
