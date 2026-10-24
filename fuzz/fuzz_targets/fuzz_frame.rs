//! Fuzz target: framing::read_frame — the length-prefixed frame reader.
//! Every byte on the wire passes through this first; it must never panic,
//! never allocate unboundedly, and never hang (MAX_PAYLOAD guard).
#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    let mut cursor = Cursor::new(data.to_vec());
    let _ = ffsync::protocol::framing::read_frame(&mut cursor);
});
