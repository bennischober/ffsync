//! Fuzz target: ManifestChunk::from_frame (§5 manifest codec).
#![no_main]
use libfuzzer_sys::fuzz_target;
use ffsync::protocol::framing::{Frame, T_MANIFEST_CHUNK};
use ffsync::protocol::message::{ManifestChunk, Message};

fuzz_target!(|data: &[u8]| {
    let frame = Frame::new(T_MANIFEST_CHUNK, data.to_vec());
    let _ = ManifestChunk::from_frame(frame);
});
