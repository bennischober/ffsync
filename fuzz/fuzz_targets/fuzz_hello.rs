//! Fuzz target: Hello::from_frame (§4 handshake codec, strict flag decode).
#![no_main]
use libfuzzer_sys::fuzz_target;
use ffsync::protocol::framing::{Frame, T_HELLO};
use ffsync::protocol::message::{Hello, Message};

fuzz_target!(|data: &[u8]| {
    let frame = Frame::new(T_HELLO, data.to_vec());
    let _ = Hello::from_frame(frame);
});
