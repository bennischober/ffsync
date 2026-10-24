//! Fuzz target: TreeBegin::from_frame (§16.2 codec — flags-gated fields,
//! entries with paths/sizes/mtimes/modes; the richest decoder surface).
#![no_main]
use libfuzzer_sys::fuzz_target;
use ffsync::protocol::framing::{Frame, T_TREE_BEGIN};
use ffsync::protocol::message::{Message, TreeBegin};

fuzz_target!(|data: &[u8]| {
    let frame = Frame::new(T_TREE_BEGIN, data.to_vec());
    let _ = TreeBegin::from_frame(frame);
});
