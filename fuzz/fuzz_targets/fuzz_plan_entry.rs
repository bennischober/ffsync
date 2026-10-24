//! Fuzz target: PlanEntry::from_frame (§6 plan codec — Local + Remote).
#![no_main]
use libfuzzer_sys::fuzz_target;
use ffsync::protocol::framing::{Frame, T_PLAN_LOCAL, T_PLAN_REMOTE};
use ffsync::protocol::message::{Message, PlanEntry};

fuzz_target!(|data: &[u8]| {
    // Try both plan frame types: the payload layout is shared, the type
    // byte is not part of the payload.
    for t in [T_PLAN_LOCAL, T_PLAN_REMOTE] {
        let frame = Frame::new(t, data.to_vec());
        let _ = PlanEntry::from_frame(frame);
    }
});
