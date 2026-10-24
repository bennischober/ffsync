//! Fuzz targets: TreeWorker / TreeFile / TreeEnd + ERROR payload decode.
#![no_main]
use libfuzzer_sys::fuzz_target;
use ffsync::protocol::framing::{Frame, T_TREE_END, T_TREE_FILE, T_TREE_WORKER};
use ffsync::protocol::message::{read_error_payload, Message, TreeEnd, TreeFile, TreeWorker};

fuzz_target!(|data: &[u8]| {
    let w = Frame::new(T_TREE_WORKER, data.to_vec());
    let _ = TreeWorker::from_frame(w);
    let f = Frame::new(T_TREE_FILE, data.to_vec());
    let _ = TreeFile::from_frame(f);
    let e = Frame::new(T_TREE_END, data.to_vec());
    let _ = TreeEnd::from_frame(e);
    // ERROR payloads are (code u8, msg bytes) — read_error_payload validates.
    let err = Frame::new(ffsync::protocol::framing::T_ERROR, data.to_vec());
    let _ = read_error_payload(&err);
});
