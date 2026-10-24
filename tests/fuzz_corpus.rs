use ffsync::protocol::framing::Frame;
use ffsync::protocol::message::*;
use std::path::PathBuf;

fn dump(name: &str, dir: &str, frame: Frame) {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fuzz/corpus")
        .join(dir);
    std::fs::create_dir_all(&p).unwrap();
    std::fs::write(p.join(name), frame.payload).unwrap();
}

#[test]
fn dump_fuzz_corpus() {
    // Hello (valid + variations)
    let h = Hello {
        version: ffsync::protocol::VERSION,
        flags: FLAG_TRUST_TIMES,
        role: Role::Sender,
        cdc_min: 16384,
        cdc_avg: 65536,
        cdc_max: 262144,
        file_size: 1024,
        file_mtime: 1_700_000_000,
        file_mtime_nanos: 5,
        path: "a.bin".into(),
    };
    dump("hello_valid", "hello", h.clone().into_frame().unwrap());
    dump(
        "hello_zero",
        "hello",
        Hello {
            version: 4,
            flags: 0,
            role: Role::Receiver,
            cdc_min: 0,
            cdc_avg: 0,
            cdc_max: 0,
            file_size: 0,
            file_mtime: 0,
            file_mtime_nanos: 0,
            path: String::new(),
        }
        .into_frame()
        .unwrap(),
    );
    // ManifestChunk
    let mc = ManifestChunk {
        hash: blake3::Hash::from_bytes([7u8; 32]),
        offset: 4096,
        length: 65536,
    };
    dump("mc_valid", "manifest", mc.into_frame().unwrap());
    // PlanEntry
    let pl = PlanEntry::Local {
        target_offset: 0,
        length: 65536,
        source_offset: 8192,
        hash: blake3::Hash::from_bytes([1u8; 32]),
    };
    dump("plan_local", "plan", pl.into_frame().unwrap());
    let pr = PlanEntry::Remote {
        target_offset: 65536,
        length: 262144,
        hash: blake3::Hash::from_bytes([2u8; 32]),
    };
    dump("plan_remote", "plan", pr.into_frame().unwrap());
    // TreeBegin with entries (metadata + excludes + parallel)
    let tb = TreeBegin {
        flags: TREE_FLAG_PARALLEL | TREE_FLAG_METADATA | TREE_FLAG_EXCLUDES | TREE_FLAG_NEGATION,
        parallel: 4,
        excludes: vec!["*.log".into(), "!keep.log".into()],
        entries: vec![
            TreeEntryWire {
                rel_path: "a/b/c.bin".into(),
                kind: 0,
                size: 12345,
                mtime_secs: 1_700_000_000,
                mtime_nanos: 9,
                mode: 0o644,
            },
            TreeEntryWire {
                rel_path: "d/".into(),
                kind: 1,
                size: 0,
                mtime_secs: 0,
                mtime_nanos: 0,
                mode: 0,
            },
        ],
    };
    dump("tree_begin_full", "tree_begin", tb.into_frame().unwrap());
    dump(
        "tree_begin_min",
        "tree_begin",
        TreeBegin {
            flags: 0,
            parallel: 0,
            excludes: vec![],
            entries: vec![],
        }
        .into_frame()
        .unwrap(),
    );
    // Tree small frames
    dump(
        "tw",
        "tree_small",
        TreeWorker { worker_index: 3 }.into_frame().unwrap(),
    );
    dump(
        "tf",
        "tree_small",
        TreeFile {
            rel_path: "x/y.txt".into(),
        }
        .into_frame()
        .unwrap(),
    );
    dump(
        "te",
        "tree_small",
        TreeEnd {
            deleted_count: 2,
            deleted_bytes: 999,
        }
        .into_frame()
        .unwrap(),
    );
    dump(
        "err",
        "tree_small",
        encode_error(ErrorCode::BadFrame, "boom").unwrap(),
    );
    // framing corpus: raw byte streams with valid + invalid length prefixes (fuzz_frame reads raw bytes)
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/frame");
    std::fs::create_dir_all(&p).unwrap();
    let mut stream = Vec::new();
    stream.extend_from_slice(&4u32.to_le_bytes());
    stream.extend_from_slice(&ffsync::protocol::framing::T_HELLO.to_le_bytes());
    stream.extend_from_slice(b"FFSY");
    std::fs::write(p.join("tiny_valid"), &stream).unwrap();
    let mut bad = Vec::new();
    bad.extend_from_slice(&u32::MAX.to_le_bytes()); // absurd length -> MAX_PAYLOAD guard
    std::fs::write(p.join("absurd_len"), &bad).unwrap();
    std::fs::write(p.join("empty"), b"").unwrap();
    // The full fuzz/corpus dir is gitignored; this test only seeds it.
}
