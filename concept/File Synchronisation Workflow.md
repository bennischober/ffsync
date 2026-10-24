The `ffsync` synchronisation process is a multi-phase pipeline designed to eliminate redundant work at every step. It uses a series of progressively deeper "diffs" to ensure that the expensive work of chunk-level analysis is only performed on the absolute minimum set of files required.

> **Status (v0.3):** Phase 1 is implemented as the tree manifest exchange and
> triage (`Design Decisions.md` D17, wire spec §16 of `concept/Protocol.md`).
> Phase 2 is **absorbed into Phase 3**: the per-file conversation's VERIFY
> (chunk-tree root comparison) is a strictly stronger identity check than a
> per-file root exchange, and with warm indexes (D15) it costs no extra scan.
> A separate whole-file-hash exchange would only save wire frames for
> touched-but-identical files with warm indexes — measured to be a marginal
> win, not implemented (recorded in D17). Phases 3–4 are the existing
> per-file state machines, run sequentially inside the tree envelope.

*   **Phase 1: Fast Metadata Reconciliation ("Shallow Diff")**
    1.  **Concurrent Scan:** Immediately after the connection is established, both client and server begin a parallel, non-blocking scan of their respective directory trees. This scan is lightweight, gathering only file metadata: `(relative_path, size, modification_time)`.
    2.  **Metadata Exchange:** The client and server stream their metadata lists to each other.
    3.  **Initial Triage:** Each peer compares the received list with its own, sorting all files into three distinct buckets:
        *   **Bucket A (Presumed Identical):** Files with matching path, size, and modification time. These files are considered "resolved" for now and are ignored in subsequent phases, representing a massive I/O and CPU saving.
        *   **Bucket B (Missing/Extra):** Files that exist on one peer but not the other.
        *   **Bucket C (Potentially Modified):** Files that exist on both peers but have a mismatch in size or modification time.

*   **Phase 2: Verifiable File-Level Hashing ("Medium Diff")**
    1.  **Targeted Hashing:** This phase focuses only on the files in **Bucket C**. Both peers now read the full contents of these files and compute a single, top-level BLAKE3 root hash for each one. This is still much faster than chunking.
    2.  **Hash Exchange:** The peers exchange the lists of `(relative_path, blake3_root_hash)` for the files in Bucket C.
    3.  **Final Triage:** Any files with matching root hashes are proven to be content-identical, despite metadata differences. They are moved to the "resolved" list. The remaining files in Bucket C are now confirmed to be different and are moved to a new list: **`Confirmed Modified`**.

*   **Phase 3: Chunk-Level Analysis & Sync Plan Generation ("Deep Diff")**
    1.  **Pipelined Chunking:** This is the most computationally intensive phase and is only performed on files from the `Confirmed Modified` list and the source's `Missing/Extra` list (Bucket B).
    2.  Both peers start the CDC and BLAKE3 chunk-hashing process for these files. Crucially, this is a **pipelined operation**:
    3.  As the destination peer finishes hashing a file, it sends its complete list of chunk hashes to the source.
    4.  The source, which may still be hashing a much larger version of the same file, uses this list to perform a "live diff" (using a `HashSet` for fast lookups).
    5.  **`SyncPlan` Generation:** Based on the results of all previous phases, the source peer generates a comprehensive `SyncPlan`. This is a list of actions for the destination, such as:
        *   `Delete(file_path)`
        *   `RequestFullTransfer(file_path)`
        *   `ReconstructFile(file_path, required_chunks_list)`

*   **Phase 4: Pipelined Data Transfer & Reconstruction**
    1.  **Plan Execution:** The source begins sending the data required to fulfill the `SyncPlan`. Thanks to the live diff in Phase 3, this transfer can start *before* the source has even finished hashing, completely eliminating idle time.
    2.  **Data Streaming:** Required raw chunks are compressed with Zstd and sent over the stream.
    3.  **File Reconstruction:** The destination peer executes the reconstruction logic).
    4.  **Verification:** Once a file is fully reconstructed, a final BLAKE3 hash is performed on the new file to guarantee its integrity matches the source's hash before it is moved into its final location.