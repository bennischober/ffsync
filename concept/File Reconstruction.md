The main challenge to solve is how to reconstruct a file from potentially out-of-order chunks, especially when modifying an existing file. The solution is the safe and efficient "out-of-place" reconstruction method:

1.  **Creating a Temporary File:** When the receiver needs to reconstruct `path/to/file.ext`, it first creates a temporary file: `path/to/.file.ext.ffsync-tmp`.

2.  **Pre-allocating Full Size:** The receiver immediately sets the length of this temporary file to the final, correct size of the source file. This is a very fast operation on most filesystems (it creates a "sparse file") and is crucial because it allows you to `seek` and write to any offset without issue.

3.  **Building a "Reconstruction Map":** The receiver creates an in-memory map of all the chunks needed for the new file. For each chunk, it knows its final offset, its length, and where to get the data from:
    *   **Source A (Local):** The chunk already exists in the old `path/to/file.ext` on the receiver's disk.
    *   **Source B (Remote):** The chunk needs to be "downloaded".

4.  **Parallel Execution:** The receiver can now start a pool of tasks to build the temporary file:
    *   **Local Copy Tasks:** These tasks read chunks from the *original* local file and write them to the correct offset in the `.tmp` file.
    *   **Network Receive Task:** This task receives chunks from the network and writes them to their correct offset in the `.tmp` file.

    Because all tasks are writing to different, pre-determined offsets in the temporary file, **the order does not matter**. They can happen concurrently and as data becomes available, maximizing performance.

5.  **Final Verification and Atomic Commit:**
    *   Once all chunks have been written to the `.tmp` file, the receiver performs one final, full BLAKE3 hash on it.
    *   If the hash matches the source's known-good hash, the operation is a success. The receiver performs an **atomic `rename`**, which instantaneously replaces the old `file.ext` with `.file.ext.ffsync-tmp`. This is safe and ensures no partially-written files are ever left in a bad state.
    *   If the hash fails, the `.tmp` file is simply deleted, and the original file is left untouched.