# Chunking Algorithm Evaluation

The evolution of the chunking mechanism in numbers. To begin with, the file that is used has a total
size of 78129430149 bytes (roughly 72.7 GiB). The file is read from a Samsung 860 QVO (2TB) SSD, so
we are already limited to a maximum read speed of about 550 MB/s.

The initial implementation used the `AsyncStreamCDC` implementation from the `fastcdc` crate with
`tokio` to read the file asynchronously. However, due to the overhead of asynchronous reading and
the fact that the SSD cannot saturate the read speed, the drive utilization was quite low. The average
drive utilization was around 12%. The whole process took 1298.0573254s.

Compared to `rsync`, this was actually a performance downgrade. `rsync` was able to read the file and
compute strong hashes in under 15 minutes.

To improve the performance, two channels were introduced to separate the reading, chunking, and hashing stages.
The chunking stage was a single-threaded implementation to allow for fast sequential reading of the file. The
hashing stage was a multithreaded (native threads) implementation to utilize multiple CPU cores for hashing.

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 221.6832065s | ~75%              | 60997            |
| FastCDC (avg 1 MiB)  | 496.4611076s | ~50%              | 109437           |

Compared to the initial approach, we already have a performance gain of 2.6x with the `FastCDC` algorithm and
a 5.8x gain with the `Fixed Size` algorithm. The drive utilization has also increased significantly, indicating
that the new implementation is better at keeping the SSD busy.

To mitigate the issues of the drive being not fully utilized, a new implementation using `memmap2` for memory
mapping, `crossbeam` for channels and native threads for true parallelism was created.

The process of reading, chunking, and hashing was split into separate and truly parallel stages:

1. `memmap2` is used to memory map the file, allowing for fast sequential access.
2. A dedicated thread reads the memory-mapped file in large blocks, chunks them using the provided chunking
   algorithm (either `FastCDC` or `FixedSize`), and sends the chunks to a channel.
3. Multiple worker threads receive the chunks from the channel, compute their strong hashes, and write the
   to a separate channel.

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 242.1889197s | 100%              | 60817            |
| FastCDC (avg 1 MiB)  | 244.5058804s | 100%              | 109146           |

To further reduce memory usage, the way how the chunking thread sends chunks to the hashing threads was modified.
Instead of sending the entire chunk data, only the offset and length of each chunk is sent. The hashing threads
then read the chunk data directly from the memory-mapped file using the provided offset and length.

```diff
@@ -1,4 +1,4 @@
 struct ChunkWork {
     offset: u64,
-    data: Vec<u8>,
+    length: usize,
 }
```

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 243.9368918s | 100%              | 60817            |
| FastCDC (avg 1 MiB)  | 242.1928239s | 100%              | 109146           |

With this final optimization, the memory usage was significantly reduced while maintaining the same
performance. The drive utilization remained at 100%, indicating that the SSD was fully utilized during
the chunking and hashing process.

> [!NOTE]
> These results are no real benchmarks but rather observations during development. Actual benchmarks
> may vary based on hardware, file types, and other factors. Benchmarks should also run in isolated
> environments to minimize interference from other processes.