

## **Section 1: Introduction and Foundational Principles**

### **1.1. Executive Summary: The Case for a New Delta Synchronization Protocol**

The synchronization of multi-gigabyte (multi-GB) file directories across high-latency, unreliable networks presents significant challenges that conventional tools, such as rsync, fail to address efficiently. The primary objective of this expert report is to detail the design of a next-generation file synchronization tool, *RustSync*, implemented in Rust, engineered to minimize synchronization time and data transfer volumes dramatically.

The proposed architecture leverages a modern, highly optimized stack: QUIC (Quick UDP Internet Connection) for transport resilience, BLAKE3 for high-speed, parallel integrity verification, Zstandard (Zstd) for rapid payload compression, and Content-Defined Chunking (CDC) for robustness against byte shifts. The current limitations of legacy systems are often rooted in transport inefficiencies, such as TCP Head-of-Line (HOL) blocking, and algorithmic fragility, specifically the susceptibility of fixed-block hashing to minor file modifications.1 By addressing these core weaknesses, this design provides a path to superior performance under adverse network conditions.

### **1.2. Design Objectives: Optimizing for High-Latency, High-Volume Data**

Performance in a high-latency environment requires strict optimization of connection establishment and metadata exchange. The design specifically targets minimizing the Round Trip Time (RTT) penalty. This optimization relies on sub-second connection establishment through QUIC’s 0-RTT capability 2, ensuring the application state can resume instantly.

Furthermore, the system requires intrinsically verifiable integrity checks, achieved through the BLAKE3 tree hashing structure.3 Finally, robust resilience to localized file modifications is paramount, which is achieved by employing Content-Defined Chunking (CDC) instead of fixed-block division.4 These three pillars—0-RTT transport, parallel verifiable integrity, and content-based resilience—form the foundation for efficient synchronization across Wide Area Networks (WANs).

## **Section 2: Architectural Critique of Rsync and Identification of Bottlenecks**

### **2.1. The Traditional Rsync Algorithm: Weak vs. Strong Checksums**

The core mechanism of rsync involves dividing files into fixed-size blocks. It utilizes a two-tiered checksum approach: a weak, fast rolling checksum (such as Adler-32 or similar variants) is used for rapid screening, and a strong, cryptographically secure hash (like MD5 or a faster alternative such as XXH3, if configured) is employed for definitive block verification.5

The synchronization process requires the destination (receiver) to calculate and transmit these checksum pairs back to the source. The source then uses its local file data and the rolling checksum mechanism to identify potential matches, generating a delta file comprising instructions for blocks that match and raw data for blocks that require updating.8

This reliance on fixed block boundaries is the primary algorithmic vulnerability of rsync. While the rolling checksum effectively handles data *appended* to a file, any insertion or deletion near the start causes a global byte shift. Since the boundaries are fixed, the rolling checksum of every subsequent block fails, leading to the transfer of a large portion, if not the entirety, of the remaining file content. This phenomenon severely limits rsync's efficiency when internal file modifications occur, making it fragile compared to Content-Defined Chunking systems.

### **2.2. Critical Overheads in Rsync**

#### **2.2.1. Transport Latency and Head-of-Line Blocking**

Rsync traditionally operates over TCP, often encapsulated within an SSH tunnel. TCP introduces the issue of Head-of-Line (HOL) blocking, where the delivery of data streams is strictly ordered. The loss of a single packet necessitates retransmission before any subsequent packets can be processed by the application, regardless of which stream they belong to. This interconnectivity catastrophically harms throughput and increases effective latency, particularly on high-loss or high-latency links.9

The network transport overhead, including the initial latency for the TCP handshake, followed by the SSH establishment, and the subsequent synchronization negotiation, imposes a substantial latency penalty. In high-RTT environments, this initial latency significantly delays the start of the data transfer, diminishing the value of the efficient delta calculation.

#### **2.2.2. Resumption Fragility and Overhead**

Resuming an interrupted transfer in rsync requires careful management of state. The client must explicitly use the \--partial option to instruct the receiver to retain the temporary file (often named .\<filename\>) upon interruption. If the network connection breaks, the rsync server processes on the receiver must be terminated "politely," typically via a SIGTERM signal or by waiting for a timeout (adjustable via \--timeout=X), to allow the server to move the partial temporary file into its proper name. Forceful termination using SIGKILL results in the deletion of the partial file, losing all progress.10

This complex state management requires application-level diligence. Furthermore, upon resumption, the protocol usually requires the partial file to be re-hashed or at least its initial sections verified. This CPU-intensive process of verification introduces significant overhead for large partial files before the transfer can safely resume, representing a major bottleneck in recovery.

## **Section 3: The Modern Data Transport Layer: QUIC Justification**

The core limitations of TCP necessitate a shift to a modern transport protocol. QUIC, designed as the foundation for HTTP/3, is based on UDP and addresses the most severe transport deficiencies that plague synchronization over WANs.

### **3.1. Elimination of Head-of-Line Blocking and Multiplexing**

QUIC fundamentally solves the TCP HOL blocking problem by implementing streams independently. Since loss recovery and flow control operate granularly per stream, the loss of a packet affecting one stream does not impede the delivery of data on parallel, independent streams.9

This stream independence is architecturally critical for RustSync. The protocol can use multiple independent QUIC streams to handle divergent synchronization tasks concurrently: one stream for high-priority directory metadata updates, others for small control messages (like Merkle diff results), and dedicated streams for large bulk data chunks. This multiplexing prevents network congestion or transient loss from stalling the entire synchronization process.

### **3.2. Connection Efficiency and Resilience**

QUIC significantly minimizes connection setup latency, achieving secure connection establishment in just 1 RTT, and leveraging 0 RTT for subsequent secure sessions.2 This dramatically cuts the latency penalty associated with establishing a robust connection, making synchronization initiation far faster on high-latency links.

Furthermore, QUIC natively supports connection migration, allowing the synchronization session to persist even if the underlying network path (IP address or port) changes. This resilience is vital for mobile clients or highly volatile WAN connections.2 QUIC also provides application-level control over packetization. RustSync can optimize delivery by instructing QUIC to prioritize low-latency delivery (immediate packet send) for small, critical control messages and use high-throughput bundling for bulk data transfer streams, balancing responsiveness and bandwidth efficiency.

### **3.3. Leveraging gRPC over QUIC (HTTP/3)**

Integrating gRPC with HTTP/3 (QUIC) results in a highly efficient and structurally robust communication system. gRPC relies on Protocol Buffers (protobuf) for compact, binary serialization, which reduces the size of control data transferred. The inherent streaming capabilities of gRPC over HTTP/2 (and improved over HTTP/3) naturally support sending and receiving large files in manageable chunks.15

Adopting gRPC and its associated protobuf schema provides critical benefits: strict schema definition ensures type safety 17, and the built-in stream management framework simplifies the application logic for handling resumable transfers.15 This approach is intrinsically superior to relying on ad-hoc state machine negotiation over raw TCP/SSH.

## **Section 4: Data Fingerprinting and Structural Integrity with BLAKE3**

Efficient delta synchronization relies on fast, verifiable data fingerprinting. The choice of cryptographic hash function dictates the system's performance limits.

### **4.1. BLAKE3: The Performance Mandate**

BLAKE3 is a cryptographic hash function optimized specifically for speed and massive parallelism. Developed as an evolution of BLAKE2, BLAKE3 supports an unbounded degree of parallelism through its internal tree structure. This allows the hashing process—which is often the most significant CPU and I/O bottleneck during the initial file scan—to scale linearly with the available CPU cores and SIMD lanes.

BLAKE3’s performance advantage is substantial, with peak single-threaded throughput measured at 4x that of BLAKE2b and 12x that of SHA-256 on modern Intel architectures. This high speed allows the system to rapidly generate the file index, minimizing the initial latency required before the synchronization phase can begin, a critical difference from legacy hash functions used in standard rsync deployments.

### **4.2. Incremental Hashing and Verified Streaming**

The BLAKE3 design natively supports crucial features for file synchronization: verified streaming and incremental updates. Its internal tree structure means that the hash state can accept an arbitrary number of writes (updates) , and the Rust blake3::Hasher implementation integrates with multithreading frameworks like rayon for parallel processing of large inputs.

This tree structure elegantly resolves the challenge of integrating data integrity checks with index generation. When the system processes chunks derived from Content-Defined Chunking, the BLAKE3 algorithm automatically generates the necessary "Parent Node Chaining Values". Therefore, the action of fingerprinting the data chunks inherently produces the intermediate hash nodes necessary for building a Merkle Tree.3 This integration streamlines the metadata pipeline, making the verification of transferred data fragments both highly efficient and cryptographically sound.

## **Section 5: Algorithmic Efficiency: Content-Defined Chunking and Merkle Structures**

### **5.1. The Necessity of Content-Defined Chunking (CDC)**

To solve the byte-shift flaw inherent in fixed-block algorithms, the synchronization protocol must adopt Content-Defined Chunking (CDC). CDC partitions a data stream into variable-length blocks (chunks) whose boundaries are determined by intrinsic patterns within the data stream, typically using rolling hash techniques.

The central benefit of CDC is its robustness against localized modification. If a small insertion or deletion occurs within a file, only the chunk containing the modification, and potentially one or two neighboring chunks, are affected. All other subsequent chunks maintain their original content and, therefore, their original cryptographic fingerprints. This property ensures superior deduplication ratios and drastically reduces the amount of data requiring transfer compared to fixed-block methods.

### **5.2. Selecting and Optimizing a High-Performance CDC Algorithm**

The main challenge associated with CDC algorithms is their computational intensity, as they require scanning the entire data stream byte-by-byte to determine chunk boundaries. To mitigate this CPU overhead, high-performance algorithms such as FastCDC (FCDC) and SeqCDC have been developed. SeqCDC, for instance, has demonstrated throughput improvements ranging from \$1.5\\times\$ to \$3.1\\times\$ compared to the state-of-the-art, especially when targeting larger chunk sizes.

Although smaller chunks (e.g., 512 B to 4 KB) tend to maximize deduplication, they generate a massive increase in metadata overhead (more BLAKE3 hashes and a larger index structure). For synchronizing multi-GB files efficiently, the optimal design necessitates using CDC implementations tuned for larger average chunk sizes (e.g., 32 KB to 256 KB). This balances indexing throughput (CPU load) against the network transfer volume required for the metadata index. Libraries such as the pure-Rust implementation of fastcdc can serve as a suitable foundation.

### **5.3. Merkle Trees and Prolly Trees for Index Synchronization**

Synchronizing large file directories efficiently requires comparing index structures, rather than performing slow, linear comparisons of raw file attributes.19 The system must employ a content-addressable storage index, specifically the Prolly Tree (Probabilistic Tree).

A Prolly Tree is a Merkle Tree structure where nodes are defined by content-defined chunking, giving it version control capabilities similar to Git. The synchronization protocol begins with the exchange of the Merkle root hash (M-root). If the M-roots differ, the peers recursively traverse the tree, comparing hash nodes layer by layer, until the divergence is localized to a set of leaf nodes corresponding to CDC chunk hashes.19 This approach minimizes the RTT cycles required for initial metadata exchange, ensuring rapid identification of the delta over high-latency networks.

Table 2 provides a comparative analysis of the proposed cryptographic and algorithmic components against traditional approaches.

Table 2: Comparative Analysis of Hashing and Chunking Algorithms

| Feature | Rsync (Traditional) | RustSync (Proposed) |
| :---- | :---- | :---- |
| **Transport** | TCP (suffers HOL blocking) | QUIC (eliminates HOL blocking) 9 |
| **Chunking Method** | Fixed-Size Blocks | Content-Defined Chunking (CDC) 4 |
| **Integrity Hash** | MD5/SHA-1 (Sequential) | BLAKE3 (Parallel & Tree-based) 3 |
| **Resilience to Byte Shift** | Low (Global Rescan required) \[1\] | High (Localized change detection) 4 |
| **Index Structure** | Flat checksum list | Prolly Tree (Merkle structure) \[21\] |

## **Section 6: Data Payload Efficiency: Zstandard Compression**

### **6.1. Zstd Performance for Dynamic Deltas**

Once the delta chunks have been identified, they must be compressed for efficient transfer. Zstandard (Zstd) is the preferred algorithm due to its optimal balance between high compression ratio and extreme speed, particularly when handling dynamically generated data streams.

For large inputs, Zstd often achieves comparable compression ratios to Gzip but is significantly faster in compression time (e.g., 10x faster compression than Gzip for a 500 MB dataset). Crucially, Zstd maintains fast decompression speeds comparable to Gzip and Brotli. Since synchronization transfers only non-redundant delta chunks, the compression latency must be minimal to avoid becoming a CPU bottleneck that limits the network interface throughput. Zstd’s low latency ensures that the data stream is not stalled while waiting for the compression engine to process the BLAKE3/CDC outputs.

### **6.2. Implementation and Context Dictionaries**

Zstd compression will be applied selectively only to the raw delta chunks identified during Phase 2\. For dynamically generated content, Zstd compression level 12 often provides a fast compromise, yielding compression times faster than Gzip and Brotli while maintaining competitive payload sizes. For highly specific datasets, such as repeated backups of structured data (e.g., configuration files or logs), implementing pre-trained Zstd dictionaries could be considered to achieve even higher compression ratios, although this adds complexity to the state management layer.

## **Section 7: Connection Architecture and P2P Challenge Resolution (Direct Connection Focus)**

To meet the project requirement for an independent, open-source tool without relying on centralized, proprietary, or self-hosted relay infrastructure, **RustSync adopts the *Direct Connection Only* model, similar to the operational requirements of rsync over SSH.** This model eliminates the need for a Rendezvous Server or a TURN relay, ensuring zero operational cost and full independence, but introduces a prerequisite for the target synchronization machine.

### **7.1. Critical Requirement: Public Addressability**

For the initiating client (e.g., the laptop executing rustsync local/ user@target:remote/) to establish a QUIC connection, the target device (the machine running the rustsync daemon in listen mode) must be directly addressable from the internet.

This is a critical requirement and a necessary trade-off for independence.22 **The target machine must be configured to have a public IP address or have its assigned synchronization port (e.g., UDP 8080\) forwarded through its Network Address Translator (NAT) router.** The use of UPnP or NAT-PMP for automated port forwarding can simplify this configuration challenge for the end-user.7

### **7.2. Connection Protocol: Bidirectional Streaming RPC**

The connection process utilizes the inherent features of gRPC over QUIC to establish a highly efficient, secure, and persistent communication channel without external dependencies.

1. **QUIC Connection:** The initiating client establishes a secure QUIC connection (using 0-RTT if available 2) directly to the publicly exposed IP and UDP port of the target server.  
2. **Authentication:** Once the QUIC tunnel provides mandatory TLS 1.3 security, the gRPC application layer handles user authorization via private/public key exchange, mirroring the security model of SSH/rsync.23  
3. **Bidirectional Tunnel:** The entire synchronization session, including all control messages and index exchange, is contained within a single **Bidirectional Streaming RPC**. This mechanism is superior to multiple sequential (unary) RPC calls as it eliminates the overhead of creating a new HTTP/3 request for every message exchange. This single, secure stream allows the client and server to exchange messages asynchronously and independently throughout the entire synchronization process, enabling the critical concurrency optimizations detailed in Section 8\.

## **Section 8: Proposed RustSync Architecture and Protocol Definition**

### **8.1. The System Architecture Overview**

RustSync employs a rigorous Client-Server architecture utilizing Rust for its high-performance, memory-safe execution environment. The core integration relies on three coupled layers: the BLAKE3/CDC layer for file analysis, the gRPC/Protobuf layer for structured communication, and the QUIC layer for resilient, multiplexed transport.17

Table 3: Proposed RustSync Protocol Stack

| Layer | Protocol/Tool | Rust Implementation | Primary Role |
| :---- | :---- | :---- | :---- |
| Transport | QUIC (HTTP/3) | quinn (or similar) | High-speed, multiplexed, reliable WAN transport \[2, 9\] |
| Application | gRPC / Protocol Buffers | tonic / prost | Structured, type-safe API, streaming control 17 |
| Fingerprinting | BLAKE3 | blake3 crate | Parallel hashing, intrinsic Merkle tree generation \[3, 24\] |
| Chunking | Content-Defined Chunking | fastcdc or SeqCDC | Resilient delta identification \[4, 25\] |
| Compression | Zstandard | zstd crate | Fast compression of data payloads \[26\] |

### **8.2. Detailed Synchronization Workflow (3 Phases)**

The synchronization process is executed in three distinct, performance-optimized phases, strategically designed to minimize idle time and maximize network utilization through concurrency:

* **Phase 1: Concurrent Index Generation and Metadata Exchange:**  
  1. **Concurrency Optimization:** Immediately after the secure bidirectional stream is established, both the client and the server **simultaneously** begin scanning their respective directories and generating their Prolly Tree indexes using parallelized BLAKE3 hashing. This concurrent processing drastically reduces the idle time experienced in sequential protocols where one peer waits for the other to complete its initial calculation.  
  2. **Metadata Exchange:** The two peers exchange intermediate index segments (Prolly Tree nodes) over the single bidirectional gRPC stream. They use Merkle traversal algorithms to quickly pinpoint the divergence point in the file structure by comparing received node hashes.19  
* **Phase 2: Chunk Identification and Delta Request:**  
  1. The receiving peer (client or server) compiles a precise list of required blocks, referencing them by their unique BLAKE3 chunk fingerprint.  
  2. This ChunkDeltaList, containing the necessary fingerprints and potentially file offsets, is batched and sent across the active bidirectional QUIC stream.  
* **Phase 3: Data Transfer, Verified Reconstruction, and Early Data Pipelining:**  
  1. **Optimization for Early Data Transfer:** Utilizing the bidirectional stream, the system does not wait for the entire Prolly Tree comparison (M-root exchange) to complete before starting the transfer. If an early exchange confirms a large delta (e.g., the target confirms a required file or directory is missing, or an initial index segment shows total divergence), the client can begin streaming the raw, Zstd-compressed data chunks immediately over the same connection. Data chunks (Phase 3\) and remaining index segments (Phase 1\) are thus **pipelined and flow concurrently** over the QUIC streams, minimizing RTT wait times and maximizing network throughput.  
  2. The sender retrieves the raw data corresponding to the requested fingerprints, applies Zstd compression, and streams the data using one or more high-throughput QUIC streams.15  
  3. **Resumability:** The gRPC message (StreamChunkRequest) supporting the transfer explicitly includes a Start byte offset, tracking the progress within the specific chunk being transferred.15 If the connection is interrupted, the client simply re-initiates the request for the missing chunk starting from the last verified byte offset, leveraging the intrinsic byte offset tracking of QUIC streams.28  
  4. The receiver decompresses the incoming data and performs a final BLAKE3 verification on the complete, received chunk, ensuring the integrity of the data matches the requested BLAKE3 fingerprint before integrating it into the file structure.

### **8.3. Reliable Resumable Transfers and Integrity**

The combination of QUIC and structured gRPC streaming eliminates the fragility associated with rsync resumption. QUIC streams natively track byte offsets, providing a clean mechanism for application-level resumption.28 The application explicitly requests data starting from a specific offset.15

The architecture inherently simplifies the integrity checking process upon interruption. Because the BLAKE3 hash of the specific chunk is known and served as the request key *before* the transfer begins, the receiver never needs to re-hash any partial file data upon resumption. Recovery requires only a single, fast verification: ensuring the final, fully received chunk matches its pre-determined BLAKE3 fingerprint. This bypasses the severe CPU overhead that plagues the resumption phase of traditional systems when dealing with large partial files.

## **Section 9: Performance Modeling and Quantitative Trade-offs**

### **9.1. Computational Budget Analysis**

The design principle of RustSync dictates a strategic investment in CPU time during Phase 1 (index generation) to achieve massive savings in network I/O and latency in Phase 2 and 3\. The computational load incurred by CDC boundary determination and parallel BLAKE3 indexing is justifiable because it minimizes the subsequent duration of network activity, which is the most costly factor in high-latency WAN environments.

The high computational throughput of parallel BLAKE3 execution, leveraging features like MMap I/O for efficient disk access in Rust implementations 24, minimizes disk access overhead when scanning multi-GB data sets, ensuring that the local file scanning phase is not bottlenecked by I/O latency or CPU constraints.

### **9.2. Network Throughput and Latency Modeling**

Modeling the expected performance gains shows that QUIC’s 0-RTT connection establishment dramatically reduces the initial setup latency compared to TCP/SSH. Furthermore, the use of Merkle tree traversal minimizes the RTT cycles required for index synchronization, moving away from rsync’s slow, iterative checksum exchange process.

Crucially, the architecture provides superior throughput stability under network degradation. When simulating typical WAN packet loss (e.g., 1–5% loss rate), QUIC's per-stream loss recovery avoids the throughput collapse caused by TCP’s HOL blocking.9 The ability to signal QUIC to expedite critical control messages via controlled packetization further ensures low operational latency for synchronization commands.

## **Section 10: Prototype Outline and Rust Implementation Roadmap**

### **10.1. Rust Crate Selection and Integration**

The realization of RustSync requires selecting high-performance, mature Rust crates:

* **QUIC Transport:** quinn provides a robust, idiomatic Rust implementation of the QUIC protocol.  
* **Hashing and Indexing:** The blake3 crate, supporting parallelism via rayon.  
* **Chunking:** fastcdc or an optimized alternative like a Rust port of SeqCDC.  
* **Compression:** The zstd crate for high-speed dynamic compression.  
* **Protocol Buffers/gRPC:** tonic (gRPC framework) and prost (Protocol Buffer code generation).11

The core code structure must be divided into modules responsible for transport handling, the Prolly Tree state management (linking BLAKE3 hashes to file offsets), and the synchronization state machine that manages the 3-phase workflow.

### **10.2. Key Data Structures and Protocol Messages**

The protocol must be formalized using gRPC message definitions, providing strict contracts for data exchange. Key Protobuf messages include:

1. MerkleRootRequest / MerkleRootResponse: Initiates synchronization by exchanging the top-level hash index.  
2. ChunkDeltaList: Transmits the list of required BLAKE3 chunk fingerprints and their corresponding file paths and offsets.  
3. StreamChunkRequest: The core message for requesting data chunks. It must include the target file path, the required BLAKE3 ID of the chunk for verification, and the mandatory Start byte offset (leveraging the resumption structure suggested by robust streaming models).15

### **10.3. Proof-of-Concept Benchmarking and Test Scenarios**

Prototype validation must be conducted against three critical performance axes:

1. **Latency Baseline Test:** Compare the time taken to initiate a synchronization session and complete the M-Root comparison (Phase 1\) for RustSync versus the full metadata exchange process of rsync over a simulated 300ms RTT link.  
2. **Byte-Shift Efficiency Test:** Quantify the actual delta transfer size when a small 1KB insertion is made early in a 1GB file, comparing the network transfer volume required by CDC-based RustSync against fixed-block rsync.  
3. **Resumption Cost Test:** Measure the time cost of recovery after a connection drop during a multi-GB transfer, specifically emphasizing the time saved by RustSync's chunk-hash verification method over the full or partial file re-hashing required by traditional systems.

#### **Referenzen**

> 1. SeqCDC: Hashless Content-Defined Chunking for Data Deduplication \- University of Waterloo, Zugriff am Oktober 30, 2025, [https\://cs.uwaterloo.ca/\~alkiswan/papers/SeqCDC\_Middleware24.pdf](https://cs.uwaterloo.ca/~alkiswan/papers/SeqCDC_Middleware24.pdf)  
> 2. Exploring Media over QUIC: A Superior Alternative to WebRTC? \- Tencent MPS, Zugriff am Oktober 30, 2025, [https\://mps.live/blog/details/media-over-quic](https://mps.live/blog/details/media-over-quic)  
> 3. BLAKE3 \- School of Computer Science and Engineering, UNSW Sydney, Zugriff am Oktober 30, 2025, [http\://www\.cse.unsw.edu.au/\~cs4601/refs/papers/blake3.pdf](http://www.cse.unsw.edu.au/~cs4601/refs/papers/blake3.pdf)  
> 4. Content-Defined Chunking \- Emergent Mind, Zugriff am Oktober 30, 2025, [https\://www\.emergentmind.com/topics/content-defined-chunking-cdc](https://www.emergentmind.com/topics/content-defined-chunking-cdc)  
> 5. RSync \- Checksums \- Jenkov.com, Zugriff am Oktober 30, 2025, [https\://jenkov.com/tutorials/rsync/checksums.html](https://jenkov.com/tutorials/rsync/checksums.html)  
> 6. Rsync Algorithm \- System Design \- GeeksforGeeks, Zugriff am Oktober 30, 2025, [https\://www\.geeksforgeeks.org/system-design/rsync-algorithm-system-design/](https://www.geeksforgeeks.org/system-design/rsync-algorithm-system-design/)  
> 7. UDP Hole Punching help \- Stack Overflow, Zugriff am November 1, 2025, [https\://stackoverflow.com/questions/6794320/udp-hole-punching-help](https://stackoverflow.com/questions/6794320/udp-hole-punching-help)  
> 8. Peer-to-peer Connection with WebRTC in Rust Using webrtc-rs, Zugriff am November 1, 2025, [https\://archive.casouri.cc/note/2024/rust-webrtc/index.html](https://archive.casouri.cc/note/2024/rust-webrtc/index.html)  
> 9. gRPC Over HTTP/3 \- kmcd.dev, Zugriff am Oktober 30, 2025, [https\://kmcd.dev/posts/grpc-over-http3/](https://kmcd.dev/posts/grpc-over-http3/)  
> 10. Resuming rsync partial (-P/--partial) on an interrupted transfer \- Stack Overflow, Zugriff am Oktober 30, 2025, [https\://stackoverflow.com/questions/16572066/resuming-rsync-partial-p-partial-on-an-interrupted-transfer](https://stackoverflow.com/questions/16572066/resuming-rsync-partial-p-partial-on-an-interrupted-transfer)  
> 11. Rust Generated Code Guide | Protocol Buffers Documentation, Zugriff am Oktober 30, 2025, [https\://protobuf.dev/reference/rust/rust-generated/](https://protobuf.dev/reference/rust/rust-generated/)  
> 12. Rendezvous \- The libp2p docs, Zugriff am November 1, 2025, [https\://docs.libp2p.io/concepts/discovery-routing/rendezvous/](https://docs.libp2p.io/concepts/discovery-routing/rendezvous/)  
> 13. dirvine/ant-quic: Futures-based QUIC implementation in Rust \- GitHub, Zugriff am November 1, 2025, [https\://github.com/dirvine/ant-quic](https://github.com/dirvine/ant-quic)  
> 14. Security Principles \- Syncthing documentation, Zugriff am November 1, 2025, [https\://docs.syncthing.net/users/security.html](https://docs.syncthing.net/users/security.html)  
> 15. Streaming Large Files Between Microservices: A gRPC ..., Zugriff am Oktober 30, 2025, [https\://dev.to/dialaeke/streaming-large-files-between-microservices-a-grpc-implementation-5ee8](https://dev.to/dialaeke/streaming-large-files-between-microservices-a-grpc-implementation-5ee8)  
> 16. libp2p \- PKC \- Obsidian Publish, Zugriff am November 1, 2025, [https\://publish.obsidian.md/pkc/Literature/PKM/Tools/Networking/libp2p](https://publish.obsidian.md/pkc/Literature/PKM/Tools/Networking/libp2p)  
> 17. gRPC HTTP/3 \- Key Benefits and Implementation Considerations \- Catchpoint, Zugriff am Oktober 30, 2025, [https\://www\.catchpoint.com/http2-vs-http3/grpc-http3](https://www.catchpoint.com/http2-vs-http3/grpc-http3)  
> 18. STUN, TURN, and ICE Servers: Essential WebRTC Infrastructure Explained | nat.io, Zugriff am November 1, 2025, [https\://nat.io/blog/stun-turn-ice-servers-webrtc](https://nat.io/blog/stun-turn-ice-servers-webrtc)  
> 19. Lecture 10: Accounts Model and Merkle Trees, Zugriff am Oktober 30, 2025, [https\://web.stanford.edu/class/ee374/lec\_notes/lec10.pdf](https://web.stanford.edu/class/ee374/lec_notes/lec10.pdf)  
> 20. luis-herasme/wasm\_p2p: This is a simple peer-to-peer library for Rust \+ WASM, built on top of WebRTC \- GitHub, Zugriff am November 1, 2025, [https\://github.com/luis-herasme/wasm\_p2p/](https://github.com/luis-herasme/wasm_p2p/)  
> 21. BLAKE3 vs FSB \- A Comprehensive Comparison \- MojoAuth, Zugriff am Oktober 30, 2025, [https\://mojoauth.com/compare-hashing-algorithms/blake3-vs-fsb/](https://mojoauth.com/compare-hashing-algorithms/blake3-vs-fsb/)  
> 22. Choosing Between gzip, Brotli and zStandard Compression | Paul Calvano, Zugriff am Oktober 30, 2025, [https\://paulcalvano.com/2024-03-19-choosing-between-gzip-brotli-and-zstandard-compression/](https://paulcalvano.com/2024-03-19-choosing-between-gzip-brotli-and-zstandard-compression/)  
> 23. Hasher in blake3 \- Rust \- Docs.rs, Zugriff am Oktober 30, 2025, [https\://docs.rs/blake3/latest/blake3/struct.Hasher.html](https://docs.rs/blake3/latest/blake3/struct.Hasher.html)  
> 24. NAT Traversal Techniques for Peer-to-Peer Connections: A Comprehensive Guide, Zugriff am November 1, 2025, [https\://www\.checkmynat.com/posts/nat-traversal-techniques-for-peer-to-peer-connections/](https://www.checkmynat.com/posts/nat-traversal-techniques-for-peer-to-peer-connections/)  
> 25. sirgallo/quicfiletransfer: a file transfer service utilizing quic \- GitHub, Zugriff am Oktober 30, 2025, [https\://github.com/sirgallo/quicfiletransfer](https://github.com/sirgallo/quicfiletransfer)  
> 26. Firewall Setup \- Syncthing documentation, Zugriff am November 1, 2025, [https\://docs.syncthing.net/users/firewall.html](https://docs.syncthing.net/users/firewall.html)