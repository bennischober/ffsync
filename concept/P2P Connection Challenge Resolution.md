

# **Architectural Deep Dive: Achieving Robust P2P Connectivity in RustSync via QUIC and Advanced NAT Traversal**

## **I. Executive Summary: The P2P Connectivity Imperative for RustSync**

The requirement for the RustSync CLI tool to establish resilient, high-performance client-to-client (P2P) synchronization mandates an architectural solution that moves beyond simple network programming to address the complexities of Network Address Translation (NAT) and firewall traversal. QUIC, built atop UDP, provides an exceptional modern transport foundation, offering benefits such as inherent encryption, fast handshakes, and stream multiplexing.1 However, QUIC alone does not guarantee client connectivity.

To achieve production-grade reliability, defined as near 100% connection success, the architecture must adopt the principles of Interactive Connectivity Establishment (ICE). This necessitates a robust, multi-stage solution involving three tiers: 1\) A centralized **Rendezvous/Signaling Server** using gRPC for coordinate exchange; 2\) **Advanced NAT Traversal** leveraging specialized, P2P-optimized QUIC implementations for coordinated UDP hole punching; and 3\) A **mandatory TURN Relay fallback mechanism** to handle highly restrictive network topologies like Symmetric NATs.2 The analysis concludes that leveraging integrated Rust libraries that abstract the complexity of ICE into the QUIC layer offers the most expedient and robust path to implementation.

## **II. Contextual Review: The Initial Connection Process and Requirements**

Successful P2P connectivity is predicated on successfully introducing two geographically separated peers who often reside behind unpredictable NAT devices. This introduction requires a standardized control mechanism separate from the bulk data transfer channel.

### **A. The Indispensable Role of the Rendezvous Server (Signaling)**

The initial connection sequence requires a publicly addressable, stable server to act as a point of contact, often referred to as a Rendezvous or Signaling Server.4 This entity is non-negotiable for practical P2P networking, as it serves two primary functions: peer discovery and coordinated candidate exchange.

Firstly, the Rendezvous Server enables peers to locate one another using abstract identifiers (such as RustSync Device IDs), allowing peers to register their availability and location.5 This function is analogous to how decentralized systems like libp2p initially utilize Rendezvous points—stable, well-connected nodes—to facilitate initial peer discovery before potentially transitioning to more distributed methods like Distributed Hash Tables (DHT).6

Secondly, and critically for NAT traversal, the Rendezvous Server acts as the relay point for connectivity metadata. When Peer A wishes to connect to Peer B, both peers use the server to exchange lists of potential connection addresses, known as "candidates" (including local, STUN-derived, and TURN-derived addresses).8 This coordinated exchange is fundamental to initiating the hole-punching attempt.4 The successful model deployed by Syncthing, which utilizes discovery servers for device registration and subsequent connection attempts, confirms the necessity of this centralized entity.5

A fundamental architectural consideration arises from this necessity: P2P ideals strive for full decentralization, yet the requirement for universal connectivity demands a public, stable Rendezvous server.4 This centralized entity represents a single point of failure and introduces management overhead, which stands in tension with the pure decentralized ideal.6 However, this centralization is strategically minimized: the server’s role is restricted strictly to the initial handshake and, potentially, acting as a TURN relay, thereby ensuring that the bulk synchronization data transfer remains purely P2P. This strategic compromise is essential for practical, high-reliability synchronization systems.

### **B. Protocol Segregation: gRPC for Control, QUIC for Transport**

The RustSync architecture utilizes a clean separation between the control plane and the data transport layer, aligning gRPC and QUIC with their respective strengths.

gRPC is selected for the control plane due to its efficiency in exchanging small, structured, high-importance messages. This encompasses Device ID registration, cluster configuration messages 10, and the exchange of NAT traversal candidates (STUN/TURN addresses). gRPC provides necessary security and serialization for this metadata exchange.

QUIC is chosen as the foundational data transport layer for core synchronization. Because QUIC is built on UDP 1, it is inherently compatible with NAT traversal techniques that rely on User Datagram Protocol (UDP) hole punching. Furthermore, QUIC provides integrated end-to-end encryption (TLS 1.3), stream multiplexing (allowing sync streams and potential in-band control messages to share a single connection), and faster connection setup compared to traditional TCP transports.1 This duality ensures that the signaling process is reliable (gRPC) while the subsequent data transfer is fast, secure, and resilient (QUIC).

## **III. The Fundamental Challenge: Network Address Translation (NAT) and Firewall Traversal**

The primary impediment to reliable P2P connectivity is the mechanism of Network Address Translation (NAT). Understanding the behavior of diverse NAT devices is necessary to design a traversal solution that guarantees success.

### **A. Why Direct Connectivity Fails (The NAT Barrier)**

NAT devices translate private internal addresses into a single public IP address, using temporary port mappings to track and manage bidirectional traffic.12 When an external peer attempts to initiate a connection, the NAT drops the incoming packet if it cannot match the packet to a previously established, active outgoing connection. In essence, the NAT acts as a gate, only allowing packets that are solicited by an internal device.

For P2P systems, this mechanism breaks direct connection attempts. A device attempting to connect to an external peer, even if it knows the public address of the target's router, cannot succeed unless a corresponding port mapping ("hole") has been created by the target device's prior outgoing traffic.12

### **B. The Suitability of UDP for Hole Punching**

UDP is the industry standard for attempting hole punching because its connectionless and stateless nature lends itself to the technique of simultaneous transmission. This method, known as UDP Hole Punching (UHP), involves both peers sending packets to each other simultaneously, aiming to create a temporary, symmetric NAT state that allows reciprocal traffic.13

The efficacy of UHP is high. It is a proven strategy, employed by applications such as Skype 12, which significantly enhances the success rate of direct communication. Real-world studies of P2P networks confirm that UHP is highly effective, with success rates between compatible peers exceeding 80%.14 Furthermore, UDP-based hole punching achieves an approximate 82% success rate, which is measurably higher than the 64% success rate typically observed with TCP hole punching.12 The selection of QUIC, which operates exclusively over UDP, thus ensures that the transport layer is optimally aligned with the most effective traversal technique.1

### **C. Classification of NAT Architectures and Failure Modes**

The challenge to UHP lies in the variety of NAT implementation behaviors. NATs are classified based on how they establish and restrict port mappings. While many types (Full Cone, Address-Restricted Cone, Port-Restricted Cone) are generally traversable using coordinated UHP, the **Symmetric NAT** poses an insurmountable challenge for direct communication.

A Symmetric NAT assigns a different external port for every new destination address and port pair.15 Consequently, if Peer A uses its external port \$P\_A\$ to communicate with the Rendezvous Server (S) during candidate discovery, the symmetric NAT will assign a new, entirely different external port \$P'\_A\$ when Peer A subsequently tries to communicate with Peer B. Since Peer B only knows \$P\_A\$ (the address advertised via S), its attempts to reach Peer A's actual temporary port \$P'\_A\$ fail.12

Because Symmetric NATs represent a consistent and substantial minority within the internet topology, relying solely on UHP is insufficient for achieving high availability. This complexity dictates that the architecture must incorporate a mandatory fallback mechanism.

Table I: Classification of NAT Types and Hole Punching Efficacy

| NAT Type | Mapping Rule | UDP Hole Punching Efficacy | Required Traversal Mechanism |
| :---- | :---- | :---- | :---- |
| Full Cone NAT | Internal IP:Port maps to one external IP:Port for all destinations. | High (Near 100%) | STUN/Simple UHP |
| Address-Restricted Cone NAT | Maps to same external IP:Port, accepts traffic only from previously seen external IP. | High (Requires coordination) | STUN/Coordinated UHP |
| Port-Restricted Cone NAT | Maps to same external IP:Port, accepts traffic only from previously seen external IP and port. | Moderate (Requires precise timing) | STUN/Coordinated UHP |
| Symmetric NAT | Maps to a *different* external IP:Port for *each* new remote destination. | Low (Typically Fails) | TURN Relay (Mandatory Fallback) |

## **IV. Comprehensive Analysis of Robust NAT Traversal Standards**

To handle the complexity detailed in Table I and guarantee maximum reliability, the industry standard is the Interactive Connectivity Establishment (ICE) framework, which orchestrates specialized protocols.

### **A. STUN (Session Traversal Utilities for NAT)**

STUN is the necessary precursor to hole punching, serving as the tool for address discovery. A STUN client behind a NAT queries an external STUN server to determine its public, **server reflexive address** and to classify the type of NAT in use.8

In the RustSync context, the application must incorporate a STUN client to gather its connectivity candidates. Libraries such as stun-agent 17 and rustun 18 exist within the Rust ecosystem, providing sans-IO protocol implementations that can be integrated into the asynchronous networking stack to perform these queries efficiently. The gathered list of host addresses and server reflexive addresses forms the foundation of the candidate list exchanged via the gRPC signaling channel.

### **B. TURN (Traversal Using Relays around NAT)**

TURN is the mandatory fallback mechanism that ensures connectivity when direct UHP attempts fail, most commonly due to Symmetric NAT restrictions.12 TURN (Traversal Using Relays around NAT) servers function as communication relays, forwarding all traffic between peers when a direct connection cannot be established.3

Real-world usage metrics, derived from platforms like Chrome, indicate that direct connections (host plus server-reflexive addresses) succeed in roughly 75-80% of sessions. Consequently, the remaining 20-25% of sessions require a relay.2 This statistic confirms that relying solely on UHP is insufficient; the TURN relay capability is indispensable for achieving high reliability. The inclusion of TURN, however, carries a significant operational burden: while public STUN servers are plentiful, free public TURN servers are practically non-existent due to the high bandwidth and operational costs associated with relaying all media traffic.3 Therefore, the RustSync project must budget for and deploy its own dedicated TURN relay infrastructure.

### **C. ICE (Interactive Connectivity Establishment) Framework**

ICE, defined in RFC 8445, is the overarching framework that coordinates the gathering, exchange, testing, and selection of connectivity paths (candidates), prioritizing direct routes (STUN/UHP) and falling back to relayed paths (TURN) only when necessary.8 The process involves two peers exchanging their comprehensive candidate lists via the signaling server, performing simultaneous connection attempts across all paths, and then selecting the most direct, successful path.8

Implementing a full ICE stack can be highly complex. While WebRTC relies on ICE, it also mandates the integration of other protocols such as DTLS for security and SCTP for data channels, which adds significant layering and complexity unnecessary for a basic file synchronization tool that only requires a secure data pipe.11

This leads to a crucial evaluation regarding implementation approach. The complexity of memory management and low-level networking is often absorbed by core libraries in Rust, moving the burden from the application developer to the library maintainer.20 Adopting a complete, standards-based ICE framework built from foundational components carries high development overhead.21 A superior strategy is to find a system that achieves the reliable outcomes of ICE—candidate coordination, priority testing, and relay fallback—but is integrated directly into the QUIC transport layer, thereby managing the complexity within a single, specialized crate.

## **V. QUIC as the P2P Transport Layer Foundation and Specialized Traversal**

QUIC’s inherent design makes it the superior choice for high-performance P2P synchronization, especially when utilizing libraries that have specialized its capabilities for network traversal.

### **A. QUIC’s Intrinsic Advantages for RustSync**

1. **UHP Compatibility:** QUIC’s foundation on UDP ensures that it is immediately compatible with UHP techniques, maximizing the probability of direct connection.1  
2. **Integrated Security:** QUIC mandates TLS 1.3 encryption directly within its handshake, simplifying the security implementation compared to traditional layered systems that require separate DTLS negotiation.1  
3. **Efficiency:** It offers stream multiplexing, allowing multiple data streams (e.g., control status, file chunks) to flow over the single connection without head-of-line blocking.  
4. **Connection Resilience:** QUIC supports connection migration, enabling a synchronization session to seamlessly transition between network interfaces (e.g., Wi-Fi to cellular) without requiring a new handshake or connection break.1

### **B. The Specialized QUIC P2P Approach (ant-quic Case Study)**

Given the complexity of manually integrating STUN/TURN/ICE libraries, a more strategic approach is to adopt P2P-optimized QUIC implementations that encapsulate the traversal logic.

The ant-quic library exemplifies this strategy within the Rust ecosystem. This implementation, built upon the established Quinn QUIC foundation, is specifically optimized for P2P networks and integrates "ICE-like candidate discovery and coordinated hole punching".22 This specialized library handles the coordination logic internally, utilizing proprietary or IETF draft standard QUIC extensions, such as extension frames like ADD\_ADDRESS and PUNCH\_ME\_NOW, to synchronize the simultaneous packet transmissions required for UHP across cone NATs.23

The decision to utilize a specialized library moves the implementation complexity from the application level to the library level.20 This specialized approach focuses on the core goal—reliable, secure P2P data transport—without the baggage of the full WebRTC stack.19 The inherent robustness of this integrated method is reflected in the claimed success metrics, aiming for "Near 100% connection success rate through sophisticated NAT handling".22 This represents a superior strategy for reliability and development efficiency compared to a modular construction using raw QUIC plus external STUN clients.

Table II: Comparative Analysis of Rust P2P Traversal Frameworks

| Approach | Complexity (Implementation) | Protocol Focus | Rust Crate Examples | P2P Success Rate (Est.) |
| :---- | :---- | :---- | :---- | :---- |
| Full ICE Stack (WebRTC style) | Very High (Requires multiple protocol integrations) | STUN/TURN/ICE, DTLS, SCTP | webrtc-rs underlying crates, wasm\_p2p \[24\] | Very High (75-80% Direct \+ Relay) 2 |
| Specialized QUIC P2P (ICE-like) | Moderate-High (Dependency on specialized crate) | QUIC Extensions, Coordinated UHP | ant-quic 22 | High (Near 100% claimed) 22 |
| Modular STUN/QUIC Only | Moderate (Manual coordination logic) | STUN, Raw Quinn/UDP sockets | Quinn \+ stun-agent 17 | Moderate (Fails Symmetric NATs) |

## **VI. Proposed High-Connectivity Solution Architecture**

The proposed solution mandates a three-tiered infrastructure that utilizes the strengths of gRPC for signaling and specialized QUIC for traversal and data transport.

### **A. Architectural Blueprint: The Three-Tier RustSync System**

1. **RustSync Peers (Clients):**  
   * The core component must be built around a P2P-optimized QUIC endpoint, such as one leveraging ant-quic.  
   * Peers utilize an integrated STUN client to gather host and server reflexive addresses.  
   * A gRPC client component handles signaling with the Rendezvous Server.  
   * Peer identity must be verified using Device IDs, derived from certificate fingerprints, to enforce cluster security.25  
2. **Rendezvous Server (Coordinator \- S):**  
   * This is a publicly hosted, highly available service.  
   * It operates a gRPC server that handles peer registration and candidate exchange services.  
   * The server's function is strictly limited to coordinating the initial handshake, minimizing the central point of control.  
3. **TURN Relay Server (Fallback \- R):**  
   * A dedicated, hosted service required to handle the 20-25% of connections that fail direct UHP.2  
   * It must be discoverable by clients and integrated into the candidate gathering process to provide Relayed Addresses (TURN candidates).  
   * Relaying data is encrypted end-to-end between peers, ensuring that the relay server itself cannot inspect the synchronization traffic.25

### **B. Integrating gRPC over QUIC**

The control plane (gRPC) and data transport (QUIC streams) are designed for complementary operation.

For the critical link between the client and the publicly hosted Rendezvous Server, the initial gRPC connection (used for candidate exchange) may rely on standard TLS/HTTP/2 over TCP for maximum reliability to the public server.

However, once a direct P2P QUIC connection is successfully established between two peers, the control plane logic should be tunneled over that QUIC connection using stream multiplexing. This allows status updates, configuration messages, and continued connection management (e.g., keep-alive checks) to benefit from the low latency and stream organization offered by QUIC, using standard Rust gRPC implementations (like tonic) adapted for QUIC transport.

## **VII. Detailed Connection Flow and Implementation Specification**

The path to reliable P2P connection must follow a disciplined, five-step sequence that explicitly manages candidate gathering and the coordinated timing of the hole punching attempt.

### **A. Peer Identity and Bootstrap (Prerequisites)**

Every RustSync peer must first establish its identity by generating a unique Device ID, which is the SHA-256 hash of its underlying certificate.5 The client must be bootstrapped with the known public addresses of the Rendezvous Server (for gRPC signaling) and the STUN/TURN servers (for candidate gathering).

### **B. The Five-Step P2P Connection Establishment Flow**

The coordinated attempt leverages the gRPC signaling server to ensure the timing and address exchange necessary for UHP.

Table III: Proposed RustSync P2P Connection Flow (Rendezvous/QUIC)

| Step | Initiating Peer (A) | Rendezvous Server (S) | Target Peer (B) | Purpose |
| :---- | :---- | :---- | :---- | :---- |
| 1\. Registration & Candidate Gathering | A gathers candidates (Host, STUN/Server Reflexive) and TURN relay addresses. | S registers A's Device ID and list of connectivity candidates (gRPC). | B gathers candidates (Host, STUN/Server Reflexive) and TURN relay addresses. | Public Address Discovery & Registration. |
| 2\. Connection Request & Exchange | A sends a gRPC request to S, listing A's Device ID and the target B's ID. | S retrieves B's candidates and relays them to A; S retrieves A's candidates and relays them to B (gRPC). | B receives A's candidates via S. | Coordinate Initial Contact and Share Candidates. |
| 3\. Coordinated Punching (UHP Attempt) | A initiates the QUIC state: Sends concurrent QUIC Initial packets to all of B’s candidate addresses. | N/A | B initiates the QUIC state: Sends concurrent QUIC Initial packets to all of A’s candidate addresses. | Establishing UHP state/connection using simultaneous transmission. |
| 4\. Connection Success (P2P Direct) | A/B complete the QUIC handshake on the first reciprocal packet received, utilizing the successful path. | N/A | A/B complete the QUIC handshake on the first successful path. | Direct QUIC P2P link established, maximizing performance. |
| 5\. Fallback to Relay (TURN) | If 4 fails (timeouts after retry periods), A/B activate the TURN relayed address candidate gathered in Step 1\. | S facilitates the session setup using TURN protocols. | A/B establish a secure QUIC connection tunneled through the TURN server. | Guarantees connectivity for symmetric NATs and complex firewalls. |

### **C. Implementation Detail: Signaling Protocol (gRPC Specification)**

The gRPC service running on the Rendezvous Server is critical for orchestrating Step 2\. It must define mechanisms for registration and candidate retrieval:

1. RegisterPeer(PeerID, CandidateList): This method allows a peer to register its unique identifier and advertise the list of potential addresses (Host, STUN-derived, TURN-derived) it can be reached at.  
2. RequestCandidates(InitiatorID, TargetID): The initiating peer calls this to retrieve the target peer's registered candidates. The server relays this list, simultaneously notifying the target peer that a connection attempt is imminent, thereby triggering the target peer to begin its simultaneous transmission (Step 3).

### **D. Implementation Detail: Timing, Path Selection, and Keep-Alive**

The success of UDP hole punching is highly sensitive to timing, requiring both peers to hit the NAT simultaneously.14 For unreliable transports like UDP, the calculation of the Retransmission Timeout (RTO) is crucial for managing the consistency of the attempt.17 A significant advantage of performing the hole punching using **QUIC Initial packets** is that QUIC integrates sophisticated RTO calculation based on established standards (similar to RFC 6298 principles).1 This allows the coordinated attempt (Step 3\) to benefit from standardized retry and timeout management, increasing the probability of a successful punch across diverse, inconsistent networks.

Path Selection requires the clients to continuously attempt connections across all candidates until the first successful handshake is completed (Step 4). The P2P-optimized QUIC library must handle the internal prioritization logic required by ICE.

Finally, once the P2P connection is established, the NAT state responsible for the "hole" will typically expire within tens of seconds without traffic.13 Therefore, the application layer must send regular keep-alive packets, usually consisting of minimal or empty UDP packets, to maintain the port mapping and sustain the connection state indefinitely.13

## **VIII. Conclusion and Strategic Recommendations**

The implementation of robust client-to-client connectivity for the RustSync CLI tool using QUIC and gRPC requires a strategic commitment to the complexities inherent in NAT traversal. Relying on simple UDP hole punching is guaranteed to fail for up to 25% of users due to Symmetric NAT limitations.2 Therefore, the architecture must adopt the full scope of the Interactive Connectivity Establishment (ICE) methodology.

The proposed solution leverages the intrinsic benefits of QUIC (UDP foundation, encryption, speed) while utilizing the gRPC service for structured signaling. Crucially, the approach seeks to manage complexity by utilizing specialized Rust P2P QUIC libraries that integrate ICE-like coordination logic.

**Strategic Recommendations for RustSync Deployment:**

1. **Mandate Specialized QUIC Implementation:** To mitigate implementation risk and maximize connectivity success, the project should adopt specialized Rust QUIC libraries designed for P2P traversal (e.g., ant-quic). This approach centralizes the complex logic of candidate gathering, coordinated hole punching, priority calculation, and connection establishment.22  
2. **Ensure Dedicated TURN Infrastructure:** The deployment of dedicated, project-owned TURN relay servers is non-negotiable for achieving high reliability, specifically targeting the approximately 20-25% of users behind restrictive NATs who cannot connect directly.2  
3. **Define and Secure the gRPC Control Plane:** The Rendezvous Server's gRPC service must be meticulously designed to handle high-throughput candidate exchange and precise timing controls necessary to initiate the coordinated hole punching sequence.  
4. **Enforce Multi-Layered Security:** While QUIC provides mandatory transport-level encryption (TLS 1.3), peer identity verification must be maintained at the application level by comparing the cryptographic Device ID fingerprints derived from the peer’s certificate.25 This ensures only authorized devices join the sync cluster, whether connecting directly or via a relay.

#### **Referenzen**

> 1. Streaming Remote rendering services: Comparison of QUIC-based and WebRTC Protocols, Zugriff am November 1, 2025, [https\://arxiv.org/html/2505.22132v1](https://arxiv.org/html/2505.22132v1)  
> 2. WebRTC Stun vs Turn Servers \- GetStream.io, Zugriff am November 1, 2025, [https\://getstream.io/resources/projects/webrtc/advanced/stun-turn/](https://getstream.io/resources/projects/webrtc/advanced/stun-turn/)  
> 3. STUN, TURN, and ICE Servers: Essential WebRTC Infrastructure Explained | nat.io, Zugriff am November 1, 2025, [https\://nat.io/blog/stun-turn-ice-servers-webrtc](https://nat.io/blog/stun-turn-ice-servers-webrtc)  
> 4. Rendezvous protocol \- Wikipedia, Zugriff am November 1, 2025, [https\://en.wikipedia.org/wiki/Rendezvous\_protocol](https://en.wikipedia.org/wiki/Rendezvous_protocol)  
> 5. Syncthing · Delft Students on Software Architecture: DESOSA 2017 \- delftswa, Zugriff am November 1, 2025, [https\://delftswa.gitbooks.io/desosa-2017/content/syncthing/chapter.html](https://delftswa.gitbooks.io/desosa-2017/content/syncthing/chapter.html)  
> 6. Rendezvous \- The libp2p docs, Zugriff am November 1, 2025, [https\://docs.libp2p.io/concepts/discovery-routing/rendezvous/](https://docs.libp2p.io/concepts/discovery-routing/rendezvous/)  
> 7. libp2p \- PKC \- Obsidian Publish, Zugriff am November 1, 2025, [https\://publish.obsidian.md/pkc/Literature/PKM/Tools/Networking/libp2p](https://publish.obsidian.md/pkc/Literature/PKM/Tools/Networking/libp2p)  
> 8. Demystifying NAT Traversal with STUN TURN and ICE \- Cisco Community, Zugriff am November 1, 2025, [https\://community.cisco.com/t5/collaboration-knowledge-base/demystifying-nat-traversal-with-stun-turn-and-ice/ta-p/4766853](https://community.cisco.com/t5/collaboration-knowledge-base/demystifying-nat-traversal-with-stun-turn-and-ice/ta-p/4766853)  
> 9. syncthing & dynamic IP \- Feature, Zugriff am November 1, 2025, [https\://forum.syncthing.net/t/syncthing-dynamic-ip/11310](https://forum.syncthing.net/t/syncthing-dynamic-ip/11310)  
> 10. How to graph my clients? \- Support \- Syncthing Community Forum, Zugriff am November 1, 2025, [https\://forum.syncthing.net/t/how-to-graph-my-clients/19554](https://forum.syncthing.net/t/how-to-graph-my-clients/19554)  
> 11. Comparing QUIC and WebRTC for Enterprise Networks \- Lightyear.ai, Zugriff am November 1, 2025, [https\://lightyear.ai/tips/quic-versus-webrtc](https://lightyear.ai/tips/quic-versus-webrtc)  
> 12. UDP Hole Punching Simplified. | WeValue Expertise \- Medium, Zugriff am November 1, 2025, [https\://medium.com/wevalue-expertise/udp-hole-punching-simplified-982fa852f02a](https://medium.com/wevalue-expertise/udp-hole-punching-simplified-982fa852f02a)  
> 13. UDP hole punching \- Wikipedia, Zugriff am November 1, 2025, [https\://en.wikipedia.org/wiki/UDP\_hole\_punching](https://en.wikipedia.org/wiki/UDP_hole_punching)  
> 14. (PDF) UDP NAT and firewall puncturing in the wild \- ResearchGate, Zugriff am November 1, 2025, [https\://www\.researchgate.net/publication/221198307\_UDP\_NAT\_and\_firewall\_puncturing\_in\_the\_wild](https://www.researchgate.net/publication/221198307_UDP_NAT_and_firewall_puncturing_in_the_wild)  
> 15. NAT Traversal Techniques in Peer-to-Peer Networks \- ResearchGate, Zugriff am November 1, 2025, [https\://www\.researchgate.net/publication/252144339\_NAT\_Traversal\_Techniques\_in\_Peer-to-Peer\_Networks](https://www.researchgate.net/publication/252144339_NAT_Traversal_Techniques_in_Peer-to-Peer_Networks)  
> 16. NAT Traversal Techniques for Peer-to-Peer Connections: A Comprehensive Guide, Zugriff am November 1, 2025, [https\://www\.checkmynat.com/posts/nat-traversal-techniques-for-peer-to-peer-connections/](https://www.checkmynat.com/posts/nat-traversal-techniques-for-peer-to-peer-connections/)  
> 17. stun\_agent \- Rust \- Docs.rs, Zugriff am November 1, 2025, [https\://docs.rs/stun-agent](https://docs.rs/stun-agent)  
> 18. sile/rustun: A Rust library for implementing STUN server and client asynchronously \- GitHub, Zugriff am November 1, 2025, [https\://github.com/sile/rustun](https://github.com/sile/rustun)  
> 19. Peer-to-peer Connection with WebRTC in Rust Using webrtc-rs, Zugriff am November 1, 2025, [https\://archive.casouri.cc/note/2024/rust-webrtc/index.html](https://archive.casouri.cc/note/2024/rust-webrtc/index.html)  
> 20. Complexity : r/rust \- Reddit, Zugriff am November 1, 2025, [https\://www\.reddit.com/r/rust/comments/vmqcdi/complexity/](https://www.reddit.com/r/rust/comments/vmqcdi/complexity/)  
> 21. UDP Hole Punching help \- Stack Overflow, Zugriff am November 1, 2025, [https\://stackoverflow.com/questions/6794320/udp-hole-punching-help](https://stackoverflow.com/questions/6794320/udp-hole-punching-help)  
> 22. dirvine/ant-quic: Futures-based QUIC implementation in Rust \- GitHub, Zugriff am November 1, 2025, [https\://github.com/dirvine/ant-quic](https://github.com/dirvine/ant-quic)  
> 23. ant-quic \- crates.io: Rust Package Registry, Zugriff am November 1, 2025, [https\://crates.io/crates/ant-quic/0.4.2](https://crates.io/crates/ant-quic/0.4.2)  
> 24. Security Principles \- Syncthing documentation, Zugriff am November 1, 2025, [https\://docs.syncthing.net/users/security.html](https://docs.syncthing.net/users/security.html)