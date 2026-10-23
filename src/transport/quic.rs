//! QUIC transport (D13 step 2, protocol spec §17): quinn as an opt-in
//! carrier beside TLS-over-TCP, same wire bytes, same D16 security model.
//!
//! The design constraint is D3's: **tokio at the edges only**. Quinn is an
//! async library — the endpoint machinery (retransmit timers, connection
//! state, keep-alives) needs a runtime to live on. This module owns that
//! runtime; everything it *returns* is synchronous:
//!
//! - [`QuicStream`] implements `Read + Write + Send` by driving quinn
//!   futures to completion at the adapter boundary, so the sync state
//!   machines (`protocol::sync`, `protocol::tree_sync`) stay 100% sync and
//!   native-thread based. The data plane never sees a future.
//! - [`client_session`] / [`server_session`] mirror the TLS session fns:
//!   the QUIC+TLS 1.3 handshake completes eagerly, the peer's presented
//!   fingerprint is returned, and NO trust decision is made here — the CLI
//!   applies the TOFU policy (prompt or `--trust`) before any sync data
//!   flows, exactly as it does for TCP.
//!
//! Security mapping (§17.2): the D16 identity (self-signed cert + PKCS#8
//! key, `identity.bin`) IS the QUIC endpoint certificate — no second
//! identity, no second pins file. QUIC mandates TLS 1.3 inside the
//! transport; there is no plaintext QUIC, so `--plaintext` is a TCP-only
//! concept and the CLI rejects the combination loudly.
//!
//! Lifecycle (§17.6 — the one real behavioral difference from TCP):
//! - a stream's FIN is `finish()`; dropping a `SendStream` auto-finishes
//!   it, which is exactly TCP's FIN-on-close — the receiver's service loop
//!   reads clean Eof, no protocol change.
//! - QUIC connection close discards unACKed *stream* data (RFC 9000
//!   §10.2.2), unlike TCP's flushing close. A final frame that is
//!   *answered* is therefore only safe when the writer holds the
//!   connection until the reply is read; the session functions do that
//!   (the returned [`QuicSession`] keeps the connection alive), and the
//!   receiver-side helper [`wait_for_peer_close`] bounds the final wait.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{
    ClientConfig, Connection, Endpoint, EndpointConfig, IdleTimeout, RecvStream, SendStream,
    ServerConfig, TokioRuntime, TransportConfig,
};

use super::{Identity, PinPolicy};

// The CLI's Listener enum needs to name this type (the QUIC listening
// socket, the counterpart of TcpListener).
pub use quinn::Endpoint as QuicEndpoint;

/// Idle timeout for QUIC connections: ffsync sessions pause during
/// receiver-side reconstruction, tree triage, and resume scans; a silent
/// peer should die loudly, but not mid-scan. Mirrors the TCP path's
/// DEFAULT_TIMEOUT_SECS (300 s) semantics.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// The process-wide tokio runtime quinn needs. Endpoint machinery only —
/// two workers are plenty; the data plane runs on ffsync's own native
/// threads and never touches this.
static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime for quinn (D3: transport edges only)")
    })
}

/// Runs `f` on the quinn runtime; used for everything that must be polled
/// to completion but has no natural thread of its own.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    runtime().block_on(f)
}

/// A bidirectional QUIC stream as a sync `Read + Write`. One conversation
/// runs on one of these, on a native thread, exactly like a TLS stream.
pub struct QuicStream {
    recv: RecvStream,
    send: SendStream,
}

impl Read for QuicStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match block_on(self.recv.read(buf)) {
            // Ok(Some(n)): data; Ok(None): peer finished the stream — clean
            // EOF, the same semantics framing::read_frame expects at session
            // end (the TCP path gets this as a 0-byte read from FIN).
            Ok(Some(n)) => Ok(n),
            Ok(None) => Ok(0),
            Err(quinn::ReadError::ConnectionLost(e)) => Err(quic_conn_err(e)),
            Err(e) => Err(io::Error::other(format!("QUIC read failed: {e}"))),
        }
    }
}

impl Write for QuicStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match block_on(self.send.write(buf)) {
            Ok(n) => Ok(n),
            Err(quinn::WriteError::ConnectionLost(e)) => Err(quic_conn_err(e)),
            Err(e) => Err(io::Error::other(format!("QUIC write failed: {e}"))),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // Quinn buffers in user space; there is no partial-flush concept on
        // a SendStream (the session layer's stream end is finish(), which
        // Drop performs automatically).
        Ok(())
    }
}

fn quic_conn_err(e: quinn::ConnectionError) -> io::Error {
    io::Error::other(format!("QUIC connection failed: {e}"))
}

fn quic_connect_err(e: quinn::ConnectError) -> io::Error {
    io::Error::other(format!("QUIC connect failed: {e}"))
}

/// Shared transport tuning: idle timeout sized for pause-heavy sync
/// sessions (receiver-side reconstruction, tree triage, resume scans).
/// Set on BOTH endpoints: quinn negotiates the effective timeout from
/// both sides' max_idle_timeout, and the 30 s default would kill a
/// connection during a long receiver pause. Defaults for windows/stream
/// counts are quinn's (100 concurrent bidi streams ≫ any K ffsync uses).
fn transport_config() -> TransportConfig {
    let mut t = TransportConfig::default();
    t.max_idle_timeout(Some(
        IdleTimeout::try_from(IDLE_TIMEOUT).expect("idle timeout"),
    ));
    t
}

/// A live QUIC session: the connection handle that keeps the transport
/// machinery alive while the sync runs, plus the first (control) stream
/// returned separately by the session functions. Worker conversations
/// (§16.8) ride additional streams on this same connection — the QUIC
/// equivalent of fresh TCP connections.
///
/// `Clone` is deliberate: `Connection` is a refcounted handle, and the
/// §16.8 worker closures need their own handle on their own thread (the
/// TCP path hands each worker a fresh socket; the QUIC path hands each a
/// clone). The connection closes when the LAST handle drops — after
/// unACKed stream data MAY be discarded, hence [`wait_for_peer_close`]
/// on the receiver's final wait (§17.6).
pub struct QuicSession {
    connection: Connection,
}

impl Clone for QuicSession {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
        }
    }
}

impl QuicSession {
    /// The peer's address (for display parity with the TCP path's
    /// "peer connected: {peer}" line).
    pub fn remote_address(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// Opens a new bidirectional stream on the session's connection —
    /// the QUIC equivalent of a fresh TCP connection (§16.8 worker).
    pub fn open_stream(&self) -> io::Result<QuicStream> {
        let (send, recv) = block_on(self.connection.open_bi())
            .map_err(|e| io::Error::other(format!("QUIC stream open failed: {e}")))?;
        Ok(QuicStream { recv, send })
    }

    /// Accepts a new bidirectional stream — the receiver-side counterpart
    /// of [`open_stream`] (a §16.8 worker arriving on the shared
    /// connection instead of a new TCP connection).
    pub fn accept_stream(&self) -> io::Result<QuicStream> {
        let (send, recv) = block_on(self.connection.accept_bi())
            .map_err(|e| io::Error::other(format!("QUIC stream accept failed: {e}")))?;
        Ok(QuicStream { recv, send })
    }

    /// Waits (bounded) for the peer to close the connection, proving our
    /// final frames were delivered (§17.6: QUIC close discards unACKed
    /// stream data; TCP close flushes). Returns early on any close event —
    /// a clean or errored close both mean delivery is decided. The caller
    /// must have FINished its own streams first (dropping them does), or
    /// a peer stuck in a read-until-FIN loop would deadlock both sides.
    pub fn wait_for_peer_close(&self, bound: Duration) {
        let _ = block_on(async { tokio::time::timeout(bound, self.connection.closed()).await });
    }
}

/// Builds the QUIC client endpoint (ephemeral UDP bind). `Endpoint::new`
/// spawns the endpoint driver via the runtime, so construction runs under
/// `block_on` (quinn's `TokioRuntime::spawn` requires a runtime context).
/// The returned `Endpoint` is consumed by `client_session`; the driver
/// keeps the connection alive independently of the handle afterwards.
fn client_endpoint() -> io::Result<Endpoint> {
    block_on(async {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        Endpoint::new(
            EndpointConfig::default(),
            None,
            socket,
            Arc::new(TokioRuntime),
        )
    })
}

/// Builds the QUIC server endpoint: our identity as the endpoint
/// certificate, the pin policy as the mutual verifier.
fn server_endpoint(
    bind: SocketAddr,
    identity: &Identity,
    pins: &PinPolicy,
) -> io::Result<Endpoint> {
    block_on(async {
        let socket = std::net::UdpSocket::bind(bind)?;
        let tls = identity.quinn_server_config(pins)?;
        // with_crypto generates a random stateless-retry token key
        // internally (anti-DoS bookkeeping, not authentication — identity
        // is the certificate).
        let mut server_config = ServerConfig::with_crypto(Arc::new(tls));
        server_config.transport_config(Arc::new(transport_config()));
        Endpoint::new(
            EndpointConfig::default(),
            Some(server_config),
            socket,
            Arc::new(TokioRuntime),
        )
    })
}

/// Handshake deadline for the client's connect: quinn retransmits
/// Initial packets indefinitely (there is no built-in connect deadline),
/// so a dead or UDP-blocked peer would hang the sender forever — the TCP
/// path fails in seconds instead. Mirrors the "is the receiver running?"
/// error shape (§17.5).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Connects to a QUIC peer (client role), performs the handshake, and
/// opens the first (control) stream. Mirrors [`super::client_session`]:
/// returns the session guard and the fingerprint the server presented;
/// NO trust decision is made here.
pub fn client_session(
    identity: &Identity,
    pins: &PinPolicy,
    server_addr: SocketAddr,
    server_name: rustls::pki_types::ServerName<'static>,
) -> io::Result<(QuicSession, QuicStream, Vec<u8>)> {
    let mut endpoint = client_endpoint()?;
    let tls = identity.quinn_client_config(pins)?;
    let mut client_config = ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)
            .map_err(|e| io::Error::other(format!("QUIC client config: {e}")))?,
    ));
    client_config.transport_config(Arc::new(transport_config()));
    endpoint.set_default_client_config(client_config);

    // quinn wants the server name as &str; the pin model does not care
    // what it is (the fingerprint is the identity). `connect()` itself
    // spawns the connection driver (tokio::spawn), so it too must run
    // inside a runtime context. The handshake await is bounded: quinn
    // retransmits Initials indefinitely, and a dead/UDP-blocked peer must
    // fail loudly (§17.5), not hang the sender.
    let name = server_name.to_str().into_owned();
    let connecting = block_on(async {
        endpoint
            .connect(server_addr, name.as_str())
            .map_err(quic_connect_err)
    })?;
    let connection = block_on(async { tokio::time::timeout(CONNECT_TIMEOUT, connecting).await })
        .map_err(|_| {
            io::Error::other(format!(
                "QUIC handshake timeout after {} s (is the receiver running, and is UDP \
                 open? QUIC needs UDP; try TCP (default) if the path blocks UDP)",
                CONNECT_TIMEOUT.as_secs()
            ))
        })?
        .map_err(quic_conn_err)?;

    let presented = peer_fingerprint(&connection)?;
    let (send, recv) = block_on(connection.open_bi())
        .map_err(|e| io::Error::other(format!("QUIC stream open failed: {e}")))?;
    Ok((
        QuicSession { connection },
        QuicStream { recv, send },
        presented,
    ))
}

/// Accepts one QUIC connection (server role) and its first (control)
/// stream. Mirrors [`super::server_session`]: returns the session guard,
/// the stream, and the presented fingerprint.
///
/// The caller MUST keep the [`QuicSession`] alive for the whole session
/// (it is the connection's last handle — dropping it closes the
/// connection) and derive worker streams via
/// [`QuicSession::accept_stream`].
pub fn server_session(endpoint: Endpoint) -> io::Result<(QuicSession, QuicStream, Vec<u8>)> {
    let incoming = block_on(async {
        endpoint
            .accept()
            .await
            .ok_or_else(|| io::Error::other("QUIC endpoint closed"))
    })?;
    // accept() is sync but spawns the connection driver (tokio::spawn),
    // so it must also run inside a runtime context.
    let connecting = block_on(async { incoming.accept() }).map_err(quic_conn_err)?;
    let connection = block_on(connecting).map_err(quic_conn_err)?;
    // The connection is now registered with the endpoint driver (which
    // keeps it alive); the endpoint handle is not needed again — all
    // streams ride this one connection.
    drop(endpoint);

    let presented = peer_fingerprint(&connection)?;
    let (send, recv) = block_on(connection.accept_bi())
        .map_err(|e| io::Error::other(format!("QUIC stream accept failed: {e}")))?;
    Ok((
        QuicSession { connection },
        QuicStream { recv, send },
        presented,
    ))
}

/// Builds a server endpoint for the CLI's receive path: bind UDP `port`
/// on all interfaces, wait for exactly one connection. This is the QUIC
/// counterpart of `TcpListener::bind` + one `accept`.
pub fn listen(bind: SocketAddr, identity: &Identity, pins: &PinPolicy) -> io::Result<Endpoint> {
    server_endpoint(bind, identity, pins)
}

/// The SHA-256 fingerprint of the certificate the peer presented in the
/// QUIC handshake.
fn peer_fingerprint(connection: &Connection) -> io::Result<Vec<u8>> {
    let identity = connection
        .peer_identity()
        .ok_or_else(|| io::Error::other("peer presented no certificate"))?;
    let certs = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .map_err(|_| io::Error::other("unexpected peer identity type"))?;
    certs
        .first()
        .map(|c| super::sha256(c.as_ref()))
        .ok_or_else(|| io::Error::other("peer presented an empty certificate chain"))
}

impl Identity {
    /// Builds the quinn server TLS config: the same rustls builder as the
    /// TCP path (our cert + the D16 client-cert verifier enforcing the pin
    /// policy), wrapped for QUIC.
    pub fn quinn_server_config(&self, pins: &PinPolicy) -> io::Result<QuicServerConfig> {
        let tls = self.server_config(pins)?;
        QuicServerConfig::try_from(tls)
            .map_err(|e| io::Error::other(format!("QUIC server config: {e}")))
    }

    /// Builds the quinn client TLS config: the same rustls builder as the
    /// TCP path (our cert as client auth + the D16 server-cert verifier).
    /// The server name plays no role in the pin model (the fingerprint IS
    /// the identity); it is passed at connect time only.
    pub fn quinn_client_config(&self, pins: &PinPolicy) -> io::Result<rustls::ClientConfig> {
        self.client_config(pins)
    }
}
