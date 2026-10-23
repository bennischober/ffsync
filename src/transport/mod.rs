//! Transport security (D16): rustls TLS 1.3 with self-signed, pinned peers.
//!
//! Model: every peer owns a long-lived self-signed certificate (generated on
//! first use, stored in the ffsync state directory). Peers authenticate
//! *mutually* by comparing the SHA-256 fingerprint of the certificate —
//! the SSH `known_hosts` mental model, stripped to what a 1:1 file-sync
//! pair needs: one out-of-band fingerprint compare on first contact, then
//! silent verification forever (TOFU pinning).
//!
//! The sync state machines are untouched: `sync::send`/`sync::receive` are
//! generic over `Read + Write`, and the TLS stream returned by
//! [`client_session`] / [`server_session`] implements both.
//!
//! Security properties (D16):
//! - encryption: TLS 1.3 (rustls, ring provider)
//! - server AND client authenticated: both sides must present a certificate
//!   whose fingerprint is pinned by the other side (or is accepted in an
//!   explicit first-use flow, where the CLI prompts before any ffsync data
//!   flows)
//! - MITM: an interposing peer presents a different certificate → its
//!   fingerprint differs → loud, terminal rejection
//! - no CA machinery, no secrets on disk (fingerprints are public)
//!
//! The certificate is not validated as a PKI object (no expiry, no name
//! checks): the fingerprint *is* the identity. We still require the
//! handshake signatures to verify against the presented key (rustls does
//! that), so the pin binds to a key the peer demonstrably controls.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};

pub mod quic;

/// This peer's long-lived identity: a self-signed certificate and its key,
/// both kept as DER (no PEM parsing anywhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// DER-encoded self-signed X.509 certificate.
    pub cert_der: Vec<u8>,
    /// DER-encoded PKCS#8 private key.
    pub key_der: Vec<u8>,
}

impl Identity {
    /// SHA-256 fingerprint of the certificate, `AA:BB:…` formatted (SSH
    /// style). This is the string users compare out-of-band on first contact.
    pub fn fingerprint(&self) -> String {
        fingerprint_of(&self.cert_der)
    }

    /// Loads the identity from the state dir, or creates it on first use.
    /// The `created` flag reports whether this call generated a new identity
    /// (the CLI prints the fingerprint prominently on first creation, per
    /// D16's UI contract).
    pub fn load_or_create() -> io::Result<(Self, bool)> {
        let dir = state_dir().ok_or_else(|| {
            io::Error::other("cannot determine ffsync state dir (set APPDATA or HOME)")
        })?;
        fs::create_dir_all(&dir)?;
        let path = dir.join(IDENTITY_FILE);
        if let Ok(bytes) = fs::read(&path) {
            if let Some(id) = decode_identity(&bytes) {
                return Ok((id, false));
            }
            // Corrupt identity file: refuse to silently regenerate — a new
            // cert would break every peer's pin silently. The user must
            // delete the file explicitly (and re-pin peers).
            return Err(io::Error::other(format!(
                "corrupt identity file: {} — delete it only if you intend to \
                 re-pin every peer that trusts this machine",
                path.display()
            )));
        }
        let id = Self::generate()?;
        let tmp = path.with_extension("pem-tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&encode_identity(&id))?;
        }
        fs::rename(&tmp, &path)?;
        // The identity file holds the private key: restrict to the owner on
        // unix. Windows ACLs are inherited from the user profile, which is
        // already owner-only for %APPDATA%.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
        Ok((id, true))
    }

    /// Generates a fresh self-signed identity (rcgen, ECDSA P-256).
    fn generate() -> io::Result<Self> {
        let certified = rcgen::generate_simple_self_signed(vec!["ffsync-peer".to_string()])
            .map_err(|e| io::Error::other(format!("identity generation failed: {e}")))?;
        Ok(Self {
            cert_der: certified.cert.der().as_ref().to_vec(),
            key_der: certified.signing_key.serialize_der(),
        })
    }

    /// Builds a rustls server config: TLS 1.3, our cert, and a client-cert
    /// verifier that enforces the given pin policy.
    pub fn server_config(&self, pins: &PinPolicy) -> io::Result<rustls::ServerConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(PeerVerifier::new(pins.clone()));
        rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(tls_err)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![CertificateDer::from(self.cert_der.clone())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone())),
            )
            .map_err(tls_err)
    }

    /// Builds a rustls client config: TLS 1.3, our cert as client auth, and
    /// a server-cert verifier that enforces the given pin policy.
    pub fn client_config(&self, pins: &PinPolicy) -> io::Result<rustls::ClientConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(PeerVerifier::new(pins.clone()));
        rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(tls_err)?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(
                vec![CertificateDer::from(self.cert_der.clone())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone())),
            )
            .map_err(tls_err)
    }
}

fn tls_err(e: rustls::Error) -> io::Error {
    io::Error::other(format!("TLS setup failed: {e}"))
}

// ---------------------------------------------------------------------------
// Pin policy and verifiers
// ---------------------------------------------------------------------------

/// How this session's peer certificate is checked.
///
/// - `Pinned(fps)`: the peer must present a certificate whose fingerprint
///   is in this set (the normal, steady-state case; loud rejection
///   otherwise). A set — any pinned peer may connect; names are human
///   labels, authorization is per-fingerprint.
/// - `FirstUse`: no pin yet — accept any handshake-valid certificate and
///   *record* the presented fingerprint in the shared slot. The CLI then
///   prompts the user (or applies `--trust`) BEFORE any ffsync data flows,
///   and persists the pin; refusing drops the connection. This is the TOFU
///   step of D16, deliberately kept out of the verifier: the verifier is a
///   silent component and must never make trust decisions by itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinPolicy {
    Pinned(Vec<Vec<u8>>),
    FirstUse,
}

impl PinPolicy {
    /// Whether the given raw fingerprint is acceptable under this policy.
    fn accepts(&self, fp: &[u8]) -> bool {
        match self {
            PinPolicy::Pinned(pins) => pins.iter().any(|p| constant_time_eq(p, fp)),
            PinPolicy::FirstUse => false,
        }
    }
}

/// Verifies the peer certificate by fingerprint. Used identically for the
/// server-side (verifying clients) and client-side (verifying the server)
/// roles — the check is symmetric, which is the point of D16.
#[derive(Debug)]
struct PeerVerifier {
    policy: PinPolicy,
    /// Signature-verification delegation to the provider.
    schemes: Arc<rustls::crypto::WebPkiSupportedAlgorithms>,
}

impl PeerVerifier {
    fn new(policy: PinPolicy) -> Self {
        let provider = rustls::crypto::ring::default_provider();
        Self {
            policy,
            schemes: Arc::new(provider.signature_verification_algorithms),
        }
    }

    /// The core check, shared by both trait impls.
    fn check(&self, end_entity: &CertificateDer<'_>) -> Result<(), rustls::Error> {
        let fp = sha256(end_entity.as_ref());
        // FirstUse: accept anything handshake-valid; the caller decides
        // after the handshake (prompt / --trust) whether to persist the
        // pin. No ffsync data flows before that decision.
        if self.policy.accepts(&fp) || matches!(self.policy, PinPolicy::FirstUse) {
            Ok(())
        } else {
            Err(rustls::Error::General(format!(
                "peer certificate fingerprint {} is not pinned — refusing \
                 (MITM?); pin it with 'ffsync trust <fingerprint>' or, for \
                 one connection, pass --trust <fingerprint>",
                fingerprint_of(end_entity.as_ref())
            )))
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl ClientCertVerifier for PeerVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.check(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.check(cert)?;
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.schemes)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.check(cert)?;
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.schemes)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.supported_schemes()
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.check(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.check(cert)?;
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.schemes)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.check(cert)?;
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.schemes)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// Sessions: TLS-wrapped TCP streams
// ---------------------------------------------------------------------------

/// A TLS server session over an accepted TCP stream. The handshake completes
/// eagerly (before the function returns) so the caller can inspect the
/// presented fingerprint and apply the FirstUse decision before any sync
/// data flows.
pub type ServerTlsStream = rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>;

/// A TLS client session over a connected TCP stream.
pub type ClientTlsStream = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

/// Wraps an accepted TCP stream in TLS (server role) and completes the
/// handshake. Returns the stream and the fingerprint the client presented.
///
/// With `PinPolicy::FirstUse`, the caller MUST consult the returned
/// fingerprint (prompt or `--trust`) and either persist the pin or drop the
/// connection before using the stream for sync data.
pub fn server_session(
    stream: std::net::TcpStream,
    identity: &Identity,
    pins: &PinPolicy,
) -> io::Result<(ServerTlsStream, Vec<u8>)> {
    let config = Arc::new(identity.server_config(pins)?);
    let conn = rustls::ServerConnection::new(config).map_err(tls_err)?;
    let mut tls = rustls::StreamOwned::new(conn, stream);
    tls.conn
        .complete_io(&mut tls.sock)
        .map_err(|e| io::Error::other(format!("TLS handshake failed: {e}")))?;
    let presented = tls
        .conn
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(|c| sha256(c.as_ref()))
        .ok_or_else(|| io::Error::other("peer presented no certificate"))?;
    Ok((tls, presented))
}

/// Connects and wraps the TCP stream in TLS (client role), completing the
/// handshake. Same FirstUse contract as [`server_session`].
pub fn client_session(
    stream: std::net::TcpStream,
    identity: &Identity,
    pins: &PinPolicy,
    server_name: rustls::pki_types::ServerName<'static>,
) -> io::Result<(ClientTlsStream, Vec<u8>)> {
    let config = Arc::new(identity.client_config(pins)?);
    let conn = rustls::ClientConnection::new(config, server_name).map_err(tls_err)?;
    let mut tls = rustls::StreamOwned::new(conn, stream);
    tls.conn
        .complete_io(&mut tls.sock)
        .map_err(|e| io::Error::other(format!("TLS handshake failed: {e}")))?;
    let presented = tls
        .conn
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(|c| sha256(c.as_ref()))
        .ok_or_else(|| io::Error::other("peer presented no certificate"))?;
    Ok((tls, presented))
}

// ---------------------------------------------------------------------------
// Fingerprints and the pins file
// ---------------------------------------------------------------------------

/// ffsync state directory: `%APPDATA%\ffsync` (Windows) or `~/.ffsync`
/// (Unix). Holds `identity.bin` and `pins.conf`.
pub fn state_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("APPDATA")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir).join("ffsync"));
    }
    let home = std::env::var_os("HOME")?;
    if home.is_empty() {
        return None;
    }
    Some(PathBuf::from(home).join(".ffsync"))
}

const IDENTITY_FILE: &str = "identity.bin";
const PINS_FILE: &str = "pins.conf";

/// SHA-256 over DER bytes (raw digest).
pub fn sha256(data: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .to_vec()
}

/// Formats raw fingerprint bytes as `AA:BB:…` (SSH style, uppercase hex).
pub fn format_fingerprint(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 3);
    for (i, b) in raw.iter().enumerate() {
        if i > 0 {
            out.push(':');
        }
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02X}");
    }
    out
}

/// SHA-256 fingerprint of a DER certificate, formatted for display.
pub fn fingerprint_of(der: &[u8]) -> String {
    format_fingerprint(&sha256(der))
}

/// Parses a `AA:BB:…` or bare-hex fingerprint into raw bytes (32 expected).
pub fn parse_fingerprint(s: &str) -> Option<Vec<u8>> {
    let cleaned: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if cleaned.len() != 64 {
        return None;
    }
    (0..32)
        .map(|i| u8::from_str_radix(&cleaned[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

/// Loads all pinned peer fingerprints (raw bytes) from `pins.conf` in
/// `state_dir`. Missing file ⇒ empty list. Corrupt lines are skipped with
/// their position noted (best-effort: one bad line should not lock the user
/// out — the affected pin is simply absent and the sync will fail closed
/// against that peer).
pub fn load_pins() -> io::Result<Vec<Vec<u8>>> {
    let Some(dir) = state_dir() else {
        return Ok(Vec::new());
    };
    load_pins_from(&dir)
}

/// Like [`load_pins`], but for an explicit directory (testable without
/// process-global env vars).
pub fn load_pins_from(dir: &Path) -> io::Result<Vec<Vec<u8>>> {
    let bytes = match fs::read(dir.join(PINS_FILE)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let text = String::from_utf8_lossy(&bytes);
    let mut pins = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        match parse_fingerprint(line) {
            Some(fp) => pins.push(fp),
            None => eprintln!(
                "warning: pins.conf line {} is not a fingerprint, skipping",
                line_no + 1
            ),
        }
    }
    Ok(pins)
}

/// Appends a fingerprint to the pins file in the state dir (atomic via
/// temp + rename). No-op if already pinned.
pub fn add_pin(raw: &[u8]) -> io::Result<()> {
    let dir = state_dir().ok_or_else(|| io::Error::other("cannot determine ffsync state dir"))?;
    add_pin_in(&dir, raw)
}

/// Like [`add_pin`], but for an explicit directory.
pub fn add_pin_in(dir: &Path, raw: &[u8]) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(PINS_FILE);
    let existing = load_pins_from(dir)?;
    if existing.iter().any(|p| constant_time_eq(p, raw)) {
        return Ok(());
    }
    let mut content = match fs::read(&path) {
        Ok(b) => String::from_utf8_lossy(&b).into_owned(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&format_fingerprint(raw));
    content.push('\n');
    let tmp = path.with_extension("conf-tmp");
    fs::write(&tmp, content)?;
    fs::rename(&tmp, &path)
}

/// Removes a fingerprint from the pins file in the state dir. No-op if
/// absent.
pub fn remove_pin(raw: &[u8]) -> io::Result<()> {
    let Some(dir) = state_dir() else {
        return Ok(());
    };
    remove_pin_in(&dir, raw)
}

/// Like [`remove_pin`], but for an explicit directory.
pub fn remove_pin_in(dir: &Path, raw: &[u8]) -> io::Result<()> {
    let path = dir.join(PINS_FILE);
    let existing = load_pins_from(dir)?;
    let kept: Vec<&Vec<u8>> = existing
        .iter()
        .filter(|p| !constant_time_eq(p, raw))
        .collect();
    if kept.len() == existing.len() {
        return Ok(()); // not pinned
    }
    let mut content = String::new();
    for p in kept {
        content.push_str(&format_fingerprint(p));
        content.push('\n');
    }
    let tmp = path.with_extension("conf-tmp");
    fs::write(&tmp, content)?;
    fs::rename(&tmp, &path)
}

// ---------------------------------------------------------------------------
// Identity file codec
// ---------------------------------------------------------------------------

/// Identity file layout: 4-byte LE cert length, cert DER, key DER.
fn encode_identity(id: &Identity) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + id.cert_der.len() + id.key_der.len());
    out.extend_from_slice(&(id.cert_der.len() as u32).to_le_bytes());
    out.extend_from_slice(&id.cert_der);
    out.extend_from_slice(&id.key_der);
    out
}

fn decode_identity(bytes: &[u8]) -> Option<Identity> {
    if bytes.len() < 4 {
        return None;
    }
    let cert_len = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    let rest = &bytes[4..];
    if cert_len == 0 || cert_len > rest.len() {
        return None;
    }
    Some(Identity {
        cert_der: rest[..cert_len].to_vec(),
        key_der: rest[cert_len..].to_vec(),
    })
}
