//! ffsync CLI: send and receive files over TCP with chunk-level delta sync.
//!
//! Usage:
//!   ffsync send <file> <host:port>   [--checksum]
//!   ffsync receive <file> <port>     [--checksum] [--from <old-file>]
//!
//! The sender connects to a receiver that is listening.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::sync::{
    Progress, ReceiveRequest, SendRequest, SyncError, SyncOutcome, receive, send,
};

/// Default I/O timeout: a peer that sends nothing for this long kills the
/// sync. Generous enough for slow disks between frames on huge files
/// (scans stream manifests continuously; the longest quiet period is a
/// peer re-scanning its temp file on resume, which is disk-bound).
const DEFAULT_TIMEOUT_SECS: u64 = 300;

const USAGE: &str = "\
ffsync: fast chunk-level file sync for big files and fast links

Usage:
  ffsync send <file> <host:port> [--checksum] [--plaintext] [--trust <fp>]
  ffsync receive <file> <port> [--checksum] [--from <old-file>]
                [--plaintext] [--trust <fp>]
  ffsync trust <fingerprint>       pin a peer fingerprint (TOFU first contact)
  ffsync untrust <fingerprint>     remove a pinned fingerprint
  ffsync fingerprints              show this peer's identity + all pins
  ffsync --help | --version

Options:
  --checksum         skip no metadata shortcuts; always compare chunk roots
                     (rsync -c semantics). Also disables the warm-start index.
  --from <old-file>  receive: serve local chunks from <old-file> instead of
                     <file> (useful when the target does not exist yet)
  --plaintext        disable TLS (LAN use or inside an SSH tunnel); D16
  --trust <fp>       non-interactive first contact: pin <fp> instead of
                     prompting; also overrides which pinned peer to expect
  -h, --help         print this help
  -V, --version      print version

Security (D16): TLS 1.3 with self-signed certificates is on by default.
Both peers authenticate mutually by cert fingerprint — compare the
fingerprints printed on first contact out-of-band (in person, chat, ...),
confirm, and every later sync verifies silently. A mismatch refuses
loudly and syncs nothing.

Notes:
  <host:port> — for IPv6 use the bracketed form, e.g. [::1]:7788.
  receive binds IPv4 (0.0.0.0) only; IPv6 destinations need a dual-stack
  proxy or an IPv4 address for now.
  Port 0 on receive binds an OS-assigned port; the actual port is printed.
";

fn die(msg: &str) -> ! {
    eprintln!("error: {}", msg);
    eprintln!("try 'ffsync --help'");
    std::process::exit(2);
}

struct ProgressDisplay {
    last_phase: Option<Progress>,
    start: Instant,
    last_print: Instant,
}

impl ProgressDisplay {
    fn new() -> Self {
        Self {
            last_phase: None,
            start: Instant::now(),
            last_print: Instant::now(),
        }
    }

    /// Total time since the sync started (for the outcome line).
    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    fn update(&mut self, p: Progress) {
        // Phase transition always prints; within a phase, throttle.
        let phase_changed = match (&self.last_phase, &p) {
            (None, _) => true,
            (Some(a), b) => std::mem::discriminant(a) != std::mem::discriminant(b),
        };
        let now = Instant::now();
        if !phase_changed && now - self.last_print < Duration::from_millis(200) {
            return;
        }
        self.last_phase = Some(p);
        self.last_print = now;

        let elapsed = now - self.start;
        let line = match p {
            Progress::Scanning(hashed, total) => {
                let pct = hashed.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!(
                    "hashing   {:>6.1}%  ({}/{} bytes, {} elapsed)",
                    pct as f64 / 10.0,
                    fmt_bytes(hashed),
                    fmt_bytes(total),
                    fmt_secs(elapsed)
                )
            }
            Progress::WarmStart(chunks) => {
                format!(
                    "warm      loaded {} chunks from index, scan skipped",
                    chunks
                )
            }
            Progress::Diffing => "diffing   comparing chunk manifests".to_string(),
            Progress::Transferring(sent, total) => {
                let pct = sent.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!(
                    "sending   {:>6.1}%  ({}/{} bytes, {} elapsed)",
                    pct as f64 / 10.0,
                    fmt_bytes(sent),
                    fmt_bytes(total),
                    fmt_secs(elapsed)
                )
            }
            Progress::Receiving(received, total) => {
                let pct = received.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!(
                    "receiving {:>6.1}%  ({}/{} bytes, {} elapsed)",
                    pct as f64 / 10.0,
                    fmt_bytes(received),
                    fmt_bytes(total),
                    fmt_secs(elapsed)
                )
            }
            Progress::Verifying => "verifying comparing final root hash".to_string(),
            Progress::NothingToDo => "up to date".to_string(),
        };
        // Single-line update via carriage return; final states end with a
        // newline via the caller printing the outcome.
        eprint!("\r{}", line);
    }

    fn finish(&mut self) {
        eprintln!();
    }
}

fn fmt_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.2} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.2} KiB", f / KIB)
    } else {
        format!("{} B", n)
    }
}

fn fmt_secs(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 60.0 {
        format!("{:.0}m{:.0}s", s / 60.0, s % 60.0)
    } else if s >= 1.0 {
        format!("{:.1}s", s)
    } else {
        format!("{:.0} ms", s * 1000.0)
    }
}

/// Parses `<host>:<port>` or bracketed IPv6 `[<host>]:<port>`.
fn parse_host_port(s: &str) -> (String, u16) {
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        // [::1]:7788 — bracketed IPv6.
        let Some(end) = rest.find(']') else {
            die("address must be [host]:port for IPv6");
        };
        let host = &rest[..end];
        let after = &rest[end + 1..];
        if !after.starts_with(':') {
            die("address must be [host]:port for IPv6");
        }
        (host, &after[1..])
    } else {
        // Split at the LAST colon so bare IPv6 without a port fails loudly
        // instead of mis-parsing.
        s.rsplit_once(':')
            .unwrap_or_else(|| die("address must be host:port (use [host]:port for IPv6)"))
    };
    let port: u16 = port.parse().unwrap_or_else(|_| die("invalid port"));
    if host.is_empty() {
        die("address must be host:port");
    }
    (host.to_string(), port)
}

fn report_outcome(role: &str, outcome: SyncOutcome, display: &mut ProgressDisplay) {
    display.finish();
    match outcome {
        SyncOutcome::InSync => println!("{}: already up to date", role),
        SyncOutcome::Transferred { bytes, chunks } => {
            let elapsed = display.elapsed();
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                format!(
                    " at {}/s",
                    fmt_bytes((bytes as f64 / elapsed.as_secs_f64()) as u64)
                )
            } else {
                String::new()
            };
            println!(
                "{}: transferred {} in {} chunks{} in {}",
                role,
                fmt_bytes(bytes),
                chunks,
                throughput,
                fmt_secs(elapsed)
            );
        }
    }
}

/// Applies the I/O timeout to a TCP stream.
fn set_stream_timeouts(stream: &TcpStream, secs: u64) {
    let d = Some(Duration::from_secs(secs));
    let _ = stream.set_read_timeout(d);
    let _ = stream.set_write_timeout(d);
}

fn run_send(
    file: PathBuf,
    addr: String,
    force_checksum: bool,
    plaintext: bool,
    trust_flag: Option<String>,
) -> std::io::Result<()> {
    if !file.is_file() {
        return Err(std::io::Error::other(format!(
            "source file not found: {}",
            file.display()
        )));
    }
    let (host, port) = parse_host_port(&addr);
    println!("connecting to {host}:{port} ...");
    let tcp = TcpStream::connect((host.as_str(), port)).map_err(|e| {
        std::io::Error::other(format!(
            "cannot connect to {host}:{port}: {e} (is the receiver running?)"
        ))
    })?;
    tcp.set_nodelay(true).ok();
    set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);

    // Transport security (D16): TLS by default; --plaintext for LAN/SSH
    // tunnels. Handshake first, then the sync protocol over the TLS stream.
    let mut stream: Box<dyn ReadWrite> = if plaintext {
        Box::new(tcp)
    } else {
        let (identity, created) = ffsync::transport::Identity::load_or_create()?;
        if created {
            println!("identity created: {}", identity.fingerprint());
            println!("(share this fingerprint out-of-band so peers can pin it)");
        }
        let policy = resolve_pin_policy(trust_flag.as_deref(), &host)?;
        let (tls, presented) = ffsync::transport::client_session(
            tcp,
            &identity,
            &policy,
            rustls_pki_types_server_name(&host)?,
        )?;
        enforce_first_use(&policy, &presented, format!("{host}:{port}"))?;
        Box::new(tls)
    };

    let display = &mut ProgressDisplay::new();
    let request = SendRequest {
        path: file.clone(),
        display_path: file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        mtime: None,
        force_checksum,
    };
    let pipeline = PipelineConfig::default();
    let outcome = match send(&mut stream, &request, &pipeline, &mut |p| display.update(p)) {
        Ok(o) => o,
        Err(e) => {
            display.finish();
            return Err(io_err(&e));
        }
    };
    report_outcome("send", outcome, display);
    Ok(())
}

fn io_err(e: &SyncError) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn run_receive(
    file: PathBuf,
    port: u16,
    force_checksum: bool,
    from: Option<PathBuf>,
    plaintext: bool,
    trust_flag: Option<String>,
) -> std::io::Result<()> {
    let local_source = from.unwrap_or_else(|| file.clone());
    if !local_source.is_file() {
        return Err(std::io::Error::other(format!(
            "local source not found: {} (create the file or pass --from <old-file>)",
            local_source.display()
        )));
    }
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    let bound = listener.local_addr()?.port();
    if port == 0 {
        println!("listening on 0.0.0.0:{bound} (OS-assigned port) ...");
    } else {
        println!("listening on 0.0.0.0:{bound} ...");
    }
    let (tcp, peer) = listener.accept()?;
    println!("peer connected: {peer}");
    tcp.set_nodelay(true).ok();
    set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);

    // Transport security (D16): TLS by default; --plaintext for LAN/SSH
    // tunnels. Handshake before any sync data (the receiver is the
    // FirstUse-decision point: nothing is written before the pin is settled).
    let mut stream: Box<dyn ReadWrite> = if plaintext {
        Box::new(tcp)
    } else {
        let (identity, created) = ffsync::transport::Identity::load_or_create()?;
        if created {
            println!("identity created: {}", identity.fingerprint());
            println!("(share this fingerprint out-of-band so peers can pin it)");
        }
        let policy = resolve_pin_policy(trust_flag.as_deref(), &peer.to_string())?;
        let (tls, presented) = ffsync::transport::server_session(tcp, &identity, &policy)?;
        enforce_first_use(&policy, &presented, peer.to_string())?;
        println!(
            "peer identity: {}",
            ffsync::transport::format_fingerprint(&presented)
        );
        Box::new(tls)
    };

    let display = &mut ProgressDisplay::new();
    let request = ReceiveRequest {
        path: file.clone(),
        local_source,
        force_checksum,
    };
    let pipeline = PipelineConfig::default();
    let outcome = match receive(&mut stream, &request, &pipeline, &mut |p| display.update(p)) {
        Ok(o) => o,
        Err(e) => {
            display.finish();
            return Err(io_err(&e));
        }
    };
    report_outcome("receive", outcome, display);
    Ok(())
}

/// The `Read + Write` object bound the sync state machines run over:
/// a plain TCP stream (`--plaintext`) or a TLS stream (default).
trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

/// Builds the pin policy from `--trust` (if given) or the pins file, for
/// both roles (send = peer host label, receive = connected peer address).
/// First-use (no pins, no flag) is allowed on both sides — the prompt
/// happens after the handshake via `enforce_first_use`.
fn resolve_pin_policy(
    trust_flag: Option<&str>,
    _peer_label: &str,
) -> std::io::Result<ffsync::transport::PinPolicy> {
    match trust_flag {
        Some(fp) => {
            let raw = ffsync::transport::parse_fingerprint(fp)
                .ok_or_else(|| std::io::Error::other(format!("not a SHA-256 fingerprint: {fp}")))?;
            Ok(ffsync::transport::PinPolicy::Pinned(vec![raw]))
        }
        None => {
            let pins = ffsync::transport::load_pins()?;
            if pins.is_empty() {
                Ok(ffsync::transport::PinPolicy::FirstUse)
            } else {
                Ok(ffsync::transport::PinPolicy::Pinned(pins))
            }
        }
    }
}

/// Converts the host string into a rustls ServerName (DNS name or IP).
/// The name is never *validated* against the certificate (D16: the
/// fingerprint is the identity) — it only feeds the TLS handshake state.
fn rustls_pki_types_server_name(
    host: &str,
) -> std::io::Result<rustls::pki_types::ServerName<'static>> {
    use rustls::pki_types::ServerName;
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host.to_string())
        .map_err(|e| std::io::Error::other(format!("invalid host: {e}")))
}

/// First-use enforcement (D16): with `PinPolicy::FirstUse`, the presented
/// fingerprint must be explicitly accepted before any sync data flows —
/// interactively, or non-interactively via `--trust <fp>` (which the caller
/// already resolved into a Pinned policy; this function then only runs for
/// genuine first use).
fn enforce_first_use(
    policy: &ffsync::transport::PinPolicy,
    presented: &[u8],
    peer_label: String,
) -> std::io::Result<()> {
    if !matches!(policy, ffsync::transport::PinPolicy::FirstUse) {
        return Ok(());
    }
    let shown = ffsync::transport::format_fingerprint(presented);
    println!();
    println!("first contact with {peer_label}");
    println!("peer fingerprint: {shown}");
    print!("trust this peer? [y/N] ");
    use std::io::Write as _;
    std::io::stdout().flush().ok();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    let yes = matches!(answer.trim(), "y" | "Y" | "yes" | "Yes" | "YES");
    if !yes {
        return Err(std::io::Error::other(format!(
            "peer not trusted — nothing was synced. To pin this fingerprint \
             non-interactively: ffsync trust {shown} (or pass --trust {shown})"
        )));
    }
    ffsync::transport::add_pin(presented)?;
    println!("fingerprint pinned (pins.conf); future syncs verify silently");
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        die("no command given");
    }
    match args[0].as_str() {
        "--help" | "-h" | "help" => {
            print!("{}", USAGE);
        }
        "--version" | "-V" | "version" => {
            println!("ffsync {}", env!("CARGO_PKG_VERSION"));
        }
        "send" => {
            let mut positional: Vec<String> = Vec::new();
            let mut force_checksum = false;
            let mut plaintext = false;
            let mut trust_flag = None;
            let mut it = args[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
                    "--plaintext" => plaintext = true,
                    "--trust" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--trust requires a fingerprint"));
                        trust_flag = Some(v.to_string());
                    }
                    "--help" | "-h" => {
                        print!("{}", USAGE);
                        return;
                    }
                    "--version" | "-V" => {
                        println!("ffsync {}", env!("CARGO_PKG_VERSION"));
                        return;
                    }
                    other => positional.push(other.to_string()),
                }
            }
            if positional.len() != 2 {
                die("send requires <file> <host:port>");
            }
            let file = PathBuf::from(&positional[0]);
            let addr = positional[1].clone();
            run_send(file, addr, force_checksum, plaintext, trust_flag).unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
        }
        "receive" => {
            let mut positional: Vec<String> = Vec::new();
            let mut force_checksum = false;
            let mut plaintext = false;
            let mut trust_flag = None;
            let mut from = None;
            let mut it = args[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
                    "--plaintext" => plaintext = true,
                    "--trust" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--trust requires a fingerprint"));
                        trust_flag = Some(v.to_string());
                    }
                    "--from" => {
                        let v = it.next().unwrap_or_else(|| die("--from requires a path"));
                        from = Some(PathBuf::from(v));
                    }
                    "--help" | "-h" => {
                        print!("{}", USAGE);
                        return;
                    }
                    "--version" | "-V" => {
                        println!("ffsync {}", env!("CARGO_PKG_VERSION"));
                        return;
                    }
                    other => positional.push(other.to_string()),
                }
            }
            if positional.len() != 2 {
                die("receive requires <file> <port>");
            }
            let file = PathBuf::from(&positional[0]);
            let port: u16 = positional[1]
                .parse()
                .unwrap_or_else(|_| die(&format!("invalid port: {}", positional[1])));
            run_receive(file, port, force_checksum, from, plaintext, trust_flag).unwrap_or_else(
                |e| {
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                },
            );
        }
        "trust" => {
            // ffsync trust <fingerprint>  (or with a name label as comment)
            let fp = args
                .get(1)
                .unwrap_or_else(|| die("trust requires a fingerprint (see 'ffsync fingerprints')"));
            let raw = ffsync::transport::parse_fingerprint(fp)
                .unwrap_or_else(|| die(&format!("not a SHA-256 fingerprint: {fp}")));
            ffsync::transport::add_pin(&raw).unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
            println!("pinned {}", ffsync::transport::format_fingerprint(&raw));
        }
        "untrust" => {
            let fp = args
                .get(1)
                .unwrap_or_else(|| die("untrust requires a fingerprint"));
            let raw = ffsync::transport::parse_fingerprint(fp)
                .unwrap_or_else(|| die(&format!("not a SHA-256 fingerprint: {fp}")));
            ffsync::transport::remove_pin(&raw).unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
            println!("unpinned {}", ffsync::transport::format_fingerprint(&raw));
        }
        "fingerprints" => {
            // This machine's identity + all pinned peers.
            match ffsync::transport::Identity::load_or_create() {
                Ok((id, _)) => println!("this peer: {}", id.fingerprint()),
                Err(e) => {
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                }
            }
            match ffsync::transport::load_pins() {
                Ok(pins) => {
                    if pins.is_empty() {
                        println!("pinned peers: (none)");
                    } else {
                        println!("pinned peers:");
                        for p in pins {
                            println!("  {}", ffsync::transport::format_fingerprint(&p));
                        }
                    }
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        other => die(&format!("unknown command: {} (try 'ffsync --help')", other)),
    }
}
