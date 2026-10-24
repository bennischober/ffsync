//! ffsync CLI: send and receive files or directory trees over TCP with
//! chunk-level delta sync.
//!
//! Usage:
//!   ffsync send <file|dir> <host:port>   [--checksum] [--no-delete] [--eager] [--dry-run]
//!   ffsync receive <file|dir> <port>     [--checksum] [--from <old-file>] [--eager]
//!
//! The sender connects to a receiver that is listening. A directory path
//! switches to tree sync (v0.3): the CONTENTS of the directory are synced
//! (no trailing-slash semantics; `dir` and `dir/` are the same thing).

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ffsync::chunking::PipelineConfig;
use ffsync::protocol::sync::{
    Progress, ReceiveRequest, SendRequest, SyncError, SyncOutcome, receive, send,
};
use ffsync::protocol::tree_sync::{
    ReadWrite, TreeOutcome, TreeProgress, TreeReceiveRequest, TreeSendRequest, tree_receive,
    tree_send,
};

/// Default I/O timeout: a peer that sends nothing for this long kills the
/// sync. Generous enough for slow disks between frames on huge files
/// (scans stream manifests continuously; the longest quiet period is a
/// peer re-scanning its temp file on resume, which is disk-bound).
const DEFAULT_TIMEOUT_SECS: u64 = 300;

const USAGE: &str = "\
ffsync: fast chunk-level file sync for big files and fast links

Usage:
  ffsync send <file|dir> <host:port> [--checksum] [--no-delete]
              [--exclude <pat>]... [--exclude-from <file>]
              [--plaintext] [--quic] [--trust <fp>]
  ffsync receive <file|dir> <port> [--checksum] [--from <old-file>]
              [--no-delete] [--exclude <pat>]... [--exclude-from <file>]
              [--plaintext] [--quic] [--trust <fp>]
  ffsync trust <fingerprint>       pin a peer fingerprint (TOFU first contact)
  ffsync untrust <fingerprint>     remove a pinned fingerprint
  ffsync fingerprints              show this peer's identity + all pins
  ffsync --help | --version

Options:
  --eager          eager diff (D8/§9.2, single-file): stream chunk DATA during
                   the manifest exchange instead of after the plan; both peers
                   need the flag; a declining peer fails the session loudly
  --checksum         skip no metadata shortcuts; always compare chunk roots
                     (rsync -c semantics). Also disables the warm-start index.
                     In tree mode this applies to every file in the tree.
  --from <old-file>  receive (single-file only): serve local chunks from
                     <old-file> instead of <file> (useful when the target
                     does not exist yet)
  --no-delete        tree mode: keep files that exist only on the receiver
                     (default IS to delete them — the sender's tree is
                     authoritative, like rsync --delete)
  --exclude <pat>    tree mode: exclude matching paths from the sync
                     (repeatable). Grammar: '*' and '?' never cross '/';
                     '**' as a whole segment matches any depth;
                     '[a-z]'/'[!a-z]' character classes; a pattern with '/'
                     matches from the root, without it matches the basename
                     at any depth; trailing '/' restricts to directories.
                     The sender's patterns rule the session (they travel in
                     the protocol); on the receiver they additionally protect
                     matching paths from --no-delete-less deletion.
  --exclude-from <f> tree mode: read one exclude pattern per line from <f>
                     ('#' comments and blank lines are ignored)
  --plaintext        disable TLS (LAN use or inside an SSH tunnel); D16
                     (TCP only — QUIC is always encrypted, §17.2)
  --quic             use the QUIC transport (quinn, TLS 1.3 inside the
                     transport, D13 step 2 / §17). Receiver listens on UDP;
                     sender connects over UDP. Same pinning model as TCP.
  --trust <fp>       non-interactive first contact: pin <fp> instead of
                     prompting; also overrides which pinned peer to expect
  -h, --help         print this help
  -V, --version      print version

Tree mode (v0.3): pass a directory instead of a file. The CONTENTS of the
directory are synced into the destination directory ('dir' and 'dir/' mean
the same thing — no rsync trailing-slash semantics). Files that match in
size and modification time are skipped; changed files transfer deltas;
files that exist only on the receiver are deleted unless --no-delete.
Deletions happen only after every transfer succeeded.

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

/// Reads an exclude file (§16.7 `--exclude-from`): one pattern per line,
/// `#` comments and blank lines ignored. Missing file is a loud error —
/// silently syncing everything the user meant to exclude would be worse.
fn read_exclude_file(path: &str, out: &mut Vec<String>) {
    let content = std::fs::read_to_string(path).unwrap_or_else(|e| {
        die(&format!("cannot read --exclude-from file {path}: {e}"));
    });
    for (n, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Reject inline comments early: a trailing " # note" is almost
        // certainly not meant as part of the pattern.
        if line.contains(" #") {
            die(&format!(
                "--exclude-from {} line {}: inline comments are not supported (use a whole-line '#' comment)",
                path,
                n + 1
            ));
        }
        out.push(line.to_string());
    }
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
            Progress::WouldChange => "would change (dry run)".into(),
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
            Progress::CreatedEmpty => "created (empty file)".to_string(),
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
        SyncOutcome::WouldChange => {
            println!("{}: WOULD CHANGE (dry run — nothing was written)", role)
        }
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

/// Shared CLI flags for send/receive (tree and single-file runners pick
/// what they need; single-file mode rejects excludes and parallel loudly).
struct CliOptions {
    force_checksum: bool,
    plaintext: bool,
    /// QUIC transport instead of TCP (D13 step 2, §17). Mutually exclusive
    /// with --plaintext (QUIC has no plaintext mode; TLS 1.3 is
    /// transport-mandated).
    quic: bool,
    trust_flag: Option<String>,
    no_delete: bool,
    excludes: Vec<String>,
    /// Parallel session width (§16.8); 1 = sequential (default).
    parallel: u16,
    /// §16.10 dry-run: preview session — the peer executes nothing and
    /// reports what would happen. Sender-side only.
    dry_run: bool,
    /// §9.2 eager diff (single-file only): stream DATA during the manifest
    /// exchange, bounded by the position gate. Both peers must pass --eager.
    eager: bool,
}

fn run_send(file: PathBuf, addr: String, opts: CliOptions) -> std::io::Result<()> {
    let CliOptions {
        force_checksum,
        plaintext,
        quic,
        trust_flag,
        no_delete,
        excludes,
        parallel,
        dry_run,
        eager,
    } = opts;
    let tree = file.is_dir();
    if tree {
        if no_delete {
            return Err(std::io::Error::other(
                "--no-delete applies to receive only (the sender never deletes)",
            ));
        }
        return run_tree_send(TreeSendOpts {
            root: file,
            addr,
            force_checksum,
            plaintext,
            quic,
            trust_flag,
            excludes,
            parallel,
            dry_run,
        });
    }
    if !excludes.is_empty() {
        return Err(std::io::Error::other(
            "--exclude/--exclude-from apply to tree mode only (pass a directory)",
        ));
    }
    if parallel > 1 {
        return Err(std::io::Error::other(
            "--parallel applies to tree mode only (pass a directory)",
        ));
    }
    if !file.is_file() {
        return Err(std::io::Error::other(format!(
            "source file not found: {}",
            file.display()
        )));
    }
    let (host, port) = parse_host_port(&addr);
    let mut stream: Box<dyn ReadWrite> = if quic {
        println!("connecting to {host}:{port} (QUIC) ...");
        let server_addr: std::net::SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| std::io::Error::other(format!("cannot resolve {host}:{port}: {e}")))?;
        let (identity, created) = ffsync::transport::Identity::load_or_create()?;
        if created {
            println!("identity created: {}", identity.fingerprint());
            println!("(share this fingerprint out-of-band so peers can pin it)");
        }
        let policy = resolve_pin_policy(trust_flag.as_deref(), &host)?;
        let name = rustls_pki_types_server_name(&host)?;
        let (_session, quic_stream, presented) =
            ffsync::transport::quic::client_session(&identity, &policy, server_addr, name)?;
        enforce_first_use(&policy, &presented, format!("{host}:{port}"))?;
        println!(
            "peer identity: {}",
            ffsync::transport::format_fingerprint(&presented)
        );
        Box::new(quic_stream)
    } else {
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
        if plaintext {
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
        }
    };

    let display = &mut ProgressDisplay::new();
    let request = SendRequest {
        path: file.clone(),
        display_path: file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        mtime: None,
        force_checksum: opts.force_checksum,
        tree: false,
        eager,
        dry_run,
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
    from: Option<PathBuf>,
    opts: CliOptions,
) -> std::io::Result<()> {
    let CliOptions {
        force_checksum,
        plaintext,
        quic,
        trust_flag,
        no_delete,
        excludes,
        parallel: _parallel, // single-file receive has no parallel sessions
        dry_run,             // §16.10: meaningless for single-file, used by tree mode
        eager,
    } = opts;
    let listener = if quic {
        let (identity, created) = ffsync::transport::Identity::load_or_create()?;
        if created {
            println!("identity created: {}", identity.fingerprint());
            println!("(share this fingerprint out-of-band so peers can pin it)");
        }
        // The peer label is unknown before accept; the policy is
        // label-independent (a fingerprint set).
        let policy = resolve_pin_policy(trust_flag.as_deref(), "peer")?;
        let bind: std::net::SocketAddr =
            format!("0.0.0.0:{port}").parse().expect("valid bind addr");
        let endpoint = ffsync::transport::quic::listen(bind, &identity, &policy)?;
        Listener::Quic(endpoint)
    } else {
        Listener::Tcp(TcpListener::bind(("0.0.0.0", port))?)
    };
    let bound = match &listener {
        Listener::Tcp(l) => l.local_addr()?.port(),
        Listener::Quic(e) => e.local_addr()?.port(),
    };
    if file.is_dir() {
        if from.is_some() {
            return Err(std::io::Error::other(
                "--from is single-file only (tree sync has no old-file concept)",
            ));
        }
        return run_tree_receive(TreeReceiveOpts {
            root: file,
            force_checksum,
            plaintext,
            trust_flag,
            no_delete,
            excludes,
            listener,
            bound,
            dry_run,
        });
    }
    if !excludes.is_empty() {
        return Err(std::io::Error::other(
            "--exclude/--exclude-from apply to tree mode only (pass a directory)",
        ));
    }
    let local_source = from.unwrap_or_else(|| file.clone());
    if !local_source.is_file() {
        return Err(std::io::Error::other(format!(
            "local source not found: {} (create the file or pass --from <old-file>)",
            local_source.display()
        )));
    }
    if port == 0 {
        println!("listening on 0.0.0.0:{bound} (OS-assigned port) ...");
    } else {
        println!("listening on 0.0.0.0:{bound} ...");
    }
    let mut quic_receive_session: Option<ffsync::transport::quic::QuicSession> = None;
    let mut stream: Box<dyn ReadWrite> = match listener {
        Listener::Tcp(listener) => {
            let (tcp, peer) = listener.accept()?;
            println!("peer connected: {peer}");
            tcp.set_nodelay(true).ok();
            set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);

            // Transport security (D16): TLS by default; --plaintext for
            // LAN/SSH tunnels. Handshake before any sync data (the receiver
            // is the FirstUse-decision point: nothing is written before the
            // pin is settled).
            if plaintext {
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
            }
        }
        Listener::Quic(endpoint) => {
            // Identity/policy were resolved pre-accept (endpoint config);
            // FirstUse enforcement happens now, after the handshake.
            let policy = resolve_pin_policy(trust_flag.as_deref(), "peer")?;
            let (session, quic_stream, presented) =
                ffsync::transport::quic::server_session(endpoint)?;
            let peer_label = session.remote_address().to_string();
            println!("peer connected: {peer_label} (QUIC)");
            enforce_first_use(&policy, &presented, peer_label)?;
            println!(
                "peer identity: {}",
                ffsync::transport::format_fingerprint(&presented)
            );
            // §17.6: our COMMIT_ACK / NOT_MODIFIED replies to the sender's
            // final frames are only provably delivered when the sender
            // closes; hold the connection until then (bounded).
            quic_receive_session = Some(session);
            Box::new(quic_stream)
        }
    };

    let display = &mut ProgressDisplay::new();
    let request = ReceiveRequest {
        path: file.clone(),
        local_source,
        force_checksum,
        tree: false,
        eager,
        apply_mode: None,
        apply_mtime: None,
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
    // §17.6 (QUIC): the sender waits for our close after its drain; hold
    // until its close proves our final replies were delivered (bounded).
    if let Some(session) = quic_receive_session {
        session.wait_for_peer_close(Duration::from_secs(10));
    }
    Ok(())
}

/// Options bundle for [`run_tree_send`] (8 params exceeded clippy's
/// limit; these are one logical unit — the sender's setup).
struct TreeSendOpts {
    root: PathBuf,
    addr: String,
    force_checksum: bool,
    plaintext: bool,
    quic: bool,
    trust_flag: Option<String>,
    excludes: Vec<String>,
    parallel: u16,
    dry_run: bool,
}

/// Tree mode (v0.3): send a directory's contents. TLS setup is shared with
/// the single-file path via [`connect_authenticated`].
fn run_tree_send(opts: TreeSendOpts) -> std::io::Result<()> {
    let TreeSendOpts {
        root,
        addr,
        force_checksum,
        plaintext,
        quic,
        trust_flag,
        excludes,
        parallel,
        dry_run,
    } = opts;
    let (host, port) = parse_host_port(&addr);

    let display = &mut TreeDisplay::new();
    let mut quic_wait: Option<ffsync::transport::quic::QuicSession> = None;
    let (stream, connect_worker) = if quic {
        // QUIC (§17): one connection, K streams. The control stream comes
        // from client_session; workers clone the session handle and open
        // their own stream (the QUIC equivalent of a fresh TCP connect).
        println!("connecting to {host}:{port} (QUIC) ...");
        let server_addr: std::net::SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| std::io::Error::other(format!("cannot resolve {host}:{port}: {e}")))?;
        let (identity, created) = ffsync::transport::Identity::load_or_create()?;
        if created {
            println!("identity created: {}", identity.fingerprint());
            println!("(share this fingerprint out-of-band so peers can pin it)");
        }
        let policy = resolve_pin_policy(trust_flag.as_deref(), &host)?;
        let name = rustls_pki_types_server_name(&host)?;
        let (session, control, presented) =
            ffsync::transport::quic::client_session(&identity, &policy, server_addr, name)?;
        enforce_first_use(&policy, &presented, format!("{host}:{port}"))?;
        println!(
            "peer identity: {}",
            ffsync::transport::format_fingerprint(&presented)
        );
        let session_for_wait = session.clone();
        let connect_worker: Box<dyn Fn(u16) -> std::io::Result<Box<dyn ReadWrite>> + Send + Sync> =
            Box::new(move |index: u16| -> std::io::Result<Box<dyn ReadWrite>> {
                let s = session.open_stream().map_err(|e| {
                    std::io::Error::other(format!("cannot open worker {index}: {e}"))
                })?;
                Ok(Box::new(s))
            });
        quic_wait = Some(session_for_wait);
        (Box::new(control) as Box<dyn ReadWrite>, connect_worker)
    } else {
        println!("connecting to {host}:{port} ...");
        let tcp = TcpStream::connect((host.as_str(), port)).map_err(|e| {
            std::io::Error::other(format!(
                "cannot connect to {host}:{port}: {e} (is the receiver running?)"
            ))
        })?;
        tcp.set_nodelay(true).ok();
        set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);
        let stream = connect_authenticated(tcp, &host, true, plaintext, trust_flag.clone())?;

        // Worker connections (§16.8): same host/port, same TLS setup, one per
        // worker index. Opened by the state machine after TREE_BEGIN.
        let connect_worker = move |index: u16| -> std::io::Result<Box<dyn ReadWrite>> {
            let tcp = TcpStream::connect((host.as_str(), port)).map_err(|e| {
                std::io::Error::other(format!("cannot connect worker {index}: {e}"))
            })?;
            tcp.set_nodelay(true).ok();
            set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);
            connect_authenticated(tcp, &host, true, plaintext, trust_flag.clone())
        };
        let connect_worker: Box<dyn Fn(u16) -> std::io::Result<Box<dyn ReadWrite>> + Send + Sync> =
            Box::new(connect_worker);
        (stream, connect_worker)
    };
    let mut stream = stream;
    let request = TreeSendRequest {
        root,
        force_checksum,
        no_delete: false, // the sender never deletes; the flag is receiver-side
        excludes,
        parallel,
        dry_run,
        connect_worker: std::sync::Arc::new(connect_worker),
    };
    let pipeline = PipelineConfig::default();
    let outcome = match tree_send(&mut stream, &request, &pipeline, &mut |p| display.update(p)) {
        Ok(o) => o,
        Err(e) => {
            display.finish();
            return Err(io_err(&e));
        }
    };
    if dry_run {
        println!("(dry run — nothing was written; the numbers below are a preview)");
    }
    report_tree_outcome("send", &outcome, display);
    // §17.6 (QUIC): delivery of our final frames is proven by *reading the
    // receiver's TREE_END report* — done inside tree_send, stream order
    // guarantees everything sent before it was received. The session handle
    // drops here, closing the connection; that close is what the receiver's
    // bounded wait observes. Do NOT wait for the peer's close on the sender
    // side: both ends holding for each other burns the full teardown bound
    // on every QUIC session (a symmetric standoff, not delivery proof).
    drop(quic_wait);
    Ok(())
}

/// Options bundle for [`run_tree_receive`] (8 params exceeded clippy's
/// limit; these are one logical unit — the receiver's setup).
struct TreeReceiveOpts {
    root: PathBuf,
    force_checksum: bool,
    plaintext: bool,
    trust_flag: Option<String>,
    no_delete: bool,
    excludes: Vec<String>,
    listener: Listener,
    bound: u16,
    dry_run: bool,
}

/// The receiver's listening socket: TCP (default) or a QUIC endpoint
/// (`--quic`, UDP).
enum Listener {
    Tcp(TcpListener),
    Quic(ffsync::transport::quic::QuicEndpoint),
}

/// Tree mode (v0.3): receive a directory's contents. The target directory is
/// created when missing (a first sync into an empty destination is the
/// normal case).
fn run_tree_receive(opts: TreeReceiveOpts) -> std::io::Result<()> {
    let TreeReceiveOpts {
        root,
        force_checksum,
        plaintext,
        trust_flag,
        no_delete,
        excludes,
        listener,
        bound,
        dry_run,
    } = opts;
    if !root.exists() {
        std::fs::create_dir_all(&root)?;
    }
    let display = &mut TreeDisplay::new();
    let (stream, accept_worker, _peer_label, session) = match listener {
        Listener::Tcp(listener) => {
            println!("listening on 0.0.0.0:{bound} (tree mode) ...");
            let (tcp, peer) = listener.accept()?;
            println!("peer connected: {peer}");
            tcp.set_nodelay(true).ok();
            set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);
            let peer_label = peer.to_string();
            let stream =
                connect_authenticated(tcp, &peer_label, false, plaintext, trust_flag.clone())?;
            // Worker connections (§16.8): accept from the same listener (the
            // sender connects them to the same port), same TLS setup.
            let accept_worker: Box<dyn Fn() -> std::io::Result<Box<dyn ReadWrite>> + Send + Sync> =
                Box::new(move || -> std::io::Result<Box<dyn ReadWrite>> {
                    let (tcp, wpeer) = listener
                        .accept()
                        .map_err(|e| std::io::Error::other(format!("cannot accept worker: {e}")))?;
                    tcp.set_nodelay(true).ok();
                    set_stream_timeouts(&tcp, DEFAULT_TIMEOUT_SECS);
                    connect_authenticated(
                        tcp,
                        &wpeer.to_string(),
                        false,
                        plaintext,
                        trust_flag.clone(),
                    )
                });
            (
                stream,
                accept_worker,
                peer_label,
                None::<ffsync::transport::quic::QuicSession>,
            )
        }
        Listener::Quic(endpoint) => {
            println!("listening on 0.0.0.0:{bound} (tree mode, QUIC) ...");
            // Identity + policy are needed BEFORE accept (they configure the
            // endpoint's TLS); FirstUse enforcement happens after the
            // handshake, exactly like TCP.
            let (identity, created) = ffsync::transport::Identity::load_or_create()?;
            if created {
                println!("identity created: {}", identity.fingerprint());
                println!("(share this fingerprint out-of-band so peers can pin it)");
            }
            // The policy needs a peer label, which exists only after accept;
            // resolve with a placeholder label — the fingerprint check is
            // label-independent (pins are a fingerprint set).
            let policy = resolve_pin_policy(trust_flag.as_deref(), "peer")?;
            let (session, control, presented) = ffsync::transport::quic::server_session(endpoint)?;
            let peer_label = session.remote_address().to_string();
            enforce_first_use(&policy, &presented, peer_label.clone())?;
            println!(
                "peer identity: {}",
                ffsync::transport::format_fingerprint(&presented)
            );
            // Worker streams (§16.8): accept more streams on the same
            // connection (the QUIC equivalent of accepting more TCP
            // connections).
            let accept_worker: Box<dyn Fn() -> std::io::Result<Box<dyn ReadWrite>> + Send + Sync> =
                Box::new({
                    let session = session.clone();
                    move || -> std::io::Result<Box<dyn ReadWrite>> {
                        let s = session.accept_stream().map_err(|e| {
                            std::io::Error::other(format!("cannot accept worker: {e}"))
                        })?;
                        Ok(Box::new(s))
                    }
                });
            (
                Box::new(control) as Box<dyn ReadWrite>,
                accept_worker,
                peer_label,
                Some(session),
            )
        }
    };
    let mut stream = stream;
    let request = TreeReceiveRequest {
        root,
        force_checksum,
        no_delete,
        excludes,
        accept_worker: std::sync::Arc::new(accept_worker),
        dry_run,
    };
    let pipeline = PipelineConfig::default();
    let outcome = match tree_receive(&mut stream, &request, &pipeline, &mut |p| display.update(p)) {
        Ok(o) => o,
        Err(e) => {
            display.finish();
            return Err(io_err(&e));
        }
    };
    if dry_run {
        println!("(dry run — nothing was written; the numbers below are a preview)");
    }
    report_tree_outcome("receive", &outcome, display);
    // §17.6: the sender's last frames (TREE_END report path) are only
    // provably delivered when the sender closes; hold the connection
    // briefly so its close lands. (The sender closes right after reading
    // our TREE_END report — this wait observes that close, usually in
    // milliseconds.)
    if let Some(session) = session {
        session.wait_for_peer_close(Duration::from_secs(10));
    }
    Ok(())
}

/// Shared transport setup (D16): TLS by default with mutual fingerprint
/// pinning; `--plaintext` bypasses. Used by all four runners (file/tree ×
/// send/receive).
fn connect_authenticated(
    tcp: TcpStream,
    peer_label: &str,
    client_role: bool,
    plaintext: bool,
    trust_flag: Option<String>,
) -> std::io::Result<Box<dyn ReadWrite>> {
    if plaintext {
        return Ok(Box::new(tcp));
    }
    let (identity, created) = ffsync::transport::Identity::load_or_create()?;
    if created {
        println!("identity created: {}", identity.fingerprint());
        println!("(share this fingerprint out-of-band so peers can pin it)");
    }
    let policy = resolve_pin_policy(trust_flag.as_deref(), peer_label)?;
    let (tls, presented) = if client_role {
        let (tls, presented) = ffsync::transport::client_session(
            tcp,
            &identity,
            &policy,
            rustls_pki_types_server_name(peer_label)?,
        )?;
        (Box::new(tls) as Box<dyn ReadWrite>, presented)
    } else {
        let (tls, presented) = ffsync::transport::server_session(tcp, &identity, &policy)?;
        (Box::new(tls) as Box<dyn ReadWrite>, presented)
    };
    enforce_first_use(&policy, &presented, peer_label.to_string())?;
    println!(
        "peer identity: {}",
        ffsync::transport::format_fingerprint(&presented)
    );
    Ok(tls)
}

// The `Read + Write` object bound the sync state machines run over —
// a plain TCP stream (`--plaintext`) or a TLS stream (default) — is
// `ReadWrite`, re-exported from tree_sync (Read + Write + Send: worker
// connections live on their own threads).
/// Tree-mode progress display: one line per file conversation plus
/// tree-level phases, mirroring ProgressDisplay's single-line discipline.
struct TreeDisplay {
    current_file: Option<String>,
    last_print: Instant,
}

impl TreeDisplay {
    fn new() -> Self {
        Self {
            current_file: None,
            last_print: Instant::now(),
        }
    }

    fn update(&mut self, p: TreeProgress) {
        match p {
            TreeProgress::Scanning => {
                self.set_line("scanning  walking the tree (metadata only)".to_string());
            }
            TreeProgress::FileStart(_idx, _total) => {
                // The FileProgress line carries the visibility.
            }
            TreeProgress::FileProgress { rel_path, progress } => {
                self.current_file = Some(rel_path);
                self.render_file_progress(progress);
            }
            TreeProgress::FileDone(idx, total) => {
                self.set_line(format!("done      {}/{} files synced", idx + 1, total));
            }
            TreeProgress::Deleting(n, bytes) => {
                self.set_line(format!("deleting  {} removed ({})", n, fmt_bytes(bytes)));
            }
        }
    }

    fn render_file_progress(&mut self, p: Progress) {
        let now = Instant::now();
        if !matches!(p, Progress::NothingToDo) && now - self.last_print < Duration::from_millis(200)
        {
            return;
        }
        self.last_print = now;
        let file = self.current_file.clone().unwrap_or_default();
        let inner = match p {
            Progress::Scanning(hashed, total) => {
                let pct = hashed.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!("hashing {:.1}% ({})", pct as f64 / 10.0, fmt_bytes(hashed))
            }

            Progress::WouldChange => "would change (dry run)".into(),
            Progress::WarmStart(chunks) => format!("warm ({} chunks from index)", chunks),
            Progress::Diffing => "diffing".to_string(),
            Progress::Transferring(sent, total) => {
                let pct = sent.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!("sending {:.1}% ({})", pct as f64 / 10.0, fmt_bytes(sent))
            }
            Progress::Receiving(received, total) => {
                let pct = received.checked_mul(100).map_or(1000, |v| v / total.max(1));
                format!(
                    "receiving {:.1}% ({})",
                    pct as f64 / 10.0,
                    fmt_bytes(received)
                )
            }
            Progress::Verifying => "verifying".to_string(),
            Progress::NothingToDo => "up to date".to_string(),
            Progress::CreatedEmpty => "created (empty file)".to_string(),
        };
        self.set_line(format!("sync      {}  {}", file, inner));
    }

    fn set_line(&mut self, line: String) {
        eprint!("\r{}", line);
    }

    fn finish(&mut self) {
        eprintln!();
    }
}

/// Prints the tree session summary (both roles).
fn report_tree_outcome(role: &str, outcome: &TreeOutcome, display: &mut TreeDisplay) {
    display.finish();
    let mut parts = Vec::new();
    parts.push(format!("{} files", outcome.files));
    if outcome.transferred_bytes > 0 {
        parts.push(format!(
            "{} transferred",
            fmt_bytes(outcome.transferred_bytes)
        ));
    }
    if outcome.in_sync > 0 {
        parts.push(format!("{} up to date", outcome.in_sync));
    }
    if outcome.deleted > 0 {
        parts.push(format!(
            "{} deleted ({})",
            outcome.deleted,
            fmt_bytes(outcome.deleted_bytes)
        ));
    }
    println!("{}: {}", role, parts.join(", "));
}

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
            let mut quic = false;
            let mut trust_flag = None;
            let mut no_delete = false;
            let mut excludes: Vec<String> = Vec::new();
            let mut parallel: u16 = 1;
            let mut dry_run = false;
            let mut eager = false;
            let mut it = args[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
                    "--plaintext" => plaintext = true,
                    "--quic" => quic = true,
                    "--trust" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--trust requires a fingerprint"));
                        trust_flag = Some(v.to_string());
                    }
                    "--no-delete" => no_delete = true,
                    "--dry-run" => dry_run = true,
                    "--eager" => eager = true,
                    "--exclude" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--exclude requires a pattern"));
                        excludes.push(v.to_string());
                    }
                    "--exclude-from" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--exclude-from requires a path"));
                        read_exclude_file(v, &mut excludes);
                    }
                    "--parallel" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--parallel requires a number"));
                        parallel = v.parse().unwrap_or_else(|_| {
                            die(&format!("invalid --parallel value: {v} (2..=16)"))
                        });
                        if !(2..=16).contains(&parallel) {
                            die(&format!("--parallel must be 2..=16 (got {parallel})"));
                        }
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
                die("send requires <file|dir> <host:port>");
            }
            if plaintext && quic {
                die(
                    "--plaintext and --quic are mutually exclusive (QUIC is always encrypted, §17.2)",
                );
            }
            let file = PathBuf::from(&positional[0]);
            let addr = positional[1].clone();
            run_send(
                file,
                addr,
                CliOptions {
                    force_checksum,
                    plaintext,
                    quic,
                    trust_flag,
                    no_delete,
                    excludes,
                    parallel,
                    dry_run,
                    eager,
                },
            )
            .unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
        }
        "receive" => {
            let mut positional: Vec<String> = Vec::new();
            let mut force_checksum = false;
            let mut plaintext = false;
            let mut quic = false;
            let mut trust_flag = None;
            let mut from = None;
            let mut no_delete = false;
            let mut excludes: Vec<String> = Vec::new();
            let mut parallel: u16 = 1;
            let mut dry_run = false;
            let mut eager = false;
            let mut it = args[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
                    "--plaintext" => plaintext = true,
                    "--quic" => quic = true,
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
                    "--eager" => eager = true,
                    "--dry-run" => dry_run = true,
                    "--no-delete" => no_delete = true,
                    "--exclude" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--exclude requires a pattern"));
                        excludes.push(v.to_string());
                    }
                    "--exclude-from" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--exclude-from requires a path"));
                        read_exclude_file(v, &mut excludes);
                    }
                    "--parallel" => {
                        let v = it
                            .next()
                            .unwrap_or_else(|| die("--parallel requires a number"));
                        parallel = v.parse().unwrap_or_else(|_| {
                            die(&format!("invalid --parallel value: {v} (2..=16)"))
                        });
                        if !(2..=16).contains(&parallel) {
                            die(&format!("--parallel must be 2..=16 (got {parallel})"));
                        }
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
                die("receive requires <file|dir> <port>");
            }
            if plaintext && quic {
                die(
                    "--plaintext and --quic are mutually exclusive (QUIC is always encrypted, §17.2)",
                );
            }
            let file = PathBuf::from(&positional[0]);
            let port: u16 = positional[1]
                .parse()
                .unwrap_or_else(|_| die(&format!("invalid port: {}", positional[1])));
            run_receive(
                file,
                port,
                from,
                CliOptions {
                    force_checksum,
                    plaintext,
                    quic,
                    trust_flag,
                    no_delete,
                    excludes,
                    parallel,
                    dry_run,
                    eager,
                },
            )
            .unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
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
