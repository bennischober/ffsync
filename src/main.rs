//! ffsync CLI: send and receive files over TCP with chunk-level delta sync.
//!
//! Usage:
//!   ffsync send <file> <host:port>   [--checksum]
//!   ffsync receive <file> <port>     [--checksum] [--from <old-file>]
//!
//! The sender connects to a receiver that is listening.

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
  ffsync send <file> <host:port> [--checksum]
  ffsync receive <file> <port> [--checksum] [--from <old-file>]
  ffsync --help | --version

Commands:
  send      push a file to a listening receiver
  receive   listen for a sender and converge <file> from its current state

Options:
  --checksum         skip no metadata shortcuts; always compare chunk roots
                     (rsync -c semantics). Also disables the warm-start index.
  --from <old-file>  receive: serve local chunks from <old-file> instead of
                     <file> (useful when the target does not exist yet)
  -h, --help         print this help
  -V, --version      print version

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

fn run_send(file: PathBuf, addr: String, force_checksum: bool) -> std::io::Result<()> {
    if !file.is_file() {
        return Err(std::io::Error::other(format!(
            "source file not found: {}",
            file.display()
        )));
    }
    let (host, port) = parse_host_port(&addr);
    println!("connecting to {host}:{port} ...");
    let mut stream = TcpStream::connect((host.as_str(), port)).map_err(|e| {
        std::io::Error::other(format!(
            "cannot connect to {host}:{port}: {e} (is the receiver running?)"
        ))
    })?;
    stream.set_nodelay(true).ok();
    set_stream_timeouts(&stream, DEFAULT_TIMEOUT_SECS);
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
    let (stream, peer) = listener.accept()?;
    println!("peer connected: {peer}");
    let mut stream: TcpStream = stream;
    stream.set_nodelay(true).ok();
    set_stream_timeouts(&stream, DEFAULT_TIMEOUT_SECS);
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
            for a in &args[1..] {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
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
            run_send(file, addr, force_checksum).unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
        }
        "receive" => {
            let mut positional: Vec<String> = Vec::new();
            let mut force_checksum = false;
            let mut from = None;
            let mut it = args[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--checksum" => force_checksum = true,
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
            run_receive(file, port, force_checksum, from).unwrap_or_else(|e| {
                eprintln!("error: {}", e);
                std::process::exit(1);
            });
        }
        other => die(&format!("unknown command: {} (try 'ffsync --help')", other)),
    }
}
