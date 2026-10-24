# Security Policy

## Supported versions

| version | security fixes |
|---------|----------------|
| 1.0.x   | yes            |
| <= 0.8.0 | none — upgrade |

## Reporting a vulnerability

Please report privately — do NOT open a public issue for anything you
believe is exploitable.

- Preferred: GitHub's private vulnerability reporting (Security tab ->
  "Report a vulnerability" on this repository).
- Alternative: contact [@bennischober](https://github.com/bennischober)
  directly.

Include: ffsync version (`ffsync --version`) on BOTH peers, both
platforms, the transport (TLS TCP / QUIC / `--plaintext`), and a minimal
reproduction. You will get an initial response as quickly as practical;
fixes land as patch releases with a CHANGELOG entry and, when warranted,
a GitHub security advisory (credit on request).

## Scope notes (what is and is not a vulnerability)

**In scope:** the wire protocol implementations (framing and codec
strictness, §12 resource limits), the TLS/QUIC configuration and the TOFU
fingerprint-pinning logic, path traversal in tree mode, and
resume/reconstruction state handling.

**Known and by design (not vulnerabilities):**

- **TOFU first contact (D16):** a man-in-the-middle present at the very
  first connection can present its own fingerprint. The mitigation is the
  documented out-of-band fingerprint compare (in person, chat, however you
  would exchange a key); after pinning, later sessions are safe.
- **`--plaintext`** sends unencrypted traffic — that is its purpose
  (trusted LANs, SSH tunnels).
- **Metadata scope (D9):** xattrs, hardlinks, and symlinks-as-entries are
  out of scope by recorded decision.

## Safe harbor

Good-faith security research and disclosure following this policy is
welcome and will not be met with legal action.
