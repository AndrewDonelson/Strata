# Security Policy

## Supported Versions

Only the latest release on the `main` branch receives security fixes.
Older tags are not actively maintained.

| Version | Supported |
|---------|-----------|
| latest (`main`) | ✅ |
| older tags | ❌ |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Send a report to **security@nlaak.com** with:

1. A description of the vulnerability and its potential impact.
2. Steps to reproduce or a proof-of-concept.
3. Affected versions / commits.
4. Any suggested mitigations (optional but appreciated).

### What to expect

| Timeline | Action |
|----------|--------|
| Within **48 hours** | Acknowledgement of your report |
| Within **7 days** | Initial assessment and severity rating |
| Within **30 days** | Patch or mitigation plan communicated to you |
| Disclosure | Coordinated public disclosure after a fix is available |

We follow [responsible disclosure](https://en.wikipedia.org/wiki/Responsible_disclosure).
If you need a CVE assigned, we are happy to coordinate that process with you.

## Security Considerations for Users

### Encryption

- Strata's built-in field-level encryption uses **AES-256-GCM**.
- The encryption key (`Config.EncryptionKey`) must be exactly **32 bytes**.
- **Never commit encryption keys** to source control. Load them from environment
  variables, a secrets manager, or a vault.

### Connection Strings

- PostgreSQL DSN (`Config.PostgresDSN`) and Redis address (`Config.RedisAddr`)
  may contain credentials. Pass them via environment variables, not hard-coded
  strings.

### Dependency Supply Chain

- Strata pins its direct dependencies in `go.sum`.
- Run `go mod verify` to confirm the module graph has not been tampered with.
- Dependabot is enabled on this repository to surface known CVEs promptly.

## Acknowledgements

We are grateful to all security researchers who responsibly disclose issues
to us. Reporters who wish to be credited will be listed here after public
disclosure (with their permission).
