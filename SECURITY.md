# Security

Report an undisclosed flaw only through GitHub private vulnerability
reporting for this repository:

https://github.com/Aryagorjipour/surface/security/advisories/new

A GitHub account is required. An undisclosed flaw must not be filed as a
public issue. A person who has no GitHub account still must not open a
public issue for an undisclosed flaw.

A flaw that is already public may be filed as a public issue.

A useful report includes what is affected, how to reproduce it, and the
impact.

This path is not for conduct reports. Conduct reports are described in
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Supported release

v0.2.0 is the supported release. v0.1.1 and v0.1.0 are older tags.

## In scope

A flaw report can cover these parts of Surface:

- the `connector` program
- the `surface` program
- session sealing
- mailbox sealing
- the release binaries and `SHA256SUMS`

## Out of scope

A Surface flaw report does not cover the operator's Vault, Redis, host, or a relay process the operator runs.

A relay the operator runs can see the size and the timing of each write. The socket also carries the request id in the clear. [DATA.md](DATA.md) lists the bytes on that socket.

## Shared token

The connector token is not per-user authentication. Anyone with the token can register and look up peers. [DATA.md](DATA.md) lists the fields those calls return.

The connector speaks cleartext unless `--tls-cert` and `--tls-key` are both set. When both are unset, register and lookup send peer keys, addresses, identities, and the bearer token in cleartext.
