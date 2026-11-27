# Changelog

## [0.2.0] - 2026-11-27

**The Borrowed Relay**

### Added

- A configured relay can carry a live chat only until a direct handshake succeeds, and no longer than 20 seconds. The same chat keeps its lines, then the relay stops.
- `/limit` sets the session chat limit from 1 to 32. `/limit save` writes `surface-limits/config.json` with an audit row. `/audit` shows recent rows, and `/audit apply` writes a new row from an older one.
- The status line warns in yellow one chat under the limit and in red when the limit is reached. Open chats stay open if the limit is lowered.

### Changed

- A failed attempt with a relay says `direct connection was not established in time; the temporary relay has stopped`. With no relay it says `direct connection was not established`.
- `--max-chats`, `SURFACE_MAX_CHATS`, or the config file applies to that run only and does not change the saved limit.
- Builds use Go 1.27.4.

## [0.1.1] - 2026-10-21

**The Quiet Relay**

### Added

- Connector container image is published to GHCR on release tags (`ghcr.io/aryagorjipour/surface-connector`).

### Changed

- Release workflow now builds and pushes the connector image alongside the binaries and SLSA attestation.

## [0.1.0] - 2026-10-21

First public release.

### Added

- With `--census` or `SURFACE_CENSUS` set, an authenticated caller can read `GET /operator/peers` and `GET /operator/pending`. Each body is a count. Both routes stay off unless that setting is on.
- A tag `vMAJOR.MINOR.PATCH` publishes `connector` and `surface` binaries, `SHA256SUMS`, and a signed SLSA provenance attestation.
- A direct chat uses a fresh key for each message. The current key cannot read earlier messages.
- With `SURFACE_MAILBOX` set to a positive duration, Surface can leave one short encrypted line for a peer who is not in a chat. The connector stores the ciphertext until that peer collects it, or until the duration ends.
- One Surface can keep several direct chats at once. `--max-chats` / `SURFACE_MAX_CHATS` sets the ceiling (default 4). `/focus` and `/chats` switch among live conversations without closing inactive sockets.
- Surface desk shows system and connection events as timed toasts instead of transcript lines.
- Surface desk supports mouse wheel scroll and click to focus panes, select peers, and dismiss toasts.
- Surface desk uses Lip Gloss styling for pane titles, chat roles, status, compose, and toast overlays.
- `just help` starts the dev Connector, a dev Surface from source, and the stage or production Connector.

### Fixed

- Accept and status responses no longer carry an admission proof. Each Surface derives the handshake key from the two peer identities snapped onto the request. Accept and reject apply only while the request is pending. A second connect for that pending pair returns the original id.
- An enabled Redis address requires a password. Request values are sealed with the same AES-GCM key as peer addresses in SQLite. A failed write fails the HTTP call.
- `--tls-cert` with `--tls-key` serves the Connector over TLS. `--direct-cert` with `--direct-key` uses `wss` for the direct socket. Leaving both unset keeps local cleartext.
- Builds use Go 1.27.3 and golang.org/x/net v0.60.0.
- The local stack allows 240 requests per source per minute, so two Surfaces on one machine can poll, connect, and accept. A refused call is reported as `too many requests`.
- A file-backed connector store keeps one SQLite connection open, so two peers can register together.
- Connector spans record the route endpoint and the HTTP status.
- A blocked direct chat prints `traversal failed`.
- `surface` accepts `--relay` and `--punch`. The connector accepts `--punch`.
- Connector `GET /health`, probed by `connector --ready`.
- The local compose file bootstraps its dev secrets before the connector starts.
- Terminal chat between two peers.
- A connector that only introduces the peers. It does not carry the chat.
- After both peers accept, they talk directly.
- A local run of the connector is published under `deployments/`, with a cache and a secret store beside it. The chat program itself is not part of that local stack.

## [Unreleased]

v0.2.0 is the current tag.

[Unreleased]: https://github.com/Aryagorjipour/surface/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Aryagorjipour/surface/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Aryagorjipour/surface/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Aryagorjipour/surface/releases/tag/v0.1.0
