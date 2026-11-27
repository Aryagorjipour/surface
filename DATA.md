# What v0.2.0 stores

This page lists stored data for v0.2.0. Run steps are in [deployments/README.md](deployments/README.md).

## Peer rows

The connector stores peers in the SQLite table `peers`.

`key`, `state`, `last_seen`, and `identity` are plaintext. `identity` is the base64 text of the peer X25519 public key. `address` is AES-GCM ciphertext in `address_enc`, with the nonce in `address_nonce`. The key is Vault `encryption_key`.

Prune deletes a row whose `last_seen` is strictly older than now minus the silence window. The silence window is three times the heartbeat. The default heartbeat is 30 seconds, so the window is 90 seconds.

The connector process can read every column. Anyone who can read the database file can read the plaintext columns. Reading `address` also needs `encryption_key`.

A caller with the connector token can look up a peer. The response includes `key`, `address`, `state`, and `identity`.

`turso_url` accepts a local libSQL `file:` URL or a hosted libSQL URL. A Turso cloud account is not required. The dev stack uses `file:/app/data/surface.db`.

## Redis peer cache

When Redis is on, the connector writes the key `peer:<key>`.

The value is JSON. The fields are `key`, `address`, `state`, `identity`, and `last_seen`. The address and the identity are plaintext.

The TTL is the silence window.

Redis is off when the address is empty or `redis:6379`. Any other address requires `redis_password`.

Anyone who can read that Redis can read the peer cache.

## Redis requests

The key `req:<id>` holds one request. The value is AES-GCM, using the same `encryption_key` as peer addresses. The first 12 bytes are the nonce. The plaintext JSON includes `request_id`, `from_key`, `from_address`, `from_identity`, `to_key`, `to_identity`, `status`, `created`, and `expires_at`.

The key `pending:<to_key>` is a set of request ids. Those ids are plaintext.

The TTL is the request lifetime. The default is 10 minutes.

## Process memory

`byID` keeps those same request fields for the request lifetime. `byToKey` keeps the ids for each target. A restart drops this memory. With Redis off, the memory is the only copy of a pending request.

The rate limiter also keeps request times in the process. The default allowance is 60 requests per source host in a 60 second window. The dev compose file sets `SURFACE_SOURCE_ALLOWANCE` to 240.

## Mailbox table

The mailbox stays off unless `SURFACE_MAILBOX` is a positive duration on the connector and on `surface`.

On the connector, a positive duration creates the SQLite table `mailbox` and mounts the mailbox routes. The columns are `id`, `slot`, `body`, and `expires_at`. A missing or empty duration leaves the mailbox off. The connector then drops the table and mounts nothing. A duration that is set and not positive is refused at startup.

`slot` is the SHA-256 of the recipient public key. `body` is the sealed blob. The table has no column for a peer name or a public key. The blob header still contains a SHA-256 fingerprint of the sender public key and an ephemeral X25519 public key. The sender name and the line are inside the AES-GCM ciphertext.

`Leave` stages one line. The line is at most 1024 bytes.

`expires_at` is the stage time plus the connector duration. The connector deletes the row when that time is reached.

A caller with the connector token can read the stored blob. Opening the line needs the recipient mailbox key.

## Mailbox files on the surface machine

When its mailbox duration is positive, `surface` writes `surface-mailbox/<my-key>/`.

That directory holds `identity`, `book`, `outbox`, and `inbox`. `book` stores peer names and public keys in the clear. `identity` is the mailbox private key, mode 0600. The connector does not receive this directory.

This mailbox key is separate from the chat identity below.

## Census

Census is off unless you set `--census` or a true `SURFACE_CENSUS` value. True values are `1`, `true`, and `t`, in any letter case. `0`, `false`, `f`, and a blank value stay off. Any other value is refused at startup.

`GET /operator/peers` and `GET /operator/pending` go through the bearer check. A missing or wrong token is rejected before the census setting is consulted. With a valid token, census off returns 404. With a valid token, census on returns a JSON count. The responses do not list peers.

## Chat limit file

`surface` keeps `surface-limits/config.json` in its working directory. The file mode is 0600. The directory mode is 0700.

The file stores `version`, `limit`, and `audit`. The audit keeps the latest 100 rows. The default limit is 4. A saved limit is a whole number from 1 to 32.

The connector does not read this file. The commands are in [CHANGELOG.md](CHANGELOG.md).

## Identity seed

If `SURFACE_IDENTITY_SEED` is set to a non-blank value, it must be base64 of 32 bytes. `surface` derives its X25519 chat identity from those bytes. It does not read `surface-identity/` in that case.

If the variable is unset or blank, `surface` uses `surface-identity/<my-key>`. A missing file is created with 32 random bytes, mode 0600. The directory mode is 0700. The key argument must be one path segment.

## Connector token

The connector reads Vault at `secret/data/surface/connector`.

The required keys are `turso_url`, `turso_token`, `encryption_key`, and `connector_token`. `encryption_key` is base64 of 32 bytes. The optional keys are `redis_addr` and `redis_password`.

`surface` requires `SURFACE_CONNECTOR_TOKEN`. It sends that value as `Authorization: Bearer`. Every route except `GET /health` requires a match. `GET /health` returns a body that starts with `ok` and does not check the token.

The token is shared. It is not a separate account per person. Anyone with the token can register and look up peers.

## Dev stack values

The dev compose file writes these values for the local stack only.

`connector_token` is `dev`. `turso_token` is `dev`. `turso_url` is `file:/app/data/surface.db`. `redis_addr` is `cache:6379`. `redis_password` is `dev-redis`. `encryption_key` is `ZGV2LXN1cmZhY2Uta2V5LWRldi1zdXJmYWNlLWtleSE=`, which decodes to 32 bytes. The Vault root token in that stack is `root`.

Do not use the dev token on a reachable connector.

## Logs and traces

`surface` and the connector write JSON slog to stdout. Logged fields include `service`, `addr`, `err`, `endpoint`, `op`, `class`, and `count`. Treat stdout as sensitive.

If `OTEL_STDOUT` is `1`, spans go to stdout and the OTLP exporter is not started. Otherwise traces stay off until `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The OTLP exporter uses an insecure client. Connector route spans record the route template and the HTTP status.

## Chat lines

The connector does not store chat plaintext.

`session.Seal` seals each line with AES-GCM. The message key comes from `chainStep`. The HKDF labels are `surface-chain-dialer-v1` and `surface-chain-listener-v1`.

## Relay socket

The relay is not a connector feature. `surface` dials a TCP address from `--relay` or `SURFACE_RELAY`.

`relayPath` writes a plaintext header, then length-prefixed messages. The header is the line `surface-relay-v1` and the request id. Each later message is a 4-byte length plus the bytes the session produced. A chat line uses the sealed frame from `session.Seal`. The handshake offer on that socket also carries the request id and the caller key in the clear. The relay can see the length of each write and the time of each write.

The attempt deadline defaults to 20 seconds, measured from the start of the attempt. The dialer shortens that deadline by the accept poll, which defaults to 1 second. The relay uses that same deadline. It stops sooner when a direct handshake replaces it.
