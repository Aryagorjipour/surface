# Deployment

Start Surface with `just`, from the repository root. `just help` is the
guide. `just` prints the same text. This page is the same set of steps.

Install [just](https://github.com/casey/just) 1.58.0 or a newer 1.x.
Docker is required for every server recipe. Go is required for the dev
client. The client recipe uses `go run` and does not build a `surface`
binary.

## Dev server

```sh
just server-dev
```

Working directory for the stack is `deployments/`. The recipe runs
`docker compose up` and reads `deployments/docker-compose.yml` only.
That starts the Connector, the cache, and the dev Vault, then loads the
local-stack secrets. The Connector listens on TCP port 8080. The
process listens for punch on UDP port 8081. Compose publishes
`8081:8081` as TCP, so that UDP port is not published to the host.
`GET /health` answers `ok` when it is up, and the compose
healthcheck runs `connector --ready`.

The dev token, the dev encryption key, and the dev Vault root token are
for this local stack only. Do not use them for stage or production.

This stack sets the source allowance to 240. Two Surfaces on one machine
share one source. Their pending polls, heartbeats, lookup, connect, and
accept fit in 240. The Connector default outside this stack stays 60.
On that default, those two Surfaces receive `too many requests`. Raise
`SURFACE_SOURCE_ALLOWANCE` for an environment where peers share a source.

Stop the stack with Ctrl+C.

## Dev client

In another terminal, after the Connector is listening:

```sh
just surface alice
just surface bob 127.0.0.1:9002
```

`just surface` runs `go run ./cmd/surface` against
`http://127.0.0.1:8080`. The peer key is required. The listen address
defaults to `127.0.0.1:9001`. The recipe passes the documented
local-stack connector token. It does not build or run a `surface`
binary. There is no stage client and no production client.

On the caller: `lookup <key>`, then `connect <key>`. On the other side:
`accept` and the printed id. `/quit` leaves the chat. `quit` exits.

When stdin is a terminal, Surface opens a keyboard UI instead of the
line prompt. The left pane lists peers learned from lookup and from
incoming requests. The right pane is the scrollback. Tab moves among
the peer list, the scrollback, and the compose line. In the peer list,
`j` and `k` move, Enter connects, `a` accepts the selected request, and
`r` rejects it. `l` focuses the compose line, which still takes the
same commands. In chat, Enter sends the line and `/quit` leaves. `q`
quits from the peer list. A pipe keeps the line commands.

## Stage and production

```sh
just server-stage
just server-production
```

Both recipes build the image from `deployments/Dockerfile.connector`
and run that image. They publish TCP 8080 and TCP 8081. The process
listens for punch on UDP `:8081`. This recipe does not publish that
UDP port. They do not start Vault dev mode. They do not use the
local-stack token, the dev encryption key, or the dev Vault root token.
They do not create a `staging` or `production` git branch.

Each recipe reads `VAULT_ADDR` and `VAULT_TOKEN` from the environment.
The address must be reachable from inside the container. The token is
the token for that environment's Vault. The Connector still reads
`secret/data/surface/connector` at startup.

If either variable is missing, empty, or whitespace, the recipe exits
non-zero before a container starts. The message names the variable and
does not print the value.

```sh
export VAULT_ADDR=https://vault.example
export VAULT_TOKEN=...
just server-stage
```

Do not put the token in the command line. The recipe forwards the two
variables into the container without printing them.

`server-dev` already binds 8080 and 8081. Starting stage or production
while that stack is up fails on the port and leaves the dev stack
running. Stop `server-dev` first.

Ctrl+C stops the container. The recipe removes it on exit.

## Run a connector for other people

Use this when people on other machines will use the connector this
machine starts.

Put the connector secrets in Vault at `secret/data/surface/connector`
before you start.

Set these required keys:

- `turso_url`
- `turso_token`
- `encryption_key` as base64 of 32 bytes
- `connector_token`

`redis_addr` and `redis_password` are optional. Leave `redis_addr`
empty, or set it to `redis:6379`, to leave Redis off. Any other
address requires `redis_password`.

Do not copy the local-stack token, encryption key, or Vault root
token. [DATA.md](../DATA.md) lists those values.

Set `VAULT_ADDR` and `VAULT_TOKEN` as the stage and production section
describes. Then run `just server-production`. `just server-stage` uses
the same Dockerfile, ports, and flags.

The recipe builds `deployments/Dockerfile.connector`. The build stage
uses `golang:1.27.4-alpine`. The recipe requires `VAULT_ADDR` and
`VAULT_TOKEN`. It sets `SURFACE_PUNCH_ADDR=:8081`. It publishes 8080
and 8081 with
`-p 8080:8080 -p 8081:8081`.

Those flags publish TCP. Punch listens on UDP. This recipe does not
publish punch to other machines. Two people who cannot dial each
other's listen address need a relay. Each `surface` sets `--relay` or
`SURFACE_RELAY`. This recipe does not run a relay.

The recipe does not mount TLS files. It does not create a DNS name.
It does not pass `--tls-cert` or `--tls-key`. When both flags are
unset, the connector speaks cleartext.

Do not put a cleartext connector on a public address. The network can
read peer addresses, peer keys, and the connector token. Use this
recipe on a private network.

Tell guests to use `http` on port 8080. This recipe does not enable
TLS. Give them that URL and the `connector_token` value. `GET /health`
on that URL returns a body that starts with `ok`. That route does not
use the token.

Tell each guest to pass a listen address the other peer can dial.
`127.0.0.1` works only for two clients on one machine. The guest
command is in [Point surface at a connector](#point-surface-at-a-connector).

`connector --ready` probes `http://127.0.0.1:8080/health` on the
connector machine. It uses `https` when a TLS certificate is set.
This recipe does not set a certificate.

## Point surface at a connector

Use this when an operator gave you a connector URL and a token.

Download the v0.2.0 `surface` binary for your machine. Check it with
[Verify a release](../README.md#verify-a-release).

Do not use `just surface` for that connector. `just surface` is a dev
client. It runs `go run ./cmd/surface` against `http://127.0.0.1:8080`
and defaults the token to `dev`.

```sh
export SURFACE_CONNECTOR_TOKEN=<token from the operator>
./surface_v0.2.0_linux_amd64 <connector-url> <my-key> <listen-addr>
```

Use the file name you downloaded. Windows names end in `.exe`.

The listen address is the address the other peer dials. Use
`127.0.0.1:9001` when both clients are on one machine. That is the
default when you omit the listen address. A person on another machine
needs an address that machine can reach.

`SURFACE_CONNECTOR_TOKEN` is required.

After the program starts, use the commands in the dev client section.
A terminal opens the keyboard UI. A pipe keeps the line commands.

Direct sockets stay cleartext unless you pass `--direct-cert` and
`--direct-key`. The just recipes do not pass those flags.

## When these pages change

Update `justfile`, the `just help` text, the README run section, and
this file in the same change when any of these change:

- Connector or Surface invocation, including flags, environment names,
  config-file keys, defaults, the listen address, `GET /health`, or
  `connector --ready`
- Compose services, images, published ports, the bootstrap secret path,
  or the local-stack credential
- Which environment may use Vault dev mode, the dev token, or
  `redis:6379` as the disabled cache address
- Punch or relay settings a person must set in order to run
- The set of just recipes, their names, or their arguments

A change that does none of those leaves this file and the justfile
untouched.
