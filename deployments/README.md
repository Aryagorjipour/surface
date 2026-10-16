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
local-stack secrets. The Connector listens on port 8080. Punch listens
on port 8081. `GET /health` answers `ok` when it is up, and the compose
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

## Stage and production

```sh
just server-stage
just server-production
```

Both recipes build the image from `deployments/Dockerfile.connector`
and run that image. They publish 8080 and 8081. Punch listens on
`:8081`. They do not start Vault dev mode. They do not use the
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
