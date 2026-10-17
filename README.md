# Surface

Peer-to-peer terminal chat with a privacy-first Connector (rendezvous + signaling).

- Direct WebSocket between peers after Connector-mediated handshake
- [Turso](https://github.com/tursodatabase/turso) (local + cloud), [Redis](https://github.com/redis/redis) cache, [Vault](https://github.com/hashicorp/vault) secrets, [OTEL](https://github.com/open-telemetry/opentelemetry-go)
- Open source ([MIT](LICENSE))
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [CHANGELOG.md](CHANGELOG.md)

## Run

From the repository root, with [just](https://github.com/casey/just) 1.58.0 or newer:

```sh
just help
```

`just` prints the same guide. It is the only way to start the dev Connector, a dev Surface, or the stage and production Connector. The dev stack allows 240 requests per source per minute so two Surfaces on one machine can connect. An interactive terminal opens a keyboard UI. A pipe keeps the line commands. The deployment steps are in [deployments/README.md](deployments/README.md).
