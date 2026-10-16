# Surface

Peer-to-peer terminal chat with a privacy-first Connector (rendezvous + signaling).

- Direct WebSocket between peers after Connector-mediated handshake
- [Turso](https://github.com/tursodatabase/turso) (local + cloud), [Redis](https://github.com/redis/redis) cache, [Vault](https://github.com/hashicorp/vault) secrets, [OTEL](https://github.com/open-telemetry/opentelemetry-go)
- Open source ([MIT](LICENSE))
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [CHANGELOG.md](CHANGELOG.md)

## Local stack

Start the Connector, the cache, and the dev Vault from a fresh clone. The contributor does not edit a file. From `surface/deployments`:

```sh
docker compose up
```

When the Connector is listening, register from the host:

```sh
SURFACE_CONNECTOR_TOKEN=dev surface http://127.0.0.1:8080 alice 127.0.0.1:9001
```

The development token, the development encryption key, and the dev Vault root token are for this local stack only.
