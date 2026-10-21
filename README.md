# Surface

Peer-to-peer terminal chat with a privacy-first Connector (rendezvous + signaling).

- Direct WebSocket between peers after Connector-mediated handshake
- One Surface can keep several direct chats and switch focus without dropping inactive sockets (`--max-chats`, `/focus`, `/chats`)
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

## Publish a release

**Latest release:** [v0.1.0](https://github.com/Aryagorjipour/surface/releases/tag/v0.1.0)

Push a tag that matches `vMAJOR.MINOR.PATCH`.
The tag has no leading zeros and no pre-release suffix.
The release workflow then builds `connector` and `surface`, writes `SHA256SUMS`, attests SLSA provenance, and uploads the GitHub release.
A second run for the same tag replaces the uploaded files.
This workflow does not sign the connector container image.

The binaries cover `linux_amd64`, `linux_arm64`, `darwin_amd64`, `darwin_arm64`, and `windows_amd64`.
Windows file names end in `.exe`.
A file name looks like `surface_v1.2.3_linux_amd64`.

## Verify a release

Download one binary and `SHA256SUMS` from the GitHub release for that tag.
In the directory that contains both files, check the digest and the signed provenance.

```sh
sha256sum -c SHA256SUMS
gh attestation verify surface_v1.2.3_linux_amd64 --repo Aryagorjipour/surface
```

`sha256sum -c` compares the file with `SHA256SUMS`.
`gh attestation verify` checks the SLSA provenance attestation from the release workflow.
Use the file name you downloaded.
The checksum file is GNU `sha256sum` format.
macOS and Windows may still warn before you open the program.
Their check is separate from the attestation.
