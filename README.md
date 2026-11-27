# Surface

Surface is terminal chat for two peers. Each person runs `surface`. A connector introduces the peers. The connector does not carry the chat.

In v0.2.0, the peers try a direct socket, then punch, then an optional relay. The relay is a TCP address that `surface` dials with `--relay` or `SURFACE_RELAY`. It carries that chat only until the direct handshake succeeds, and no longer than 20 seconds.

## Status

The release is [v0.2.0](https://github.com/Aryagorjipour/surface/releases/tag/v0.2.0), The Borrowed Relay, published on 2026-11-27.

Two people can chat in a terminal after the connector introduces them. One `surface` can keep several direct chats open. The limit commands are in [CHANGELOG.md](CHANGELOG.md). Punch and the relay are optional. The mailbox stays off unless `SURFACE_MAILBOX` is a positive duration on the connector and on `surface`.

Every caller uses one shared token. The connector does not keep user accounts. This repository does not publish a connector address. You get the URL and the token from the operator.

The connector speaks cleartext unless `--tls-cert` and `--tls-key` are both set. A direct socket uses `ws` unless you set `--direct-cert` and `--direct-key`. With both set, the socket uses `wss`. Do not put a cleartext connector on a public address. The network can read peer addresses, peer keys, and the connector token.

The relay is temporary and optional. Stored fields are in [DATA.md](DATA.md). The run book is [deployments/README.md](deployments/README.md). The release notes are in [CHANGELOG.md](CHANGELOG.md). The license is [MIT](LICENSE).

## Run

The dev stack, stage, and production steps are in [deployments/README.md](deployments/README.md). `just help` prints the same guide.

## Use a connector

The operator gives you the connector URL and the token. Run a v0.2.0 release binary. The steps are in [deployments/README.md](deployments/README.md).

```sh
export SURFACE_CONNECTOR_TOKEN=<token>
./surface_v0.2.0_linux_amd64 <connector-url> <my-key> <listen-addr>
```

The listen address is the address the other peer dials. The default is `127.0.0.1:9001`. Use that when both clients are on one machine. A person on another machine needs an address that machine can reach.

`just surface` is a dev client only. It talks to the connector on this machine and uses the local-stack token. There is no stage client and no production client. The command is in [deployments/README.md](deployments/README.md).

## Publish a release

The latest release is [v0.2.0](https://github.com/Aryagorjipour/surface/releases/tag/v0.2.0).

Push a tag that matches `vMAJOR.MINOR.PATCH`.
The tag has no leading zeros and no pre-release suffix.
The release workflow then builds `connector` and `surface`, writes `SHA256SUMS`, attests SLSA provenance, and uploads the GitHub release.
A second run for the same tag replaces the uploaded files.
The workflow pushes `ghcr.io/aryagorjipour/surface-connector` with the git tag and with `latest`.
For this release the image is `ghcr.io/aryagorjipour/surface-connector:v0.2.0`.
This workflow does not sign the connector container image.

The binaries cover `linux_amd64`, `linux_arm64`, `darwin_amd64`, `darwin_arm64`, and `windows_amd64`.
Windows file names end in `.exe`.
A file name looks like `surface_v0.2.0_linux_amd64`.

## Verify a release

Download one binary and `SHA256SUMS` from the GitHub release for that tag.
In the directory that contains both files, check the digest and the signed provenance.

```sh
sha256sum -c SHA256SUMS
gh attestation verify surface_v0.2.0_linux_amd64 --repo Aryagorjipour/surface
```

`sha256sum -c` compares the file with `SHA256SUMS`.
`gh attestation verify` checks the SLSA provenance attestation from the release workflow.
Use the file name you downloaded.
The checksum file is GNU `sha256sum` format.
macOS and Windows may still warn before you open the program.
Their check is separate from the attestation.

## Other pages

- [Run a connector](deployments/README.md)
- [What v0.2.0 stores](DATA.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Security](SECURITY.md)
- [MIT license](LICENSE)
