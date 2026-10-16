# Changelog

## [Unreleased]

### Added

- Connector spans record the route endpoint and the HTTP status.
- A blocked direct chat prints `traversal failed`.
- `surface` accepts `--relay` and `--punch`. The connector accepts `--punch`.
- Connector `GET /health`, probed by `connector --ready`.
- The local compose file bootstraps its dev secrets before the connector starts.
- Terminal chat between two peers.
- A connector that only introduces the peers. It does not carry the chat.
- After both peers accept, they talk directly.
- A local run of the connector is published under `deployments/`, with a
  cache and a secret store beside it. The chat program itself is not part
  of that local stack.
