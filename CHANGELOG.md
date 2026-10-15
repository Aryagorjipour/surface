# Changelog

## [Unreleased]

### Added

- Terminal chat between two peers.
- A connector that only introduces the peers. It does not carry the chat.
- After both peers accept, they talk directly.
- A local run of the connector is published under `deployments/`, with a
  cache and a secret store beside it. The chat program itself is not part
  of that local stack.
