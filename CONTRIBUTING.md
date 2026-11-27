# Contributing

Start from the current `main` line. Do the work on a short-lived branch
taken from that line. Offer the work back as a pull request.

Do not push directly to `main`. Do not force-push `main`.

The pull request says what a person can observe. Link a public issue when
one exists.

Published checks must pass before acceptance. Those checks are vet, test,
both program builds (`cmd/connector` and `cmd/surface`), the reproducible
release build, govulncheck, and lint.

## Local checks

Run these from the repository root. CI uses Go 1.27.4 from `go.mod`.

```sh
go vet ./...
go test -race -count=1 -timeout 5m ./...
go build -o bin/connector ./cmd/connector
go build -o bin/surface ./cmd/surface
bash scripts/release_test.sh
bash scripts/govulncheck.sh
golangci-lint run --timeout 5m
```

Use golangci-lint v2.14.0 for the last command. CI pins that version.

`just` 1.58.0 or newer and Docker are for the run book. See [deployments/README.md](deployments/README.md). The commands in this section do not need them.

A change that adds behavior includes a check that fails before the change
and passes after it.

With one maintainer, that person may accept their own pull request after
the checks pass. When a second maintainer exists, that person must approve.

Delete the short-lived branch after acceptance. `main` stays releasable.

Conduct reports are described in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
Flaw reports are described in [SECURITY.md](SECURITY.md).

Do not add assistant-instruction files to this repository.
