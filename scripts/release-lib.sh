#!/usr/bin/env bash

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  printf 'release-lib.sh is sourced by the release scripts\n' >&2
  exit 1
fi

release_govulncheck_module=golang.org/x/vuln/cmd/govulncheck@v1.9.0

release_binaries=(
  "connector ./cmd/connector"
  "surface ./cmd/surface"
)

release_platforms=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
  "windows amd64"
)

release_version_ok() {
  [[ "${1-}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

release_toolchain() {
  local gomod=$1
  local v
  v=$(awk '/^go [0-9]/ { print $2; exit }' "$gomod")
  if ! [[ "$v" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'release: go.mod go line must be a full version\n' >&2
    return 1
  fi
  printf 'go%s\n' "$v"
}

release_artifact_name() {
  local bin=$1
  local version=$2
  local os=$3
  local arch=$4
  local name="${bin}_${version}_${os}_${arch}"
  if [ "$os" = windows ]; then
    name="${name}.exe"
  fi
  printf '%s\n' "$name"
}
