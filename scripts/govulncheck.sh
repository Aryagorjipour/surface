#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$root/scripts/release-lib.sh"
cd "$root"

toolchain=$(release_toolchain "$root/go.mod")
GOTOOLCHAIN="$toolchain" go run "$release_govulncheck_module" ./...
