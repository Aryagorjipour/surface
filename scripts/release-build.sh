#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$root/scripts/release-lib.sh"

if [ "$#" -ne 2 ]; then
  printf 'usage: release-build.sh VERSION OUTDIR\n' >&2
  exit 2
fi

version=$1
outdir=$2

if ! release_version_ok "$version"; then
  printf 'release-build: version must look like v1.2.3\n' >&2
  exit 1
fi

if [ -z "$outdir" ]; then
  printf 'release-build: output directory is required\n' >&2
  exit 1
fi

resolved=$(realpath -m -- "$outdir")
root_resolved=$(realpath -m -- "$root")
case "$resolved" in
  / | "$root_resolved" | "$root_resolved/.git" | "$root_resolved/.git"/*)
    printf 'release-build: refusing output directory %s\n' "$outdir" >&2
    exit 1
    ;;
esac

parent=$(dirname -- "$resolved")
mkdir -p -- "$parent"
stage=$(mktemp -d "${parent}/release-build.XXXXXX")
sums=
cleanup() {
  if [ -n "$sums" ] && [ -f "$sums" ]; then
    rm -f -- "$sums"
  fi
  if [ -d "$stage" ]; then
    rm -rf -- "$stage"
  fi
}
trap cleanup EXIT

toolchain=$(release_toolchain "$root/go.mod")

for entry in "${release_binaries[@]}"; do
  bin=${entry%% *}
  pkg=${entry#* }
  for plat in "${release_platforms[@]}"; do
    os=${plat%% *}
    arch=${plat#* }
    name=$(release_artifact_name "$bin" "$version" "$os" "$arch")
    env -u GOFLAGS -u GOEXPERIMENT -u GOAMD64 -u GOARM64 -u GO386 -u GOPPC64 \
      GOTOOLCHAIN="$toolchain" \
      CGO_ENABLED=0 \
      GOOS="$os" \
      GOARCH="$arch" \
      go -C "$root" build -trimpath -buildvcs=false -ldflags='-buildid= -s -w' -o "$stage/$name" "$pkg"
  done
done

sums=$(mktemp)
(
  cd "$stage"
  sha256sum -- *
) | LC_ALL=C sort -k2 > "$sums"
mv -- "$sums" "$stage/SHA256SUMS"

trap - EXIT
rm -rf -- "$resolved"
mv -- "$stage" "$resolved"
