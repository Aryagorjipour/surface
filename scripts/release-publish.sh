#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$root/scripts/release-lib.sh"

tag=${1:-${GITHUB_REF_NAME-}}
dir=${2:-dist}
repo=${GITHUB_REPOSITORY:-Aryagorjipour/surface}

if ! release_version_ok "$tag"; then
  printf 'release-publish: tag must look like v1.2.3\n' >&2
  exit 1
fi

if [ ! -f "$dir/SHA256SUMS" ]; then
  printf 'release-publish: %s is missing\n' "$dir/SHA256SUMS" >&2
  exit 1
fi

shopt -s nullglob
files=("$dir"/*)
if [ "${#files[@]}" -eq 0 ]; then
  printf 'release-publish: %s is empty\n' "$dir" >&2
  exit 1
fi

notes=$(mktemp)
trap 'rm -f -- "$notes"' EXIT
cat > "$notes" <<EOF
Binaries for connector and surface.
SHA256SUMS lists each binary and its SHA-256 digest.
Each file on this release has a signed SLSA provenance attestation.

To verify a download, run these commands in the directory that contains the file and SHA256SUMS.

    sha256sum -c SHA256SUMS
    gh attestation verify FILE --repo ${repo}
EOF

if gh release view "$tag" >/dev/null 2>&1; then
  gh release upload "$tag" "${files[@]}" --clobber
else
  gh release create "$tag" "${files[@]}" --verify-tag --title "$tag" --notes-file "$notes"
fi
