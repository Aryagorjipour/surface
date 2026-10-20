#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build=$root/scripts/release-build.sh
publish=$root/scripts/release-publish.sh

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

assert_format() {
  local path=$1
  local name kind
  name=$(basename -- "$path")
  kind=$(file -b -- "$path")
  case "$name" in
    *_linux_amd64)
      [[ "$kind" == *"ELF"* && "$kind" == *"x86-64"* ]] || die "$name is $kind"
      ;;
    *_linux_arm64)
      [[ "$kind" == *"ELF"* && "$kind" == *"aarch64"* ]] || die "$name is $kind"
      ;;
    *_darwin_amd64)
      [[ "$kind" == *"Mach-O"* && "$kind" == *"x86_64"* ]] || die "$name is $kind"
      ;;
    *_darwin_arm64)
      [[ "$kind" == *"Mach-O"* && "$kind" == *"arm64"* ]] || die "$name is $kind"
      ;;
    *_windows_amd64.exe)
      [[ "$kind" == *"PE32+"* ]] || die "$name is $kind"
      ;;
    *)
      die "no format check for $name"
      ;;
  esac
}

expect_reject_build() {
  local version=$1
  local dest
  dest=$(mktemp -d)
  rmdir -- "$dest"
  if bash "$build" "$version" "$dest"; then
    die "accepted version $version"
  fi
  if [ -e "$dest" ]; then
    die "version $version created $dest"
  fi
}

release_build_contract() {
  local work a b c name listed
  work=$(mktemp -d)
  a=$work/a
  b=$work/b
  c=$work/c

  (cd /tmp && bash "$build" v0.0.0 "$a")
  (cd /tmp && bash "$build" v0.0.0 "$b")
  diff -rq "$a" "$b"

  GOFLAGS='-toolexec=false' GOTOOLCHAIN=go1.0.0 bash "$build" v0.0.0 "$c"
  diff -rq "$a" "$c"
  bash "$build" v0.0.0 "$a"
  diff -rq "$a" "$b"

  if find "$work" -maxdepth 1 -name 'release-build.*' | grep -q .; then
    die 'stage directory left behind'
  fi

  local expected=$work/expected
  cat > "$expected" <<'EOF'
SHA256SUMS
connector_v0.0.0_darwin_amd64
connector_v0.0.0_darwin_arm64
connector_v0.0.0_linux_amd64
connector_v0.0.0_linux_arm64
connector_v0.0.0_windows_amd64.exe
surface_v0.0.0_darwin_amd64
surface_v0.0.0_darwin_arm64
surface_v0.0.0_linux_amd64
surface_v0.0.0_linux_arm64
surface_v0.0.0_windows_amd64.exe
EOF
  (cd "$a" && find . -type f -printf '%f\n' | LC_ALL=C sort > "$work/got-names")
  diff -u "$expected" "$work/got-names"

  listed=$work/listed
  awk '{ print $2 }' "$a/SHA256SUMS" > "$listed"
  grep -v '^SHA256SUMS$' "$expected" > "$work/bins"
  diff -u "$work/bins" "$listed"
  grep -E '^[0-9a-f]{64}  [^[:space:]]+$' "$a/SHA256SUMS" > "$work/sum-lines"
  diff -q "$a/SHA256SUMS" "$work/sum-lines" >/dev/null
  LC_ALL=C sort -c -k2 "$a/SHA256SUMS"
  (cd "$a" && sha256sum -c SHA256SUMS)

  while IFS= read -r name; do
    [ "$name" = SHA256SUMS ] && continue
    assert_format "$a/$name"
  done < "$work/got-names"

  expect_reject_build v1.2
  expect_reject_build 1.2.3
  expect_reject_build v1.2.3-rc1
  expect_reject_build v01.2.3
  expect_reject_build ''

  if bash "$build"; then
    die 'missing arguments were accepted'
  fi
  if bash "$build" v0.0.0 "$root"; then
    die 'repository root was accepted as the output directory'
  fi
  if bash "$build" v0.0.0 /; then
    die 'refused path / was accepted'
  fi

  rm -rf -- "$work"
}

release_publish_contract() {
  local stub log dir
  stub=$(mktemp -d)
  log=$stub/log
  cat > "$stub/gh" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${GH_LOG:?}"
if [ "$1" = release ] && [ "$2" = view ]; then
  if [ "${GH_RELEASE_EXISTS:-0}" = 1 ]; then
    exit 0
  fi
  exit 1
fi
exit 0
EOF
  chmod +x "$stub/gh"

  dir=$(mktemp -d)
  printf 'binary\n' > "$dir/connector_v1.2.3_linux_amd64"
  printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  connector_v1.2.3_linux_amd64\n' > "$dir/SHA256SUMS"

  GH_LOG=$log PATH="$stub:$PATH" GITHUB_REPOSITORY=Aryagorjipour/surface \
    bash "$publish" v1.2.3 "$dir"
  grep -q -- '--verify-tag' "$log" || die 'first publish skipped --verify-tag'
  grep -q 'release create v1.2.3' "$log" || die 'first publish did not create the release'
  grep -q 'connector_v1.2.3_linux_amd64' "$log" || die 'first publish omitted the binary'
  grep -q 'SHA256SUMS' "$log" || die 'first publish omitted SHA256SUMS'

  : > "$log"
  GH_LOG=$log GH_RELEASE_EXISTS=1 PATH="$stub:$PATH" \
    bash "$publish" v1.2.3 "$dir"
  grep -q 'release upload v1.2.3' "$log" || die 'republish did not upload'
  grep -q -- '--clobber' "$log" || die 'republish did not replace assets'
  if grep -q 'release create' "$log"; then
    die 'republish created a second release'
  fi

  rm -f -- "$dir/SHA256SUMS"
  : > "$log"
  if GH_LOG=$log PATH="$stub:$PATH" bash "$publish" v1.2.3 "$dir"; then
    die 'missing SHA256SUMS was published'
  fi
  if [ -s "$log" ]; then
    die 'gh ran without SHA256SUMS'
  fi

  if PATH="$stub:$PATH" GH_LOG=$log bash "$publish" v1.2 "$dir"; then
    die 'bad tag was published'
  fi

  rm -rf -- "$stub" "$dir"
}

release_build_contract
release_publish_contract
printf 'release contract ok\n'
