#!/bin/sh
# Installs the zenoh-web binary from a GitHub release.
#   curl -fsSL https://raw.githubusercontent.com/jeff-hykin/zenoh-web/main/install.sh | sh
# env: ZENOH_WEB_VERSION (e.g. v0.1.0; default: latest release), ZENOH_WEB_INSTALL_DIR (default: ~/.local/bin)
set -eu

repo="jeff-hykin/zenoh-web"
install_dir="${ZENOH_WEB_INSTALL_DIR:-$HOME/.local/bin}"

fail() {
    echo "zenoh-web install: $*" >&2
    exit 1
}

case "$(uname -s)" in
    Linux) os="unknown-linux-gnu" ;;
    Darwin) os="apple-darwin" ;;
    *) fail "unsupported OS $(uname -s) (supported: Linux, macOS)" ;;
esac
case "$(uname -m)" in
    x86_64 | amd64) arch="x86_64" ;;
    aarch64 | arm64) arch="aarch64" ;;
    *) fail "unsupported CPU $(uname -m) (supported: x86_64, aarch64)" ;;
esac
target="$arch-$os"

if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1" -o "$2"; }
    fetch_stdout() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO "$2" "$1"; }
    fetch_stdout() { wget -qO- "$1"; }
else
    fail "needs curl or wget"
fi

version="${ZENOH_WEB_VERSION:-}"
if [ -z "$version" ]; then
    version="$(fetch_stdout "https://api.github.com/repos/$repo/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)"
    [ -n "$version" ] || fail "could not find the latest release of $repo"
fi
case "$version" in
    v*) ;;
    *) version="v$version" ;;
esac

archive="zenoh-web-${version#v}-$target.tar.gz"
base_url="https://github.com/$repo/releases/download/$version"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT INT TERM

echo "downloading $archive ($version)"
fetch "$base_url/$archive" "$work_dir/$archive" || fail "no $archive in release $version"
fetch "$base_url/SHA256SUMS" "$work_dir/SHA256SUMS" || fail "no SHA256SUMS in release $version"

expected="$(awk -v name="$archive" '$2 == name || $2 == "*" name { print $1 }' "$work_dir/SHA256SUMS")"
[ -n "$expected" ] || fail "$archive is not listed in SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$work_dir/$archive" | awk '{ print $1 }')"
elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$work_dir/$archive" | awk '{ print $1 }')"
else
    fail "needs sha256sum or shasum to verify the download"
fi
[ "$expected" = "$actual" ] || fail "checksum mismatch for $archive (expected $expected, got $actual)"

tar -xzf "$work_dir/$archive" -C "$work_dir"
binary="$(find "$work_dir" -type f -name zenoh-web | head -n 1)"
[ -n "$binary" ] || fail "no zenoh-web binary inside $archive"
mkdir -p "$install_dir"
# install to a temp name then rename, so replacing a running binary is safe
cp "$binary" "$install_dir/.zenoh-web.tmp"
chmod 755 "$install_dir/.zenoh-web.tmp"
mv -f "$install_dir/.zenoh-web.tmp" "$install_dir/zenoh-web"
echo "installed $install_dir/zenoh-web ($version, $target)"

case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) echo "note: $install_dir is not on your PATH; add it, e.g.: export PATH=\"$install_dir:\$PATH\"" ;;
esac
