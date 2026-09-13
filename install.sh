#!/usr/bin/env bash

set -euo pipefail

REPO="openglacier/core"
INSTALL_DIR="/usr/local/bin"
TARGET="${CORE_TARGET:-}"

if [[ "$(id -u)" -ne 0 ]]; then
    SUDO="sudo"
else
    SUDO=""
fi

if [[ -z "$TARGET" ]]; then
    case "$(uname -s)" in
        Linux)
            case "$(uname -m)" in
                x86_64)
                    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
                        TARGET="x86_64-unknown-linux-musl"
                    else
                        TARGET="x86_64-unknown-linux-gnu"
                    fi
                    ;;
                aarch64)
                    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
                        TARGET="aarch64-unknown-linux-musl"
                    else
                        TARGET="aarch64-unknown-linux-gnu"
                    fi
                    ;;
                armv7l)
                    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
                        TARGET="armv7-unknown-linux-musleabihf"
                    else
                        TARGET="armv7-unknown-linux-gnueabihf"
                    fi
                    ;;
                armv6l|armv5tel)
                    TARGET="arm-unknown-linux-gnueabihf"
                    ;;
                i686|i386)
                    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
                        TARGET="i686-unknown-linux-musl"
                    else
                        TARGET="i686-unknown-linux-gnu"
                    fi
                    ;;
                riscv64)
                    TARGET="riscv64gc-unknown-linux-gnu"
                    ;;
                ppc64le)
                    TARGET="powerpc64le-unknown-linux-gnu"
                    ;;
                s390x)
                    TARGET="s390x-unknown-linux-gnu"
                    ;;
                loongarch64)
                    TARGET="loongarch64-unknown-linux-gnu"
                    ;;
                *)
                    echo "Unsupported architecture: $(uname -m)" >&2
                    exit 1
                    ;;
            esac
            ;;
        Darwin)
            case "$(uname -m)" in
                x86_64)
                    TARGET="x86_64-apple-darwin"
                    ;;
                arm64)
                    TARGET="aarch64-apple-darwin"
                    ;;
                *)
                    echo "Unsupported architecture: $(uname -m)" >&2
                    exit 1
                    ;;
            esac
            ;;
        *)
            echo "Unsupported operating system: $(uname -s)" >&2
            exit 1
            ;;
    esac
fi

case "$TARGET" in
    x86_64-unknown-linux-gnu)
        ASSET_TARGET="linux-x86_64-gnu"
        ;;
    x86_64v3-unknown-linux-gnu)
        ASSET_TARGET="linux-x86_64-v3-gnu"
        ;;
    x86_64-unknown-linux-musl)
        ASSET_TARGET="linux-x86_64-musl"
        ;;
    aarch64-unknown-linux-gnu)
        ASSET_TARGET="linux-aarch64-gnu"
        ;;
    aarch64-unknown-linux-musl)
        ASSET_TARGET="linux-aarch64-musl"
        ;;
    armv7-unknown-linux-gnueabihf)
        ASSET_TARGET="linux-armv7-gnu"
        ;;
    armv7-unknown-linux-musleabihf)
        ASSET_TARGET="linux-armv7-musl"
        ;;
    arm-unknown-linux-gnueabihf)
        ASSET_TARGET="linux-armv6-gnu"
        ;;
    i686-unknown-linux-gnu)
        ASSET_TARGET="linux-i686-gnu"
        ;;
    i686-unknown-linux-musl)
        ASSET_TARGET="linux-i686-musl"
        ;;
    riscv64gc-unknown-linux-gnu)
        ASSET_TARGET="linux-riscv64-gnu"
        ;;
    powerpc64le-unknown-linux-gnu)
        ASSET_TARGET="linux-ppc64le-gnu"
        ;;
    s390x-unknown-linux-gnu)
        ASSET_TARGET="linux-s390x-gnu"
        ;;
    loongarch64-unknown-linux-gnu)
        ASSET_TARGET="linux-loongarch64-gnu"
        ;;
    x86_64-apple-darwin)
        ASSET_TARGET="macos-x86_64"
        ;;
    aarch64-apple-darwin)
        ASSET_TARGET="macos-aarch64"
        ;;
    *)
        echo "Unsupported target: $TARGET" >&2
        exit 1
        ;;
esac

API_URL="https://api.github.com/repos/${REPO}/releases/latest"
RELEASE_JSON="$(curl -fsSL "$API_URL")"

ASSET_URL="$(
    printf '%s' "$RELEASE_JSON" |
        grep -oE '"browser_download_url":[[:space:]]*"[^"]+"' |
        sed -E 's/^"browser_download_url":[[:space:]]*"//; s/"$//' |
        grep -- "-${ASSET_TARGET}\.tar\.gz$" |
        head -n1 || true
)"

if [[ -z "$ASSET_URL" ]]; then
    echo "No release asset found for target: $TARGET" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

FILE="$TMP_DIR/$(basename "$ASSET_URL")"

curl -fsSL "$ASSET_URL" -o "$FILE"

tar -xzf "$FILE" -C "$TMP_DIR"

OGD="$(find "$TMP_DIR" -type f -name ogd | head -n1)"
OGCLI="$(find "$TMP_DIR" -type f -name ogcli | head -n1)"

if [[ -z "$OGD" ]]; then
    echo "ogd binary not found in release" >&2
    exit 1
fi

if [[ -z "$OGCLI" ]]; then
    echo "ogcli binary not found in release" >&2
    exit 1
fi

$SUDO install -m 0755 "$OGD" "$INSTALL_DIR/ogd"
$SUDO install -m 0755 "$OGCLI" "$INSTALL_DIR/ogcli"

echo "Installed ogd and ogcli for $TARGET"