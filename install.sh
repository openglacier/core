#!/usr/bin/env bash

set -euo pipefail

REPO="openglacier/core"
INSTALL_DIR="/usr/local/bin"

if [[ "$(id -u)" -ne 0 ]]; then
    SUDO="sudo"
else
    SUDO=""
fi

case "$(uname -s)" in
    Linux)
        case "$(uname -m)" in
            x86_64)
                if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
                    TARGET="x86_64-unknown-linux-musl"
                elif grep -qE '(^|[[:space:]])(avx2|bmi1|bmi2|f16c|fma|lzcnt|movbe)([[:space:]]|$)' /proc/cpuinfo; then
                    TARGET="x86_64v3-unknown-linux-gnu"
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

API_URL="https://api.github.com/repos/${REPO}/releases/latest"

RELEASE_JSON="$(curl -fsSL "$API_URL")"

ASSET_URLS="$(
    printf '%s' "$RELEASE_JSON" |
        grep -oE '"browser_download_url":[[:space:]]*"[^"]+"' |
        sed -E 's/^"browser_download_url":[[:space:]]*"//; s/"$//' |
        grep "$TARGET" || true
)"

if [[ -z "$ASSET_URLS" ]]; then
    echo "No release asset found for target: $TARGET" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

while IFS= read -r URL; do
    [[ -z "$URL" ]] && continue

    FILE="$TMP_DIR/$(basename "$URL")"

    curl -fsSL "$URL" -o "$FILE"

    case "$FILE" in
        *.tar.gz|*.tgz)
            tar -xzf "$FILE" -C "$TMP_DIR"
            ;;
        *.tar)
            tar -xf "$FILE" -C "$TMP_DIR"
            ;;
        *.zip)
            unzip -q "$FILE" -d "$TMP_DIR"
            ;;
        *)
            chmod +x "$FILE"
            ;;
    esac
done <<< "$ASSET_URLS"

OGD="$(find "$TMP_DIR" -type f -name ogd -perm -u+x | head -n1)"
OGCLI="$(find "$TMP_DIR" -type f -name ogcli -perm -u+x | head -n1)"

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