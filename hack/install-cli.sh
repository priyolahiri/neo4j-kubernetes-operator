#!/usr/bin/env sh
#
# Install the kubectl-neo4j plugin.
#
#   curl -sSL https://raw.githubusercontent.com/priyolahiri/neo4j-kubernetes-operator/main/hack/install-cli.sh | sh
#
# Prefer downloading and reading this script before running it — piping any
# installer straight into a shell means trusting the network round-trip.
#
# Environment:
#   VERSION      release to install, without the leading "v" (default: latest)
#   INSTALL_DIR  destination directory (default: /usr/local/bin)
#
# Checksum verification is MANDATORY, not best-effort: the script aborts if it
# cannot verify, rather than installing an unverified binary. An installer that
# silently degrades to "no integrity check" is worse than one that fails.
set -eu

REPO="priyolahiri/neo4j-kubernetes-operator"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*"; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required but not installed"; }
need curl
need tar

# --- platform -----------------------------------------------------------
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
  darwin|linux) ;;
  mingw*|msys*|cygwin*)
    die "Windows is not supported by this script — download the .zip asset from https://github.com/${REPO}/releases" ;;
  *) die "unsupported operating system: ${os}" ;;
esac

arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) die "unsupported architecture: ${arch}" ;;
esac

# --- version ------------------------------------------------------------
if [ -z "${VERSION:-}" ]; then
  info "resolving latest release..."
  # Ask the API for the latest tag rather than relying on the /latest redirect,
  # so a failure here is a clear error instead of a 404 much later.
  VERSION="$(curl -sSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name" *: *"v\{0,1\}\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$VERSION" ] || die "could not determine the latest release — set VERSION explicitly"
fi
VERSION="${VERSION#v}"

# This filename is a public contract shared with the release workflow, the
# release-notes template and the CLI guide; scripts/check-cli-asset-names.sh
# fails CI if they ever disagree. Do not change it here alone.
ARCHIVE="kubectl-neo4j_${VERSION}_${os}_${arch}.tar.gz"
CHECKSUMS="kubectl-neo4j_${VERSION}_checksums.txt"
BASE="https://github.com/${REPO}/releases/download/v${VERSION}"

tmp="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf '$tmp'" EXIT INT TERM

info "downloading kubectl-neo4j ${VERSION} (${os}/${arch})..."
curl -sSLf -o "${tmp}/${ARCHIVE}" "${BASE}/${ARCHIVE}" \
  || die "download failed — does release v${VERSION} publish ${ARCHIVE}?"
curl -sSLf -o "${tmp}/${CHECKSUMS}" "${BASE}/${CHECKSUMS}" \
  || die "could not fetch ${CHECKSUMS}; refusing to install an unverified binary"

# --- verify -------------------------------------------------------------
if command -v shasum >/dev/null 2>&1; then
  SUMCMD="shasum -a 256"
elif command -v sha256sum >/dev/null 2>&1; then
  SUMCMD="sha256sum"
else
  die "neither shasum nor sha256sum found; refusing to install an unverified binary"
fi

expected="$(grep " ${ARCHIVE}\$" "${tmp}/${CHECKSUMS}" | awk '{print $1}')"
[ -n "$expected" ] || die "${ARCHIVE} is not listed in ${CHECKSUMS}"
actual="$(cd "$tmp" && $SUMCMD "${ARCHIVE}" | awk '{print $1}')"
[ "$expected" = "$actual" ] || die "checksum mismatch for ${ARCHIVE} (expected ${expected}, got ${actual})"
info "checksum verified"

# --- install ------------------------------------------------------------
tar -xzf "${tmp}/${ARCHIVE}" -C "$tmp"
[ -f "${tmp}/kubectl-neo4j" ] || die "archive did not contain kubectl-neo4j"
chmod +x "${tmp}/kubectl-neo4j"

if [ -w "$INSTALL_DIR" ]; then
  mv "${tmp}/kubectl-neo4j" "${INSTALL_DIR}/kubectl-neo4j"
elif command -v sudo >/dev/null 2>&1; then
  info "${INSTALL_DIR} is not writable; using sudo"
  sudo mv "${tmp}/kubectl-neo4j" "${INSTALL_DIR}/kubectl-neo4j"
else
  die "${INSTALL_DIR} is not writable and sudo is unavailable — set INSTALL_DIR to a writable directory"
fi

info "installed ${INSTALL_DIR}/kubectl-neo4j"
case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) info "run: kubectl neo4j validate -f your-manifests/" ;;
  *) info "note: ${INSTALL_DIR} is not on your PATH — add it, then run: kubectl neo4j validate -f your-manifests/" ;;
esac
