#!/usr/bin/env bash
# Verify the kubectl-neo4j release asset naming convention is identical in
# every place that pins it.
#
# Why this exists: the asset filename is a PUBLIC contract. krew plugin
# manifests, install scripts, and anyone's CI pin the exact string, so renaming
# assets silently breaks every existing installer. Three files spell the
# convention out independently:
#
#   .github/workflows/release.yml        — builds and names the archives
#   .github/release-notes-template.md    — tells users what to download
#   docs/user_guide/guides/cli.md        — the install instructions
#   hack/install-cli.sh                  — downloads and verifies them
#
# This repo already learned that "two of three surfaces being right is the
# shape of drift review misses" (CLAUDE.md, on the CRD catalogue). Same shape,
# same guard.
set -euo pipefail

fail() { echo "ERROR: $*" >&2; exit 1; }

RELEASE_WF=".github/workflows/release.yml"
NOTES_TMPL=".github/release-notes-template.md"
CLI_DOC="docs/user_guide/guides/cli.md"
INSTALL_SH="hack/install-cli.sh"

for f in "$RELEASE_WF" "$NOTES_TMPL" "$CLI_DOC" "$INSTALL_SH"; do
  [ -f "$f" ] || fail "$f not found"
done

# The workflow is the source of truth: it builds `kubectl-neo4j_<ver>_<os>_<arch>`
# with a .tar.gz for unix and a .zip for windows, plus one checksums file.
grep -q 'stem="kubectl-neo4j_\${CLEAN_TAG}_\${goos}_\${goarch}"' "$RELEASE_WF" \
  || fail "$RELEASE_WF no longer builds kubectl-neo4j_<version>_<os>_<arch>; update this check and every consumer below"
grep -q 'kubectl-neo4j_\${CLEAN_TAG}_checksums.txt' "$RELEASE_WF" \
  || fail "$RELEASE_WF no longer produces kubectl-neo4j_<version>_checksums.txt"

# Consumers must use the same stem, with __VERSION__ / shell-var substitution.
grep -q 'kubectl-neo4j___VERSION___linux_amd64\.tar\.gz' "$NOTES_TMPL" \
  || fail "$NOTES_TMPL does not reference kubectl-neo4j___VERSION___linux_amd64.tar.gz"
grep -q 'kubectl-neo4j___VERSION___windows_amd64\.zip' "$NOTES_TMPL" \
  || fail "$NOTES_TMPL does not reference the windows .zip asset"
grep -q 'kubectl-neo4j___VERSION___checksums\.txt' "$NOTES_TMPL" \
  || fail "$NOTES_TMPL does not reference the checksums asset"

grep -q 'kubectl-neo4j_\${VERSION}_\${OS}_\${ARCH}\.tar\.gz' "$CLI_DOC" \
  || fail "$CLI_DOC install instructions do not match the released asset name"
grep -q 'kubectl-neo4j_\${VERSION}_checksums\.txt' "$CLI_DOC" \
  || fail "$CLI_DOC does not tell users how to fetch the checksums file"

# The install script builds the same names at runtime. If it drifts, users get
# a 404 from a URL they never typed, which is the least debuggable failure of
# the four surfaces.
grep -q 'ARCHIVE="kubectl-neo4j_\${VERSION}_\${os}_\${arch}\.tar\.gz"' "$INSTALL_SH" \
  || fail "$INSTALL_SH no longer builds kubectl-neo4j_<version>_<os>_<arch>.tar.gz"
grep -q 'CHECKSUMS="kubectl-neo4j_\${VERSION}_checksums\.txt"' "$INSTALL_SH" \
  || fail "$INSTALL_SH no longer fetches kubectl-neo4j_<version>_checksums.txt"

echo "check-cli-asset-names: OK — release workflow, release notes template, CLI guide and install script agree on the asset naming convention."
