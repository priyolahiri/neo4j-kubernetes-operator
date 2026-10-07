#!/usr/bin/env bash
# Verify every docs-site link in the release notes template points at the
# release's own docs version.
#
# Why this exists: the docs site is versioned (mike) and has no unversioned
# pages, so https://priyolahiri.github.io/neo4j-kubernetes-operator/user_guide/...
# is a 404. The template hard-coded such links, so every GitHub release from
# v1.13.0 to v1.19.0 shipped with two to five of them. A /main/ link resolves
# but describes unreleased code, which is wrong for a release page.
#
# The template writes __DOCS_VERSION__; release.yml replaces it with vX.Y, the
# same path pages-docs.yml publishes a tag under.
set -euo pipefail

fail() { echo "ERROR: $*" >&2; exit 1; }

NOTES_TMPL=".github/release-notes-template.md"
RELEASE_WF=".github/workflows/release.yml"
DOCS_WF=".github/workflows/pages-docs.yml"
SITE="https://priyolahiri.github.io/neo4j-kubernetes-operator/"

for f in "$NOTES_TMPL" "$RELEASE_WF" "$DOCS_WF"; do
  [ -f "$f" ] || fail "$f not found"
done

# The Helm chart repository lives at /charts and is not versioned.
bad=$(grep -o "${SITE}[^)\" ]*" "$NOTES_TMPL" | grep -v -E "^${SITE}(__DOCS_VERSION__/|charts\b)" || true)
[ -z "$bad" ] || fail "$NOTES_TMPL links docs pages outside the release's version; use ${SITE}__DOCS_VERSION__/...:
$bad"

# Both workflows must derive the version the same way: vX.Y.Z with the patch dropped.
grep -q 's|__DOCS_VERSION__|${TAG_NAME%.\*}|g' "$RELEASE_WF" \
  || fail "$RELEASE_WF no longer replaces __DOCS_VERSION__ with \${TAG_NAME%.*}"
grep -q 'short="${full_tag%.\*}"' "$DOCS_WF" \
  || fail "$DOCS_WF no longer publishes a tag under vX.Y; update __DOCS_VERSION__ in $RELEASE_WF to match"

echo "check-release-notes-links: OK — release notes link the release's own docs version."
