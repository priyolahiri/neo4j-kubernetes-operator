#!/usr/bin/env bash
# Verify every kubectl-neo4j command is documented, and every CLI doc page is
# reachable from the mkdocs nav.
#
# Why this exists: the CLI grew from one command to six in a single sitting, and
# at one point every command was documented while the CLI was absent from the
# README, the docs landing page and every troubleshooting guide. That is the
# failure this repo already knows by name — "two of three surfaces being right
# is the shape of drift review misses" (CLAUDE.md, on the CRD catalogue). The
# next command added without a page would repeat it silently.
set -euo pipefail

fail() { echo "ERROR: $*" >&2; exit 1; }

MAIN="cmd/kubectl-neo4j/main.go"
DOCS_DIR="docs/user_guide/cli"
NAV="mkdocs.yml"

[ -f "$MAIN" ] || fail "$MAIN not found"
[ -d "$DOCS_DIR" ] || fail "$DOCS_DIR not found"

# Commands come from main.go's dispatch switch, so this cannot drift from the
# binary's real behaviour. `version` and `help` are excluded: they need no page.
#
# A while-read loop rather than `mapfile`, which needs bash 4 — macOS still
# ships bash 3.2, and a check that only runs in CI is half a check.
commands=$(sed -n 's/^	case "\([a-z-]*\)":$/\1/p' "$MAIN" | grep -vE '^(version|help)$' | sort -u)
[ -n "$commands" ] || fail "found no commands in $MAIN — has the dispatch switch changed shape?"

count=0
while IFS= read -r cmd; do
  [ -n "$cmd" ] || continue
  count=$((count + 1))
  if ! grep -rqs -- "kubectl neo4j ${cmd}" "$DOCS_DIR"; then
    fail "command '${cmd}' exists in $MAIN but no page under $DOCS_DIR documents it"
  fi
done <<EOF
$commands
EOF

# Every page must be reachable from the nav, or it is published but unfindable.
for page in "$DOCS_DIR"/*.md; do
  rel="${page#docs/}"
  grep -qs -- "$rel" "$NAV" || fail "$page is not listed in the $NAV nav — it would be published but unreachable"
done

# The CLI must be discoverable from the two catalogue surfaces, which is exactly
# what was missing when this check was written.
grep -qs "user_guide/cli/" README.md || fail "README.md does not link to the CLI documentation"
grep -qs "user_guide/cli/" docs/index.md || fail "docs/index.md does not link to the CLI documentation"

echo "check-cli-docs: OK — ${count} command(s) documented, $(ls -1 "$DOCS_DIR"/*.md | wc -l | tr -d ' ') page(s) in the nav, linked from README and the docs landing page."
