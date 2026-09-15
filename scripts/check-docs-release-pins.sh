#!/usr/bin/env bash
# Verify every documented install command names the current operator release.
#
# Why this exists: README.md's quick-install pinned v1.13.0 two releases after
# v1.15.0 shipped. Nothing was wrong with the line — it was correct when
# written, and no generator, test or drift check looks at it. A reader
# following the README got an operator two releases old while believing they
# had installed the current one.
#
# Two kinds of surface, checked differently:
#
#   UNPINNED — a quick-start, where the reader wants "the current one". These
#   must use GitHub's `releases/latest/download/` path, which always resolves
#   to the newest release's asset and therefore cannot go stale. A version
#   pinned here is the defect this script exists for.
#
#   PINNED — anything reproducible (GitOps, runbooks, upgrade instructions,
#   `go install`), where a moving target is wrong. These must agree with each
#   other and with the newest tag.
#
# Docs legitimately name old versions in history and migration prose, so only
# lines that INSTALL something are inspected. Add a row when you add an install
# path.
#
# Currency needs git tags, which a shallow CI checkout does not have. That half
# skips with a notice rather than failing, so per-PR the script still earns its
# place as a consistency guard: a partial bump that updates some surfaces and
# misses others is the failure this repo keeps hitting ("two of three surfaces
# being right is the shape of drift review misses" — CLAUDE.md, on the CRD
# catalogue).
set -euo pipefail

fail() { echo "ERROR: $*" >&2; exit 1; }

# label|file|extended-regex capturing a bare x.y.z version
PINNED=(
  "installation.md Helm --version|docs/user_guide/installation.md|^[[:space:]]*--version ([0-9]+\.[0-9]+\.[0-9]+)"
  "installation.md RELEASE_VERSION|docs/user_guide/installation.md|^RELEASE_VERSION=v([0-9]+\.[0-9]+\.[0-9]+)"
  "installation.md chart-version example|docs/user_guide/installation.md|for example, .([0-9]+\.[0-9]+\.[0-9]+)."
  "migration_guide.md CRD refresh|docs/user_guide/migration_guide.md|releases/download/v([0-9]+\.[0-9]+\.[0-9]+)/"
  "multi_cluster.md targetRevision|docs/user_guide/guides/multi_cluster.md|targetRevision: v([0-9]+\.[0-9]+\.[0-9]+)"
  "cli/install.md go install|docs/user_guide/cli/install.md|cmd/kubectl-neo4j@v([0-9]+\.[0-9]+\.[0-9]+)"
  "cli/install.md VERSION|docs/user_guide/cli/install.md|^[[:space:]]*VERSION=([0-9]+\.[0-9]+\.[0-9]+)"
)

# label|file — must install from releases/latest/download/ and pin nothing.
UNPINNED=(
  "README quick-install|README.md"
)

for row in "${UNPINNED[@]}"; do
  IFS='|' read -r label file <<< "$row"
  [ -f "$file" ] || fail "$file not found (surface: $label)"
  grep -q 'releases/latest/download/' "$file" \
    || fail "$label ($file) should install from releases/latest/download/, which never goes stale"
  if grep -qE 'releases/download/v[0-9]+\.[0-9]+\.[0-9]+/' "$file"; then
    fail "$label ($file) pins a release tag:
$(grep -nE 'releases/download/v[0-9]+\.[0-9]+\.[0-9]+/' "$file")
  A quick-start must use releases/latest/download/ — a pin here is exactly the
  line that went two releases stale and prompted this check."
  fi
done

labels=(); versions=()
for row in "${PINNED[@]}"; do
  IFS='|' read -r label file pattern <<< "$row"
  [ -f "$file" ] || fail "$file not found (surface: $label)"

  mapfile -t found < <(grep -oE "$pattern" "$file" \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | sort -u)

  case ${#found[@]} in
    0) fail "no version pin found for '$label' in $file.
  Either the install command changed shape (update the pattern in $0) or the
  pin was removed. Both need a human." ;;
    1) ;;
    *) fail "'$label' in $file pins several versions: ${found[*]}" ;;
  esac
  labels+=("$label"); versions+=("${found[0]}")
done

pinned="${versions[0]}"
for v in "${versions[@]}"; do
  if [ "$v" != "$pinned" ]; then
    echo "ERROR: documented install commands pin different releases:" >&2
    for i in "${!labels[@]}"; do
      printf '  %-42s v%s\n' "${labels[$i]}" "${versions[$i]}" >&2
    done
    echo "  Bump them together — a reader following one page gets a different" >&2
    echo "  operator than a reader following another." >&2
    exit 1
  fi
done

latest="$(git tag --list 'v[0-9]*' --sort=-v:refname 2>/dev/null | head -1 || true)"
if [ -z "$latest" ]; then
  echo "✓ install docs agree on v${pinned} (currency unchecked: no git tags in this checkout)"
  exit 0
fi
if [ "v${pinned}" = "$latest" ]; then
  echo "✓ install docs agree on v${pinned}, the newest release"
  exit 0
fi

# Ahead vs. behind are different situations and only one is a defect.
#
# AHEAD is release prep in flight: the docs bump lands on main before the tag
# is pushed, so between those two moments every PR would fail a strict equality
# check. That would teach people to ignore this script.
#
# BEHIND is the rot this exists to catch — a release shipped and the docs still
# send readers to the previous one.
newest="$(printf '%s\n' "v${pinned}" "$latest" | sort -V | tail -1)"
if [ "$newest" = "v${pinned}" ]; then
  echo "✓ install docs pin v${pinned}, ahead of the newest tag ${latest} — assuming a release in flight."
  echo "  If no release is being prepared, the docs name a version that does not exist."
  exit 0
fi

fail "install docs pin v${pinned}, but ${latest} has shipped.
  Readers following the install instructions get an operator that is behind.
  Bump every PINNED surface listed in $0."
