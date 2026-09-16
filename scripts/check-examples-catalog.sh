#!/usr/bin/env bash
# Verify the examples/ catalogue matches what is actually on disk.
#
# Why this exists: examples/README.md's "Directory Structure" list had been
# missing cross-cluster-replication/ since that capability shipped, and was
# about to be missing composite-databases/ too. Nothing generates that list and
# nothing read it, so a whole capability's examples were invisible to anyone
# browsing the directory — the same failure the CRD catalogue check was written
# for ("two of three surfaces being right is the shape of drift review misses"
# — CLAUDE.md).
#
# Two checks, both exact and offline:
#
#   1. Every examples/ subdirectory is listed, and every listed directory
#      exists. The second half matters as much as the first: a list naming a
#      directory that was renamed or removed sends readers to a 404.
#
#   2. Every example .yaml is named in its own directory's README, or in the
#      top-level one. An example nobody links to is one nobody finds. This
#      held for all 72 files when the check was written, so it enforces the
#      standard already in place rather than imposing a new one.
set -euo pipefail

fail() { echo "ERROR: $*" >&2; exit 1; }

ROOT_README="examples/README.md"
[ -f "$ROOT_README" ] || fail "$ROOT_README not found"

# The list is the block of "- **`name/`**" bullets. Read the names from the
# bullets rather than a line range, so reordering or adding prose around them
# does not break the check.
mapfile -t listed < <(grep -oE '^- \*\*`[^`]+/`\*\*' "$ROOT_README" \
  | sed -E 's/^- \*\*`(.*)\/`\*\*$/\1/' | sort -u)
mapfile -t ondisk < <(find examples -maxdepth 1 -mindepth 1 -type d \
  -exec basename {} \; | sort -u)

[ ${#listed[@]} -gt 0 ] || fail "no directory bullets found in $ROOT_README.
  The list format changed — update the pattern in $0."

problems=0

for d in "${ondisk[@]}"; do
  if ! printf '%s\n' "${listed[@]}" | grep -qx "$d"; then
    echo "ERROR: examples/$d/ exists but is not listed in $ROOT_README" >&2
    echo "  Add:  - **\`$d/\`** - <what it shows>" >&2
    problems=1
  fi
done

for d in "${listed[@]}"; do
  if [ ! -d "examples/$d" ]; then
    echo "ERROR: $ROOT_README lists examples/$d/, which does not exist" >&2
    echo "  Readers following that entry find nothing. Remove or rename it." >&2
    problems=1
  fi
done

# 2. Every example file is reachable from a README.
for d in "${ondisk[@]}"; do
  for f in examples/"$d"/*.yaml; do
    [ -e "$f" ] || continue
    base=$(basename "$f")
    if grep -qF "$base" "examples/$d/README.md" 2>/dev/null; then continue; fi
    if grep -qF "$base" "$ROOT_README"; then continue; fi
    echo "ERROR: $f is named in no README" >&2
    echo "  Add it to examples/$d/README.md (or $ROOT_README) — an example" >&2
    echo "  nobody links to is one nobody finds." >&2
    problems=1
  done
done

[ "$problems" = "0" ] || exit 1

files=$(find examples -mindepth 2 -name '*.yaml' | wc -l | tr -d ' ')
echo "check-examples-catalog: OK — ${#ondisk[@]} directory(ies) listed, ${files} example(s) reachable from a README."
