#!/usr/bin/env bash
# Verify every Markdown table in docs/ actually renders as a table.
#
# Why this exists: all five tables on the Fault Tolerance page shipped as
# literal rows of pipes. Python-Markdown's `tables` extension needs a table to
# begin a new block; written directly under a paragraph line it is swallowed as
# lazy continuation of that paragraph:
#
#     **Fault Tolerance Matrix:**
#     | Scenario | Available Nodes |
#     |----------|-----------------|
#
#   → <p><strong>Fault Tolerance Matrix:</strong> | Scenario | ... </p>
#
# The source looks perfectly correct in an editor and in GitHub's preview,
# which is lenient here — so this only shows up on the published site, and a
# user reported it rather than any check. Nothing else looks at rendered
# output.
#
# The rule: a table's header row must be preceded by a blank line, a heading,
# or another table row. A heading is fine because it closes the preceding
# block; that exception was verified against Python-Markdown with this site's
# own extension set, not assumed — three tables sit directly under headings in
# installation.md and render correctly.
#
# Fenced code blocks are skipped: pipes inside them are content, not tables.
set -euo pipefail

found=$(awk '
  FNR == 1 { infence = 0; prev = "" }
  {
    line = $0
    stripped = line; sub(/^[ \t]+/, "", stripped)
    if (stripped ~ /^```/) { infence = !infence; prev = line; next }
    if (infence)           { prev = line; next }

    # A header row is a "|...|" line whose NEXT line is the delimiter row.
    if (stripped ~ /^\|/ && FNR > 1) {
      if ((getline nextline) > 0) {
        nstripped = nextline; sub(/^[ \t]+/, "", nstripped)
        if (nstripped ~ /^\|[ \t:|-]+\|[ \t]*$/) {
          pstripped = prev; sub(/^[ \t]+/, "", pstripped)
          if (pstripped != "" && pstripped !~ /^\|/ && pstripped !~ /^#/) {
            printf "%s:%d: table does not start a new block\n", FILENAME, FNR
            printf "    preceding line: %s\n", substr(pstripped, 1, 70)
            printf "    table header  : %s\n", substr(stripped, 1, 70)
          }
        }
        # Re-examine the consumed line as an ordinary line next iteration.
        prev = line
        line = nextline
        stripped = nstripped
      }
    }
    prev = line
  }
' $(find docs -name '*.md' | sort))

if [ -n "$found" ]; then
  echo "ERROR: Markdown table(s) that will render as literal pipes:" >&2
  echo "$found" >&2
  echo "  Put a blank line between the preceding paragraph and the table." >&2
  exit 1
fi

# --- Box diagrams ---------------------------------------------------------
#
# The same class of breakage, one layer down: a rectangle whose sides do not
# line up renders as a mess on the site exactly as it does here. The network-
# mode CCDR diagram drifted across three different right-border columns.
#
# Only TRUE RECTANGLES are checked — a block containing a `┌───┐` top edge.
# Tree listings (`├── foo`) and state-machine flows use the same characters
# with deliberately ragged right edges, and 6 of the 8 diagrams in docs/ are
# one of those. Measured before this rule was written, so it starts at zero
# false positives rather than teaching people to ignore it.
diag=$(python3 - <<'PY'
import pathlib, re, sys
BOX = set('┌┐└┘│├┤┬┴┼')
TOP = re.compile(r'┌─{3,}┐')
bad, count = [], 0
for p in sorted(pathlib.Path('docs').rglob('*.md')):
    lines, infence, cur, start = p.read_text().split('\n'), False, [], 0
    def flush(block, ln0):
        global count
        if len(block) < 2 or not any(TOP.search(l) for l in block):
            return                       # a tree or a flow, ragged by design
        count += 1
        widths = sorted({len(l) for l in block})
        right = sorted({len(l) - 1 - next(j for j, c in enumerate(reversed(l)) if c in BOX)
                        for l in block})
        if len(widths) > 1 or len(right) > 1:
            bad.append(f"{p}:{ln0}: box diagram sides do not line up\n"
                       f"    line widths        : {widths}\n"
                       f"    right-border column: {right}")
    for i, ln in enumerate(lines):
        if ln.startswith('```'):
            if infence: flush(cur, start)
            cur, infence, start = [], not infence, i + 2
            continue
        if infence and any(c in BOX for c in ln):
            cur.append(ln)
    if infence and cur: flush(cur, start)
if bad:
    print('\n'.join(bad)); sys.exit(1)
print(count)
PY
) || {
  echo "ERROR: box diagram(s) whose sides do not line up:" >&2
  echo "$diag" >&2
  echo "  Every line of a rectangle must be the same width, with its right" >&2
  echo "  border in the same column." >&2
  exit 1
}

tables=$(grep -rhcE '^\s*\|[ :|-]+\|\s*$' docs --include='*.md' | paste -sd+ - | bc)
echo "check-docs-tables: OK — ${tables} table(s) starting a new block, ${diag} box diagram(s) aligned."
