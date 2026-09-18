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

tables=$(grep -rhcE '^\s*\|[ :|-]+\|\s*$' docs --include='*.md' | paste -sd+ - | bc)
echo "check-docs-tables: OK — ${tables} table(s), each starting a new block."
