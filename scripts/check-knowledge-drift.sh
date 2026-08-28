#!/usr/bin/env bash
#
# check-knowledge-drift.sh
#
# Guards the docs/knowledge/ base against bit-rot. The knowledge base re-homes
# CLAUDE.md's invariant checklist and pins each rule to the code that enforces
# it: "Pinned by `TestXxx`" references and backticked `internal/.../foo.go`
# file paths. Over time, code gets renamed/moved while the docs keep citing the
# old symbol — exactly the drift that let AGENTS.md describe a BANNED
# architecture. This script makes such drift a CI failure.
#
# What it extracts from docs/knowledge/*.md (pragmatic, obvious forms only):
#   1. Test-name pins:  `TestXxx` and `_FooBar` continuation pins (the
#      "Pinned by `TestA` + `_B`" pattern) -> must exist as `func TestXxx`
#      or as a `_FooBar` test-name fragment somewhere in *_test.go.
#   2. File-path refs:  backticked paths like `internal/controller/x.go`,
#      `api/v1beta1/y.go`, `cmd/main.go`, `config/...yaml`,
#      `internal/.../z_test.go` -> the file must exist on disk.
#
# It deliberately does NOT try to validate every backticked token (config keys,
# Cypher, env vars, prose identifiers produce too many false positives). It
# matches the two high-signal, low-noise reference shapes above.
#
# Exit 1 (with a per-reference WARN) if any extracted reference does not
# resolve; exit 0 when the knowledge base is fully grounded. If docs/knowledge/
# does not exist yet, it exits 0 (nothing to check).
#
# Portable to macOS bash 3.2: no associative arrays, no mapfile, no `grep -P`.
#
# Usage: scripts/check-knowledge-drift.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

KNOWLEDGE_DIR="docs/knowledge"

if [ ! -d "${KNOWLEDGE_DIR}" ]; then
  echo "check-knowledge-drift: ${KNOWLEDGE_DIR}/ not present yet — nothing to check."
  exit 0
fi

# Gather the knowledge markdown files (tracked + on-disk). If none, exit clean.
KNOWLEDGE_FILES="$(git ls-files "${KNOWLEDGE_DIR}/*.md" 2>/dev/null || true)"
if [ -z "${KNOWLEDGE_FILES}" ]; then
  KNOWLEDGE_FILES="$(find "${KNOWLEDGE_DIR}" -name '*.md' -type f 2>/dev/null || true)"
fi
if [ -z "${KNOWLEDGE_FILES}" ]; then
  echo "check-knowledge-drift: no ${KNOWLEDGE_DIR}/*.md files — nothing to check."
  exit 0
fi

unresolved=0
checked=0

# Cache the set of test-symbol-bearing files once: all *_test.go across the
# tree (excluding agent scratch worktrees). Reused for each pin so we don't
# re-glob the whole tree per reference.
TEST_GO_FILES="$(git ls-files '*_test.go' | grep -Ev '(^|/)\.claude/worktrees/' || true)"

# symbol_in_tests <symbol>
#   True if <symbol> appears in any *_test.go file (as a func decl or as a
#   continuation fragment like "_BackupRefMissingCR_IsPermanent").
symbol_in_tests() {
  local sym="$1"
  [ -z "${TEST_GO_FILES}" ] && return 1
  # shellcheck disable=SC2086
  printf '%s\n' ${TEST_GO_FILES} | tr '\n' '\0' \
    | xargs -0 grep -lE "${sym}" >/dev/null 2>&1
}

# --- Pass 1: test-name pins --------------------------------------------------
# Extract backticked tokens that look like Go test identifiers:
#   TestXxxYyy            (full test func names)
#   _FooBar_Baz          (continuation pins from "Pinned by `TestA` + `_B`")
# Pull every backticked span, then keep tokens matching those shapes.
TEST_PINS="$(grep -hoE '`[^`]+`' ${KNOWLEDGE_FILES} 2>/dev/null \
  | tr -d '`' \
  | grep -E '^(Test[A-Za-z0-9_]+|_[A-Za-z][A-Za-z0-9_]+)$' \
  | sort -u || true)"

if [ -n "${TEST_PINS}" ]; then
  while IFS= read -r sym; do
    [ -z "${sym}" ] && continue
    checked=$((checked + 1))
    if ! symbol_in_tests "${sym}"; then
      echo "WARN [knowledge-drift]: test pin \`${sym}\` not found in any *_test.go" >&2
      unresolved=$((unresolved + 1))
    fi
  done <<EOF
${TEST_PINS}
EOF
fi

# --- Pass 2: source file-path refs -------------------------------------------
# Extract backticked tokens that look like in-repo file paths under the
# code/config roots and assert each file exists on disk.
PATH_REFS="$(grep -hoE '`[^`]+`' ${KNOWLEDGE_FILES} 2>/dev/null \
  | tr -d '`' \
  | grep -E '^(internal|api|cmd|config|charts|scripts|test)/[A-Za-z0-9_./-]+\.(go|ya?ml|sh)$' \
  | sort -u || true)"

if [ -n "${PATH_REFS}" ]; then
  while IFS= read -r ref; do
    [ -z "${ref}" ] && continue
    checked=$((checked + 1))
    if [ ! -e "${ref}" ]; then
      echo "WARN [knowledge-drift]: referenced path \`${ref}\` does not exist" >&2
      unresolved=$((unresolved + 1))
    fi
  done <<EOF
${PATH_REFS}
EOF
fi

# --- Pass 3: CRD-kind / Go identifier refs -----------------------------------
# Extract backticked `Neo4jXxx` identifiers and assert each appears somewhere in
# the Go source. Added after two knowledge rules were found citing things that
# did not exist: rule 18 routed multi-tenant access through an opt-in CR name
# that appeared exactly once in the whole repo — in that rule's own text.
#
# Deliberately a substring existence check against the WHOLE Go tree, not an
# exact-symbol check against api/v1beta1/ only: `Neo4jUserReconciler` (controller)
# and `Neo4jBackupStatus` (types) are both legitimate, and the goal is catching
# names that exist NOWHERE, with zero false positives on real ones.
#
# Convention this enforces: backticks assert a real symbol. A correction note
# discussing something that never existed must NOT backtick it.
GO_SRC_FILES="$(git ls-files '*.go' | grep -Ev '(^|/)\.claude/worktrees/' || true)"

KIND_REFS="$(grep -hoE '`Neo4j[A-Za-z0-9_]+`' ${KNOWLEDGE_FILES} 2>/dev/null \
  | tr -d '`' \
  | sort -u || true)"

if [ -n "${KIND_REFS}" ] && [ -n "${GO_SRC_FILES}" ]; then
  while IFS= read -r sym; do
    [ -z "${sym}" ] && continue
    checked=$((checked + 1))
    # shellcheck disable=SC2086
    if ! printf '%s\n' ${GO_SRC_FILES} | tr '\n' '\0' \
        | xargs -0 grep -l "${sym}" >/dev/null 2>&1; then
      echo "WARN [knowledge-drift]: identifier \`${sym}\` not found in any .go file" >&2
      unresolved=$((unresolved + 1))
    fi
  done <<EOF
${KIND_REFS}
EOF
fi

# --- Pass 4: quoted test-case names on "Pinned-by" lines ---------------------
# Ginkgo/table test cases are pinned by their DESCRIPTION, not a Go symbol, e.g.
#   **Pinned-by:** `foo_test.go` ("Cluster with cross-namespace target is rejected")
# Pass 1 only sees backticked `TestXxx` symbols, so these slipped through — which
# is how rule 80 kept a green check while pinning two tests that did not exist.
#
# Scoped tightly to keep false positives at zero: only double-quoted spans on a
# line that actually declares a pin, at least 20 characters, and containing a
# space (a test-case description reads like prose). That filter admits real
# descriptions while excluding the "4G" / "kubectl" / "p" noise that quoting
# produces elsewhere on those lines. Matched with grep -F: descriptions contain
# regex metacharacters (+, ., *) and must be compared literally.
CASE_PINS="$(grep -hiE '^[[:space:]]*[-*]?[[:space:]]*\*\*?[Pp]inned.?by' ${KNOWLEDGE_FILES} 2>/dev/null \
  | grep -oE '"[^"]{20,}"' \
  | tr -d '"' \
  | grep ' ' \
  | sort -u || true)"

if [ -n "${CASE_PINS}" ] && [ -n "${TEST_GO_FILES}" ]; then
  while IFS= read -r desc; do
    [ -z "${desc}" ] && continue
    checked=$((checked + 1))
    # shellcheck disable=SC2086
    if ! printf '%s\n' ${TEST_GO_FILES} | tr '\n' '\0' \
        | xargs -0 grep -lF "${desc}" >/dev/null 2>&1; then
      echo "WARN [knowledge-drift]: test-case pin \"${desc}\" not found in any *_test.go" >&2
      unresolved=$((unresolved + 1))
    fi
  done <<EOF
${CASE_PINS}
EOF
fi

# --- Verdict -----------------------------------------------------------------
if [ "${unresolved}" -ne 0 ]; then
  echo "" >&2
  echo "check-knowledge-drift: FAILED — ${unresolved} of ${checked} reference(s) do not resolve." >&2
  echo "Update docs/knowledge/ to cite the current symbol/path, or fix the rename." >&2
  exit 1
fi

echo "check-knowledge-drift: OK — ${checked} knowledge reference(s) all resolve."
