# Neo4j Version Support Policy

This page defines which Neo4j versions the operator supports, and the policy by
which that set changes over time. The goal is a small, predictable support
matrix that never lags behind the database it manages.

## Supported versions (current)

| Track | Operator behaviour | Notes |
|---|---|---|
| **LTS** | **5.26.x** — validated & supported | Last SemVer release; the 5-line Long-Term-Support version. |
| **Feature (CalVer)** | **validated** against one anchor per release (e.g. `2026.04`); **allowed** (best-effort) for any newer `2025.x+`/`2026.x+` in the same major | CalVer is auto-detected (`major >= 2025`), so future releases run without a code change — but "runs" is best-effort, not "tested". See [What CalVer support means](#what-calver-support-means). |

Enterprise images only (`neo4j:5.26-enterprise`, `neo4j:2025.xx.x-enterprise`,
…). Community is not supported. Versions older than 5.26 (4.x, 5.0–5.25) are
**not** supported.

### What CalVer support means

The CalVer track releases frequently (monthly, cumulative). The operator does
**not** claim to *support* every CalVer that lands between operator releases —
that would be a guarantee for releases that didn't exist when the operator
shipped. Instead, two distinct things:

- **Validated (tested, guaranteed):** each operator release pins **one anchor
  CalVer** in CI (named in its release notes) and stands behind it.
- **Allowed (best-effort):** the operator does not upper-bound CalVer, so it
  *accepts and runs* any newer CalVer in the supported major via version-
  agnostic code paths + auto-detection. This is expected to work, but is **not
  a tested guarantee**.

Why the distinction is load-bearing: the operator **emits config and Cypher that
Neo4j strictly validates** (`server.config.strict_validation.enabled=true`,
Cypher 25 DDL). A CalVer released *after* an operator version can rename or
remove a config key the operator writes, or change a Cypher default — and the
pod won't start. (This is exactly the class of break fixed in the
de-duplicate-conf-keys and `server.directories.certificates` work.) So a brand-
new, untested CalVer genuinely *can* break the operator; the fix then ships in
the next operator release.

This also mirrors Neo4j itself: the vendor supports only the **latest** CalVer
(you must stay current to receive fixes), so the operator cannot support a
mid-window CalVer more strongly than the database does.

**Recommendation:** for production, run the operator-validated anchor CalVer (or
5.26 LTS). Newer CalVers are fine to run and usually work unchanged, but treat
them as best-effort until a subsequent operator release validates against them.

## Background: Neo4j's two release tracks

Since Neo4j 5, Neo4j ships on two tracks, and the cadence/lifecycle numbers are
what drive this policy:

| | LTS (feature-stable) | Feature release (CalVer) |
|---|---|---|
| Cadence | every ~2 years | frequent, cumulative |
| Vendor support lifecycle | ~3.5 years | only until the next release |
| Migration overlap | ~1 year between consecutive LTSs | n/a |
| Path to LTS | — | a feature line becomes the next LTS after ~2 years |

Because the **LTS support lifecycle (~3.5y) is longer than the LTS cadence
(~2y)**, two LTS lines are supported by Neo4j *at the same time* for roughly the
last ~1.5 years of the older one. That overlap is the crux of this policy.

Today this maps to: **5.26 = the active LTS**, **2025.x/2026.x = the feature
track**.

## The policy

> The operator supports **the current Neo4j LTS line plus the current CalVer
> feature line**. When a new LTS reaches GA it is **added**; the previous LTS is
> **dropped only when it reaches Neo4j end-of-life**, not when the new LTS ships.

Three rules make this precise:

1. **Add the new LTS at its GA.** The first operator release after a new LTS
   GAs adds it to the supported set and CI.
2. **Keep the old LTS until *Neo4j's* EOL for that line.** The operator must
   never refuse a Neo4j version that Neo4j itself still supports. Dropping the
   old LTS the moment a new one ships would strand still-supported production
   clusters on a frozen operator — so we hold it through the vendor overlap
   window, until the old LTS's ~3.5-year lifecycle ends.
3. **Validate one CalVer anchor; allow the rest best-effort.** "Supported
   CalVer" is not "every release in the window" — each operator release pins one
   anchor CalVer in CI; newer CalVers are *allowed* (forward-compatible) but
   best-effort until a later release validates them. See
   [What CalVer support means](#what-calver-support-means).

### Steady state vs. transition

- **Steady state = exactly two anchors** (current LTS + current CalVer). This is
  the bounded matrix the policy optimises for — minimal CI cost and code paths.
- **Transition = briefly three anchors** (old LTS + new LTS + CalVer) during the
  vendor overlap window, narrowing back to two when the old LTS reaches Neo4j
  EOL. That short-lived third lane is the deliberate, time-boxed cost of not
  being stricter than the database.

### Worked example (5.26 → next LTS)

```
5.26 GA (Dec 2024) ──────────────────────────────── 5.26 Neo4j EOL (~mid 2028)
                              new CalVer LTS GA (~2yr later)
                              │
   operator supports:        │
   5.26 + CalVer ────────────┤ + new LTS  (3 anchors, overlap) ──┐
                             add new LTS                          drop 5.26 here
                                                                  (2 anchors again)
```

When the new LTS GAs, the operator supports **5.26 + new LTS + CalVer** for the
overlap. 5.26 is dropped only at *its* Neo4j EOL — not at the new LTS's launch.

## What "supported" means in the operator

- **Hard gate (rejected by the version validator):** genuinely unsupported
  versions — anything older than the current LTS (pre-5.26 today), or a line
  past its Neo4j EOL.
- **Soft (allowed, may warn):** a CalVer newer than the one the current operator
  release was validated against. A brand-new CalVer must not be rejected the day
  it ships, so the validator does not hard-block "newer than tested" within a
  supported track.
- **CI anchors:** the integration suites run against the supported anchors
  (today: `5.26-enterprise` + latest CalVer). The steady-state invariant is
  *exactly two* anchors; a transition window may run three.

## When the next LTS lands — maintenance checklist

The change is small and contained. Adding a new LTS / eventually dropping 5.26
touches:

- the Neo4j version validator's allowed/minimum set (`internal/validation/`),
- the CI matrix anchors (`.github/workflows/integration.yml`, `integration-tests.yml`),
- the "Supported Neo4j versions" line in `CLAUDE.md`,
- the support matrix at the top of this page.

## FAQ

**I'm on 5.26 and a new LTS just shipped — will the next operator drop me?**
No. 5.26 stays supported until Neo4j's own EOL for the 5.26 line (~mid 2028),
regardless of how many operator releases ship in between.

**Do you support every CalVer that ships in a given window?**
No. Each operator release *validates* one anchor CalVer; every newer CalVer in
the major is *allowed* (forward-compatible) and usually works, but on a
best-effort basis — we can't guarantee a release that didn't exist when the
operator shipped, and because the operator emits strictly-validated config and
Cypher, a future CalVer can break it until the next operator release catches up.
See [What CalVer support means](#what-calver-support-means). This matches Neo4j,
which itself supports only the latest CalVer.

**Why not just keep supporting every LTS forever?**
Each supported line multiplies CI cost, branch maintenance, and
inter-version compatibility surface. Bounding the matrix to "current LTS +
feature line" (briefly +1 during overlap) is the whole point — it keeps the
operator fast to maintain without ever lagging the database's own lifecycle.
