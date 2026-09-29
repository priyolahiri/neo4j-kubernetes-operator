# Design: Cypher language defaulting on CalVer

> **Status:** proposed, not built. No PM roadmap governs this repo
> ([wiki: `project-k8s-operator` closed 2026-06-18](#)), so this is a
> maintainer decision.

## 1. The problem

Some statements the operator emits are Cypher 25 syntax. On a CalVer server the
`system` database still defaults to **Cypher 5**, and the failure is not a
warning — the parser reports invalid input, which reads as though the feature
does not exist:

```
Invalid input 'DATABASE': expected a graph pattern
There is no procedure with the name `dbms.promoteReplicaDatabase` registered
```

Today the fix is a per-statement literal, `cypher25Prefix`, and a comment
asking the next author to remember:

```go
// Current users: AUTH RULE (here) and CREATE REPLICA DATABASE (replicas.go).
// The second was missed until the v1.15.0 journey ran it against a real
// 2026.08 server; if you add a third, prefix it.
const cypher25Prefix = "CYPHER 25 "
```

**That convention has already failed twice.**

1. `CREATE REPLICA DATABASE` and `dbms.promoteReplicaDatabase` shipped without
   it and were only caught by a live journey against a real 2026.08 server —
   no unit test can see a parser's opinion.
2. The comment is *already stale*: `composite.go` is a third user, added after
   the comment was written, and the comment does not mention it.

A rule enforced by a comment is a rule that degrades. The question is what
replaces it.

## 2. The lever that looks right and is not

`db.query.default_language=CYPHER_25` would make everything default to Cypher
25 with no per-statement discipline at all. The operator already sets it — but
only on the property-sharding path, where sharding requires it.

Doing it generally is wrong, for a reason the setting's own documentation
states:

> "The default language of a database determines which language is used to
> evaluate queries that **do not explicitly select a language**."

It affects **every query the server receives**, not just the operator's. A user
whose application sends Cypher 5 would silently have it parsed as Cypher 25.
That is a breaking change to someone else's workload, introduced by upgrading
the operator, and it is **not a dynamic setting** — it needs a restart to undo.

The operator's job is to make its *own* statements parse. It has no business
changing what a user's queries mean.

**Rejected.**

## 3. Proposal

Two changes, neither user-visible.

### 3.1 Make the prefix structural, not remembered

Every Cypher-25-only statement the operator sends goes through the `system`
database. Route those through one helper that applies the prefix, instead of
each call site prepending a constant:

```go
// systemDDL25 runs a statement whose syntax is a Cypher 25 language feature.
// The prefix is applied here so no call site has to remember it — the
// mechanism that failed for CREATE REPLICA DATABASE in v1.15.0.
func (c *Client) systemDDL25(ctx context.Context, query string, params map[string]any, desc string) error
```

The prefix stays a literal prepend rather than a server setting, so it changes
nothing outside the statement it is attached to. It is already safe when the
database default is 25.

### 3.2 A guard that fails the build, not a comment that asks nicely

A unit test over `internal/neo4j`: any statement string containing a
Cypher-25-only construct must carry the prefix. Seed the construct list from
what we already know needs it — `AUTH RULE`, `REPLICA DATABASE`,
`promoteReplicaDatabase`, `COMPOSITE`-with-`DEFAULT LANGUAGE` — and extend it
when a new one is found.

This is the part that matters. The prefix helper prevents *new* call sites from
forgetting; the test catches the case the helper cannot see, which is someone
writing a raw `session.Run` with 25-only syntax.

## 4. What this deliberately does not do

- **Does not change any user-facing default.** `spec.config` can still set
  `db.query.default_language` for anyone who wants it; the operator will not
  set it on their behalf outside sharding.
- **Does not try to detect Cypher 25 syntax generally.** The guard is a
  keyword list, not a parser. A list that misses a construct fails the same way
  today's comment does — but a list is greppable and testable, and a comment is
  neither.

## 5. Open question

Should the prefix be conditional on the server being CalVer, or unconditional?

Unconditional is simpler and the constant's comment already says prepending is
safe when the default is already 25. But on the **5.26 LTS** a `CYPHER 25`
prefix is not merely redundant — the LTS has no Cypher 25, so the prefix is a
parse error. Every current user of the prefix is a CalVer-only feature
(auth rules, replicas, composites' `DEFAULT LANGUAGE`), so the question has
not bitten yet.

**Recommendation:** make the helper take the resolved server `*Version` and
apply the prefix only when `IsCalver`. That way a future 25-only statement on a
mixed-version deployment fails with a capability error rather than a syntax
error.
