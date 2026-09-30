# Design: Cypher language defaulting on CalVer

> **Status:** built — §3 (the `Cypher25()` helper and guard) in #414; §5 and
> the §5.9 sharding change in the `serverDefaultCypherLanguage` PR. No PM roadmap governs this repo
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

## 5. `spec.serverDefaultCypherLanguage` — the server-level default

Decided: the language becomes a first-class field on
`Neo4jEnterpriseCluster` / `Neo4jEnterpriseStandalone`, **stamped at creation**
so an operator upgrade never changes a running cluster.

### 5.1 Why not just default it on CalVer

Tempting, and wrong for clusters that already exist. `db.query.default_language`
is **not dynamic**, and the cluster controller reconciles the ConfigMap "with
immediate updates and pod restarts". So defaulting it on a live CalVer cluster
would, on operator upgrade alone, roll every Neo4j pod **and** silently change
how the user's application queries are parsed. Two unrequested things at once.

For a *new* deployment the modern default is clearly right. The tension is
only with clusters that already exist, so the resolution is to distinguish
them.

### 5.2 Naming

`defaultCypherLanguage` is taken: `Neo4jDatabase`, `Neo4jCompositeDatabase`
and `Neo4jShardedDatabase` all use it for the per-database `DEFAULT LANGUAGE
CYPHER n` DDL clause. This field is a different layer — the *server* setting
`db.query.default_language`, which supplies the default for databases that do
not specify one.

Use **`spec.serverDefaultCypherLanguage`** on the deployment kinds. Reusing the
bare name across two layers would make "which default wins" unanswerable from
the spec alone.

### 5.3 The LTS rule is "never emit", not "emit CYPHER_5"

`db.query.default_language` **does not exist in 5.26** — it is absent from the
whole 5.x configuration reference. With the operator's strict config
validation, writing it there reproduces the `UDC.PACKAGING` failure exactly:

```
Unrecognized setting. No declared setting with name: db.query.default_language
```

and the server refuses to start. On the LTS the field is a *validation rule*,
never a value to write.

### 5.4 Resolution matrix

| `spec.serverDefaultCypherLanguage` | 5.26 LTS | CalVer |
|---|---|---|
| unset, **new** deployment | no key emitted; stamp `CYPHER_5` | emit `CYPHER_25`; stamp it |
| unset, **pre-existing** deployment | no key emitted; stamp `CYPHER_5` | **no key emitted**; stamp `CYPHER_5` |
| `CYPHER_5` | no key emitted (already the server default) | emit `CYPHER_5` |
| `CYPHER_25` | **reject** — validator names the version | emit `CYPHER_25` |

### 5.5 "New" has to be detected, not assumed

This is the crux. A cluster that already exists has **no stamp**, and so does
a brand-new one. Treating "no stamp + CalVer + unset" as `CYPHER_25` would
reintroduce the exact breaking change this design avoids, on the first
reconcile after an operator upgrade.

The discriminator is the workload, not the CR: stamp during the same reconcile
that **creates** the StatefulSet. If the StatefulSet already exists and there
is no stamp, the cluster predates this field — stamp `CYPHER_5` and emit
nothing. `sts.UID != ""` is the existence check the codebase already uses for
this (never `ResourceVersion`, which is populated for objects that were never
created).

Once stamped, `status.effectiveCypherLanguage` is authoritative for an unset
spec. Re-deriving it on every reconcile is what would make the value drift
under the user.

### 5.6 Changing it later is a restart, and should say so

Setting the field explicitly on a running cluster is an intentional change:
emit the new value, roll the pods. That is correct, but the field's
documentation must say it restarts the database — not doing so is how this
becomes a surprise a second time — and that it only sets the language of
databases created afterwards (§5.8).

### 5.7 Upgrading 5.26 → CalVer does not silently switch language

A cluster stamped `CYPHER_5` on the LTS keeps that stamp when its image moves
to CalVer, so the operator emits `CYPHER_5` and the queries keep meaning what
they meant. Getting Cypher 25 is then an explicit edit. Consistent with the
rest of this design: the operator does not redefine a user's queries as a side
effect of something else they asked for.

### 5.8 Neo4j fixes a database's language when it is created

Measured on 2026.06.0 (3-server sharding cluster, 2026-09-29), and the
premise of the rest of §5 depends on it:

| Step | Result |
|---|---|
| Server on `CYPHER_25`; create `neo4j` (bootstrap), `langnone` | both `defaultLanguage = CYPHER 25` |
| Restart the servers with the setting removed (server default → `CYPHER_5`) | `neo4j` and `langnone` **still `CYPHER 25`** |
| Create `plain5` under `CYPHER_5` | `CYPHER 5` |
| Restore `CYPHER_25`, restart | `plain5` **still `CYPHER 5`**; a new `plain25` gets `CYPHER 25` |

So `db.query.default_language` is, in effect, **the language a database gets
when it is created without one of its own**. It is stamped on the database at
creation and never re-read; changing the setting later moves nothing that
already exists. Only `ALTER DATABASE … SET DEFAULT LANGUAGE` does.

Consequences for this design:

- **§5.5 still stands, with a smaller blast radius than it implies.** Emitting
  `CYPHER_25` on an existing cluster would not change any existing database —
  but it would silently change every database created afterwards, which is
  still the surprise stamping exists to prevent.
- **§5.6 must say both halves.** Changing the field rolls the pods *and*
  applies only to databases created from then on. The field's documentation
  has to point at `ALTER DATABASE … SET DEFAULT LANGUAGE` for existing ones.
  (The operator could offer to ALTER them; that is a separate, explicit
  decision, not a side effect of this field.)

### 5.9 Interaction with property sharding — resolved: an artifact

Sharding sets `db.query.default_language=CYPHER_25` today, which collided
with `serverDefaultCypherLanguage: CYPHER_5`. The design said not to arbitrate
the conflict before checking whether it needs to exist. **It does not.**
Measured on 2026.06.0 by issuing the operator's sharded `CREATE DATABASE`
directly (the CRD only admits `defaultCypherLanguage: "25"`):

| Sharded family, server on `CYPHER_25` | parent | `-g000` | `-p000`, `-p001` |
|---|---|---|---|
| `SET DEFAULT LANGUAGE CYPHER 5` | `CYPHER 5` | `CYPHER 5` | `CYPHER 5` |
| `SET DEFAULT LANGUAGE CYPHER 25` | `CYPHER 25` | `CYPHER 25` | `CYPHER 25` |
| no clause | `CYPHER 25` (server) | `CYPHER 25` | `CYPHER 25` |

**The shard sub-databases inherit the parent's language, not the server's.**
And sharding does not need Cypher 25 at run time: on the Cypher 5 family, a
write, a read of a property stored on a property shard, an explicit `CYPHER 25`
query and Cypher-5-only syntax (`id()`) all succeeded. With the servers
restarted **without** the setting at all, a new sharded family with no
language clause came up `CYPHER 5` throughout and read and wrote property-shard
data normally. Only the sharding DDL is Cypher 25 — and it already goes
through `Cypher25()` (§3).

**Decision — the first branch:**

- Remove `db.query.default_language` from `buildPropertyShardingConfig` and
  from the sharding validator's `requiredSettings`. Enabling sharding stops
  changing the language of every database created on the cluster afterwards.
- Make the sharded `CREATE` **always** emit `SET DEFAULT LANGUAGE CYPHER 25`
  (defaulting `spec.defaultCypherLanguage`, whose CRD enum is already `"25"`
  only), so a sharded family keeps Cypher 25 whatever the server default is.
- Ship it with the §5 field, not before: after the change a sharding cluster's
  *non-sharded* databases created later get the server default (Cypher 5 on
  CalVer unless `serverDefaultCypherLanguage` says otherwise). Existing
  databases are unaffected (§5.8). Release-note it.

The earlier note that "enabling property sharding changes the query language
for every application" overstated it: per §5.8 it changes the language of
databases **created after** sharding is enabled — including, on a cluster
created with sharding on, the default `neo4j` database. The sharding guide
says so (#413).

## 6. Resolved: the prefix is not version-conditional

The question was whether §3's helper should apply `CYPHER 25` only on CalVer.
It has since bitten, in the other direction: the remote-alias builder
prefixed **every** remote alias, and the 5.26 LTS rejects the directive
itself (`25 is not a valid option for cypher version`), so stored-credential
remote constituents never worked on 5.26 — fixed in #414, verified on 5.26.31
and 2026.08.1.

The answer that came out of it: `Cypher25()` wraps **only statements whose
syntax exists only in Cypher 25**, and a statement that must also run on 5.26
is never wrapped. Whether a Cypher-25-only feature may be used on a given
server is a *validation* question, answered where every other capability gate
is (e.g. OIDC forwarding is refused on 5.26 at apply time) — not something the
prefix helper should decide by silently dropping the directive.
`TestCypher25Guard` pins the first half, and
`TestRemoteAliasStatementPinsCypher25OnlyForOIDC` the second.
