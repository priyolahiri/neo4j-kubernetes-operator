# Design: database references in `Neo4jRole` privileges

> **Status:** proposed, not built. No PM roadmap governs this repo, so this is
> a maintainer decision.

## 1. The problem

`Neo4jRole.spec.privileges` is `[]string` — each entry a complete Cypher
statement:

```yaml
privileges:
  - "GRANT ACCESS ON DATABASE analytics TO analytics_reader"
  - "GRANT MATCH {*} ON GRAPH analytics NODES * TO analytics_reader"
```

`analytics` is a substring of free text, not a reference. So:

- Nothing ties the privilege to the `Neo4jDatabase` that owns that name.
- A name the cluster does not have is only discovered at apply time. Neo4j
  **refuses** a grant on a database that does not exist (5.26: `Database 'x'
  does not exist`; CalVer: `42N00 graph reference not found`), and **drops** a
  role's privileges along with a dropped database — verified on 5.26.31 and
  2026.08.1. (An earlier draft said Neo4j accepts such a grant in silence. It
  does not.)
- So a misspelt or not-yet-created database, or one dropped from under a role,
  leaves part of the role's spec simply absent from the server.
- There is no ordering and no cascade into the CR.

The operator compensates after the fact. It skips a grant that names a
missing database rather than failing on it every reconcile, and reports it as
`PrivilegesResolve=False/DatabaseNotFound`; it warns on the role
(`PrivilegeTargetDropped`) when a `Neo4jDatabase` it grants on is deleted
(#408); and it reports `GraphPrivilegeOnComposite` when a `GRAPH` privilege
names a composite (accepted, persisted, inert). All of it exists *because* the
name is text, extracted by `PrivilegeDatabaseTargets`, which is deliberately
conservative and can miss.

## 2. Why it is a string today

The controller reconciles by reading `SHOW ROLE <name> PRIVILEGES AS
COMMANDS` and applying the difference against the desired state. **Neo4j
emits privileges as Cypher strings** — but not the strings you wrote: it
expands lists, singularises plurals, fills in default segments, renames verbs
and resolves aliases. So the desired side is Neo4j's own rendering of each
spec statement, learned from the operator's grants (learn mode, the default)
or from a throwaway probe role (probe mode) — #409, #410. Keeping the desired
state as Cypher is what makes that possible: the server renders it, and the
diff is a comparison between two server renderings. `DerivePrivilegeRevoke`
follows from the same choice: revokes are derived textually rather than asked
of the user.

The alternative — a structured spec — needs the round trip:

| direction | difficulty |
|---|---|
| structure → Cypher (to apply) | easy, mechanical |
| Cypher → structure (to diff) | a Cypher privilege parser |

## 3. Why we are not writing a parser

There is no Go library for this. Several openCypher parsers exist
([sulpher](https://pkg.go.dev/github.com/ha1tch/sulpher),
[go-opencypher](https://github.com/jtejido/go-opencypher),
[leiysky/parser](https://pkg.go.dev/github.com/leiysky/parser)) but privilege
administration is **not openCypher** — it is Neo4j's own dialect, and those
parsers put it out of scope explicitly.

Writing one: the command envelope is small and a week's work —

```
GRANT|DENY  [IMMUTABLE] <priv> ON {HOME GRAPH | GRAPH[S] {*|name,…}} [entity] TO   role,…
REVOKE [GRANT|DENY] [IMMUTABLE] <priv> ON … [entity] FROM role,…
```

— but three things make the full job much larger:

1. **The action vocabulary is open.** Dozens of verbs across graph, database
   and DBMS scopes, and Neo4j adds to it every release.
2. **`FOR pattern`** (property-based access control) embeds an expression,
   which drags general Cypher expression parsing back in.
3. **Two grammars at once.** This operator supports 5.26 LTS *and* CalVer.

And the failure mode is security-shaped: a misparse that silently drops a
`DENY` is worse than no parser.

**Rejected.**

## 4. Proposal: render, don't parse

Parsing is only needed to turn *Neo4j's output* back into structure. It is not
needed to let a user express a reference.

Add an **optional structured form** alongside the string form. The operator
renders it to Cypher, and from there it is an ordinary statement: normalised
to Neo4j's stored form and diffed exactly like a hand-written one. The
renderer's output format does not matter — only that Neo4j accepts it.

```yaml
spec:
  privileges:                       # unchanged, still supported
    - "GRANT TRAVERSE ON GRAPH legacy TO analytics_reader"
  privilegeRules:                   # new, optional
    - grant: ACCESS
      onDatabase: analytics         # a structured field, not a substring
    - grant: MATCH
      properties: "*"
      onGraph: analytics
      nodes: "*"
```

### 4.1 It is a structured field, not a Kubernetes object reference

**Decided:** `onDatabase` / `onGraph` take a database **name**, matching every
other database reference in this operator — `targetDatabase` on composite
constituents and aliases, `upstreamDatabase` on replicas,
`seedSourceDatabase` on sharded databases. Only *clusters* get a CR ref
(`clusterRef`, `instanceRef`).

That convention exists because most real databases are not `Neo4jDatabase`
CRs: the default `neo4j` database is deliberately unmanaged, a replica is
named by the operator rather than by a CR, shard sub-databases are created
internally by Neo4j, and users create databases out of band. A CR ref would
work for a minority and force the string form on everyone else — which is the
problem, not a fix for it.

So the win is **moving the name out of the Cypher string into a field**, not
object-reference semantics. Be precise about what that does and does not buy:

**It does buy** — none of which needs a parser:

- **Refusal before any grant** of a privilege naming a database that does not
  exist, by the inline validator (there are no admission webhooks — invariant
  1), as a clear validation error on the field instead of a skip plus a
  `PrivilegesResolve=False` condition.
- **Exactness.** The check reads a field instead of extracting a name from
  text. `PrivilegeDatabaseTargets` is deliberately conservative and feeds a
  warning precisely because it can be wrong; a field cannot.
- **A watch**: the role re-reconciles when the database appears, matched by
  name — the same shape as `Neo4jUser` watching `Neo4jRole`.
- **Composite awareness at write time** — `onGraph` naming a composite is
  refusable up front, rather than reported inert afterwards.

**It does not buy** — and an earlier draft of this document wrongly claimed the
first two:

- **Rename following.** A name is a spelling. Rename the database and the
  privilege still names the old one; you get a rejection or a condition, not a
  fix.
- **Ownership or cascade.** Deleting a database makes Neo4j drop its
  privileges, but nothing cascades into the `Neo4jRole`: its spec still asks
  for them.
- Anything at all for privileges left in the string form.

### 4.2 Scope

Model only what people actually write: `ACCESS`, `TRAVERSE`, `READ`, `MATCH`,
`WRITE` over `DATABASE` and `GRAPH`, with node/relationship/element qualifiers.
Everything else — DBMS privileges, `FOR` property rules, `IMMUTABLE` — stays in
the string form, which remains first-class and is not deprecated.

This is the whole point of the split: the structured form does not have to be
complete, because the string form is the escape hatch. A parser would have had
to be complete.

## 5. Costs and risks

| risk | mitigation |
|---|---|
| Two ways to express one privilege | Reject a CR where a `privilegeRules` entry renders to a statement already present in `privileges` — same canonical form, caught by the canonicaliser we already have |
| Rendered Cypher must match what Neo4j stores | No longer a risk: since #409/#410 every statement, rendered or hand-written, is compared by Neo4j's own stored form. The renderer only has to emit Cypher Neo4j accepts; the fixture-replay tests cover the stored forms |
| Scope creep toward "model everything" | The table in §4.1 is the contract; new verbs need a decision, not a reflex |
| Users assume a name field means cascade-delete | It does not. Neo4j drops a dropped database's privileges, but the role's spec keeps asking for them, and the role reports the database missing — document explicitly |

## 6. What this does not fix

Existing string privileges stay strings. There is no migration, because
migrating would need the parser this design exists to avoid. A user who wants
references rewrites those entries by hand.

## 7. Decisions and what is still open

**Decided — build it.** `PrivilegesResolve` detects the problem after the
fact; refusing a bad privilege at apply time is worth the API surface.

**Decided — a name, not a CR ref.** See §4.1. Follows the operator's existing
convention and works for databases it does not manage.

**Resolved since the first draft:**

1. *Should `Neo4jDatabase` deletion warn when a role still references it?*
   Yes — built in #408 (`PrivilegeTargetDropped`, on each affected role and
   once on the database).
2. *Must the rendered statement reproduce the canonical form exactly?* Moot.
   The diff compares Neo4j's stored renderings (#409/#410), so the renderer
   only has to emit Cypher the server accepts.

**Still open:** nothing blocking. The field list in §4.2 is the build scope.
