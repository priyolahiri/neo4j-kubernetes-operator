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
- Renaming or deleting the database leaves the role granting on nothing —
  **Neo4j accepts a grant on a database that does not exist, without
  complaint.**
- There is no ordering, no cascade, and no admission-time rejection.

The operator compensates after the fact: `PrivilegesResolve=False` when a
privilege names a database the cluster does not have, and reason
`GraphPrivilegeOnComposite` when it names a composite as a `GRAPH` (accepted,
persisted, inert). Both exist *because* the name is text.

## 2. Why it is a string today

The controller reconciles by reading `SHOW ROLE <name> PRIVILEGES AS
COMMANDS`, canonicalising both sides, and applying the difference. **Neo4j
emits privileges as Cypher strings.** Keeping the desired state in the same
representation makes the diff a string comparison against the server's own
output. `DerivePrivilegeRevoke` follows from the same choice: revokes are
derived textually rather than asked of the user.

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
renders it to Cypher; the diff is unchanged, because the rendered string is
canonicalised against `SHOW … AS COMMANDS` exactly as a hand-written one is.

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

- **Admission-time rejection** of a privilege naming a database that does not
  exist, instead of a `PrivilegesResolve=False` condition afterwards.
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
- **Ownership or cascade.** Deleting a database does not revoke anything.
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
| Rendered Cypher must canonicalise identically to `SHOW` output | Directly testable: render → canonicalise → compare against the server's own emission, in the existing integration suite |
| Scope creep toward "model everything" | The table in §4.1 is the contract; new verbs need a decision, not a reflex |
| Users assume a ref means cascade-delete | It does not. Deleting a database does not revoke privileges — document explicitly |

## 6. What this does not fix

Existing string privileges stay strings. There is no migration, because
migrating would need the parser this design exists to avoid. A user who wants
references rewrites those entries by hand.

## 7. Decisions and what is still open

**Decided — build it.** `PrivilegesResolve` detects the problem after the
fact; refusing a bad privilege at apply time is worth the API surface.

**Decided — a name, not a CR ref.** See §4.1. Follows the operator's existing
convention and works for databases it does not manage.

**Still open:**

1. Should `Neo4jDatabase` deletion *warn* when a role still references it?
   Useful independently of this design and far cheaper than it — worth doing
   whether or not the structured form is built.
2. Does the rendered statement need to reproduce the operator's own
   canonical form exactly, or merely canonicalise to the same value? The
   latter is sufficient for the diff and is what the tests should assert;
   pinning the former would couple the renderer to a formatting detail.
