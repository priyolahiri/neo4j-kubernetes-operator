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
      onDatabase:
        databaseRef: analytics      # a real reference
    - grant: MATCH
      properties: "*"
      onGraph:
        databaseRef: analytics
      nodes: "*"
```

What the reference buys, none of which needs a parser:

- **Admission-time rejection** of a privilege naming a database that does not
  exist, instead of a `PrivilegesResolve=False` condition afterwards.
- **A watch**: the role re-reconciles when the database lands, the same way
  `Neo4jUser` already watches `Neo4jRole`.
- **Composite awareness at write time** — `onGraph` + a composite target is
  refusable up front, rather than reported inert later.
- **Rename detection**, because the ref resolves to an object, not a spelling.

### 4.1 Scope

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

## 7. Open questions

1. **Is the added API surface worth it**, given `PrivilegesResolve` already
   *detects* the problem? The difference is refusal versus report — real, but
   it is one condition versus a new spec shape to maintain forever.
2. **Does `databaseRef` point at a `Neo4jDatabase` CR, or a database name in
   the cluster?** A CR ref is stronger but only works for operator-managed
   databases; plenty of real databases are not.
3. Should `Neo4jDatabase` deletion *warn* when a role still references it?
   That is useful independently of this design and much cheaper.
