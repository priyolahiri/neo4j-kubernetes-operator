# Composite Database Examples

A composite database is a single query endpoint over several constituent
databases. It **stores no data of its own** — no topology, no store, no indexes
or constraints, no seeding. The constituents hold everything, and the composite
is the way in.

Applications connect to the **composite** and address the parts through it:

```bash
cypher-shell -d cineasts
```

```cypher
USE cineasts.latest
MATCH (m:Movie) RETURN m.title
```

## Examples

| File | What it shows |
|---|---|
| `composite-local.yaml` | Two local databases behind one composite. Apply the whole file at once — a constituent whose target does not exist yet is skipped and retried |
| `composite-privileges.yaml` | Granting access **through** a composite: `ACCESS` on the composite, plus `ACCESS` and the graph privileges on each constituent's target |
| `composite-remote-oidc.yaml` | A constituent in another DBMS, authenticated by forwarding the querying user's own OIDC token. Prefer this: nothing is stored, and no keystore is needed |
| `composite-remote-credentials.yaml` | The same, with stored credentials — which requires `spec.remoteAliasKeystore` on the deployment |

## Three things the server will not warn you about

**Constituents are reachable only from a session on the composite.** `USE
cineasts.latest` from any other database is refused with `42N04`, which then
advises you to "connect to `cineasts.latest` directly". **Ignore that** — doing
so fails too (`22N51`). Connect to the composite and `USE` from there. From a
composite session, `RETURN graph.names()` lists what is reachable.

**The composite must exist before any constituent alias.** Neo4j accepts
`CREATE ALIAS cineasts.latest` with no composite `cineasts` present, producing
an ordinary alias whose name merely contains a dot — and the composite can then
never be created (`42N87`, an error that never mentions ordering). Declaring
constituents inline on the `Neo4jCompositeDatabase`, as these examples do, is
what makes the ordering guaranteed rather than hoped for. If a stray dotted
alias is already in the way, the CR reports `Failed` with reason
`CompositeDatabaseNameBlocked` and names the alias to drop.

**Graph privileges on a composite do nothing.** `GRANT MATCH {*} ON GRAPH
cineasts …` is accepted, persisted, and inert. See
`composite-privileges.yaml`.

## Applying these

Replace `prod-cluster` with your deployment's name, and check before you apply:

```bash
kubectl neo4j validate -f composite-local.yaml
kubectl apply -f composite-local.yaml
kubectl get neo4jcompositedatabase -n neo4j
```

`kubectl neo4j explain CompositeDatabaseNameBlocked` (or any condition, phase
or reason) explains what a status is telling you.

## See also

- [Composite databases guide](../../docs/user_guide/guides/composite_databases.md) — the full walkthrough
- [`Neo4jCompositeDatabase` API reference](../../docs/api_reference/neo4jcompositedatabase.md) — every field
- [Database aliases](../../docs/user_guide/guides/database_aliases.md) — plain aliases, which are a different CRD
