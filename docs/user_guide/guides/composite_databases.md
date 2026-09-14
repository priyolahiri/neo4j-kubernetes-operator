# Composite databases

A composite database is a single query endpoint over several constituent
databases. Applications **connect to the composite** and address the parts
through it:

```console
$ cypher-shell -d cineasts
```

```cypher
USE cineasts.latest
MATCH (m:Movie) RETURN m.title
```

!!! warning "Constituents are reachable only from a session on the composite"

    Running `USE cineasts.latest` while connected to some other database is
    refused:

    ```
    42N04: Failed to access database identified by `cineasts.latest` while
           connected to session database `neo4j`.
           Connect to `cineasts.latest` directly.
    ```

    **Ignore that last sentence** — connecting to the constituent by its
    qualified name does not work either (`22N51: graph reference not found`).
    Connect to the **composite** (`-d cineasts`, or your driver's database
    parameter), then `USE` the constituent from there.

    From a composite session, `RETURN graph.names()` lists what is reachable.

It **stores no data of its own**. No topology, no store, no indexes or
constraints, no seeding — the constituent databases hold everything, and the
composite is the way in.

## Creating one

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jCompositeDatabase
metadata:
  name: cineasts
  namespace: neo4j
spec:
  clusterRef: prod-cluster
  constituents:
    - name: latest
      targetDatabase: movies-latest
    - name: upcoming
      targetDatabase: movies-upcoming
```

Each constituent becomes an alias `cineasts.<name>`. The target databases do
not have to exist yet — a constituent whose target is missing is skipped and
retried, so you can apply a composite and its databases together.

```console
$ kubectl get neo4jcompositedatabase cineasts -n neo4j
NAME       CLUSTER        PHASE   READY   AGE
cineasts   prod-cluster   Ready   True    30s
```

`status.observedConstituents` is read back from the server, so it tells you
what the DBMS actually exposes rather than repeating your spec.

## Constituents are managed with the composite

They are declared inline rather than as separate CRs, for a reason worth
knowing about.

!!! warning "The composite must exist before any constituent"

    Neo4j accepts `CREATE ALIAS cineasts.latest` **even when no composite
    `cineasts` exists**. You get an ordinary alias whose name happens to
    contain a dot — `SHOW ALIASES` lists it, and it is even queryable. But the
    composite can then never be created:

    ```
    42N87: The database or alias name `cineasts` conflicts with the name
           `cineasts.latest` of an existing database or alias.
    ```

    The error never mentions ordering, and the only way out is dropping the
    alias. Because this CR owns the composite and its constituents together,
    the ordering is guaranteed. If a stray dotted alias is already in the way,
    the CR reports `Failed` with reason `CompositeDatabaseNameBlocked` and
    names the alias to drop.

By default `spec.constituents` is authoritative: an alias in the composite's
namespace that is not in spec gets removed. Set `enforceConstituents: false`
to leave out-of-band constituents alone, in which case the operator only ever
adds.

## Privileges go on the constituents, never on the composite

This is the mistake to watch for, and Neo4j gives you no warning about it:

```cypher
-- accepted. persisted. does nothing.
GRANT MATCH {*} ON GRAPH cineasts NODES * TO analyst
```

Graph privileges attach to the **constituent target databases**. A user reading
through the composite needs:

1. `ACCESS` on the composite,
2. `ACCESS` on each constituent's target database,
3. the graph privileges (`MATCH`, `TRAVERSE`, …) on those target databases.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRole
spec:
  clusterRef: prod-cluster
  name: analyst
  privileges:
    - "GRANT ACCESS ON DATABASE cineasts TO analyst"          # the composite
    - "GRANT ACCESS ON DATABASE `movies-latest` TO analyst"   # the constituent
    - "GRANT MATCH {*} ON GRAPH `movies-latest` NODES * TO analyst"
```

A `Neo4jRole` whose privileges name a composite as a **graph** reports
`PrivilegesResolve=False` with reason `GraphPrivilegeOnComposite`, because that
privilege will silently do nothing.

## Stopping a composite

`STOP DATABASE cineasts` takes the endpoint offline. **The constituent
databases stay online** — only the way in closes, and queries through the
composite fail with "database unavailable". Starting it restores access. So
stopping a composite is an access-layer switch, not a data operation.

## Deleting

`deletionPolicy: Delete` (the default) issues `DROP COMPOSITE DATABASE
cineasts CASCADE ALIASES`. `CASCADE ALIASES` is required — a plain drop is
refused while constituents exist — and it removes only the constituent
**aliases**. The databases they target are untouched.

Use `deletionPolicy: Retain` to keep the composite in Neo4j when the CR goes
away.

## Limits

| | |
|---|---|
| Local constituents only | Remote constituents (targeting another DBMS) are not modelled. They require `dbms.security.keystore.path` and `dbms.security.keystore.password` on every server before one can be created at all. |
| No options | Composites have none. The operator never emits an `OPTIONS` clause. |
| No topology | There is no store to place. |
| `defaultCypherLanguage` is CalVer-only | The `DEFAULT LANGUAGE CYPHER` clause does not parse on the 5.26 LTS. It is also the only property of a composite that can be changed after creation. |
| Writes are single-graph | A transaction may read across constituents but write to only one. |
| No nesting | A composite cannot contain another composite. |
