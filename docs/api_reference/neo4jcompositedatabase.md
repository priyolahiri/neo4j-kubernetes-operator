# Neo4jCompositeDatabase

A composite database is a single query endpoint over several constituent
databases. It **stores no data of its own** — no topology, no store, no indexes
or constraints, no seeding. Its content is entirely the constituent aliases
declared in its spec.

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

Queried from a session **on the composite** — a constituent is not reachable
from any other database's session (`42N04`), and connecting to the constituent
by its qualified name does not work either (`22N51`), despite what the first
error suggests:

```console
$ cypher-shell -d cineasts
```

```cypher
USE cineasts.latest
MATCH (m:Movie) RETURN m.title
```

## Why this is a separate Kind

`Neo4jDatabase` is almost entirely topology, options, storage and seeding —
every one of which is meaningless on a composite, and rejected by the server.
A `composite: true` flag would leave most of that spec silently inert.

## Spec

| Field | Type | Description |
|---|---|---|
| `clusterRef` | `string` | **Required.** The `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` in the same namespace that hosts this composite and its constituents. |
| `name` | `string` | The composite database name in Neo4j. Defaults to `metadata.name`. Neo4j database names accept only ASCII letters, digits, dots and dashes — **underscores are rejected by the server**. A dot is rejected here: `<composite>.<constituent>` is how constituents are addressed, so a dotted composite name could never have any. |
| `constituents` | [`[]CompositeConstituent`](#compositeconstituent) | **Required, at least one.** The databases this composite exposes. Each becomes an alias `<composite>.<name>`. |
| `enforceConstituents` | `bool` | Default `true`. Makes `constituents` authoritative: an alias in the composite's namespace that is absent from spec is dropped. Set `false` to leave out-of-band constituents alone, in which case this CR only ever adds. Same posture as `Neo4jRole.enforcePrivileges`. |
| `defaultCypherLanguage` | `string` | `"5"` or `"25"`. **CalVer only** — the `DEFAULT LANGUAGE CYPHER` clause does not parse on the 5.26 LTS, and the validator rejects it there rather than letting the server emit a raw syntax error. This is the **only** property of a composite that can be altered after creation. |
| `wait` | `bool` | Default `true`. Blocks creation until the composite is available, via the Cypher `WAIT` clause. |
| `deletionPolicy` | `string` | `Delete` (default) or `Retain`. See [Deletion](#deletion). |

### CompositeConstituent

| Field | Type | Description |
|---|---|---|
| `name` | `string` | **Required.** The constituent's name within the composite. The resulting alias is `<composite>.<name>`, which is also how queries address it. May not contain a dot — it is already namespaced. |
| `targetDatabase` | `string` | **Required.** The database the constituent resolves to — in this DBMS when `remote` is unset, or on the remote DBMS when it is set. A local target need not exist yet: the controller skips that constituent and retries. A remote target is never checked, because the operator cannot see the other DBMS. |
| `remote` | [`RemoteConstituent`](#remoteconstituent) | Points the constituent at a database in **another** Neo4j DBMS. Omit for a local constituent. |

### RemoteConstituent

| Field | Type | Description |
|---|---|---|
| `url` | `string` | **Required.** The remote DBMS's Bolt endpoint, e.g. `neo4j+s://other.example.com:7687`. Prefer a `+s` scheme — this connection carries query traffic between two DBMSs, and with stored credentials it carries the authentication too. |
| `oidcCredentialForwarding` | `bool` | Forwards the querying user's own OIDC token instead of storing any credential. **Requires Cypher 25**, so CalVer only. Mutually exclusive with `credentialsSecretRef`. |
| `credentialsSecretRef` | `string` | A Secret in this namespace with `username` and `password` keys. **Requires `spec.remoteAliasKeystore` on the deployment.** Mutually exclusive with `oidcCredentialForwarding`. |
| `driverSettings` | `map[string]string` | Passed through to the alias's `DRIVER` clause (e.g. `connection_timeout`). Emitted verbatim as Cypher map values. |

#### Choosing a mode

**Prefer `oidcCredentialForwarding` where both DBMSs trust the same identity
provider.** No credential is stored, so there is nothing to leak, rotate or
encrypt — and it needs no keystore at all.

`credentialsSecretRef` stores a username and password on the alias. Neo4j
encrypts them in the system database, which is why it requires
`spec.remoteAliasKeystore`; without one the server refuses alias creation
outright. The operator passes the password as a **Cypher parameter**, never
interpolated into the statement text, so it does not reach the query log.
`SHOW ALIASES` never returns a password on any alias.

A composite with a `credentialsSecretRef` constituent on a deployment that has
no keystore is rejected at apply time, naming the field to set — rather than
failing later inside the server with an internal `50N09`/`50N00` error that
mentions neither the CR nor the constituent.

## Ordering: the composite always comes first

Neo4j accepts `CREATE ALIAS cineasts.latest` **even when no composite
`cineasts` exists**. The result is an ordinary alias whose name merely contains
a dot — listed by `SHOW ALIASES`, and even queryable. But the composite can
then never be created:

```
42N87: The database or alias name `cineasts` conflicts with the name
       `cineasts.latest` of an existing database or alias.
```

This CR owns the composite and its constituents together, which is what makes
the ordering guaranteed rather than hoped for. If a stray dotted alias already
occupies the namespace, the CR reports `Failed` with reason
`CompositeDatabaseNameBlocked` and names the alias to drop — the server's own
error never mentions ordering.

## Deletion

`deletionPolicy: Delete` issues `DROP COMPOSITE DATABASE <name> CASCADE
ALIASES`. `CASCADE ALIASES` is **not optional**: a plain drop is refused while
constituents exist. It removes the constituent **aliases** only — the databases
they target are untouched.

`deletionPolicy: Retain` leaves everything in Neo4j and releases the finalizer.

If the referenced deployment is gone or not Ready at deletion time, the
finalizer is released anyway rather than wedging the CR in `Terminating` for a
database that may no longer exist.

## Privileges do not go on the composite

This is the most common mistake, and Neo4j does not warn about it:

```cypher
-- accepted, persisted, and does NOTHING
GRANT MATCH {*} ON GRAPH cineasts NODES * TO analyst
```

Graph privileges belong on the **constituent target databases**. A user reading
through a composite needs `ACCESS` on the composite *and* `ACCESS` plus the
graph privileges on each constituent's target. A `Neo4jRole` whose privileges
name a composite reports `PrivilegesResolve=False`.

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | `Pending`, `Creating`, `Ready`, `Failed`. |
| `message` | `string` | Human-readable detail for the current phase. |
| `observedConstituents` | `[]string` | The constituent list **read back from the server**, fully qualified (`cineasts.latest`). This is what the DBMS actually exposes, which legitimately differs from spec when `enforceConstituents` is `false`. |
| `observedGeneration` | `int64` | The `metadata.generation` this status reflects. |
| `conditions` | `[]Condition` | See below. |

### Conditions

| Type | Reasons | Meaning |
|---|---|---|
| `Ready` | `CompositeDatabaseReady`, `CompositeDatabaseFailed`, `CompositeDatabaseNameBlocked`, `ClusterNotReady`, `ConnectionFailed`, `ValidationFailed` | True when the composite exists and its constituents match spec. |
| `ClusterNotReady` | `ClusterNotReady`, `ClusterReady` | Mirrors the readiness of the referenced deployment. |

## Limitations

- **Remote constituents need setup.** OIDC credential forwarding requires Cypher 25 (CalVer only). Stored native credentials require `spec.remoteAliasKeystore` on the deployment — without it Neo4j refuses to create the alias.
- **No `OPTIONS`.** Composites have none; the operator never emits the clause.
- **No topology.** A composite has no store to place.
- Composites cannot be nested, and cannot hold data themselves.
