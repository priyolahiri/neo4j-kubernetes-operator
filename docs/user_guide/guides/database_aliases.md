# Database aliases

A **database alias** is a second name for an existing database. Applications
connect to the alias; the operator controls what it points at. Re-pointing an
alias is instant and requires no client change.

`Neo4jDatabaseAlias` manages local aliases declaratively — created, re-pointed
on drift, and dropped with the CR.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabaseAlias
metadata:
  name: app
  namespace: neo4j
spec:
  clusterRef: prod-cluster
  targetDatabase: app-v2
```

```bash
kubectl get neo4jdatabasealias -n neo4j
# NAME   CLUSTER        TARGET   PHASE   READY   AGE
# app    prod-cluster   app-v2   Ready   True    30s
```

Connect with `neo4j://…/app` and you reach `app-v2`.

---

## Why aliases matter here

**Cypher has no `RENAME DATABASE`.** `ALTER DATABASE` can change access mode,
topology, default language and options — but not the name. Once a database is
created, its name is permanent, and the only "rename" available is
create-copy-drop.

An alias is the supported way to decouple *the name applications use* from *the
name the database has*.

---

## Use cases

### Blue/green database swaps

Rebuild a dataset alongside the live one, then cut over atomically:

```yaml
# Applications only ever address `app`.
spec:
  clusterRef: prod-cluster
  targetDatabase: app-blue     # change to app-green to cut over
```

Change `targetDatabase`, and the operator issues
`ALTER ALIAS … SET DATABASE TARGET`. No connection strings change, no
application redeploy. Roll back by changing it back.

### Stable names across a rebuild

Import pipelines that produce `catalog-2026-08-01`, `catalog-2026-08-08`, … can
keep applications on `catalog` and re-point the alias when a new build passes
validation. The dated databases remain available for rollback.

### Environment-neutral connection strings

The same manifest set can address `graph` in every environment, with the alias
resolving to `graph-dev`, `graph-staging` or `graph-prod` per cluster — without
templating the database name into every client.

### Cross-cluster replication failover

The case that motivated this CRD. A replica created as `foo-replica` keeps that
name through promotion, so DR clients would otherwise have to change connection
strings mid-incident. An alias avoids that entirely:

| | `foo` on the DR cluster resolves to | Clients get |
|---|---|---|
| Steady state | `foo-replica`, a read-only replica | reads |
| After promotion | `foo-replica`, now a standard database | reads **and writes** |

Aliases can target a database that is **still a replica**, so create it at
replica-setup time. Nothing needs running inside the failover window — the same
connection string silently gains write capability when promotion completes.

See the [Cross-Cluster Replication guide](cross_cluster_replication.md).

---

## Behaviour

**Drift is reconciled.** If someone re-points the alias out of band, the
controller restores `spec.targetDatabase` on the next loop. `spec` is
authoritative.

**The target need not exist yet.** The CR reports `Pending` and retries, so an
alias and the database it fronts can be applied together in one `kubectl apply`.

**Dropping an alias never touches the database behind it.** `deletionPolicy:
Delete` (the default) removes only the alias; `Retain` leaves even that in
place and just releases the finalizer.

**Observed state is published.** `status.observedTarget` is read back from
`SHOW ALIASES FOR DATABASE`, so you can see what the alias actually resolves to
rather than only what was requested.

---

## Restrictions

| Rule | Why |
|---|---|
| The alias name may not equal its target | Neo4j rejects it, and it is never what was meant |
| `system` can be neither aliased nor used as an alias name | Not permitted by Neo4j |
| Names may not contain a backtick | Alias DDL takes no query parameters for identifiers, so names are interpolated; the validator rejects backticks as an injection guard |
| Local aliases only | Remote aliases (`… AT '<url>' USER … PASSWORD …`) and composite-database constituents are not modelled by this CRD |

Alias names follow the same rules as database names: 3–63 characters, starting
with an ASCII letter.

---

## Aliases and privileges

Privileges are granted on the **database**, not the alias. Granting access on
`app-v2` is what lets a user reach it through the `app` alias — there is no
separate privilege surface to maintain, and re-pointing an alias does not
require any privilege change.

If you re-point `app` from `app-blue` to `app-green`, make sure the relevant
roles hold privileges on `app-green` *before* the cutover, or users will lose
access at exactly the wrong moment. `Neo4jRole` makes this a spec change on the
role rather than a manual `GRANT`.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Pending`, "target database … does not exist yet" | The target has not been created. Normal when applying alias and database together. |
| `Pending`, cluster not Ready | Cluster still bootstrapping. |
| `Failed`, "an alias cannot target a database of the same name" | `spec.targetDatabase` equals the alias name (which defaults to `metadata.name`). |
| Alias keeps reverting | Expected — `spec.targetDatabase` is authoritative and out-of-band changes are corrected. |

```bash
kubectl describe neo4jdatabasealias app -n neo4j
kubectl get neo4jdatabasealias app -n neo4j -o jsonpath='{.status.observedTarget}'
```

Verify directly in Neo4j:

```cypher
SHOW ALIASES FOR DATABASE YIELD name, database, location;
```

## See also

- [Neo4jDatabaseAlias API reference](../../api_reference/neo4jdatabasealias.md)
- [Cross-Cluster Replication](cross_cluster_replication.md)
- Neo4j Operations Manual → Database administration → *Managing database aliases*
