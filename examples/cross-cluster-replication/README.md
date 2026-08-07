# Cross-Cluster Replication (CCDR) Examples

Replicating a database from one Neo4j cluster to a read-only replica on
another, for disaster recovery.

Apply these **in order**, across **two** Kubernetes clusters. Files 01 goes on
the upstream; 02–04 go on the downstream.

| File | Cluster | What it does |
|---|---|---|
| `01-upstream-backup-chain.yaml` | upstream | `Neo4jBackup` with `mode: replication-source` — produces the differential chain and publishes `status.replicationPullURI` |
| `02-downstream-replica.yaml` | downstream | `Neo4jReplicaDatabase` — the read-only replica, seeded from and following that chain |
| `03-failover-alias.yaml` | downstream | `Neo4jDatabaseAlias` — the stable name clients use, created **before** any failover |
| `04-promotion.yaml` | downstream | `Neo4jReplicaPromotion` — the one-way failover action. Apply only during an actual failover |

## What you need

- **Downstream cluster on Neo4j 2026.08+** (enforced by the operator).
- Upstream on 2025.01+ (not enforced — the operator cannot inspect another cluster).
- Object storage (S3/GCS/Azure) reachable from **both** Kubernetes clusters.

**No network path between the two Kubernetes clusters is required.** Backup-based
replication needs no load balancer, no cross-cluster TLS trust, and no
NetworkPolicy changes. `source.mode: network` is *not* supported by this
operator — see the guide for why.

## The three things people get wrong

1. **`pullURI` assembled by hand.** Read it from the upstream backup CR:
   ```bash
   kubectl get neo4jbackup inventory-chain -n prod -o jsonpath='{.status.replicationPullURI}'
   ```
2. **A bucket lifecycle rule expiring the chain.** The operator cannot see
   lifecycle rules. One that expires objects under the chain path breaks
   replication and forces a full rebuild. Exclude the path from expiry.
3. **Creating the alias during the outage.** Aliases can target a database
   that is still a replica — create it at setup time (file 03) so nothing has
   to be run inside the failover window.

## Also remember

- Privileges and roles are **not** replicated. Apply the same `Neo4jUser` /
  `Neo4jRole` / `Neo4jRoleBinding` CRs to the downstream cluster.
- Restoring the **upstream** database detaches every replica — the internal
  database ID changes and the replicas must be rebuilt.
- Promotion is **irreversible** and makes the outstanding replication lag
  permanent data loss.

## See also

- [Cross-Cluster Replication guide](../../docs/user_guide/guides/cross_cluster_replication.md)
- [Database aliases guide](../../docs/user_guide/guides/database_aliases.md)
