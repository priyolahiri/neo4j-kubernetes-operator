# `export`

Authors a downstream manifest whose input lives in an **upstream resource's status** — for a downstream on a *different* Kubernetes cluster.

```bash
kubectl neo4j export replica-database dr-copy \
    --from-backup nightly --cluster-ref dr --upstream-database neo4j > replica.yaml

kubectl neo4j export replica-database dr-copy \
    --from-cluster prod --cluster-ref dr --upstream-database neo4j > replica.yaml
```

## Why it is this narrow

Several status fields on this operator's CRs exist purely to be pasted into another resource's spec. `Neo4jBackup.status.replicationPullURI` is declared with the comment that assembling it by hand *"is the single most likely thing for a user to get wrong, so the operator publishes it instead."*

Within **one** Kubernetes cluster the operator already closes that loop itself:

- `Neo4jReplicaDatabase.spec.source.upstreamBackupRef` resolves the pull URI live from the upstream `Neo4jBackup`
- `spec.source.upstreamClusterRef` resolves network addresses live from the upstream cluster's `status.internalAddresses`

**Prefer those refs on the same cluster.** They stay correct when the upstream changes; a generated literal does not.

What neither can do is cross a cluster boundary — both resolve through a `Get` against their own API server, and the CRD says so in as many words: for an upstream on a different Kubernetes cluster, *"paste it from that CR's own status by hand."* That paste is what this command replaces, and it is the whole of its scope.

## What it emits

A `Neo4jReplicaDatabase` manifest on **stdout**, and nothing else — so it can be redirected to a file or piped into `kubectl apply --context other-cluster` without stripping anything. Notes and warnings go to stderr.

```
$ kubectl neo4j export replica-database dr-copy --from-backup nightly \
      --cluster-ref dr --upstream-database neo4j
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaDatabase
metadata:
  name: dr-copy
  namespace: neo4j
spec:
  clusterRef: dr
  upstreamDatabase: neo4j
  source:
    mode: backup
    pullURI: s3://prod-backups/nightly-chain/
```

with, on stderr:

```
source.pullURI taken from Neo4jBackup neo4j/nightly status.replicationPullURI
note: the upstream reads its bucket with Secret "cloud-creds". The DOWNSTREAM cluster
      needs its own credentials for the same bucket — set source.credentialsSecretRef
      there, or bind a workload identity. This command cannot copy a Secret between
      clusters.
```

### `--seed-from-latest`

Off by default. When set, `source.seedURI` is built by joining the newest **successful** run's `artifactFilename` onto the pull URI. If no successful run carries one — the operator parses that name out of the Job's pod log, so it can be missing even on a run that worked — the command **fails rather than reconstructs it**. A replica can seed from the chain in `pullURI` alone.

### Network mode and the in-cluster trap

`--from-cluster` prefers `status.crossClusterReplication.addresses`, which are routable off-cluster. If only `status.internalAddresses` exist, it emits them **with a warning**: those are in-cluster DNS names that resolve across namespaces on one Kubernetes cluster and not from a separate one. The warning names both fixes — enable `spec.crossClusterReplication` on the upstream for a real second cluster, or use `upstreamClusterRef` if the downstream is on this one after all.

## Validated by construction

Every manifest is run through the operator's **own `ReplicaValidator`** before it is printed. A command that emitted something [`validate`](validate.md) would then reject would be worse than no command — so if validation fails, nothing is written and the failure is reported as a bug worth filing.

## See also

- [`validate`](validate.md) — the same validators, run against manifests you wrote
- [Cross-Cluster Replication](../guides/cross_cluster_replication.md) — when you need the proxy, and why
