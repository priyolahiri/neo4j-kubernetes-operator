# Upgrades

This guide explains how to upgrade your Neo4j Enterprise clusters.

## Rolling Upgrades

The operator supports rolling upgrades to minimize downtime. To upgrade your cluster, update the `spec.image.tag` field in the `Neo4jEnterpriseCluster` resource to a newer version:

```bash
kubectl patch neo4jenterprisecluster <name> \
  --type=merge \
  -p '{"spec":{"image":{"tag":"2025.01.0-enterprise"}}}'
```

The operator performs a safe, ordered rolling upgrade:

1. **Freezes all pods** — sets the StatefulSet partition to block any automatic restarts.
2. **Restarts pods highest-ordinal-first** — pods `N-1`, `N-2`, … `1` are restarted in order. Kubernetes StatefulSet partition-based rolling always restarts pods from highest ordinal to lowest, so pod `0` is naturally the last to restart.
3. **Verifies the server and its databases after each pod** — after Kubernetes reports the pod Ready, the operator checks that the server is `Enabled`/`Available` in `SHOW SERVERS`, and then, on that server, that every one of its databases has caught up: `dbms.cluster.statusCheck` must report each clustered database replicating with the server's own copy `APPLYING`, and `SHOW DATABASES` must show every database on it at its requested status (which covers single-primary databases and secondaries). Only then does the next pod restart. A server is Available as soon as it rejoins, while its databases can still be starting or catching up; restarting the next server in that window would leave a database with three primaries one working copy. While the walk waits, `status.upgradeStatus.message` names the databases it is waiting for. A database that has not caught up within `upgradeTimeout` stops the upgrade (`Failed`, or `Paused` with `autoPauseOnFailure`), and `status.upgradeStatus.lastError` names it.
4. **Rolls pod 0 last** — the operator attempts to detect which pod holds the primary role for the `system` database (via `SHOW DATABASES`) and logs a warning if it is not ordinal 0. Ordinal 0 is always the last pod rolled regardless, so the system-database primary is preserved as long as possible.
5. **Waits for cluster stabilisation** — after all pods are updated the operator waits for the cluster health and consensus state to be consistently stable before marking the upgrade complete.

Writes stay available throughout. As a server shuts down, Neo4j hands the leadership of each database it leads to another server, so a restart does not leave a database waiting for an election. Clients still see brief, retryable interruptions around each restart, a few failed writes and pauses of a few seconds, so use the drivers' retrying transaction functions (`executeWrite` and equivalents), which ride over them. Longer pauses, of about 10 seconds, came from Neo4j's 30-second cluster connect timeout meeting a restarted server's new IP address; the operator lowers that timeout to 5 seconds (see [Writes while a restarted server rejoins](../clustering.md#writes-while-a-restarted-server-rejoins)).

If you have made databases read-only on individual servers with `dbms.setConfigValue('server.databases.read_only', …)`, upgrade with those settings removed. Neo4j can hand a leadership to a server that is read-only for that database, and that handover is lost: the database then has no leader for about 20 seconds (`dbms.cluster.raft.leader_failure_detection_window`).

Watch progress with:

```bash
# StatefulSet rollout progress
kubectl rollout status statefulset/<cluster>-server -n <namespace>

# Operator upgrade status
kubectl get neo4jenterprisecluster <name> -o jsonpath='{.status.upgradeStatus}'

# Each server's state, kept current during the upgrade
kubectl get neo4jenterprisecluster <name> \
  -o jsonpath='{range .status.diagnostics.servers[*]}{.name}{"\t"}{.health}{"\n"}{end}'

# Operator logs
kubectl logs -n neo4j-operator-system -l control-plane=controller-manager -f | grep -i upgrade
```

### Upgrade phases and resumability

The upgrade runs as a state machine whose state is persisted in
`status.upgradeStatus`, so you can follow it with `kubectl` and it survives
operator restarts:

| Phase | What's happening |
|---|---|
| `Staging` | Pre-upgrade health check, version-compatibility check, StatefulSet frozen (partition = replica count), new image applied to the template |
| `Rolling` | Pods restart one at a time, highest ordinal first; after each pod, Kubernetes readiness, Neo4j cluster membership (`SHOW SERVERS`) **and** that pod's databases (`dbms.cluster.statusCheck`, `SHOW DATABASES`) are verified before the partition is lowered |
| `Stabilizing` | All pods updated; the operator waits for consistently stable cluster health and consensus |
| `Verifying` | Every server's reported version is checked against the target before the upgrade is declared done |
| `Completed` / `Failed` / `Paused` | Terminal. `Paused` means a health check failed with `autoPauseOnFailure: true` — the cluster is left as-is for you to investigate |

Two status fields drive the machine and are useful when watching a long
upgrade: `currentPartition` (which StatefulSet partition step the rollout is
on — counts down toward 0) and `stepStartTime` (when the current step began;
each per-pod step is bounded by `upgradeTimeout`/`healthCheckTimeout`).

**Resumability.** Because the phase and partition are persisted in status, an
operator restart (or leader change) mid-upgrade resumes exactly where it left
off — already-rolled pods are not rolled again.

**Changing the target mid-upgrade.** If you patch `spec.image.tag` again while
an upgrade is running, the state machine retargets to the new image: pods not
yet rolled go straight to the new version, and pods already rolled to the
intermediate version are rolled again. You don't need to wait for the first
upgrade to finish. A new target below the version the cluster ran before the
upgrade (`status.version`) is refused instead (see [Version checks](#version-checks)).

**Version verification across SemVer/CalVer.** The `Verifying` phase compares
each server's `SHOW SERVERS` version against the image tag, normalizing
Neo4j's kernel alias (the `2025.01.x` CalVer releases report kernel `5.27.x` —
both are accepted as the same version).

**Image changes while the cluster is not Ready are deferred.** Changing
`spec.image` while the cluster is `Forming`, `Degraded`, mid-restore, or
otherwise not `Ready` — or while it is `Ready` but a server is unavailable (its
`Degraded` condition is `True`) — does not roll pods immediately: the operator
holds the change (you'll see an `UpgradeDeferred` event on the cluster) and the
rolling-upgrade state machine performs it once the cluster is `Ready` with every
server available — a version change must never sneak in as an ungated restart
while quorum is still forming, and rolling the other servers while one is down
could cost the cluster its majority.

One deliberate exception: if **no server pod is ready** — the typical case
being a first deploy stuck in `ImagePullBackOff` on a mistyped tag, but also a
cluster that has lost every server — there is no formed quorum to protect, so
a corrected `spec.image` applies immediately, without the upgrade state
machine. Fix the tag and the pods re-pull; you do not need to delete and
recreate the cluster. The tag is still checked against `status.version` first
(see [Version checks](#version-checks)), and once every server is ready on the
new image the operator records it as `status.version`.

### Version checks

Every reconcile checks `spec.image.tag` against `status.version`, the version every server runs. It is recorded when the cluster first becomes `Ready`, after each verified upgrade, and when the cluster becomes `Ready` with every server on an image that reached them without the upgrade state machine (the no-ready-server exception above). It does not move while an upgrade is in flight or after one fails — so setting the tag back to it, or on to a later target, is always allowed.

A `Neo4jEnterpriseStandalone` is held to the same rule. Its `status.version` is recorded once its pod is ready on the current image, so it does not move while the pod is being replaced, nor for an image that never starts: setting the tag back to the version the server runs is always allowed. A downgrade is refused the same way (the standalone turns `Failed` with a `ValidationFailed` event saying `the standalone runs …`), and the server keeps the version it runs.

A tag the cluster cannot move to (see [Supported Upgrade Paths](#supported-upgrade-paths)) is refused before anything changes: the cluster turns `Failed`, with a `ValidationFailed` event and a message that names the field and what the servers run:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "2025.10.0-enterprise": downgrades are not supported: the servers run 2026.08.1-enterprise (status.version), and Neo4j cannot go back to an earlier version. Set the tag to 2026.08.1-enterprise or later; running an earlier version means restoring a backup taken before the upgrade
```

The servers keep running the version they ran; nothing else in the spec is applied until the tag is fixed. Set it back and the next reconcile carries on. The check applies mid-upgrade too: a new target below the version the servers ran before the upgrade is refused rather than rolled, and the upgrade waits where it was until the tag is fixed.

`kubectl neo4j validate --connect` runs the same check against the running cluster before you apply (see [`validate`](../cli/validate.md#checking-cross-references-with-connect)). Offline it cannot: a manifest does not say what the cluster runs.

Not checked:

- **No `status.version` yet** — a cluster that has not been `Ready` since the operator was installed. There is nothing verified to compare with; the operator records the version at the next `Ready`, for a cluster created by an older release too.
- **A tag without a patch number.** `5.26-enterprise` and `2026.08-enterprise` float to the newest patch, so moving between `5.26.12-enterprise` and `5.26-enterprise` is not treated as a downgrade.

## Upgrade Strategy

Configure the upgrade strategy with `spec.upgradeStrategy`:

| Strategy | Description |
|---|---|
| `RollingUpgrade` (default) | Restarts one pod at a time; cluster stays available throughout |
| `Recreate` | On a cluster, does what `RollingUpgrade` does, and raises a `ValidationWarning` (see below). On a standalone, its one pod is deleted and created again on the new image, as with `RollingUpgrade` |

Neo4j documents only rolling upgrades for a cluster — one server at a time, or by adding new servers — on 5.x and on the 2025–2026 series ([Upgrade and Migration Guide](https://neo4j.com/docs/upgrade-migration-guide/current/)). Stopping every server at once and starting them on a new version is not a path it describes, so the operator does not take it: a cluster is always upgraded by the rolling-upgrade state machine above. `Recreate` used to stop the operator from starting that machine, so a `Ready` cluster kept its old image and no other change to its spec was applied while the tag differed; it also stopped every other change to the server pod template (configuration, plugins, TLS) from reaching a running server.

```yaml
spec:
  upgradeStrategy:
    strategy: RollingUpgrade
    preUpgradeHealthCheck: true   # validate cluster health before upgrading (default true)
    autoPauseOnFailure: true      # pause the upgrade for manual intervention if a check fails (default true)
    upgradeTimeout: 30m           # per-pod step: readiness, rejoin and databases caught up (default 30m)
    healthCheckTimeout: 5m        # post-upgrade version verification (Verifying) timeout (default 5m)
    stabilizationTimeout: 3m      # post-upgrade cluster-stabilization wait (default 3m)
    postUpgradeHealthCheck: true  # validate cluster health after upgrade (default true)
```

The three timeouts are Go durations above zero: `30m`, `1h30m`, `90s`. A value that does not parse, such as `"5 minutes"`, or one of zero or less, such as `"0s"` or `"-5m"`, is refused (the cluster turns `Failed` with a `ValidationFailed` event), and `kubectl neo4j validate` reports it before you apply. A step fails once its timeout has passed since it started, so at zero or less a step failed at its first check unless it was already done; an `upgradeTimeout` of zero or less failed every upgrade while staging.

Servers always restart one at a time, as Neo4j's rolling upgrade requires. `maxUnavailableDuringUpgrade` is accepted for compatibility but has no effect; a value other than `1` raises a `ValidationWarning` event.

When `preUpgradeHealthCheck` is enabled, the operator verifies connectivity to Neo4j before changing the image. If the check fails and `autoPauseOnFailure` is `true`, the upgrade is blocked until you intervene rather than proceeding against an unhealthy deployment.

For the full field reference see the [API Reference](../../api_reference/neo4jenterprisecluster.md).

## Supported Upgrade Paths

These follow Neo4j's [Upgrade and Migration Guide](https://neo4j.com/docs/upgrade-migration-guide/current/). Neo4j does not support downgrades: going back to an earlier version means restoring a backup taken before the upgrade, into a deployment that runs the earlier version.

| From | To | Supported |
|---|---|---|
| SemVer 5.26.x | SemVer 5.26.y (later patch) | ✅ |
| SemVer 5.x before 5.26 | SemVer 5.26.y | ✅ (the step Neo4j requires before CalVer) |
| SemVer 5.26.x | CalVer 2025.y / 2026.y | ✅ (only 5.26.x — the last SemVer LTS — may cross to CalVer) |
| SemVer 5.x before 5.26 | CalVer | ❌ (upgrade to 5.26 first) |
| CalVer 2025.x | CalVer 2025.y / 2026.y (newer minor, patch, or year) | ✅ |
| CalVer 2025.x | SemVer 5.y | ❌ (downgrade) |
| Any | earlier version | ❌ (downgrade) |
| Neo4j 4.x | anything | ❌ (4.4 to 5 is a migration, not an upgrade) |
