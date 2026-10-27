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
| `Staging` | StatefulSet frozen (partition = replica count), new image applied to the template. Before it, the version check and the pre-upgrade health check; if one refuses, nothing changes on the servers (see [When an upgrade stops](#when-an-upgrade-stops-paused-and-failed)) |
| `Rolling` | Pods restart one at a time, highest ordinal first; after each pod, Kubernetes readiness, Neo4j cluster membership (`SHOW SERVERS`) **and** that pod's databases (`dbms.cluster.statusCheck`, `SHOW DATABASES`) are verified before the partition is lowered. While a server is unavailable — the one restarting, or one that cannot start on the target — the cluster's `Degraded` condition is `True` and names it; the phase stays as it is |
| `Stabilizing` | All pods updated; the operator waits for consistently stable cluster health and consensus |
| `Verifying` | Every server's reported version is checked against the target before the upgrade is declared done |
| `Completed` | Every server verified on the target; `status.version` is the target |
| `Failed` / `Paused` | A check or a step's timeout failed: `Paused` with `autoPauseOnFailure: true`, `Failed` otherwise. The servers stay where the upgrade left them — see [When an upgrade stops](#when-an-upgrade-stops-paused-and-failed) |

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
upgrade to finish. An `UpgradeRetargeted` event says so, once per new target
("Rolling upgrade retargeted from … to … during Rolling"). A new target below the version the cluster ran before the
upgrade (`status.version`) is refused instead, and so is one below the current
target once a server the upgrade restarted has started on it: that server would
be rolled back (see [Version checks](#version-checks)).

A server that has moved on to the intermediate version never comes back on the
version before the upgrade, even if its pod is recreated (a node drain, an
eviction) while the upgrade is being redirected. Kubernetes recreates a server
the upgrade has not released yet from the version the cluster ran before the
upgrade, so the new target is staged at the lowest server that runs the
intermediate version, not with every server frozen:

- The servers already on the intermediate version are moved on to the new
  target by Kubernetes, highest first, each once the one before is ready. Only
  Kubernetes readiness is checked between them — a server pod is ready once
  Neo4j reports it available in the cluster, or after its first 5 minutes
  (see Pod Readiness in [Clustering](../clustering.md)); the upgrade's own checks (the
  server back in the cluster, its databases caught up) apply from the lowest of
  them down, as for every other server. Usually only one server has moved on
  when you redirect, and it gets the full checks.
- That write restarts servers at once, so it waits until every other server is
  ready.
- If every server already runs the intermediate version (the walk had finished
  rolling), the operator waits for Kubernetes to record it as the StatefulSet's
  current revision, then stages the new target with every server frozen, and
  every server gets the full checks.

The same applies when you redirect a `Paused` or `Failed` upgrade.

**Version verification across SemVer/CalVer.** The `Verifying` phase compares
each server's `SHOW SERVERS` version against the image tag, normalizing
Neo4j's kernel alias (the `2025.01.x` CalVer releases report kernel `5.27.x` —
both are accepted as the same version). A tag that names a patch must match
it exactly. A tag without one, such as `2026.08-enterprise` or
`5.26-enterprise`, is the newest patch of its line (on Docker Hub,
`2026.08-enterprise` is the same image as `2026.08.1-enterprise`), so servers on
any patch of that line pass: `2026.08.1` passes for `2026.08-enterprise`,
`2026.09.0` does not.

**Image changes while the cluster is not Ready are deferred.** Changing
`spec.image` while the cluster is `Forming`, `Degraded`, mid-restore, or
otherwise not `Ready` — or while it is `Ready` but a server is unavailable (its
`Degraded` condition is `True`) — does not roll pods immediately: the operator
holds the change (you'll see an `UpgradeDeferred` event on the cluster once the
hold has lasted more than one reconcile) and the
rolling-upgrade state machine performs it once the cluster is `Ready` with every
server available — a version change must never sneak in as an ungated restart
while quorum is still forming, and rolling the other servers while one is down
could cost the cluster its majority. The hold also covers a storage expansion
made meanwhile: the expansion deletes the StatefulSet (leaving its pods
running), and the StatefulSet is created again on the image the servers run, not
the new one.

A storage expansion creates the StatefulSet again **as it was**, in the same
reconcile, with the new volume claim templates: the same pod template — the
plugins' settings, init containers and volumes included — the same replica
count, and a held upgrade's partition. No server restarts for it. Changes made
together with the expansion (a new image, a plugin) are applied by the next
reconcile, with their usual checks. If the operator restarts between the delete
and the create, it reads the template from the revision the orphaned pods run.

While an upgrade is held, the servers below its partition run another version
than the StatefulSet's template, and Kubernetes would create any of them that
restarts from a new StatefulSet's template — the upgrade's target, without its
checks. So the operator creates the StatefulSet in two steps: first on the
template those servers run, with every server frozen, then, once Kubernetes has
recorded that as the StatefulSet's current revision, with the held template and
partition back. This takes a few seconds; nothing restarts, and an upgrade
resumed meanwhile waits for it. Every server that restarts afterwards comes back
on the version it ran.

One deliberate exception: if **no server pod is ready** — the typical case
being a first deploy stuck in `ImagePullBackOff` on a mistyped tag, but also a
cluster that has lost every server — there is no formed quorum to protect, so
a corrected `spec.image` applies immediately, without the upgrade state
machine. Fix the tag and the pods re-pull; you do not need to delete and
recreate the cluster. The tag is still checked against `status.version` first
(see [Version checks](#version-checks)), and once every server is ready on the
new image the operator records it as `status.version`.

### When an upgrade stops: Paused and Failed

When a check fails or a step runs past its timeout, the upgrade stops: `Paused`
when `autoPauseOnFailure` is `true`, `Failed` otherwise.
`status.upgradeStatus.lastError` says why, and an `UpgradePaused` or
`UpgradeFailed` Warning event is emitted, once per target and cause. The
cluster's phase does not change: a stopped upgrade is not a failed cluster. It
carries the `Stalled` condition (`True`, reason `UpgradeFailed` or
`UpgradePaused`) until the upgrade goes on, so GitOps health checks see that
the image change did not land: Flux reads it as Failed, and the published Argo
CD check reads the upgrade's phase (see [GitOps](../../gitops/README.md)).

In both, **the servers stay where the upgrade left them.** The StatefulSet keeps
the partition the upgrade had reached: the servers it rolled run the target, and
the ones it had not reached keep the version they run (`status.version`). No
server restarts onto the target except through the upgrade's own per-server
checks (rejoin and database catch-up), so a cluster stopped half-way runs two
versions until the upgrade goes on, as it does at every step of a rolling
upgrade. The
rest of the cluster's reconciliation keeps running: configuration, services,
diagnostics, and the cluster's `Ready` status, which users, roles, databases and
backups wait for. Once every server is available the cluster is `Ready`, and
`status.message` says the upgrade is paused or failed. A server that is down is
reported as for any cluster: `Ready` with the `Degraded` condition, then
`Degraded` after the grace period.

**Refused before it starts.** The version check and the pre-upgrade health check
run before anything changes on the servers. If one of them refuses, the cluster
stays `Ready` — its `status.message` names the stopped upgrade — and the event is
emitted once for that refusal, not again while the same check keeps refusing
with the same error. A `Failed` one is tried again every minute, and the rest of
the cluster's reconciliation runs in between; a `Paused` one waits for you, as
below. If you set the tag back to the version the servers run instead, the
operator drops the record of the upgrade that never started, and the message no
longer mentions it.

What happens next:

- **Failed:** the operator retries the upgrade itself once the cluster is
  `Ready` with every server available. The retry starts from where it stopped:
  the servers already on the target are checked again without a restart, and
  the rest are rolled one at a time through the same checks. A retry that stops
  again with the same cause is not announced again (`lastError` keeps it), and
  the cluster stays `Ready` throughout, so there is no `ClusterReady` on each
  retry. Each retry waits a full step timeout before it stops, and restarts no
  server it already rolled. While a server is down — one the upgrade restarted
  that cannot start on the target, say — the upgrade is not retried and the
  cluster is reported as for any server short: `Ready` with the `Degraded`
  condition `True` naming the server, then phase `Degraded` once it has been
  short for longer than the grace period (5 minutes by default). The retry
  follows once every server is back. A retry that the pre-upgrade check
  refuses is tried again every minute, with the cluster's reconciliation
  running in between.
- **Paused:** the upgrade waits for you. Investigate with `lastError`, then
  either:
  - set `spec.image.tag` to another target (a later version, or the same line
    once you have a fixed image): the upgrade resumes towards it; or
  - set `spec.upgradeStrategy.autoPauseOnFailure` to `false`: the operator
    retries the upgrade as it retries a `Failed` one. Set it back to `true`
    afterwards if you want the next failure to pause again.

  Either way the upgrade resumes once the cluster is `Ready` with every server
  available, and the `UpgradeStarted` event says it resumed. While it rolls,
  `status.message` says the upgrade is in progress.
- **A server that cannot start on the target.** When the servers the upgrade
  restarted cannot start on its target — the image cannot be pulled (a typo, a
  tag not published yet), or Neo4j keeps exiting on it — the cluster is short of
  them, so it is not `Ready` with every server available. Set `spec.image.tag`
  to another target (a fixed image, or a later version): the upgrade goes on
  towards it straight away and replaces those servers first — they are down, so
  no server that is up restarts — then rolls the rest one at a time through the
  usual checks. The fixed image may be earlier than the target when no server
  ever started on the target (an image that cannot be pulled never runs; see
  [Version checks](#version-checks)): an upgrade paused on
  `2026.09.9-enterprise`, a tag that was never published, goes on towards
  `2026.09.0-enterprise`. The `UpgradeStarted` event names the servers it
  replaces, and while the cluster is short of them `status.message` says the
  upgrade is in progress and that they are replaced first. Each server is
  replaced once, on the new tag. Where the StatefulSet does not replace a
  server that is down itself (Kubernetes 1.35.0–1.35.3, with
  `MaxUnavailableStatefulSet` on), the operator deletes its pod, with one
  `ServerPodReplaced` event, and the StatefulSet creates it on the new tag. This applies only when the servers that are
  down are the highest-numbered ones, every one of them failing to start on the
  image it runs — one the upgrade restarted, or one running another version
  than the servers that are up — and Neo4j reports no other server unavailable;
  otherwise the upgrade waits until every server is back. Setting the tag those
  servers already run does not replace them: the same image cannot start there
  either. A `Paused` upgrade whose tag you have not changed waits for you.
- **Going back:** you can set the tag back to `status.version` only while no
  server has started on the target (the upgrade stopped while staging, before
  its first server restarted, or the restarted servers never started because
  their image could not be pulled); the upgrade then runs to that version,
  which every server that is up still runs. Once a server has started on the
  target, a tag below the target is refused (see [Version checks](#version-checks)):
  Neo4j does not support downgrades, and going back means restoring a backup
  taken before the upgrade.
  For a target without a patch, such as `5.26-enterprise`, "below the target"
  means below the patch the restarted servers report, which the operator
  records in `status.upgradeStatus.resolvedTargetVersion` (see
  [Tags without a patch](#tags-without-a-patch)).

**The number of servers waits for the upgrade.** A rolling upgrade changes no
topology. A change of `spec.topology.servers` made while an upgrade is in
progress, or while a `Paused` or `Failed` one has restarted some of the servers
(or staged its target), is not applied until the upgrade completes: added
servers would start on the target, and a scale-down's drain would run
mid-upgrade. The `TopologyChangeDeferred` condition says so, with both counts.
Once the upgrade completes, the change is applied as usual: a scale-down
drains its servers first.

### Version checks

Every reconcile checks `spec.image.tag` against `status.version`, the version every server runs. It is recorded when the cluster first becomes `Ready`, after each verified upgrade, and when the cluster becomes `Ready` with every server on an image that reached them without the upgrade state machine (the no-ready-server exception above). It does not move while an upgrade is in flight or after one is `Paused` or `Failed`.

So while an upgrade is unfinished, `status.version` is the version of the servers it has not rolled. The servers it has rolled run its target (`status.upgradeStatus.targetVersion`) once they start on it, so the tag is also checked against what they started on: the operator records it in `status.upgradeStatus.startedTargetVersion`, which is `status.version` when the upgrade starts and becomes the target once a restarted server's Neo4j container has started on it — it runs, or has exited after running (a crash loop counts). A container still waiting on an image it cannot pull (`ErrImagePull`, `ImagePullBackOff`, `InvalidImageName`) or one that could not be created (`CreateContainerConfigError`) has not started. A later target, or the version a server started on, is allowed; anything below it, `status.version` included, is refused, because it would roll those servers back:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "2026.08.1-enterprise": downgrades are not supported: the upgrade to 2026.09.0-enterprise (status.upgradeStatus.targetVersion) has already restarted servers on it, and Neo4j cannot go back to an earlier version. Set the tag to 2026.09.0-enterprise or later; running an earlier version means restoring a backup taken before the upgrade
```

When a server started on an earlier target and the upgrade was then pointed at a later one, the message names the earlier one and the field:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "2026.08.1-enterprise": downgrades are not supported: servers this upgrade restarted have run 2026.09.0-enterprise (status.upgradeStatus.startedTargetVersion), and Neo4j cannot go back to an earlier version. Set the tag to 2026.09.0-enterprise or later; running an earlier version means restoring a backup taken before the upgrade
```

Before a server has started on the target (the upgrade stopped while staging, before its first server restarted, or with the restarted servers unable to pull the image), any tag from `status.version` up is allowed: an earlier fixed tag, or `status.version` itself. An upgrade a previous operator release paused or failed has no `startedTargetVersion`; until a server starts on its target, it keeps that release's rule — once the upgrade has restarted a server, the tag is held to the target.

#### Plugins that cannot follow

A change of `spec.image.tag` is also refused while a `Managed` `Neo4jPlugin` of the cluster (or standalone) runs on the image the servers run and cannot run on the new one: the new image's entrypoint does not list its name (`n10s` on CalVer from 2025.05), or the JAR it downloads for it does not run there (`n10s` from 5.26.22). Every server the upgrade restarted would not start. Nothing rolls; the refusal names the plugin:

```
Update validation failed: spec.image.tag: Invalid value: "2026.08.1-enterprise": Neo4jPlugin semantics cannot follow this image change from 5.26.21-enterprise, which it runs on: its spec.name "n10s" is not a plugin the neo4j:2026.08.1-enterprise image can install: … The servers would not start on the new image. Change the plugin first — delete it, set spec.enabled: false, or move it to installMode: VerifiedDownload with a JAR built for 2026.08.1-enterprise — and the image change goes ahead; a server the change restarts before the plugin's own write is replaced by that write
```

Leave the tag as it is. A deleted plugin is taken off at once, before the upgrade restarts a server. A disabled or `VerifiedDownload` plugin writes its change only once the cluster is `Ready`, so the upgrade starts first: its first server comes up on the new image with the old name and fails, and the plugin's change — which a server failing on it does not wait for — replaces that server within about a minute (one `PluginSettingsFailing` Warning on the plugin), after which the upgrade goes on; each server restarts once. Setting the tag back to change the plugin on the old image is not advised: a JAR built for the new release need not run there. A `VerifiedDownload`, `PreBaked` or disabled plugin is not judged, and neither is a plugin the running image already cannot run (it is refused and taken off by itself; see the [plugins guide](plugins.md#which-plugins-managed-installs)).

#### Tags without a patch

`status.version` and `status.upgradeStatus.targetVersion` are image tags. A tag without a patch, such as `5.26-enterprise` or `2026.08-enterprise`, floats to the newest patch of its line, so it does not say which patch the servers run, and a patch of the same line cannot be ordered against it. For such a tag the operator records the version the servers report (`SHOW SERVERS`, `version`), and the tag is held to it too:

| Field | Records | When |
|---|---|---|
| `status.runningVersion` | What the servers report for a `status.version` without a patch (the highest, if they differ). Empty when `status.version` names a patch | On a `Ready` pass, from the `SHOW SERVERS` the formation check already runs (no extra query), written when it changes; and when an upgrade completes. A standalone asks once it is `Ready`, and again only when its container runs another image |
| `status.upgradeStatus.resolvedTargetVersion` | What the servers an upgrade restarted report, for a target without a patch | When a restarted server passes the upgrade's availability check, or, for a `Paused` or `Failed` upgrade, at the next `Ready` |

So after an upgrade to `5.26-enterprise` that paused once server-2 ran 5.26.32, setting the tag back to `5.26.31-enterprise` is refused:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "5.26.31-enterprise": downgrades are not supported: the upgrade to 5.26-enterprise (status.upgradeStatus.targetVersion) has already restarted servers on it, and they run 5.26.32 (status.upgradeStatus.resolvedTargetVersion); Neo4j cannot go back to an earlier version. Set the tag to 5.26.32 or later; running an earlier version means restoring a backup taken before the upgrade
```

and once it has completed, the same tag is refused against what every server runs:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "5.26.31-enterprise": downgrades are not supported: the servers run 5.26.32 (status.runningVersion, what 5.26-enterprise in status.version resolved to), and Neo4j cannot go back to an earlier version. Set the tag to 5.26.32 or later; running an earlier version means restoring a backup taken before the upgrade
```

`5.26.32-enterprise`, a later patch, `5.26-enterprise` itself and any later line are accepted. Moving from `5.26-enterprise` to `5.26.32-enterprise` changes the server pod template, so it runs as an upgrade and every server restarts once, even when both tags are the same image. There is no way round that restart: Kubernetes restarts a container whose image reference changes, whatever image it resolves to, and the operator cannot tell what a tag resolves to without asking the registry, with its credentials, before any server pulls it. To avoid the extra restart, keep the tag without a patch until your next upgrade, and name a patch from then on.

When an upgrade starts, the operator also asks the servers what they run, and refuses a tag below the highest version one of them reports (the upgrade turns `Paused` or `Failed` before anything changes, and `lastError` names the version). This catches a patch the servers run that the operator has not recorded yet.

A `Neo4jEnterpriseStandalone` is held to the same rule. Its `status.version` is recorded once its pod is ready on the current image, so it does not move while the pod is being replaced, nor for an image that never starts: setting the tag back to the version the server runs is always allowed. A downgrade is refused the same way (the standalone turns `Failed` with a `ValidationFailed` event saying `the standalone runs …`), and the server keeps the version it runs.

A tag the cluster cannot move to (see [Supported Upgrade Paths](#supported-upgrade-paths)) is refused before anything changes: the cluster turns `Failed`, with a `ValidationFailed` event and a message that names the field and what the servers run:

```
Update validation failed: validation failed: spec.image.tag: Invalid value: "2025.10.0-enterprise": downgrades are not supported: the servers run 2026.08.1-enterprise (status.version), and Neo4j cannot go back to an earlier version. Set the tag to 2026.08.1-enterprise or later; running an earlier version means restoring a backup taken before the upgrade
```

The servers keep running the version they ran; nothing else in the spec is applied until the tag is fixed. Set it back and the next reconcile carries on. The check applies mid-upgrade too: a new target below the version the servers ran before the upgrade, or below the current target once a server runs it, is refused rather than rolled, and the upgrade waits where it was until the tag is fixed.

**Tags pinned to a digest.** A tag may carry the digest it was pinned to, such as `2026.08.1-enterprise@sha256:…`. The version is the tag's (2026.08.1 here), for the version checks as for everything else, and the `app.kubernetes.io/version` label of the objects the operator creates carries the tag without the digest, cut to the 63 characters a label value allows.

`kubectl neo4j validate --connect` runs the same check against the running cluster before you apply (see [`validate`](../cli/validate.md#checking-cross-references-with-connect)). Offline it cannot: a manifest does not say what the cluster runs.

Not checked:

- **No `status.version` yet** — a cluster that has not been `Ready` since the operator was installed. There is nothing verified to compare with; the operator records the version at the next `Ready`, for a cluster created by an older release too.
- **The patch of a tag without one, before the operator has read what the servers run.** Moving from `5.26-enterprise` to `5.26.12-enterprise` (or back) is checked against `status.runningVersion` or `status.upgradeStatus.resolvedTargetVersion` once they are recorded (see [Tags without a patch](#tags-without-a-patch)), and, when an upgrade starts with `preUpgradeHealthCheck` on, against what the servers report then. Not checked: a target changed while an upgrade is rolling, before the server it restarted has passed its availability check (the operator records what it runs there); and an upgrade started with `preUpgradeHealthCheck: false` before the first `Ready` pass under an operator that records them. The `Verifying` phase accepts any patch of the line for such a tag (see [Version verification](#upgrade-phases-and-resumability)).
- **A patch Neo4j changed without a new tag, on a standalone.** A floating tag pulled again (`imagePullPolicy: Always`, or a new node) can start a server on a newer patch. A cluster records it on its next `Ready` pass, and checks the servers again when an upgrade starts; a standalone records it on the pass after its container restarts on the new image, and a tag set before then is not refused.

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

When `preUpgradeHealthCheck` is enabled, the operator checks the cluster before changing the image: a majority of servers available, a majority enabled, a leader, and no server running a version above the target. If the check fails and `autoPauseOnFailure` is `true`, the upgrade is `Paused` before anything changes, rather than proceeding against an unhealthy deployment; resume it as described in [When an upgrade stops](#when-an-upgrade-stops-paused-and-failed). Without `autoPauseOnFailure` it is `Failed`, and tried again every minute while the cluster is `Ready` with every server available. Either way the cluster stays `Ready`.

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
