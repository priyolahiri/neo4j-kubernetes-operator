# Release Verification

The **pre-release verification journey**: a manual + LLM-driven walk of the real
user scenarios on a clean cluster, run from `main` before cutting any release.
It is the canonical source of truth for **what we verify, on which deployment,
at what size, and why** — keep it current as the product grows.

!!! info "Why this exists alongside the automated suites"
    Unit and integration tests ([Testing](testing.md)) validate the **code
    against itself**. This journey validates the **published instructions and
    end-to-end behaviour against a clean machine** — the only check that catches
    a doc that lies, a dead image tag, a tutorial ordering bug, or a
    CR-name/logical-name mismatch. Real passes have caught all of these
    (most recently the sharded `target.name` doc bug, a `#270` follow-up).

The [`verify-journey` skill](https://github.com/priyolahiri/neo4j-kubernetes-operator/blob/main/.claude/skills/verify-journey/SKILL.md)
executes this document. **LLM agents and developers should follow the matrix
below verbatim**; the skill holds only the mechanics (build, kind-load retry,
teardown, reporting) and defers to this page for *what* to test.

## Ground rules

- **KIND only. Enterprise images only** (`neo4j:*-enterprise`) — the project
  [invariants](../knowledge/invariants.md).
- **Follow the published docs literally** for Phases 1–2. A wrong/out-of-order
  step *is* a finding — record the exact doc location, don't silently fix it.
- **One Enterprise deployment in the cluster at a time** (the *anti-wedge*
  rule). Running standalone + cluster + sharding JVMs concurrently on a laptop
  VM wedges Bolt (the HTTP probe reports `Ready` but ports 7687/6362 time out).
  Phases run **sequentially with full teardown between them**; the Kind cluster,
  operator, and cert-manager stay up across phases. **One deliberate exception:**
  Phase 5 Part C runs two small clusters concurrently, because network-mode
  replication cannot be tested sequentially — kept small (2 servers each) to
  bound the wedging risk this rule exists to avoid.
- **Restore must be walked on BOTH standalone and cluster** — the mechanism
  differs (see the routing table) and one never covers the other.

## Capability routing — why each lands where it does

Each capability is verified on the **minimum deployment that actually exercises
its code path**. Standalone is the cheap workhorse; a capability moves to the
cluster only when it *needs* clustering.

| Capability | Where | Why |
|---|---|---|
| Database lifecycle (create/show/drop) | Standalone | Same `CREATE DATABASE` Cypher everywhere |
| Database **topology** (`n PRIMARIES m SECONDARIES`) | Cluster | Meaningless on standalone (validator only warns) |
| Users / Roles / RoleBindings | Standalone | Auth model is deployment-independent |
| Plugins (APOC, …) | Standalone (ConfigMap path) | Cluster uses the **env-var** path; ConfigMap path is the cheaper of the two to smoke |
| **Backup → restore** | **Both** | Standalone restores via the `neo4j-admin` path (supports PITR `--restore-until`); cluster restores via the in-place **Cypher** path (PITR is *rejected* on clusters). Separate code paths. |
| Property sharding | Cluster (CalVer) | Cluster-only by nature; needs a CalVer image and is memory-heavy |
| **Aura orchestration** (`Aura*` CRDs) | **Phase 4 — no Kind deployment**; needs Aura API credentials | Talks to a *cloud* API, not a local DBMS, so no phase above exercises it. Read-only checks need only a client ID/secret; write checks mutate real cloud resources — see the phase for what is and is not safe |
| **Aura Fleet Management** | Standalone (+ Aura creds for `provision`) | The plugin/registration path is deployment-independent, so the cheap phase covers it; operator-driven `provision` additionally needs Aura API credentials |
| **Cross-cluster replication** (`Neo4jReplicaDatabase`, `Neo4jReplicaPromotion`) | **Phase 5 — two deployments, run SEQUENTIALLY**; needs a `2026.08+` image | Needs an upstream *and* a downstream, which looks like it breaks the one-deployment-at-a-time rule — but backup-based replication couples the two only through a bucket, so the upstream can be torn down before the downstream comes up. See the phase |
| **Database aliases** (`Neo4jDatabaseAlias`) | Standalone | Alias DDL is deployment-independent; the CCDR failover behaviour is covered in Phase 5 |
| **Operator→Neo4j TLS verification** (CA path + `tls.crt` pinning) | Standalone | Standalone never reads `ca.crt` server-side (no cluster SSL policy), so removing that key from the TLS Secret isolates the *operator's* client verification with nothing else changing. On a cluster the same edit also breaks intra-cluster mTLS and the two failures are indistinguishable |
| **`kubectl-neo4j` — offline half** (`validate`, `explain <term>`, exit codes) | **Phase 0**, no deployment | It reads manifests and its own guidance map. Nothing it checks needs a running database, so it costs no memory and can run before the first pod exists |
| **`kubectl-neo4j` — the failure paths** (`diagnose`, `preflight`) | **Phase 0**, on resources built to fail | `diagnose` exists for the layer *below* the CR — an unschedulable pod, `ImagePullBackOff`, a PVC that never binds. Those have to be caused on purpose, and a deployment that never schedules starts no JVM, so this is the one place they can be produced without touching the one-deployment-at-a-time rule |
| **`kubectl-neo4j` — the healthy lens** (`status`, `connect`/`cypher`, `support-bundle`, `validate --connect`) | **Rides Phases 1–3**, no deployment of its own | The CLI is a lens on whatever is deployed. Giving it a phase of its own would mean standing up a fourth Enterprise JVM for it to look at — forbidden by the anti-wedge rule, and it would show nothing the deployments already in Phases 1–3 do not |
| **`kubectl-neo4j export replica-database`** | **Phase 5** | Its only inputs are an upstream `Neo4jBackup.status.replicationPullURI` and an upstream cluster's `status.internalAddresses`, which exist nowhere else in the journey |

## The phase plan

Run **Phases 0-3 every time** (Phase 3 included — sharding regularly surfaces
issues the lighter phases miss). Phase 0 is minutes and no memory, and it goes
first for a second reason: it proves the operator, the CRDs and the CLI are
wired before a real deployment costs twenty. Phase 4 (Aura) needs cloud credentials rather
than a Kind cluster: run its **read-only sweep** whenever anything under
`internal/aura/` or an `Aura*` CRD changed. Phase 5 (CCDR) needs a `2026.08+`
image, which is above the pinned CI anchor — run it whenever anything under the
replication CRDs changed **and** such an image is available; if it is not, say
so in the log rather than recording the phase as passed.

### Phase 0 — CLI (no Neo4j deployment)

`kubectl-neo4j` ships on the operator's own tag, under the operator's own
support statement, so it is verified on the same pass. It is not a deployment
but a lens on one — which is why most of its scenarios ride Phases 1–3 (see the
routing table). What lands *here* is everything that needs no healthy database:
the offline half, and the failure paths that have to be caused deliberately.

Build it from the same tree as the operator and reach it **through `kubectl`**,
because plugin discovery by binary name is part of what is being verified:

```bash
make build-cli          # stamps -X main.version=$(git describe --tags --always --dirty)
export PATH="$PWD/bin:$PATH"
kubectl plugin list | grep kubectl-neo4j
```

**Part A — offline (no cluster contact):**

| Scenario | Verify |
|---|---|
| Plugin discovery | `kubectl neo4j` prints usage when invoked *through* `kubectl`, not only as `./bin/kubectl-neo4j`. The binary name is the contract |
| The good case | `kubectl neo4j validate -f config/samples/` → exit `0` |
| The bad case | a manifest with a known-bad spec (`spec.config.dbms.default_database`, an out-of-range `serverRoles[].serverIndex`) → one line per finding, errors before warnings, each sorted by field path, exit `1` |
| Every error, not the first | one manifest broken in several places at once — a bad `spec.image`, a deprecated `spec.config` key, an out-of-range `serverRoles` index, a missing `acceptLicenseAgreement` — must report **all** of them from a single run. The fix-one-rerun loop that #354 removed must not be back |
| Ruleset banner | every non-`--quiet` run ends `validated against operator rules <version>`. On a journey build that is a `git describe` string such as `v1.14.0-41-g3aa9f21`, **not** a release version — only a tagged build prints a clean one. If it reads `dev` the ldflags did not apply, and the skew check in Part B is silently disabled |
| Skip taxonomy — the distinction that matters | a kind whose validator resolves cross-references (`Neo4jDatabase`) skips with *"resolves cross-references; re-run with `--connect`"*. A kind with **no operator-side validator at all** (`Neo4jRestore`, `Neo4jReplicaPromotion`, any `Aura*`) skips with *"no operator-side validator … `kubectl apply --dry-run=server`"*. Sending a user to `--connect` for a check that does not exist is the exact bug this wording was written to prevent |
| Pending is not failure | a manifest whose dependency cannot be satisfied *yet* renders `…` and does **not** change the exit code, including under `--strict` |
| Exit-code contract | `0` clean · `1` a validation error, or a warning under `--strict` · `2` a usage problem (bad flag, unreadable file, undecodable YAML). Documented as stable, so CI users pin them |
| stdin | rendering the Helm chart with `helm template` and passing it to `validate -f -` reports normally |
| `explain` a term | `kubectl neo4j explain ServersHealthy` → meaning plus guidance; `explain --list` enumerates everything it knows |
| `explain` admits a gap | an invented term (`explain NotARealCondition`) prints *no explanation for …*, points at `--list`, and exits `2` — it never guesses. The other half of this, an unrecognised phase on a live resource naming the CLI's own version, is only reachable against an operator newer than the CLI; note it as unreachable rather than recording it as passed |

**Part B — the failure paths (operator up, no Neo4j running):**

Every resource below is built *not to start*, so none of them runs a JVM and
none of them counts against the one-deployment-at-a-time rule. Apply, read,
delete.

| Scenario | Verify |
|---|---|
| Nothing ever reconciled | `kubectl scale -n <operator-ns> deployment/<operator-deploy> --replicas=0`, apply a standalone, wait past the 2-minute grace → `status` shows it with no phase, and `diagnose` reports *"has no status after …"* and names all three causes (operator not running, no RBAC for the kind, namespace-scoped and not watching). Scale back up afterwards. This is the only signal #282 has |
| Unschedulable | a standalone requesting more memory than any Ready node has → `diagnose` reports the pod as unschedulable **with the scheduler's own message**, plus the node-capacity guidance and the 1.5Gi Enterprise floor; exit `1` |
| Image will not pull | the same manifest with a typo'd tag → `ImagePullBackOff` named as the cause, rather than the CR's `Pending` restated |
| StorageClass missing | `spec.storage.className` naming a class that does not exist → the operator **refuses to build the StatefulSet at all**, so there is no PVC and no pod: the CR goes `Failed`, `status` shows the operator's own message, and `diagnose` adds the `StorageClassNotFound` event. Then run `preflight -f` on the same file: it must have caught this **before** apply. Confirm the two agree — that pairing is the whole argument for `preflight` existing |
| PVC never bound | this appears in the **unschedulable** case above, not the missing-class one: with a `WaitForFirstConsumer` class the PVC stays `Pending` until a pod is scheduled, so `diagnose` reports it alongside the scheduling failure |
| `preflight` never overclaims | every run, clean or not, ends with the shape-only line (*no bucket, registry or endpoint was contacted*) |
| Backup credential shape | a `Neo4jBackup` whose `credentialsSecretRef` is absent, then one that exists but is missing a key the Job mounts → reported distinctly. This replaces the `kubectl run backup-auth-check --image=amazon/aws-cli …` ritual, which today is only reached *after* a backup has failed |
| Crash-loop is recognised | a 1Gi memory limit against the example's 2G heap does **not** produce an OOMKill: Neo4j validates its own memory settings first and exits 3 with *"Invalid memory configuration - exceeds physical memory"*. `diagnose` must name the crash-loop with its restart count and point at `kubectl logs --previous` — the reason is in the log of the run that died. True `OOMKilled` / exit 137 needs a JVM that starts and then grows past the limit; record it as **not run** rather than faking it |
| Version skew warning | the kustomize dev deploy sets `OPERATOR_VERSION=latest`, which suppresses the check **by design**, so force it: `kubectl -n <operator-ns> set env deployment/<operator-deploy> OPERATOR_VERSION=v0.0.1` → `validate --connect` prints `⚠ version skew` naming both versions. Set it back to `latest` after. CI never has two versions, so this is reachable only by hand |

→ **Delete every broken resource** before Phase 1, and confirm the operator is
back to 1 replica.

### Phase 1 — Standalone (1 pod, ~2Gi)

`Neo4jEnterpriseStandalone` → `Ready`, then on it:

| Scenario | Verify |
|---|---|
| Standalone reconciles | `status.phase=Ready`, pod `1/1` |
| Database lifecycle | `Neo4jDatabase` Ready; `SHOW DATABASES` shows it `online` |
| Users / Roles / RoleBinding | `Neo4jUser`/`Role`/`RoleBinding` Ready; `SHOW USERS`/`SHOW ROLES`. **Reference at least one role by its hyphenated CR `metadata.name`** (not `spec.name`) and confirm the grant lands (no `RolesPending`) and a `RolesResolved` event fires. |
| Plugin (APOC) | `Neo4jPlugin` Ready; `RETURN apoc.version()` returns a version (standalone **ConfigMap** path) |
| Backup → restore | `neo4j-admin` path with `stopCluster: true`: add a marker node → back up → delete the marker → restore → confirm the marker returns |
| All-databases restore (standalone) | two user DBs → `Neo4jBackup` `allDatabases: true` → mutate both → `Neo4jRestore` `allDatabases: true` (`stopCluster: true`, `options.replaceExisting: true`); confirm both round-trip via the single offline Job and `status.databaseResults` are all `Completed` (`system` excluded) (#288) |
| Standalone recommended labels | `kubectl get pods -l app.kubernetes.io/name=neo4j` returns the standalone pod (it carries `app.kubernetes.io/{name,instance,managed-by}`) |
| `system` is not restorable | a `Neo4jRestore` with `database: system` → `Failed` with an actionable message |
| Database alias | `Neo4jDatabaseAlias` → Ready; connect via the alias name and reach the target; `SHOW ALIASES FOR DATABASE` lists it. Then re-point `spec.targetDatabase` at a second database and confirm the same connection string now reaches the new one |
| Alias drift is corrected | `ALTER ALIAS` it elsewhere by hand → next reconcile restores `spec.targetDatabase` |
| TLS: CA path | `spec.tls.mode: cert-manager` + `ca-cluster-issuer` → Ready, diagnostics collected, and **no** `PINNING` line in the operator log |
| TLS: certificate pinning | scale `cert-manager` to 0 (so it cannot restore the key), `kubectl patch secret <name>-tls-secret --type=json -p '[{"op":"remove","path":"/data/ca.crt"}]'`, then nudge a reconcile. Expect the operator to log `verifying the server by PINNING its certificate from 'tls.crt'` with a `pinnedFingerprintSHA256` equal to `openssl x509 -in tls.crt -noout -fingerprint -sha256`, **and** `status.diagnostics.lastCollected` to keep advancing — the pinned connection is a real authenticated session, not a fallback that merely logs |
| TLS: pin rejects a mismatch | with `ca.crt` still absent, replace the Secret's `tls.crt` with a *different* certificate carrying the same SANs and do **not** restart the Neo4j pod (it keeps serving the keystore it loaded at startup). Next reconcile must fail with `x509: certificate signed by unknown authority` in `status.diagnostics.collectionError`. Restore `tls.crt` → the next reconcile recovers. This is the only place the pin is proven to *reject*; CI cannot reach it because every CI Secret has `ca.crt` |
| CLI: `status` on a healthy namespace | one `kubectl neo4j status -n <ns>` lists the standalone **and** its Database/User/Role/Plugin CRs with their phases; `--problems` prints nothing while everything is Ready. This replaces `kubectl get` against 26 kinds |
| CLI: `connect` tells the truth about TLS | run it around the TLS scenarios above: with TLS enabled, `connect <name>` prints `bolt+s://` and states that plain `bolt://` is rejected; before it, `bolt://`. The scheme is derived from the deployment, so a wrong one here is a real bug in the command customers reach for first |
| CLI: `cypher` never moves the password | `kubectl neo4j cypher <name> -c "SHOW DATABASES"` returns the same rows as the by-hand `kubectl exec`, and the exec line `connect` prints references `$DB_USERNAME` / `$DB_PASSWORD` **by name**. The password must appear in neither the printed command nor your shell history |
| CLI: `explain` against a live resource | `kubectl neo4j explain Neo4jEnterpriseStandalone/<name>` prints the phase, then each condition with the operator's *own* message and the guidance for it |
| CLI: `support-bundle` withholds the secret | `support-bundle -n <ns> -o bundle.tar.gz`, extract, and **grep the whole archive for the admin password** — zero hits. `REDACTIONS.txt` is present and enumerates what was withheld; Secrets appear as names, types and key names only; `valueFrom` references are still there, deliberately. Grepping for the real password is the check that matters — a redactor is only as good as its worst path |
| CLI: `validate --connect` and `preflight` on what is deployed | a `Neo4jDatabase` naming a cluster that does not exist → error; the same manifest naming this standalone → validated, and the kinds skipped offline in Phase 0 are no longer skipped. `preflight -n <ns>` with no arguments checks the deployed resources and still ends with the shape-only line |

→ **Tear down the standalone fully** before Phase 2 — but **KEEP the backup PVC**
(and the `Neo4jBackup` CR that owns it, so retention does not prune the artifact).
Phase 2's cross-topology scenario restores that standalone-produced artifact into
the cluster, and the one-deployment-at-a-time rule means you cannot stand the
standalone back up alongside the cluster to re-make it. Deleting the PVC here
costs a full standalone rebuild-and-teardown cycle later, so treat "a retained
standalone backup" as one of Phase 1's outputs.

### Phase 2 — Cluster, 3 servers (~6Gi)

`Neo4jEnterpriseCluster` with `servers: 3` → `Ready`; pods
`{cluster}-server-0..2`; `SHOW SERVERS` lists 3 `Enabled`/`Available`. Then:

| Scenario | Verify |
|---|---|
| Cluster forms | 3 members `Enabled`/`Available`; server-based pod names |
| Database **with topology** | e.g. `3 PRIMARIES`; `SHOW DATABASE <db>` shows the primaries `online` |
| Backup → restore (cluster) | in-place **Cypher** path: back up one DB (`instanceRef` + `database`), restore into a **new** database, confirm the data round-trips |
| All-databases restore (cluster) | `Neo4jBackup` `allDatabases: true` → `Neo4jRestore` `allDatabases: true` (cloud-backed); confirm every user DB round-trips and `status.databaseResults` are all `Completed` (#222) |
| Cross-topology restore | restore the **retained standalone backup** from Phase 1 into the **cluster** via `instanceRef`; confirm the data round-trips. The cluster path stands up a short-lived `backup-seed-proxy-<restore>` Deployment that serves the backup PVC over HTTP, so the seed URI in `status` is an `http://…` URL, not a `file://` path |
| CLI: a healthy cluster reads as healthy | `kubectl neo4j status` shows the cluster `Ready` next to the topology-bearing `Neo4jDatabase`; `diagnose` reports `3/3 pods ready` and exits `0` rather than inventing a problem. A diagnostic that cries wolf on a healthy cluster is worse than none |
| CLI: `diagnose` during a restart is not a failure | delete one server pod (`kubectl delete pod <cluster>-server-1`) and run `diagnose` inside the restart window: the not-ready pod is marked `…`, the exit code stays `0`, and the pod returns. Enterprise takes tens of seconds to open Bolt, so an exit code that failed during a normal restart would be useless in the loop it exists for |

3 servers (not 2) keeps split-brain / 3-primary quorum behaviour in the routine
walk. → **Tear down the cluster fully** before Phase 3.

### Phase 3 — Property sharding (cluster, `2026.06-enterprise`, 3 × 2Gi)

Sharding's documented floor is **4Gi + 1 core per server**. To fit a laptop we
deliberately relax it — **this phase is operator-mechanics verification, not
doc-following**. The relax is operator-side and DEV/TEST only:

```bash
# Downgrades the 4Gi/1-core hard rejects to warnings.
kubectl -n <operator-ns> set env deployment/<operator-deploy> \
  NEO4J_SHARDING_RELAX_MEMORY_MIN=true
kubectl -n <operator-ns> rollout status deployment/<operator-deploy>
```

Use **`2026.06-enterprise`** (the pinned CI anchor) so local sharding matches
what the [extended suite](ci_and_workflows.md) validates. On a `servers: 3`
cluster (~2Gi each):

| Scenario | Verify |
|---|---|
| Sharding cluster | `CALL dbms.components()` → `2026.06.x` enterprise; `status.propertyShardingReady=true` |
| Sharded database | `Neo4jShardedDatabase` whose **`metadata.name` differs from `spec.name`** (e.g. CR `products-sharded` / `spec.name: products`) → Ready; `SHOW DATABASES WHERE name STARTS WITH '<logical>'` lists the graph + property shards `online` |
| Sharded backup **by CR name** | `Neo4jBackup` `kind=ShardedDatabase` with `target.name` = the **CR metadata name** → `Succeeded`; `status.history[].shardArtifacts` lists every shard. (Using the *logical* name fails preflight — the operator resolves the logical name from the CR's `spec.name`.) |
| CLI: the sharded CR is legible | `status -n <ns>` lists the `Neo4jShardedDatabase` by its **CR `metadata.name`** — the same name vs. `spec.name` distinction the row above exists to catch — with its phase, alongside the cluster. `explain Neo4jShardedDatabase/<cr>` either decodes its conditions or says plainly that it carries none this CLI knows, which is the correct answer rather than a guess |

→ **Tear down**, then delete the Kind cluster.

### Phase 4 — Aura (no Kind deployment; needs Aura API credentials)

**BETA / best-effort surface.** The `Aura*` CRDs talk to the Aura cloud API, so
nothing in Phases 1-3 touches them. This phase is **optional but strongly
recommended** whenever an `Aura*` CRD or `internal/aura/` changed, because the
published Aura OpenAPI spec is known to disagree with the live API (see
`docs/knowledge/operations.md` id 87).

**Take that disagreement seriously.** As of 2026-08-01 every Aura surface except
CMK has been driven against a real account, and three of them — fleet
provisioning, IP filters and invites — were **broken in ways the unit suite could
not see**, because the fixtures asserted the client matched the *spec* rather
than the API. A green `make test-unit` is not evidence that an Aura write path
works.

**Cheap technique worth reusing:** to learn an enum or a required field without
mutating anything, send a deliberately invalid value — the API answers with the
full list of accepted values, or names the missing field, and changes nothing.

| Surface | Live-verified | |
|---|---|---|
| Instances (v1 lifecycle), snapshots, restore | 2026-08-01 | ✅ |
| Databases + per-database backup/restore (v2beta1) | 2026-07-31 | ✅ |
| IP filters (v2beta1) | 2026-08-01 | ✅ contract corrected |
| Console RBAC: members, invites (v2beta1) | 2026-08-01 | ✅ invite contract corrected |
| Fleet provisioning (v2beta1) | 2026-07-31 | ✅ contract corrected |
| **Customer-managed keys (v1)** | — | ❌ **never exercised; needs a real cloud KMS key** |

Set up an `AuraProviderConfig` (or an inline `credentialsSecretRef`) with an API
client ID + secret from the Aura console.

**Read-only sweep — always safe, do this every time:**

| Scenario | Verify |
|---|---|
| Credentials + auth | A token from `/oauth/token` authenticates **both** v1 and v2beta1 calls |
| Org/project discovery | `GET /v2beta1/organizations` and `…/projects` return `{id,name}` |
| Read shapes still match | Org users return `user_id` + `organization_roles[]` with `organization-*` values; ip-filters return a **bare** array (no `data` envelope); the v1 CMK list returns only `id`/`name`/`tenant_id`; `DatabaseSummary` returns only `id` |
| Fleet reads | Fleet deployment single-GET **is** `data`-wrapped; token field is `auto_rotated`; server field is `mode_constraint` (singular); shard/txn/lag data appears only on `…/servers/{id}/databases` |

A GET-only sweep needs no throwaway resources and cannot damage anything. It is
what caught four real client bugs during the 2026-07-30 re-diff.

**Write checks — ONLY in a disposable Aura project, never production:**

| Scenario | Verify |
|---|---|
| `AuraInstance` lifecycle | create → Ready → resize (`memory`) → pause → resume → delete; `status.instanceId` pinned via the `neo4j.com/external-instance-id` annotation. **Set `deletionPolicy: Delete` on the throwaway instance** — the default is `Orphan`, so otherwise deleting the CR leaves the instance running and billing (see the teardown note below), and the delete path itself goes untested |
| `AuraDatabase` + backup | **Needs an instance created with `multiDatabase: true`** — a Business Critical tier alone is not enough, and the flag cannot be added later (`docs/knowledge/operations.md` id 90). Against any other instance expect `Ready=False`, reason `InstanceNotMultiDatabase`, with **no requeue**. On a multi-database instance: `AuraDatabaseBackup` reaches `Completed` (**not** on first observe — an empty status must read as `Pending`, and the backup does not appear in the backups LIST until it completes, so it must be polled by ID); `AuraDatabaseRestore` reports `Submitted`, never `Completed` |
| Multi-database create | An `AuraInstance` with `multiDatabase: true` + `organizationId` is created via **v2beta1** (v1 `type` names are translated), then remains manageable through v1; `status.atProvider.multiDatabase` reports `true`. On a v1-created instance the field reports the **true** value from the v2beta1 probe — `false` is correct there, and it stays **absent** (unknown) only when no `organizationId` is resolvable, since the v1 GET carries no such field |
| Console RBAC | `AuraProjectMember` for an existing **org** member is added without an invite; role PATCH bodies are accepted (they are `{organization_roles:[…]}` / `{project_roles:[…]}`, not a scalar) |
| Fleet `provision` | Deployment registered, token minted into the Secret, DBMS registers; deleting the Secret with the default `tokenPolicy` **refuses to rotate** and says why |

→ **Tear down** every resource created here; Aura bills by the hour. Deleting the
CRs is **not** sufficient proof: `deletionPolicy` defaults to `Orphan`, so a CR
delete can leave a running instance behind. Confirm with
`GET /v1/instances/<id>` → **404** and a project instance list of **0** before you
call the phase done.

### Phase 5 — Cross-cluster replication (two deployments; sequential for backup mode, concurrent for network mode; needs `2026.08+`)

**Run this only when a `2026.08+` enterprise image is available.** Hosting a
replica requires it, and the check is enforced — on an older image the CR fails
with `ReplicaVersionTooOld` and nothing else in this phase can proceed. That is
itself scenario 1: confirm the gate fires rather than silently doing nothing.

Parts A/B below (backup mode, `source.mode: backup`) look like they break the
**one Enterprise deployment at a time** ground rule. They do not, and the
reason is the point of that mode: the two clusters are coupled **only through
the object store**, never over the network. So the upstream produces its
chain, gets torn down, and *then* the downstream comes up and pulls from the
bucket. Two deployments, never concurrent.

Network mode (`source.mode: network`, Parts C/D below) needs a live network
path, but "two clusters" doesn't mean leaving Kind or leaving this single
deployment session: Part C runs the real replication mechanism between two
`Neo4jEnterpriseCluster`s in different namespaces on **this same Kind
cluster**, reachable over ordinary in-cluster DNS — no proxy, no
`crossClusterReplication`, no LoadBalancer needed. Part D then smoke-tests
the `crossClusterReplication` proxy toggle itself, which plain Kind cannot
exercise end-to-end (see Part D for why). Between them they cover everything
this journey can reach; see each part for exactly what it does and does not
cover.

Stand up MinIO as the shared bucket (same pattern the backup specs use) for
Parts A/B.

**Part A — upstream (standalone is enough):**

| Scenario | Verify |
|---|---|
| Replication-source backup | `Neo4jBackup` `mode: replication-source` + `database` + `schedule` → `status.replicationPullURI` is populated and points at the chain directory. **Copy this value** |
| Chain-breaking configs rejected | the same CR with `retention` set, or `allDatabases: true`, or no `schedule` → phase `Invalid`, message names the rule |
| A FULL then a DIFF | let (or force) two runs so the bucket holds a full **and** a differential — a seed-only chain does not exercise the pull path |
| CLI replaces the copy step | `kubectl neo4j export replica-database <name> --from-backup <backup> --cluster-ref <downstream> --upstream-database <db>` emits a `Neo4jReplicaDatabase` on **stdout only** — redirect it and confirm the file is a clean manifest with the notes on stderr — carrying the same `pullURI` you copied by hand above. Then run `validate -f` on the generated file: the command already validates before printing, so a manifest `validate` rejects must never appear |

→ **Tear the upstream down completely.** Leave the MinIO bucket intact.

**Part B — downstream (fresh deployment, `2026.08+`):**

| Scenario | Verify |
|---|---|
| Replica seeds | `Neo4jReplicaDatabase` with `source.pullURI`/`seedURI` from Part A → phase `Replicating`; `SHOW DATABASES` shows `type=replica`, `access=read-only`, `writer=false` on **every** copy including primaries |
| Replica is read-only | a write via `cypher-shell` fails; the same query with `--access-mode=read` succeeds |
| Alias pre-staged against a replica | `Neo4jDatabaseAlias` targeting the replica → `Ready` **while the target is still a replica** (this is what removes work from the failover window) |
| Promotion | `Neo4jReplicaPromotion` → `Completed`; `status.observedLagTxIds` recorded; `SHOW DATABASES` now shows a standard read-write type |
| Alias survives promotion | the **same** alias now resolves to a read-write database — no CR change, no client reconfiguration |
| Replica CR goes inert | the `Neo4jReplicaDatabase` is `phase: Promoted`; **delete it and confirm the database is NOT dropped** |
| Out-of-band promotion is safe | on a second replica, promote by hand at a `cypher-shell`, then let the operator reconcile → it goes `Promoted` (reason `DetectedOutOfBand`) rather than "correcting" the drift by dropping the database |

That last row is the one worth doing carefully: it is the failure mode the
whole observe-before-act design exists to prevent, and it is invisible to unit
tests.

**Part C — network mode core mechanism, same Kind cluster, no proxy (two concurrent deployments):**

**Automated** — `test/integration/ccdr_same_cluster_network_mode_test.go`,
`Label("extended")`, gated by `isCCDRReplicaCompatible()` (dormant on the
default CI anchor; runs when dispatched with `neo4j-version:
2026.08-enterprise+`). Uses `source.upstreamClusterRef` rather than a
hand-typed `source.addresses` FQDN — exercises the same underlying mechanism
via the newer, higher-level API. Re-run manually only if you want the
hand-typed-address path specifically, or a version this repo's CI cannot yet
dispatch.

This is the highest-value scenario in this phase: it is the first time
`CREATE REPLICA DATABASE ... OPTIONS {replicaConfig: {remote, addresses}}`
runs against a real server, rather than being inferred from documentation.

Deploy two small `Neo4jEnterpriseCluster`s (2 servers each, CI-sized
resources) in different namespaces on **this same Kind cluster** — e.g.
`prod-cluster` in namespace `prod`, `dr-cluster` in namespace `dr`. Leave
`spec.crossClusterReplication` **unset** on the upstream; do not create the
proxy at all. Point the downstream `Neo4jReplicaDatabase` directly at the
upstream's plain internal pod FQDN — reachable across namespaces on the same
cluster by ordinary in-cluster DNS, no Service changes needed:

```
prod-cluster-server-0.prod-cluster-headless.prod.svc.cluster.local:6000
```

**This deliberately breaks the one-deployment-at-a-time rule** — network
replication cannot be tested sequentially the way backup mode can, the two
clusters must be up together. Keep both small (2 servers, CI-appropriate
resource requirements) to limit the Bolt-wedging risk the anti-wedge rule
exists for; if Bolt does wedge here, that is itself a real finding about
concurrent-deployment resource limits, not a test artifact to work around.

| Scenario | Verify |
|---|---|
| Network replica seeds from a plain internal FQDN | `Neo4jReplicaDatabase` with `source.mode: network`, `source.addresses` set to the upstream's server-0 pod FQDN above → phase `Replicating`; `SHOW DATABASES` shows `type=replica`, `access=read-only`, `writer=false` |
| One address is sufficient | list only server-0's address, not all N — replication still reaches every ordinal, confirming the design doc's Q1 finding that the upstream hands back the addresses the downstream actually uses |
| Malformed/missing address rejected before any Cypher runs | `source.addresses: []` or a host with no port → validator error, no `CREATE REPLICA DATABASE` attempt |
| Promotion works the same as backup mode | `Neo4jReplicaPromotion` against this replica → `Completed`, `SHOW DATABASES` now shows a standard read-write type |
| CLI replaces the hand-typed FQDN | `kubectl neo4j export replica-database <name> --from-cluster prod-cluster --cluster-ref dr-cluster --upstream-database <db>` emits the same manifest with `source.addresses` read from the upstream's `status.internalAddresses` — the FQDN typed by hand above. Confirm they match. Note in the log that on **one** Kubernetes cluster `source.upstreamClusterRef` is the better answer and the docs say so; the exported literal is for a downstream on a different cluster, which is the only case no ref can reach |

This scenario does **not** exercise `spec.crossClusterReplication` itself —
no proxy, no advertised-address override, no cert SAN, no NetworkPolicy rule.
That gate depends on the proxy's Service reaching a real
`status.loadBalancer.ingress` state, which is identical whether one Kind
cluster or two are involved (see Part D) — this scenario is about the
replication *mechanism*, deliberately isolated from that gate.

→ **Tear down both deployments**, then delete the Kind cluster.

**Part D — network mode proxy toggle, Kind-only smoke check (single deployment):**

**Automated** — `test/integration/ccdr_proxy_test.go`, `Label("core")`. Needs
no version gate (nothing here hosts a replica), so it runs on every PR
against both CI anchors, not just on dispatch — the manual walk below is
useful only for exploring interactively, not for verifying this before a
release.

Part C covers the replication mechanism; this part covers the one piece it
deliberately left out — `spec.crossClusterReplication` itself. That gate
depends on the proxy's Service reaching a real `status.loadBalancer.ingress`
state, and a `type: LoadBalancer` Service never gets one on plain Kind (no
cloud provider) — this repo has **no LoadBalancer-on-Kind tooling today**, no
MetalLB, no `cloud-provider-kind`, in the Makefile, dev scripts, or Kind
cluster config. That's unbuilt local tooling, not a hard requirement for real
cloud infrastructure or even for a second Kind cluster (two Kind clusters
share a Docker network by default and can already route to each other, the
same trick used for local multi-cluster testing elsewhere — Cilium
ClusterMesh, Istio multi-cluster). Until the LoadBalancer piece exists, full
end-to-end verification of the proxy path itself is not run here.

What *is* verifiable on a single plain Kind cluster is that the toggle itself
is safe — it must never break normal cluster formation, whether or not a real
load balancer is present to service it:

| Scenario | Verify |
|---|---|
| Proxy resources render | `Neo4jEnterpriseCluster` with `spec.crossClusterReplication.enabled: true` → a `<name>-ccdr` ConfigMap/Deployment/Service appear, and the cluster still reaches `Ready` (this proves the toggle doesn't wedge formation even when the LoadBalancer never gets an address on plain Kind) |
| NetworkPolicy stays additive | with `spec.networkPolicy.enabled: true` too → the existing 3 ingress rules are unchanged and a 4th rule admits only the `ccdr-proxy` pod on port 6000 |
| Advertised address stays internal until ready | `status.crossClusterReplication.ready` stays `false` (Kind has no LoadBalancer controller by default) → `server.cluster.advertised_address` in the rendered ConfigMap is still the pod FQDN, not the proxy's hostname |
| Toggling off tears down cleanly | flip `enabled` back to `false` → the ConfigMap/Deployment/Service are deleted and `status.crossClusterReplication` clears |

→ **Tear down**, then delete the Kind cluster.

## Coverage at a glance

| | CLI (Phase 0) | Standalone | Cluster (3) | Sharding (2026.06) | Aura (Phase 4) | CCDR (Phase 5, 2026.08+) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Reconcile → Ready | | ✅ | ✅ | ✅ | ✅ (instance) | ✅ |
| Database lifecycle | | ✅ | | | ✅ (`AuraDatabase`) | |
| Database topology | | | ✅ | | n/a (Aura-managed) | ✅ (replica) |
| Users / Roles / Bindings | | ✅ | | | n/a (console-RBAC only) | |
| Plugins (APOC) | | ✅ (ConfigMap) | | | n/a | |
| Backup → restore | | ✅ (neo4j-admin) | ✅ (Cypher) | ✅ (sharded) | ✅ (per-DB, API) | ✅ (chain as source) |
| Property sharding | | | | ✅ | | |
| Aura Fleet Management | | ✅ (plugin + token) | | | ✅ (`provision`) | |
| Aura console RBAC | | | | | ✅ | |
| Aura read-contract sweep | | | | | ✅ (GET-only) | |
| Cross-cluster replication | | | | | | ✅ backup mode + network mode mechanism (same-cluster); network mode proxy is toggle-only (needs LoadBalancer-on-Kind tooling this repo doesn't have yet) |
| Database aliases | | ✅ | | | | ✅ (survives promotion) |
| Operator→Neo4j TLS (CA + pinned) | | ✅ | | | n/a (HTTPS to Aura) | |
| CLI: offline (`validate`, `explain`, exit codes) | ✅ | | | | | |
| CLI: failure paths (`diagnose`, `preflight`) | ✅ (caused on purpose) | ✅ (`preflight` on what is deployed) | ✅ (`…` during a restart) | | | |
| CLI: healthy lens (`status`, `connect`/`cypher`, `support-bundle`) | | ✅ | ✅ | ✅ (`status`) | | |
| CLI: manifest authoring (`export replica-database`) | | | | | | ✅ (both modes) |
| CLI ↔ operator version skew | ✅ (forced by hand) | | | | | |

## Keeping this current

When you **add or change a capability**, update this page in the same PR:

1. Add a row to the **routing table** (which deployment, and *why* there).
2. Add the scenario + its in-DB check to the relevant **phase**.
3. Tick the **coverage** matrix.
4. If it changes the operator install or sizing, update the phase headers.
5. If it adds or changes a **CLI** command, put it in the phase whose
   deployment it reads — Phase 0 if it needs none, or a broken resource if what
   it reports is a failure. A command verified only against a healthy cluster
   is verified in the state nobody runs it in.

When a release fixes a specific bug, add a one-line scenario that would have
caught it (the v1.12.2 pass added the standalone-label, `system`-reject,
role-CR-name, and sharded-backup-by-CR-name checks). Record each run below.

## Verification log

| Release | Date | Result | Findings |
|---|---|---|---|
| v1.12.2 | 2026-06-14 | ✅ all phases pass | Doc bug: `backup_restore.md` sharded `target.name` said *logical name*, must be *CR name* (fixed, `#270` follow-up). v1.12.2 surfaces (#260/#268/#269/#270) verified live. |
| _(pre-merge, PR #333)_ | 2026-08-07 | ⚠️ partial — Phases 1 (alias scenarios) + 5A/5B validation only | **Bug found + fixed:** `spec.auth` is optional on Cluster/Standalone, but five call sites dereferenced `Spec.Auth.AdminSecret` unguarded — a `Neo4jDatabase` against a standalone without `spec.auth` panicked the reconciler and the CR sat with an empty status forever, with nothing on the CR explaining why. Widest site was `ResolvedTarget.NewClient`, shared by the user/role/binding/authrule and replication controllers. Fixed to use the existing nil-safe helpers. **Also:** `make deploy-dev-local` reports "deployment configured" without restarting the pod when the `:dev` tag is unchanged, so a rebuilt binary silently does not take effect — needs `kubectl rollout restart`. **Verified pass:** alias create/resolve, blue-green re-point, drift correction, alias delete does not drop the target; replica version gate (2026.08 required, refused on 2026.06), network-mode rejection naming `advertised_address`, replication-source R1/R2/R4 rejections, `status.replicationPullURI` published. **Not run:** replica seeding, promotion, and alias-survives-promotion — need a 2026.08+ image, which is not yet published (highest on Docker Hub is 2026.06). |
| _(pre-merge, PR #343)_ | 2026-08-21 | ⚠️ partial — Phase 1 TLS scenarios only | **Bug found + fixed:** the operator's "insecure TLS fallback" for Secrets without `ca.crt` had never worked. It set `InsecureSkipVerify: true`, but the Bolt driver derives that field from the URI scheme and resets it (`connector.tlsConfig()`), so Go verified against system roots and every connection failed `x509: certificate signed by unknown authority` — while the operator logged that verification was disabled. So `spec.tls.strictPeerValidation: false` could not connect at all, and the warning named a state that never existed. Replaced with pinning `tls.crt` as a one-certificate `RootCAs`. **A/B on one cluster, same Secret:** pre-pinning binary failed 6/6 reconciles; pinning binary connected and collected diagnostics. **Also verified:** CA path unchanged (no `PINNING` line, diagnostics fresh), pinned fingerprint matches `openssl`, a mismatched `tls.crt` is refused, and restoring it recovers. **Lesson:** handshake unit tests dialed the config verbatim and so could not see the driver's override — they now emulate it (`asDriverWouldUse`). |
| _(pre-release, v1.15.0 candidate)_ | 2026-09-03 | ⚠️ Phases 0-2 walked; Phase 3 not run; Phase 5 blocked — **all findings below fixed and re-verified live the same day** | **3 blockers.** (1) **Flags after a positional argument are silently ignored** across `diagnose`/`explain`/`connect`/`cypher`/`preflight`/`export` — stdlib `flag` stops parsing at the first non-flag arg. It fails in the wrong direction: the wrong namespace is queried and reported as "not found". Every documented `export` invocation fails outright; `preflight Neo4jBackup/nightly -n neo4j` and `cypher prod -n neo4j` from the published docs silently target `default`. (2) **`cypher -c` can hang forever** and leaves an orphaned `cypher-shell` inside the database pod: it execs with `-i` and no `--non-interactive`, so the shell runs the query then reads stdin for more. Twelve orphans accumulated across two hangs and the database stopped answering over Bolt *and* HTTP while the pod stayed `Ready` — the anti-wedge signature, reached with a single deployment. (3) **`cypher` is unusable against a cluster**: it dials `bolt://localhost` on one chosen pod instead of routing, and the default `neo4j` database has 1 primary, so 2 of 3 servers answer "Database neo4j not found"; there is no database flag to work around it. **Also:** cross-topology restore (this doc's own Phase 2 scenario) **fails** — a single-database restore from an `allDatabases` backup demands `artifactFilename`, which such a backup never sets, while `databaseArtifacts` holds the exact filename; the error tells the user to "re-run the backup with a recent operator", which does not help. `Neo4jPlugin` phase flaps Ready↔Installing forever while APOC works. `support-bundle` collects no operator logs though its help text and docs promise them. `explain` does not know the phase `Ready`. Two validator asymmetries: `dbms.mode` is rejected on standalone but accepted on cluster; heap-vs-limit is rejected on cluster but accepted on standalone (demonstrated: a standalone that validated clean crash-looped). Three published examples fail validation (`plugins/standalone-plugin-example.yaml`, `property_sharding/advanced-property-sharding.yaml`, `end-to-end/complete-deployment.yaml`) — **and all 8 of those errors pass `kubectl apply --dry-run=server`, which answers design Q1 empirically: `validate` earns its place.** **Fixes verified live on a fresh cluster afterwards:** the documented `export` form parses; `cypher -c` returns on a cluster (5/5, zero orphaned shells, writes included); `diagnose Kind/name -n ns` works; `explain Ready` gives guidance; the support bundle carries `operator/<ns>/<pod>/manager.log` (found via the Deployment's selector — the identifying label is on the Deployment, not its pods) with redaction intact; the plugin phase held `Ready` across 12 samples while APOC answered `apoc.version()`, and plugin reconciles fell from ~1/second to 2/minute. **Verified pass:** standalone Ready in 96s from the published URL, database/user/role incl. hyphenated CR-name resolution and all three privileges, APOC, backup→restore round trip, `system` not restorable, alias create/re-point/drift-correction, support-bundle redaction (grepped for all three real passwords, zero hits), cluster formation 3/3 with 3-primary topology, `diagnose` on healthy vs. restarting pods. |
| v1.14.0 | 2026-08-02 | ✅ all 4 phases pass (19/19 scenarios) — 3 bugs + 4 doc gaps found | **Bugs:** (1) deleting a `Neo4jPlugin` against a live deployment never finalizes — the removal Job is re-created every reconcile and fails `already exists`, so the CR stays Terminating and blocks namespace deletion; (2) `examples/property_sharding/development-property-sharding.yaml` does not boot — `spec.propertySharding.config` bypasses the memory-key exclusion that `spec.config` gets, so a user `heap.max_size` contradicts the operator-derived `heap.initial_size` and the JVM refuses to start; (3) `make dev-up` loads no `aura*` controller (the dev `-controllers` default omits all 12), so every Aura CR is accepted and then silently ignored — no status, no events, no logs. **Doc gaps:** Phase 1 teardown destroys the backup Phase 2's cross-topology scenario needs; Phase 4 teardown leaves a billing instance because `deletionPolicy` defaults to `Orphan`; the `multiDatabase`-stays-absent rule holds only with no org configured; sharding examples pin `2026.04` while the CI anchor is `2026.06`. **Also:** a crash-looping member surfaces only as `ConnectivityDegraded`, which points at Bolt rather than the container. |

## See also

- [Testing](testing.md) — the automated unit/integration suites and the
  core/extended label split.
- [CI/CD & Workflows](ci_and_workflows.md) — what runs per-PR vs. the manual extended suite.
- [`kubectl-neo4j` CLI](../user_guide/cli/index.md) — the command docs Phase 0
  and the per-phase CLI rows follow, including the exit-code contracts.
- [Backup & Restore](../user_guide/guides/backup_restore.md),
  [Property Sharding](../user_guide/property_sharding.md) — the user docs this
  journey follows.
- [Project Invariants](../knowledge/invariants.md) — the hard constraints
  (KIND-only, Enterprise-only, etc.) every phase respects.
