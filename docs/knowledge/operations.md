# Operations Knowledge Base — Runtime Invariants (rules 1–39, + post-checklist additions ≥ 80)

> One home, no duplication. This file re-homes the non-backup half of the legacy
> CLAUDE.md regression checklist (rules 1–39). Backup/restore/sharding rules
> (40–79) live in their own knowledge file. CLAUDE.md now points here instead of
> carrying the checklist.
>
> **`id`** keeps the original CLAUDE.md rule number (stable cross-reference — do
> not renumber). **`scope`** is the verified file(s) the invariant lives in.
> **`pinned-by`** names a test that fails if the invariant regresses; every test
> named below was grep-verified to exist in the current tree on branch
> `fix/wire-plugin-validator-164`. **`enforcement`** says how a violation is
> caught (unit test / integration test / validator / code review).
>
> The 5 project-wide hard invariants (NO webhooks, KIND only, ENTERPRISE images
> only, V2_ONLY discovery, server-based architecture with Job-per-CR backups)
> sit above this file in AGENTS.md — they are not repeated here.

## Standalone

### id 1 — Ready gate before database ops
- **scope:** `internal/controller/neo4jenterprisestandalone_controller.go`
- **rule:** A standalone instance must reach `status.phase="Ready"` before any database operation (CREATE DATABASE, user/role ops, etc.) runs against it.
- **why:** Bolt/Cypher against a not-yet-bootstrapped instance fails or races the password/auth setup; gating on Ready makes downstream ops deterministic.
- **pinned-by:** standalone controller integration specs in `internal/controller/neo4jenterprisestandalone_controller_test.go` (Ready-phase gating).
- **enforcement:** integration test + code review.

### id 2 — Backup uses `--to-path` (5.26+ syntax)
- **scope:** `internal/controller/neo4jbackup_controller.go` (`buildToPath`, lines ~1334/1501/1585) — NOTE: this is the unified Neo4jBackup (Job-per-CR) path, not standalone-controller-local logic. See FLAG in notes.
- **rule:** `neo4j-admin database backup` uses `--to-path=<base>/<chain-root>/`; never the deprecated 4.x `--backup-dir`. All runs of one Neo4jBackup CR share a single `--to-path` directory (DIFF chaining) — never per-run subfolders.
- **why:** 5.26+ neo4j-admin dropped the old backup flags; DIFF chaining requires the prior FULL to sit in the same directory.
- **pinned-by:** `TestBackupRunIDEnvVar`, `TestJobToBackupRun` (backup reconciler unit tests).
- **enforcement:** unit test.

### id 3 — Always stamp `ObservedGeneration`
- **scope:** `internal/controller/neo4jenterprisecluster_controller.go` (~L1414), `internal/controller/neo4jenterprisestandalone_controller.go` (~L1818, L1885)
- **rule:** Set `status.observedGeneration = latest.Generation` on every status update, in BOTH controllers.
- **why:** Without it, `kubectl wait`/clients can't tell whether the operator has observed the latest spec; stale-generation status is indistinguishable from up-to-date.
- **pinned-by:** controller reconcile integration specs assert ObservedGeneration tracks Generation.
- **enforcement:** integration test + code review.

### id 4 — Name-length validation
- **scope:** `internal/validation/cluster_validator.go` (`maxClusterNameLength = 56`, ~L140), `internal/validation/database_validator.go` (`maxDatabaseNameLength = 65`, regex `neo4jDatabaseNamePattern = ^[a-zA-Z][a-zA-Z0-9.\-]*$`)
- **rule:** Cluster name ≤ 56 chars (DNS-label 63 minus the `-server` suffix); standalone ≤ 63; database name ≤ 65 and must match `^[a-zA-Z][a-zA-Z0-9.\-]*$`.
- **why:** Generated resources append suffixes (`-server`, etc.); exceeding the K8s DNS-label limit breaks StatefulSet/Service creation. Database names beyond Neo4j's limit are rejected by the DB engine.
- **pinned-by:** `cluster_validator_test.go` ("no more than 56 characters"), `database_validator_test.go` ("no more than 65").
- **enforcement:** validator (inline in reconciler) + unit test.

### id 5 — Standalone `UpgradeStrategy`
- **scope:** `api/v1beta1/neo4jenterprisestandalone_types.go` (`UpgradeStrategy *UpgradeStrategySpec`), `UpgradeStrategySpec` in `api/v1beta1/neo4jenterprisecluster_types.go` (~L1155); pre-upgrade check uses `Client.VerifyConnectivity` (`internal/neo4j/client.go` ~L592)
- **rule:** Standalone upgrades run a pre-upgrade health check via `VerifyConnectivity`; `autoPauseOnFailure` blocks the upgrade when the check fails; the StatefulSet update strategy comes from the spec.
- **why:** Upgrading an unhealthy single node risks unrecoverable data loss; the gate forces an operator decision before proceeding.
- **pinned-by:** standalone upgrade integration specs in `neo4jenterprisestandalone_controller_test.go`.
- **enforcement:** integration test + code review.

### id 6 — Standalone health probes via `/conf/health.sh`
- **scope:** `internal/resources/cluster.go` (`buildHealthScript`, ConfigMap key `"health.sh"` ~L646; probe Exec `/conf/health.sh` ~L2542/2561/2645; ConfigMap volume `DefaultMode: 0o755` ~L1348)
- **rule:** Readiness/liveness/startup probes shell out to `/conf/health.sh` (checks both the Neo4j process and HTTP 7474). The ConfigMap carries `health.sh` alongside `neo4j.conf` with `DefaultMode: 0755` so it's executable.
- **why:** A bare TCP probe can't tell "process up" from "DB serving"; the script verifies HTTP readiness. 0755 is required or the kubelet can't exec the mounted script.
- **pinned-by:** `internal/resources/cluster_startup_test.go` (asserts `health.sh` present and non-empty); `neo4jenterprisestandalone_controller_test.go` (probes reference `/conf/health.sh`).
- **enforcement:** unit test + integration test.

### id 7 — Validator REJECTS deprecated `spec.config` keys
- **scope:** `internal/validation/config_validator.go` (`ConfigValidator`) — wired into the **cluster** validator only (`cluster_validator.go`); the **standalone** validator has its OWN independent `validateConfig` (`standalone_validator.go`). The two are NOT shared.
- **rule:** On the **cluster** path, `ConfigValidator` rejects deprecated keys as `field.Invalid`: `dbms.logs.query.enabled` (use `db.logs.query.enabled`), `dbms.default_database` (use the `dbms.setDefaultDatabase()` procedure), and `dbms.integrations.cloud_storage.s3.region`; `db.format` is rejected as `field.Forbidden` (NOT `field.Invalid`). The **standalone** `validateConfig` independently rejects `db.format` (Forbidden), `dbms.mode`, clustering keys, SSL keys, and control chars — but does **not** reject those three deprecated cluster keys. Always use the `db.*` namespace for 5.x+.
- **why:** These keys silently no-op or fail in 5.26+; rejecting at admission time surfaces the mistake before the pod crash-loops.
- **pinned-by:** validator unit tests for deprecated-key rejection.
- **enforcement:** validator (inline) + unit test.

### id 8 — Storage expansion is orphan-delete + PVC patch
- **scope:** `internal/controller/storage_expansion.go` (~L293), `internal/controller/standalone_storage_expansion.go` (~L220); orphan-delete note in `neo4jenterprisecluster_controller.go` (~L556)
- **rule:** To grow storage: orphan-delete the StatefulSet (not a regular delete — keep the pods/PVCs), compare spec size vs ACTUAL PVC size (not old-spec vs new-spec), wrap PVC patches in `retry.RetryOnConflict`, validate the StorageClass has `allowVolumeExpansion=true` BEFORE patching, and never shrink a PVC.
- **why:** StatefulSet `volumeClaimTemplates` are immutable; orphan-delete lets you recreate the STS pointing at resized PVCs without destroying data. Patching a PVC on a non-expandable StorageClass wedges it.
- **pinned-by:** storage-expansion integration specs.
- **enforcement:** integration test + validator (`allowVolumeExpansion` precheck) + code review.

## TLS & Bolt client

### id 9 — TLS CA auto-discovery from cert-manager Secret
- **scope:** `internal/neo4j/client.go` (`buildTLSConfig`, ~L208; secret name `fmt.Sprintf("%s-tls-secret", resourceName)` ~L220)
- **rule:** `buildTLSConfig()` auto-loads the CA from the cert-manager-generated Secret named `{resourceName}-tls-secret`. `TrustedCASecret` is an explicit override; `InsecureSkipVerify` is a last-resort fallback only.
- **why:** Cluster SSL defaults to strict; the operator's own Bolt client must trust the same CA the pods present, or every reconcile connection fails handshake.
- **pinned-by:** TLS client tests in `internal/neo4j/client_test.go`.
- **enforcement:** unit test + code review.

### id 10 — Every client constructor handles TLS
- **scope:** `internal/neo4j/client.go` — `NewClientForEnterprise` (~L326), `NewClientForEnterpriseStandalone` (~L254), `NewClientForPod` (~L160) all call `buildTLSConfig()`; split-brain detector uses dynamic `bolt+s://`
- **rule:** All three client constructors MUST call `buildTLSConfig()`. The split-brain detector switches scheme to `bolt+s://` when TLS is on.
- **why:** A constructor that skips TLS config silently downgrades to plaintext or fails handshake against a strict cluster; the bug only shows under TLS.
- **pinned-by:** `internal/neo4j/client_test.go` (per-constructor TLS coverage).
- **enforcement:** unit test + code review.

### id 11 — Outbound Bolt URI uses the ROUTING scheme
- **scope:** `internal/neo4j/client.go` (URI builders); only legitimate plain `bolt://` user is `internal/controller/splitbrain_detector.go`
- **rule:** The operator's outbound URI is `neo4j://` / `neo4j+s://` (routing), never `bolt://`. The Go driver only honors `AccessModeWrite` under routing; plain `bolt://` lands wherever the ClusterIP steers it → `Neo.ClientError.Cluster.NotALeader`. The split-brain detector is the ONLY component allowed to use `bolt://` (it must target a specific pod).
- **why:** Writes (CREATE DATABASE, ALTER USER, GRANT …) must reach the leader; routing fetches the leader address, ClusterIP `bolt://` does not.
- **pinned-by:** `internal/neo4j/uri_test.go` — `TestBuildConnectionURIForEnterprise`, `TestBuildConnectionURIForStandalone`.
- **enforcement:** unit test + code review. (See FLAG in notes — CLAUDE.md cites the file, not these exact test names.)

### id 12 — Tight Bolt driver timeouts on the cluster path
- **scope:** `internal/neo4j/client.go` — `NewClientForEnterprise` (cluster): `ConnectionAcquisitionTimeout=10s`, `SocketConnectTimeout=5s`, `MaxTransactionRetryTime=15s` (~L349-355). `NewClientForPod` also uses 10s/5s (~L173-174). NOTE: `NewClientForEnterpriseStandalone` deliberately uses larger 30s/15s/30s (~L271-277) for startup tolerance.
- **rule:** Keep the cluster/pod client at 10s/5s/15s. Under routing these gate routing-table-fetch retries against an unreachable cluster; bumping to 30s+ stalls the reconcile work queue.
- **why:** A slow timeout multiplied by routing retries blocks the controller worker for minutes, starving every other CR.
- **pinned-by:** client timeout assertions in `internal/neo4j/client_test.go`.
- **enforcement:** unit test + code review. (See FLAG in notes — the 10s/5s/15s figures are the CLUSTER path; standalone is intentionally different.)

### id 13 — TLS Secret volume `DefaultMode=0440`
- **scope:** `internal/resources/cluster.go` (TLS volume `DefaultMode: 0o440` ~L1399)
- **rule:** The projected TLS Secret volume uses `DefaultMode=0440` (owner+group read). Neo4j runs as UID/GID 7474 with `FSGroup=7474`.
- **why:** The private key must be readable by the neo4j group but not world-readable; 0440 satisfies both Neo4j's permission check and least-privilege.
- **pinned-by:** `TestBuildStatefulSet_TLSVolumeDefaultMode0440` (`internal/resources/cluster_tls_test.go`).
- **enforcement:** unit test.

## Users / Roles / Privileges

### id 14 — `GetUserRoles` is buggy — do not use
- **scope:** `internal/neo4j/client.go` (`GetUserRoles` ~L2139 — buggy); use `internal/neo4j/users.go` `ListUserRoles` (~L612) or `ShowUser` (~L80)
- **rule:** `GetUserRoles` queries `SHOW USER PRIVILEGES YIELD role` and returns one row per privilege (duplicated/wrong). Use `Client.ListUserRoles` or `Client.ShowUser` instead.
- **why:** Privilege-row count ≠ role count; using `GetUserRoles` over-reports roles and breaks drift reconciliation.
- **pinned-by:** `test/integration/neo4juser_test.go` (user controller role-sync specs exercise `ListUserRoles`/`ShowUser` indirectly). NOTE: the `internal/neo4j` user helpers have **no direct unit test** — known gap; do not cite a non-existent `users_test.go`.
- **enforcement:** integration test (indirect) + code review (the buggy `GetUserRoles` is retained but must not gain callers).

### id 15 — Password rotation via Secret hash
- **scope:** `internal/controller/neo4juser_controller.go`; `Neo4jUser.status.passwordSecretHash`
- **rule:** `status.passwordSecretHash` stores the SHA-256 of the referenced Secret value; rotation is detected on hash change. The password is never persisted in a CR field. Skip `SET PASSWORD` entirely when only `externalAuth` is configured.
- **why:** Storing a hash (not the password) lets the controller detect rotation without leaking the secret into etcd or status.
- **pinned-by:** user controller integration specs (rotation-on-hash-change).
- **enforcement:** integration test + code review.

### id 16 — `ALTER USER` clause ordering is REMOVE → ADD → SET (not merely REMOVE-before-SET)
- **scope:** `internal/neo4j/users.go` (`AlterUserOptions` builder ~L166)
- **rule:** On a single `ALTER USER` statement the documented order is **all REMOVE clauses, then all ADD clauses, then all SET clauses**. Use the `AlterUserOptions` builder — never hand-roll ALTER USER strings.
- **why:** Neo4j rejects out-of-order compound ALTERs; the builder enforces the order centrally. This rule previously read "REMOVE before SET", which is *currently* sufficient only by accident: `ADD` exists solely for `ADD TAG[S]`, and the operator has **no tag support at all** (no `Neo4jUser` field, no clause emitted), so it never generates an ADD. That makes the omission a latent trap rather than a live bug — whoever adds tag support will naturally append `ADD TAGS` after the SET clauses and get a syntax error this rule would not have warned them about. Corrected against the current operations manual (`authentication-authorization/manage-users`), which also documents `SET TAG[S]` / `REMOVE TAG[S]` / `REMOVE ALL TAG[S]` on both `CREATE USER` and `ALTER USER` as an unimplemented capability gap.
- **pinned-by:** `test/integration/neo4juser_test.go` (ALTER USER exercised via user-update specs). NOTE: the `AlterUserOptions` builder has **no direct unit test** — known gap worth closing, and the ADD position is unpinned by construction since no code path emits one.
- **enforcement:** integration test (indirect) + code review — **PROSE-ONLY — at risk** for the ADD position until a tag-bearing clause exists.

### id 17 — Missing custom roles do NOT fail reconcile
- **scope:** `internal/controller/neo4juser_controller.go` (`ConditionTypePendingDependencies` / `ConditionReasonRolesPending` set ~L254; watch on `Neo4jRole` in `SetupWithManager` ~L615)
- **rule:** A referenced custom role that doesn't exist yet must NOT fail the user reconcile. Set the `PendingDependencies` condition and requeue; the user controller watches `Neo4jRole` so the user re-reconciles when the role lands.
- **why:** CRs are applied in arbitrary order; failing hard on a not-yet-created role would wedge legitimate apply-everything-at-once workflows.
- **pinned-by:** user controller integration specs (pending-dependency + watch re-reconcile).
- **enforcement:** integration test + code review. Condition constants in `internal/controller/events.go` (`ConditionTypePendingDependencies` L170, `ConditionReasonRolesPending` L178).

### id 18 — Same-namespace `clusterRef` only
- **scope:** `internal/controller/cluster_resolver.go`; user/role validators in `internal/validation/`
- **rule:** `clusterRef` for users/roles must be in the same namespace — cross-namespace refs are not supported in v1. Multi-tenant access goes through an opt-in `Neo4jClusterAccessGrant` CR.
- **why:** Cross-namespace privilege grants are a security boundary; v1 keeps the blast radius inside one namespace.
- **pinned-by:** validator unit tests (cross-namespace rejection).
- **enforcement:** validator (inline) + unit test.

### id 19 — Identifier quoting in Cypher
- **scope:** `internal/neo4j/auth_rules.go` (`escapeBackticks` ~L144), `internal/neo4j/users.go`, `internal/neo4j/privileges.go`
- **rule:** Role/user names go through `escapeBackticks()` before interpolation into Cypher identifiers. NEVER `fmt.Sprintf` user-controlled names into Cypher unescaped. Passwords and provider IDs go through driver parameters (`$param`), never string interpolation.
- **why:** Cypher identifier injection — a name with a backtick can escape the identifier and execute arbitrary Cypher with admin privileges.
- **pinned-by:** Cypher-escaping unit tests in `internal/neo4j/` (e.g. privileges/users/auth_rules tests).
- **enforcement:** unit test + code review.

### id 20 — Privilege drift via `SHOW ROLE PRIVILEGES AS COMMANDS`
- **scope:** `internal/neo4j/privileges.go` (`CanonicalisePrivilegeStatement` ~L44, `DerivePrivilegeRevoke` ~L347); `internal/controller/neo4jrole_controller.go`
- **rule:** Source of truth is `Neo4jRole.spec.privileges`. The controller canonicalises both desired and observed sides (`CanonicalisePrivilegeStatement`), diffs them as sets, and derives REVOKE statements (`DerivePrivilegeRevoke`). Immutable rows are excluded from revokes; drift is surfaced via `status.privilegeDrift`.
- **why:** Privilege statements have many textually-equivalent forms; canonicalising both sides prevents oscillation, and deriving REVOKEs (rather than trusting user input) prevents the controller from revoking immutable system privileges.
- **pinned-by:** `TestCanonicalisePrivilegeStatement`, `TestDerivePrivilegeRevoke`, `TestDerivePrivilegeRevoke_Errors`, `TestCanonicalisePrivilegeStatement_PBAC`, `TestDerivePrivilegeRevoke_PBAC` (`internal/neo4j/privileges_test.go`).
- **enforcement:** unit test.

### id 21 — Privilege statement validation
- **scope:** `internal/validation/role_validator.go`
- **rule:** Entries in `Neo4jRole.spec.privileges` MUST start with `GRANT` or `DENY` (REVOKE is rejected — the operator derives revokes) and MUST end with `TO <spec.name>`.
- **why:** REVOKE-as-input would let a role spec revoke arbitrary privileges; pinning the suffix to the role's own name prevents a role from granting privileges to a different role.
- **pinned-by:** `internal/validation/role_validator_test.go`.
- **enforcement:** validator (inline) + unit test.

### id 22 — `Neo4jRoleBinding` never creates or drops users
- **scope:** `internal/controller/neo4jrolebinding_controller.go`
- **rule:** A RoleBinding only manages role grants for externally-provisioned users (SSO/LDAP first-login). It never creates or drops a user. An absent user → `UserNotFound` and the binding waits.
- **why:** RoleBindings exist to authorize users provisioned by an external IdP; creating users would duplicate `Neo4jUser`'s job and clash on lifecycle.
- **pinned-by:** rolebinding controller integration specs (UserNotFound wait).
- **enforcement:** integration test + code review.

### id 23 — RoleBinding/User overlap rejected
- **scope:** `internal/validation/rolebinding_validator.go`
- **rule:** A `Neo4jRoleBinding` is rejected when its `clusterRef`+`username` match an existing `Neo4jUser` in the same namespace.
- **why:** Two controllers managing the same user's roles would fight; the validator forces one owner.
- **pinned-by:** `internal/validation/rolebinding_validator_test.go`.
- **enforcement:** validator (inline) + unit test.

### id 24 — `enforceExclusive` defaults to false
- **scope:** `internal/controller/neo4jrolebinding_controller.go`; `Neo4jRoleBinding.spec.enforceExclusive`
- **rule:** `enforceExclusive` defaults to false (the binding manages only `.spec.roles` + `status.grantedRoles`). `true` revokes any role on the user not listed in `.spec.roles`. Never flip the default.
- **why:** Default-exclusive would silently revoke roles granted by other tools/IdPs on first reconcile — a destructive surprise.
- **pinned-by:** rolebinding controller integration specs (exclusive vs additive).
- **enforcement:** integration test + code review.

### id 25 — Diagnostics user/role lists are capped
- **scope:** `internal/controller/diagnostics_users_roles.go` (`maxDiagnosticUsers = 50`, `maxDiagnosticRoles = 50` ~L34-35; full count in `UserCount`/`RoleCount`)
- **rule:** User/role lists surfaced in diagnostics are bounded by `maxDiagnosticUsers` / `maxDiagnosticRoles` (50 each); the full count goes in `UserCount` / `RoleCount`. Never remove the caps without a pruning strategy.
- **why:** A cluster with thousands of users would bloat the CR status object (etcd value-size limits) and slow every reconcile that writes status.
- **pinned-by:** diagnostics unit tests in `internal/controller/` (cap behavior).
- **enforcement:** unit test + code review.

## Truststore / Volumes

### id 26 — Truststore init container seeds from JDK cacerts FIRST
- **scope:** `internal/resources/cluster.go` (`BuildTrustStoreInitContainer` ~L3121; seed script copies `${JAVA_HOME}/lib/security/cacerts` → `/truststore/truststore.jks` ~L3130-3134)
- **rule:** The init container MUST copy `$JAVA_HOME/lib/security/cacerts` to `/truststore/truststore.jks` BEFORE importing user CAs. The seed makes `spec.trustedCASecrets` purely additive.
- **why:** Without seeding, the JKS contains only user CAs and Neo4j loses trust in public CAs (Let's Encrypt etc.) — egress to cloud storage / OIDC providers breaks.
- **pinned-by:** truststore init-container builder tests in `internal/resources/`.
- **enforcement:** unit test + code review.

### id 27 — `trustedCASecrets` Secret-name = keytool alias (must be unique)
- **scope:** `internal/validation/truststore_validator.go`
- **rule:** Each `spec.trustedCASecrets` Secret name is used directly as the keytool alias in the JKS, so names must be unique; the validator rejects duplicate Secret names. Keep the alias derivation statically derivable from the spec.
- **why:** keytool fails on duplicate aliases, breaking the whole truststore build; rejecting duplicates at admission gives a clear error instead of an init-container crash-loop.
- **pinned-by:** `internal/validation/truststore_validator_test.go`.
- **enforcement:** validator (inline) + unit test.

### id 28 — Legacy `spec.auth.trustStore` folds into `trustedCASecrets`
- **scope:** `internal/resources/cluster.go` (`CollectTrustedCASecrets` ~L3092)
- **rule:** Legacy `spec.auth.trustStore` is folded into the plural `spec.trustedCASecrets` via `CollectTrustedCASecrets`. Never wire the legacy field directly — doing both produces duplicate volumes/init containers and the JKS build fails on duplicate alias.
- **why:** Two code paths producing the same volume → duplicate-alias keytool failure; a single collection function dedups.
- **pinned-by:** `CollectTrustedCASecrets` builder tests in `internal/resources/`.
- **enforcement:** unit test + code review.

### id 29 — `extraVolumeMounts` reserved paths rejected
- **scope:** `internal/validation/truststore_validator.go` (`reservedMountPaths` ~L34)
- **rule:** The validator rejects `spec.extraVolumeMounts` at any reserved path: `/data`, `/logs`, `/conf`, `/ssl`, `/plugins`, `/truststore`, `/truststore-ca`, `/var/lib/neo4j`, and its standard subdirectories (`/var/lib/neo4j/{data,logs,conf,plugins,certificates}`).
- **why:** A user mount at a reserved path shadows operator-managed volumes (config, certs, data) and silently breaks the deployment.
- **pinned-by:** `internal/validation/truststore_validator_test.go` (reserved-path cases).
- **enforcement:** validator (inline) + unit test.

## Auth / AuthRule / OIDC

### id 30 — AUTH RULE Cypher requires `CYPHER 25` prefix
- **scope:** `internal/neo4j/auth_rules.go` (`cypher25Prefix = "CYPHER 25 "` L32; prepended to every AUTH RULE statement — SHOW/CREATE/ALTER/DROP ~L62-245)
- **rule:** Every AUTH RULE statement prepends `cypher25Prefix`. The 2026.x system DB defaults to Cypher 5; without the prefix you get `42I06: Invalid input 'AUTH'`. Keep the prefix even after the default flips.
- **why:** AUTH RULE syntax is Cypher 25-only; the system DB's default language is not guaranteed to be 25, so the prefix is mandatory.
- **pinned-by:** auth_rules Cypher-prefix unit tests in `internal/neo4j/`.
- **enforcement:** unit test + code review.

### id 31 — `oidc-`-prefixed provider name in ABAC config
- **scope:** `internal/controller/neo4jauthrule_controller.go` (`abacAuthorizationProvidersKey = "dbms.security.abac.authorization_providers"` ~L54, precondition check ~L518); `internal/validation/auth_validator.go` (`strings.HasPrefix(provider, "oidc-")` ~L87); cluster authz providers emitted in `internal/resources/cluster.go` (`dbms.security.authorization_providers` ~L2818)
- **rule:** `dbms.security.abac.authorization_providers` values must use the same form as `dbms.security.authorization_providers` — `oidc-<name>` for OIDC providers. The authrule controller checks the cluster has `dbms.security.abac.authorization_providers` set (a precondition it reads, not one it writes).
- **why:** Mismatched provider naming between the two keys means ABAC rules never match the configured authorization provider, silently denying access.
- **pinned-by:** `internal/validation/auth_validator_test.go` (oidc-prefix); `internal/resources/auth_config_test.go` (`oidc-okta`, `oidc-azure` provider strings).
- **enforcement:** validator + unit test + code review. NOTE: scope file is the AUTHRULE controller, not an `auth_config.go` (that file does not exist — `BuildAuthConfig` lives in `internal/resources/cluster.go`). See FLAG in notes.

### id 32 — Authrule controller in the `--controllers` default list
- **scope:** `cmd/main.go` (dev-mode `controllersToLoad` default includes `authrule` ~L136; production `setupProductionControllers` wires it unconditionally ~L307/550)
- **rule:** The dev-mode `--controllers` default string MUST include `authrule`; production (`setupProductionControllers`) wires it unconditionally.
- **why:** Dropping `authrule` from the dev default means AUTH RULE CRs silently never reconcile in dev/test — a hard-to-diagnose "my rule does nothing".
- **pinned-by:** main wiring is covered by controller-registration smoke checks; default string is asserted to contain `authrule`.
- **enforcement:** code review + integration smoke.

### id 33 — LDAP `useStartTLS` defaults to true for plain `ldap://`
- **scope:** `internal/resources/cluster.go` (`buildLDAPConfig`; `use_starttls` defaulting ~L2874-2892)
- **rule:** When `useStartTLS` is nil and the host is plain `ldap://` → emit `dbms.security.ldap.use_starttls=true`. `ldaps://` hosts skip StartTLS. An explicit `false` is honored.
- **why:** Secure-by-default — a plain `ldap://` bind would send the LDAP system password in cleartext; StartTLS upgrades the connection unless the user explicitly opts out.
- **pinned-by:** `TestBuildAuthConfig_LDAP_UseStartTLSDefault` (`internal/resources/auth_config_test.go`, 6 cases).
- **enforcement:** unit test.

## Network / Metrics / Audit

### id 34 — NetworkPolicy peer-rule ports mirror pod ContainerPorts
- **scope:** `internal/resources/networkpolicy.go` (`BuildNetworkPolicyForEnterprise` ~L63)
- **rule:** The peer (intra-cluster) rule covers `6000/7000/7688/7689`. Adding an intra-cluster ContainerPort to the StatefulSet without adding it here silently breaks pod-to-pod traffic on enforcing CNIs.
- **why:** Once a NetworkPolicy selects a pod, all non-listed traffic is denied; a new cluster port not mirrored here is dropped between pods.
- **pinned-by:** `TestBuildNetworkPolicyForEnterprise_PeerPortsRestrictedToCluster` (`internal/resources/networkpolicy_test.go`).
- **enforcement:** unit test.

### id 35 — NetworkPolicy public rule MUST include port 2004 (Prometheus)
- **scope:** `internal/resources/networkpolicy.go` (`BuildNetworkPolicyForEnterprise`)
- **rule:** The public ingress rule must include port 2004 (Prometheus scrape) alongside HTTP/HTTPS/Bolt. Once any rule selects the pod, it's fully isolated — omitting 2004 silently kills metrics scraping.
- **why:** Same isolation semantics as id 34: a selected pod denies everything not explicitly allowed, so the metrics port must be listed.
- **pinned-by:** `TestBuildNetworkPolicyForEnterprise_PublicPortsOpen` (`internal/resources/networkpolicy_test.go`).
- **enforcement:** unit test.

### id 36 — `BuildNetworkPolicy*` returns nil when disabled
- **scope:** `internal/resources/networkpolicy.go`; standalone reconciler uses `reflect.DeepEqual` to skip churn
- **rule:** `BuildNetworkPolicy*` returns nil when NetworkPolicy is disabled, and the reconcilers short-circuit on nil. The standalone path additionally uses `reflect.DeepEqual` to avoid resourceVersion churn.
- **why:** Returning an empty policy would isolate pods even when the feature is off; nil + short-circuit means "don't manage it at all". DeepEqual prevents needless update writes that bump resourceVersion and re-trigger reconciles.
- **pinned-by:** networkpolicy builder unit tests (nil-when-disabled).
- **enforcement:** unit test + code review.

### id 37 — Metrics JMX + CSV disabled UNCONDITIONALLY
- **scope:** `internal/resources/cluster.go` — emitted in the main `BuildConfigMapForEnterprise` body (~L1661-1668), deliberately **NOT** inside `BuildMonitoringConfig` (the code comment at ~L1661 says so explicitly); it sits OUTSIDE the `monitoring.enabled` branch (which begins ~L1679), so `server.metrics.{jmx,csv}.enabled=false` is emitted regardless of `monitoring.enabled`
- **rule:** `server.metrics.jmx.enabled=false` and `server.metrics.csv.enabled=false` are emitted unconditionally, OUTSIDE the `monitoring.enabled` branch. JMX is unauthenticated remote management; CSV writes pod-ephemeral files that fill disk.
- **why:** These are security/stability kill-switches, not monitoring features — gating them on `monitoring.enabled` would re-enable an unauthenticated JMX management port whenever monitoring is off.
- **pinned-by:** `TestBuildConfigMapForEnterprise_MetricsHardening` (`internal/resources/cluster_tls_test.go`), `TestBuildMonitoringConfig` (`internal/resources/cluster_test.go`).
- **enforcement:** unit test.

### id 38 — `spec.audit` emission order (audit wins over monitoring; user config wins over both)
- **scope:** `internal/resources/cluster.go` (`BuildAuditConfig` ~L2083, runs AFTER `BuildMonitoringConfig` ~L2015)
- **rule:** `BuildAuditConfig` runs after `BuildMonitoringConfig`; both touch `db.logs.query.obfuscate_literals` and last-write-wins gives audit priority over monitoring. User `spec.config` is appended last and wins over both. No `dbms.security.audit.*` keys (4.x, removed) — use `security.log` / `query.log`.
- **why:** Deterministic precedence: audit's obfuscation intent must override the monitoring default, and an explicit user override must beat both.
- **pinned-by:** `TestBuildAuditConfig_PrecedenceOverMonitoring` (`internal/resources/audit_config_test.go`).
- **enforcement:** unit test.

### id 39 — `spec.audit.Enabled` is a hint, not a stomping default
- **scope:** `internal/resources/cluster.go` (`BuildAuditConfig` ~L2083)
- **rule:** `Enabled=true` with `ObfuscateQueryLiterals` nil → emit `obfuscate_literals=true`. An explicit value (true OR false) always wins. Exactly ONE `obfuscate_literals` line is emitted.
- **why:** "Enabled" should imply safe obfuscation by default, but must never override an operator who explicitly set `false`; emitting the key twice would make behavior config-order-dependent.
- **pinned-by:** `TestBuildAuditConfig_ExplicitObfuscateFalseDespiteEnabled` (`internal/resources/audit_config_test.go`).
- **enforcement:** unit test.

## Testing / CI harness

> ids ≥ 80 are post-checklist additions (the original CLAUDE.md checklist was 1–79; ids 40–79 are the backup/restore/sharding rules in `docs/knowledge/backup-restore.md`).

### id 80 — Integration in-pod exec must be bounded (never raw shared-context `kubectl exec`)
- **scope:** `test/integration/integration_suite_test.go` (`boundedExec`, `execOut`, `podExecTimeout`); every in-pod exec call site across the `test/integration/*_test.go` specs.
- **rule:** Every in-pod `kubectl exec` in an integration spec MUST go through `boundedExec` / `execOut` — which run `kubectl exec <pod> -n <ns> -- <cmd…>` under a per-attempt `podExecTimeout` context (60s) with `cmd.WaitDelay`. Never call `exec.CommandContext(ctx, "kubectl", "exec", …)` directly with the shared spec `ctx` / `SpecContext`: it carries no per-attempt deadline.
- **why:** The shared spec context is only cancelled at the SpecTimeout, so a stuck kubelet exec stream (seen under CI node load) blocks `cmd.CombinedOutput()` indefinitely — and Gomega's `Eventually` cannot retry a function that never returns. One hung exec therefore burns the entire ~10-minute spec budget instead of retrying. This produced the 5.26 `Neo4jUser end-to-end "creates, rotates and drops a user"` flake: the operator had already created the user (Ready + visible in `SHOW USERS`), but the test's "authenticate as appuser" exec never returned. A bounded attempt is killed at `podExecTimeout` and the surrounding `Eventually` retries a fresh exec.
- **pinned-by:** the `boundedExec` / `execOut` helpers in `test/integration/integration_suite_test.go`. No dedicated unit test guards test-harness usage — **known gap**. Manual check: `grep -rn 'exec.CommandContext(ctx, "kubectl"' test/integration/` must return only the helper (0 raw call sites today).
- **enforcement:** convention + code review — **PROSE-ONLY — at risk**. Not covered by the `unit-tests` job (test-harness code, not operator code) and deliberately out of scope for `scripts/check-invariants.sh` (that guard is the 5 hard invariants only, and its non-test grep helper skips `_test.go`). Landed with the fix in PR #302.

### id 81 — AuraIPFilter runs on the v2beta1 API; contract follows the official v2beta1 `IpFilter` schema (BETA)
- **scope:** `internal/aura/ipfilter_v2beta1.go` (whole file); `internal/controller/auraipfilter_controller.go`; `api/v1beta1/auraipfilter_types.go`.
- **rule:** IP filtering is only exposed on the Aura API **v2beta1**, an unstable beta. The client shape is taken from the official v2beta1 OpenAPI spec (the `IpFilter` schema + the ip-filters paths). Three landmines the spec pins — respect them, don't "simplify" them back: (1) filters are **organization-scoped** at `/organizations/{org}/ip-filters` (built by `orgIPFilterPath`), NOT under a project or instance; (2) the ip-filter endpoints return the object/array **directly, NOT wrapped in a `{"data":…}` envelope** — v2beta1's envelope is per-endpoint, not global (as of the 2026-07-30 spec ~18 operations are bare: org ip-filters, agents, import jobs, parts of fleet-manager, and the instance-scoped ip-filters read; the rest are wrapped, so check the endpoint rather than assuming either way); (3) the allowlist is `allow_list:[{address,prefix_len,description}]` (CIDR split into address + prefix length) and a filter is *applied* to instances via `filtered_entities.instances` — there is no `cidrs` field and no per-filter `projectId`/`instanceRef`. The v2beta1 request path (`doV2JSON`) is deliberately separate from the stable v1 `doJSON` so the v1 client is untouched by beta churn. The create/update *request* body is not itself schema'd upstream (the POST has no requestBody); it mirrors the documented `IpFilter` response shape.
- **why:** v2beta1 allows breaking changes without a version bump, and the operator emits strictly-validated payloads — so a silent beta change surfaces as a runtime failure. The first (reconstructed) cut got the path, envelope, and body all wrong; keeping the shape pinned to the published schema + isolated in one file keeps the blast radius to one file if v2beta1 moves again. Do NOT build further v2beta1 features on this foundation without re-diffing against the current published spec.
- **pinned-by:** `TestIPFilterLifecycle`, `TestIPFilterV2Base`, `TestIPFilterIDAcceptsStringOrNumber`, and `TestIPFilterDeleteTreats404AsSuccess` (`internal/aura/ipfilter_v2beta1_test.go`) pin the org-scoped path, the bare (envelope-less) body, and the `allow_list`/`filtered_entities` shape against the published schema. They assert the client matches the *spec*, NOT a live account — there is **no live-API contract test — known gap** pending a validated account.
- **enforcement:** unit test (matches published v2beta1 schema) + **PROSE-ONLY — at risk** for live-account drift. Contract corrected from the official v2beta1 spec after the initial reconstructed cut was found wrong (org-scope, envelope, body).

### id 82 — Aura databases + console-RBAC are API-driven v2beta1 CRDs; self-managed CRDs are cluster-only (BETA)
- **scope:** `internal/aura/database_v2beta1.go`, `internal/aura/members_v2beta1.go`; `internal/controller/auradatabase_controller.go`, `internal/controller/auradatabasebackup_controller.go`, `internal/controller/auradatabaserestore_controller.go`, `internal/controller/auraorganizationmember_controller.go`, `internal/controller/auraprojectmember_controller.go`, `internal/controller/aurainvite_controller.go`; the matching `api/v1beta1/aura{database,databasebackup,databaserestore,organizationmember,projectmember,invite}_types.go`.
- **rule:** Aura resources are managed via **Aura-native, API-driven CRDs on the v2beta1 API — NEVER by pointing `Neo4jDatabase`/`Neo4jUser`/`Neo4jRole`/`Neo4jRoleBinding` at an Aura instance** (that Cypher-over-Bolt path was removed; those CRDs are cluster/standalone-only via `clusterRef`). Landmines the spec pins:
  1. Database endpoints are **instance-nested** (`/organizations/{org}/projects/{project}/instances/{instance}/databases`, built by `dbCollectionPath`) and **ARE `{"data":…}`-wrapped** (unwrapped by `doV2Data`). Aura owns database topology per tier, so `AuraDatabase` has **no topology field**.
  2. `DatabaseSummary` carries **only `id`** — no name and no status. So a database **cannot be adopted by name**; the external-ID annotation is the sole adoption mechanism, and `AuraDatabase.status.phase` cannot reflect Aura-side state.
  3. The **restore body's field is `id`**, not `backup_id` (this body IS schema'd, with `id` required). `DatabaseBackup.status` is a **required** enum `Pending|InProgress|Completed|Failed`, and **`legacy_status` belongs to INSTANCES only** — never to databases or backups. An empty backup status must NOT be read as success: the create response carries only an `id`.
  4. Database **restore is asynchronous and NOT observable** — the spec suggests polling the database GET, but that response has no status field. The phase is therefore `Submitted`, never `Completed`.
  5. v2beta1 "users" are **console/org/project identity**, NOT in-database Neo4j users — there is no API to manage a DBMS user inside an Aura instance.
  6. There are **THREE distinct role vocabularies**, all lowercase-hyphenated, and no SCREAMING_SNAKE form exists anywhere in the API: org = `organization-owner|-admin|-member`; project = `project-admin|-member|-viewer|-metrics-integration-reader`; and **inside an invite body**, project roles are spelled `namespace-viewer|-member|-admin|-metrics-integration-reader`. Roles are **arrays**, and both PATCH bodies set `additionalProperties:false` with the array `required` and `minItems=maxItems=1` — a scalar `{"role":…}` is a hard 400. The user identifier is `user_id`, not `id`.
  7. **There is no `GET /organizations/{org}/invites/{id}`** — only DELETE. Read one invite via the LIST endpoint (`FindInvite`). Adding a user to a project takes a **`user_id` UUID**, never an email.
  Only the **`AuraDatabase` create body** and the fleet-manager deployment/token bodies are un-schema'd upstream — those are BETA/best-effort. The member/invite/restore bodies ARE fully schema'd; treat them as binding.
- **why:** Aura rejects the full `CREATE DATABASE` grammar over Bolt (topology/seed are Aura-managed) and exposes no in-DB user API, so the Cypher path silently supported only a subset and failed opaquely; API-driven CRDs model exactly what Aura offers. Landmines 2–7 are not hypothetical: the first cut of this surface got the role vocabularies, both PATCH bodies, the invite body, the read field names, the restore field name and the backup status field all wrong, and shipped an invite reconciler polling an endpoint that does not exist — the whole console-RBAC surface was non-functional against the real API. It was caught by a spec re-diff on 2026-07-30 before ever being released.
- **pinned-by:** `TestDatabaseLifecycle` + `TestBackupStatusConstantsMatchSpec` (`internal/aura/database_v2beta1_test.go`) pin the instance-nested path, the data envelope, the thin `DatabaseSummary`, the restore body's `id` field and the backup status enum; `TestMembersAndInvites` + `TestRoleVocabulariesMatchSpec` (`internal/aura/members_v2beta1_test.go`) pin all three role vocabularies, both PATCH bodies, the invite body, `user_id`, and that the client never calls `GET /invites/{id}`. **These fixtures must be written from the SPEC, not from the client** — see enforcement.
- **enforcement:** unit test (fixtures + request-body assertions taken from the published v2beta1 schema) + **PROSE-ONLY — at risk** for live-account drift and the undocumented create bodies. **There is still no live-API contract test — known gap** pending a validated account. Historical caution: this rule previously claimed "unit test (matches published v2beta1 schema)" while the fixtures actually echoed the *client's own invented shapes* back at it, so the suite was green against a contract the API never served. A fixture that mirrors the client proves nothing; assert request bodies, and derive every fixture from the spec.

### id 83 — Self-managed Bolt CRDs are cluster/standalone-only; the Cypher-over-Bolt Aura path must not return
- **scope:** `api/v1beta1/neo4jdatabase_types.go`, `api/v1beta1/neo4juser_types.go`, `api/v1beta1/neo4jrole_types.go`, `api/v1beta1/neo4jrolebinding_types.go`; `internal/controller/cluster_resolver.go`; `internal/controller/neo4jdatabase_controller.go`.
- **rule:** `Neo4jDatabase`/`Neo4jUser`/`Neo4jRole`/`Neo4jRoleBinding` target a Neo4jEnterpriseCluster/Neo4jEnterpriseStandalone via a **required `clusterRef`** — and nothing else. Do NOT reintroduce the removed Cypher-over-Bolt Aura path: no `auraInstanceRef` spec field (nor its `has(self.clusterRef) != has(self.auraInstanceRef)` CEL), no `NewClientForAura` Bolt client, no `ResolvedTarget.AuraInstance` arm / `ResolveAuraInstanceRef`, no `auraTierSupportsMultiDatabase` tier gate, and no `AuraInstance` watch on these controllers. Aura databases/access are managed by the Aura-native v2beta1 CRDs instead — see id 82.
- **why:** Aura does not accept the full `CREATE DATABASE` grammar over Bolt (topology/seed are Aura-managed, not user-settable — the operator sent them blindly and relied on runtime rejection), and in-database Neo4j users/roles have no Aura equivalent at all (Aura governs access via console-RBAC, a different concept). The Bolt path silently supported only a subset and failed opaquely; it was removed in PR #305. Full replacement contract: id 82.
- **pinned-by:** `TestResolveDatabaseHost` (`internal/controller/neo4jdatabase_resolvehost_test.go`) asserts the host resolver returns only cluster/standalone (no Aura arm); `TestTargetRefKey` (`internal/controller/role_resolution_test.go`) asserts the dependent-CR match key is clusterRef-only. The **absence** of the `auraInstanceRef` field is a schema fact enforced by the blocking `check-drift` gate (generated CRDs), NOT by a unit test — **known gap:** no unit test fails purely on a re-added Aura targeting arm until these resolver/key tests are updated alongside it.
- **enforcement:** unit test (resolver + match key) + `check-drift` (CRD schema) + **PROSE-ONLY — at risk** for the banned symbols themselves. No `scripts/check-invariants.sh` grep guard was added: that script is reserved for the 5 constitution invariants (this is a runtime/controller convention), and a naive `auraInstanceRef` grep would false-positive on the historical-reference comment in `scripts/check-apiref-drift/main.go`.

### id 86 — Fleet Manager provisioning is Phase 0 of a THREE-phase flow; the deployment token is one-shot
- **scope:** `internal/aura/fleet_v2beta1.go`; `internal/controller/aura_fleet_provision.go`; the `reconcileAuraFleetProvision` / `deprovisionAuraFleet` call sites and `writeFleetStatus` in `internal/controller/neo4jenterprisecluster_controller.go` + `internal/controller/neo4jenterprisestandalone_controller.go`; `api/v1beta1/fleet_target.go`; `internal/validation/fleet_validator.go`.
- **rule:** `spec.auraFleetManagement.provision` adds a **Phase 0** in front of the existing two fleet phases, and all three stay separate: (0) register a Fleet Manager deployment + mint its token into a Secret, (1) merge `fleet-management` into `NEO4J_PLUGINS`, (2) once Ready, `CALL fleetManagement.registerToken($token)`. `provision` and `tokenSecretRef` are **mutually exclusive**. Six landmines:
  1. **The token is returned EXACTLY ONCE** and can never be read back. Persist it to the Secret before touching status, and write the `neo4j.com/external-fleet-deployment-id` annotation **immediately after CreateDeployment, before minting** — that annotation, not status, is the idempotency guard against registering a second deployment.
  2. **Never rotate a token that has already been registered** just because the Secret went missing: rotating invalidates the DBMS's working registration to fix a missing file. `tokenPolicy: CreateIfMissing` (default) refuses and explains; `Rotate` is the explicit opt-in. When registration never succeeded, replacing the token is free and is done automatically.
  3. `POST .../token` and `PATCH .../token` are **STRICTLY COMPLEMENTARY, and the operator cannot tell which state it is in**: POST works only when the deployment has NO token, PATCH only when it HAS one, and *both* signal the wrong state with **HTTP 500** (an internal-sounding "failed to create api key … no rows in result set"), never a 4xx. `GetDeployment` does NOT disambiguate — see landmine 3a. The only reliable sequence is therefore **probe with POST, and on ANY failure fall back to PATCH**; policy then decides whether rotating is permitted. Because `IsTransient` treats 5xx as retryable, an implementation without that fallback **retries forever without progressing**. (This entry previously claimed POST returns 400 and that `GetDeployment` should drive the decision — both were wrong, and the resulting logic could not have worked.)
  3a. **`DetailedDeployment.token` is absent until a running DBMS has CLAIMED the token.** A freshly minted, never-used token reports `token: null` and `dbms: null`, indistinguishable from no token at all. So token metadata (`tokenExpiryTime`, `tokenAutoRotate`, …) legitimately stays empty between provisioning and first registration, and token presence must NEVER drive the mint decision. `DELETE .../token` on a token-less deployment also returns **500, not 404**, so `IsNotFound` will not make it idempotent — treat it as non-fatal.
  4. Deliberate refusals here are marked with `refusef` / `isAuraRefusal` and checked BEFORE `aura.IsTransient`, because it reports every non-`APIError` as retryable — **see id 88**, the single home for that trap, which applies to all Aura reconcilers.
  5. **`setFleetManagementStatus` must mutate `status.auraFleetManagement` in place, never replace the struct** — Phase 0 writes deploymentId/token metadata/telemetry into the same block, and a wholesale replace silently wipes all of it on every registration update.
  6. Fleet telemetry lives in `status.auraFleetManagement.servers`/`.databases`, deliberately **NOT** merged into `status.diagnostics`: that is the operator's own Bolt-derived view, this is Aura's plugin-reported view, and the two can legitimately disagree. Both are capped at `maxFleetTelemetryItems` with untruncated counts alongside, and collection is strictly non-fatal (`telemetryError`).
  The deployment name defaults to `<namespace>-<name>` capped at **30 characters** (the API's documented limit) so two same-named clusters in different namespaces cannot collide in one Aura project. The deployment/token **request bodies are un-schema'd upstream** — BETA/best-effort.
- **why:** This closes the one manual step in fleet onboarding (console wizard → copy token → create Secret). Landmine 4 was a real bug caught by its own test: the refusal in landmine 2 was silently swallowed as transient, so the operator would have retried forever while telling the user nothing. Landmine 5 was latent in the pre-existing code — the two status writers would have fought each other. Phase 0 is non-fatal throughout because an unreachable Aura API must never wedge a cluster reconcile, exactly like `CollectDiagnostics`.
- **pinned-by:** `TestFleetProvision_*` (`internal/controller/aura_fleet_provision_test.go`) pin the annotation-before-mint ordering, adoption by name, the refusal to rotate a registered token, the `Rotate` opt-in, the never-registered replacement path, telemetry bounding/opt-in/non-fatality, and Orphan-vs-Delete cleanup; `TestFleetDeploymentNameCapped` pins the 30-char cap; `TestFleetDeploymentsAndTokens` (`internal/aura/fleet_v2beta1_test.go`) pins the envelope handling and every live-vs-spec field name below. Fixtures are taken from **observed live responses**, not the spec, wherever the two disagree.
- **enforcement:** unit test + CEL (`provision` XOR `tokenSecretRef`, credentials XOR, name cap, both enums) + inline validator (`validateAuraFleetProvision`) + **PROSE-ONLY — at risk** for the undocumented request bodies. Read paths were verified against a live Aura account on 2026-07-30, and the **fleet WRITE paths — deployment create/delete and token create/rotate/delete — were verified by exercising the full lifecycle in a disposable Aura project on 2026-07-31**, which is what exposed landmines 1, 3 and 3a. The **Aura DATABASE write paths (create database, backup, restore, delete) were then verified live on 2026-07-31** against a purpose-built multi-database instance — which is what produced id 90 and the extra divergences in id 87.

### id 91 — A one-shot Aura CR's terminal guard must name the phase the controller actually writes
- **scope:** `internal/controller/auradatabaserestore_controller.go` (the guard at the top of `Reconcile`, and `markSubmitted`); by analogy every other one-shot Aura reconciler (`aurarestore_controller.go`, `aurasnapshot_controller.go`, `auradatabasebackup_controller.go`).
- **rule:** `AuraDatabaseRestore`'s terminal success phase is **`Submitted`**, not `Completed`, and the one-shot guard must list it. `markSubmitted` deliberately refuses to write `Completed` — Aura restores asynchronously and the v2beta1 database endpoint returns only an `id` with no status (id 87 / the `DatabaseSummary` landmine), so completion is genuinely unobservable and claiming it would assert something the operator cannot know. When you add or change a one-shot Aura CR, the guard and the writer must be read **together**: a guard that tests a phase nothing writes is dead code that looks correct.
- **why:** The guard listed only `"Completed"` and `"Error"`. Because nothing ever wrote `"Completed"`, it never fired: every subsequent reconcile — an operator restart, a cache resync, any watch event on the CR — re-submitted the **same restore**, silently overwriting a database that had already been restored. Data loss, from a two-word mismatch between a guard and its writer, in a controller that had **no test coverage at all**. Found while auditing the user-facing docs for id 90, because the API reference documented a `Completed` phase the code cannot produce — the doc drift and the bug had the same root.
- **pinned-by:** `TestAuraDatabaseRestore_IsOneShot` (`internal/controller/aura_native_controllers_test.go`) reconciles three times and asserts `RestoreDatabase` was called **exactly once**, and that the terminal phase is `Submitted`. Verified to fail (3 calls) against the pre-fix guard.
- **enforcement:** unit test. The other one-shot reconcilers were checked and are safe **by a different mechanism, which is worth knowing before you "make them consistent"**: `aurarestore` does guard on phase but uses the same `auraRestore*` constants its writers use (so the two cannot drift apart), while `aurasnapshot` and `auradatabasebackup` have **no phase guard at all** — they are idempotent through their recorded external ID (`status.snapshotId` / `status.backupId`), which is the more robust pattern. **PROSE-ONLY — at risk**: nothing mechanically ties a guard's phase set to the phases its controller writes, so a raw string guard like the one that caused this is not detectable by grep.

### id 90 — `multi_database` is fixed at instance creation, lives only on v2beta1, and is UNKNOWABLE for v1-created instances
- **scope:** `internal/aura/instance_v2beta1.go` (`CreateInstanceV2`, `GetInstanceV2`, `InstanceTypeV2`); `internal/controller/aura_multidatabase.go`; `AuraInstanceSpec.MultiDatabase` / `.OrganizationID` and `AuraInstanceObservation.MultiDatabase` / `.DefaultDatabaseID` in `api/v1beta1/aurainstance_types.go`; the create branch in `internal/controller/aurainstance_controller.go` (`observeOrCreate`) and the refusal path in `internal/controller/auradatabase_controller.go`; `aura.ReasonMultiDBOnly` / `IsMultiDatabaseOnly` in `internal/aura/errors.go`.
- **rule:** An Aura instance can host databases beyond its own built-in one only if it was created **multi-database**, and that is decided at creation and never again — Aura publishes no conversion API. Four consequences, all load-bearing:
  1. **Creating one requires v2beta1.** v1's create body has no such field, so an instance created through v1 — i.e. every instance this operator made before `spec.multiDatabase` existed — can never host an `AuraDatabase`. `spec.multiDatabase: true` switches the CREATE call to `POST /v2beta1/organizations/{org}/projects/{project}/instances`; everything afterwards (observe, resize, pause/resume, upgrade, delete) stays on v1, which does see v2beta1-created instances. The tier must be translated (id 87 item 11) — never pass a v1 tier name — and the AuraDS tiers have no v2beta1 equivalent at all.
  2. **The tier is NOT the control.** "Business Critical / dedicated" is necessary but not sufficient: a business-critical instance created through v1 is still single-database, and its database create is refused just like a free one. Documentation that says "multi-database tiers" without saying "created with the flag" is wrong.
  3. **The verdict is THREE-VALUED — yes / no / unknown — and must never be collapsed to two.** `multi_database` appears only on the v2beta1 instance **detail** (not v1's GET, not even the v2beta1 LIST), and that endpoint 500s for v1-created instances (id 87 item 10). So `status.atProvider.multiDatabase` unset means UNKNOWN, and an unknown verdict must still attempt the create. Recording "false" on a probe failure would wrongly block every AuraDatabase against the majority of instances. Because the underlying fact is immutable, the probe is **one-shot**, guarded by the `neo4j.com/multi-database-probed` annotation — re-probing would burn an API call every reconcile forever on precisely the instances where it cannot work.
  4. **The API's refusal is a 409 and must be reclassified as TERMINAL.** `{"message":"Only multi database Instances can add databases","reason":"multi-db-only"}` arrives with HTTP 409 — the same status Aura uses for "another operation is in flight". `IsConflict` therefore excludes `ReasonMultiDBOnly` explicitly, and the AuraDatabase controller translates it into a refusal (`Ready=False`, reason `InstanceNotMultiDatabase`) with **no requeue**.
  Also: fields the v2beta1 create silently drops (id 87 item 12) are **refused** in combination with `multiDatabase`, in CEL and again in `multiDatabaseCreateRequest`, rather than quietly ignored.
- **why:** Before this, the three `AuraDatabase*` CRDs could not work against any operator-created instance, and the way they failed told the user nothing: a bare 409 whose message ("Only multi database Instances can add databases") names no field, no fix, and no reason it will never succeed — retried every 30 seconds forever because `IsConflict` reads any 409 as "come back later" (the id 88 trap in a different guise). The three-valued rule is the subtle half: the obvious implementation reads the probe's failure as "not multi-database" and then refuses every AuraDatabase on every v1-created instance — turning a clear error for a few users into a silent wall for all of them. Discovered by walking the write paths live on 2026-07-31: create database → backup → restore → delete against a purpose-built multi-database Business Critical instance, after the same sequence had been refused on a free instance and on a v1-created enterprise-db one.
- **pinned-by:** `TestAuraInstance_MultiDatabaseCreatesViaV2beta1` pins that v1 create is NOT used, the tier translation, the org-scoped path, the annotation/status write, and that the next reconcile's wholesale `atProvider` rebuild does not erase the facts; `TestAuraInstance_ProbeFailureRecordsUnknownAndStopsAsking` pins one-shot probing, `unknown` (not false) on a 500, and that Ready survives; `TestAuraDatabase_RefusesKnownSingleDatabaseInstanceWithoutCallingTheAPI` and `TestAuraDatabase_TranslatesTheLiveMultiDBOnly409` pin both refusal routes, the `InstanceNotMultiDatabase` reason, `RequeueAfter == 0`, and that the message names `spec.multiDatabase` instead of echoing the API's; `TestAuraDatabase_UnknownVerdictStillAttemptsTheCreate` pins the three-valued rule from the other side (all in `internal/controller/aura_multidatabase_test.go`); `TestMultiDatabaseCreateRequest` pins the refusals for unsupported tiers and silently-dropped fields; `TestMultiDatabaseOnlyIsTerminalNotAConflict` (`internal/aura/errors_v2beta1_shape_test.go`) pins the 409 reclassification while keeping `ongoing-database-operation` retryable; `TestInstanceTypeV2MapsTheVocabularies`, `TestCreateInstanceV2LiveContract` and `TestGetInstanceV2ReadsMultiDatabase` (`internal/aura/instance_v2beta1_test.go`) pin the client against verbatim live payloads.
- **enforcement:** unit test + CEL (`multiDatabase` immutable, tier restricted to the four v2beta1-mappable types, incompatible-field combinations rejected) + runtime refusal in the reconcilers. **PROSE-ONLY — at risk** for the claim that a v2beta1-created instance stays fully manageable through v1: verified live for GET/DELETE and by v1 recognising the instance in its 409 operation guard, but resize and pause/resume on such an instance were not exercised to completion.

### id 89 — An admin password beginning with `-` bricks the container; reject it, and never generate one
- **scope:** `internal/validation/admin_password_validator.go` (`ValidateAdminSecretPassword`); the call sites in `internal/validation/cluster_validator.go` (`resolveAdminSecretName`) and `internal/controller/neo4jenterprisestandalone_controller.go` (`standaloneAdminSecretName`); the `NEO4J_AUTH` construction in `internal/resources/cluster.go`; `randomPassword` in `test/integration/integration_suite_test.go`.
- **rule:** The admin password (key `password` in the Secret named by `spec.auth.adminSecret`, else the operator default) **must not begin with `-`**, and any generated password must anchor its first character. Both the cluster and standalone paths export `NEO4J_AUTH="<user>/<password>"`; the **upstream** Neo4j entrypoint then runs `neo4j-admin dbms set-initial-password <password>`, whose picocli parser reads a leading `-` as an **option**, so the positional parameter never binds. Validate inline (invariant 1) — the mis-parse is upstream, so the operator cannot fix it at runtime and rejecting the CR is the only way to make it comprehensible. A missing/unreadable Secret is deliberately NOT an error (it may be created after the CR, and the env var is projected `Optional:false`, so Kubernetes already blocks container creation with a clear message).
- **why:** The runtime symptom is maximally misleading. The container dies with `Missing required parameter: '<password>'` and **exit code 64** (picocli's usage-error code) — output that never mentions the password's shape — then CrashLoopBackOffs forever, and because the bad value is baked into the StatefulSet it never self-heals. It presents purely as "Neo4j won't start". `randomPassword` used `base64.RawURLEncoding`, whose alphabet includes `-`, so **1.58% of generated passwords** (measured over 200k samples; theoretical 1/64) tripped it — an intermittent CI failure that was **twice misdiagnosed as a Neo4j CalVer regression** (PR #314's 2026.04 cell, and again on the 2026-07-30 post-merge Extended run), and which motivated a CalVer anchor bump that had nothing to do with the actual cause. It was only identified once `.github/actions/collect-logs` was fixed to actually collect pod logs (id 84) — before that, the container output was never captured at all. Secondary effect worth knowing: a crash-looping JVM restarting for ~95 minutes on a 2-vCPU runner starves the node, which is a plausible aggravator for the unrelated-looking `boundedExec` exec-hang timeouts of id 80 seen in the same run.
- **pinned-by:** `TestValidateAdminSecretPassword_RejectsLeadingDash` and `TestValidateAdminSecretPassword_ToleratesAbsentSecretOrKey` (`internal/validation/admin_password_validator_test.go`) pin rejection of leading `-` (including after whitespace, and `--`), that the message names `set-initial-password` rather than restating the useless runtime error, that it never echoes the password, and that an absent Secret/key/value is tolerated; `TestResolveAdminSecretName` pins that validation inspects the SAME Secret the StatefulSet mounts. The generator anchor is **not** test-pinned — a 1-in-64 property cannot be asserted reliably in one run; it is enforced by construction (`"p" + …`) plus the comment at the call site.
- **enforcement:** runtime-enforced (inline validator rejects the CR) + unit test + convention for the generator — **PROSE-ONLY — at risk** for any FUTURE password source that bypasses `ValidateAdminSecretPassword` (e.g. a new CRD that projects its own `NEO4J_AUTH`). Not grep-guarded: the violating value is runtime data in a Secret, not a source pattern.

### id 88 — `aura.IsTransient` treats EVERY non-`APIError` as retryable; a deliberate refusal must be marked or it vanishes
- **scope:** `internal/aura/errors.go` (`IsTransient`); every `aura.IsTransient(err)` call site in `internal/controller/` (all the `aura*_controller.go` reconcilers plus `internal/controller/aura_fleet_provision.go`).
- **rule:** `IsTransient` returns `true` for **any** error that is not an `*aura.APIError` — by design, on the assumption that such an error is a transport-level failure that never reached the API. Therefore an error the operator raises **itself** — a policy refusal, an unrecoverable-state decision, a "managementPolicies forbids this" — is ALSO reported as transient. When such an error can reach an `IsTransient` branch, it must carry a marker type that is checked FIRST (`fleetRefusalError` / `refusef` / `isAuraRefusal` in `internal/controller/aura_fleet_provision.go` is the reference implementation), or be routed to a terminal `fail(...)`/status write that never consults `IsTransient` (what the Aura CMK, instance, database, member and invite reconcilers do today). Never hand a bare `fmt.Errorf` decision of your own to an `IsTransient` branch.
- **why:** The failure is silent and total: the reconcile returns `nil` with a requeue, so the operator retries forever, emits no event, and writes no status message — the user is told nothing at all while nothing progresses. This is not hypothetical. The fleet provisioner's "refusing to rotate an already-registered token" decision (id 86 landmine 2) was swallowed exactly this way; the operator would have looped indefinitely without ever surfacing the explanation. It was caught only because a test asserted the *status message* rather than just the absence of the rotate call. As of 2026-07-30 no other Aura reconciler is affected — each routes its own refusals to `fail(...)` — but the trap is latent for every new one, and the `IsTransient` fallback is correct and should NOT be changed to fix this.
- **pinned-by:** `TestFleetProvision_RefusesToRotateARegisteredToken` (`internal/controller/aura_fleet_provision_test.go`) asserts the refusal reaches `status.auraFleetManagement.message` and not just that no rotate happened — asserting only the side effect's absence would pass even when the message is lost. **No guard covers the other reconcilers — known gap:** their correctness rests on routing refusals to `fail(...)`, which nothing mechanically checks.
- **enforcement:** unit test (fleet path) + convention — **PROSE-ONLY — at risk** for every other Aura reconciler. Deliberately **not** grep-guarded: catching "a self-raised error reaches an `IsTransient` branch" needs dataflow, not a pattern, and every candidate grep (`fmt.Errorf` near `IsTransient`) false-positives on the many legitimate wrapped API errors.

### id 87 — Where the live Aura API disagrees with its own published spec (verified 2026-07-30, extended 2026-07-31)
- **scope:** `internal/aura/fleet_v2beta1.go`, `internal/aura/ipfilter_v2beta1.go`, `internal/controller/aura_fleet_provision.go` (`parseFleetTime`); anything else built by reading `https://api.neo4j.io/v2beta1/spec.json`.
- **rule:** The published v2beta1 spec is **not** a reliable description of the live API. Where they differ, **LIVE WINS** — do not "correct" the code back to the spec. Observed differences, all confirmed against a real account:
  1. **EVERY fleet-manager endpoint is `{"data":…}`-wrapped** — the single-deployment GET, `POST deployments`, and both `POST`/`PATCH .../token`. The spec declares all four **bare** and is wrong about all four (the GET as `allOf:[DetailedDeployment,{}]`, the writes with no envelope). Decoding bare yields an all-zero struct **with no error**, which is the worst kind of failure: `CreateDeployment` returned an empty ID — so the external-ID annotation was written empty and every reconcile registered ANOTHER deployment — and the token calls returned an empty token. `POST deployments` also returns **HTTP 200, not the documented 201**. Verified by exercising the full lifecycle live on 2026-07-31.
  2. **`token.auto_rotate` (spec) is `token.auto_rotated` (live).**
  3. **`Server.mode_constraints` (spec) is `Server.mode_constraint` (live, singular).**
  4. **`IpFilter` returns an undocumented `brain_ip_addresses_enabled` boolean** absent from the spec entirely. The operator drops it; a PATCH sends only declared fields, so it is not clobbered — but that is inference, not verified.
  5. **`IpFilter.updated_at` is declared `format: date-time` but returns RFC1123** (`"Tue, 09 Jun 2026 14:45:10 GMT"`), while fleet token times return RFC3339 on the same API. Never assume one timestamp layout: `parseFleetTime` tries RFC3339(Nano) and RFC1123(Z) and treats Go's zero time as "never".
  6. **Live responses carry extra undocumented fields throughout** — `id`/`deployment_id`/`health`/`os_arch` on servers, `id`/`claimed_by` on tokens, `id`/`name`/`deployment_id` on both database shapes, `server_id`/`last_seen` on server databases.
  7. **Error body shapes differ BY API VERSION**: v1 returns `{"errors":[{message,reason}]}`, v2beta1 returns a bare `{message,reason}`. Both must be handled.
  8. **The OAuth token endpoints are interchangeable.** v2beta1 declares `tokenUrl: /v2beta1/oauth/token`, but a token from the v1 `/oauth/token` authenticates v2beta1 calls and vice versa. The operator authenticating once at the v1 URL is correct — this was previously flagged as an open risk and is now closed.
  9. **Two different database schemas on two similar endpoints** — see id 86 landmine 5. `.../deployments/{id}/databases` is `Database` (sizing); `.../servers/{sid}/databases` is `ServerDatabase` (role/writer/txn/lag/shards). Confirmed live: the deployment-level response genuinely has no shard or transaction data.
  10. **The v2beta1 instance GET returns HTTP 500 for a v1-created instance** — not 404 — with a body that leaks an internal address and an unrendered Go template: `invalid status code 404 [GET /aura-instances/{{.Instance_id}}]: https://console-api-private.default.svc.cluster.local.:443/aura-instances/<id>`. The instance's *other* v2beta1 sub-resources (`…/databases`, `…/ip-filters`) resolve fine for the same instance; it is the instance GET alone that breaks. Anything reading that endpoint must treat every error as "unknown", never as "absent" or "false" — see id 90.
  11. **The v2beta1 tier vocabulary differs from v1's**, and the API itself is the authority: a bad `type` yields *"Input should be 'virtual-dedicated-cloud', 'business-critical', 'professional' or 'free'"*. v1 uses `free-db` / `professional-db` / `business-critical` / `enterprise-db`. Only `business-critical` is spelled the same in both. The v2beta1 instance create also takes **no `version`** and **no `tenant_id`/`project_id`** (the project is in the path; Aura picks the version), and requires only `name` + `type`.
  12. **v2beta1 SILENTLY IGNORES unknown request fields.** POSTing `totally_unknown_field` alongside a deliberately-bad `type` produced only the `type` error. So v1-only fields sent to a v2beta1 create are dropped, not rejected — the caller gets a 202 and an instance that does not match what it asked for. Callers must refuse such combinations themselves.
  13. **A THIRD error shape exists on v2beta1**: besides the bare `{message,reason}` of item 7, schema validation returns v1's `{"errors":[{field,message}],"message":…,"reason":"validation-error"}`, and *body* validation returns **plain text** (`- at '': missing property 'id'`). Also seen: capitalised keys (`{"Message":…,"Reason":…}`) — harmless only because Go's JSON decoding is case-insensitive. All four must round-trip through `newAPIError` with the status code preserved.
  14. **v1's instance `status` and v1's own operation guard can disagree.** While database-level v2beta1 operations were in flight, `GET /v1/instances/{id}` reported `status: running` while `POST /v1/instances/{id}/pause` refused with 409 *"currently undergoing an operation: creating"*. Do not treat v1 `status: running` as proof that a lifecycle action will be accepted. (Also: v1 rejects a bodyless POST without `Content-Type: application/json` — HTTP 415.)
- **why:** Five of the first nine cost real rework. Items 1–3 were shipped wrong on the strength of the spec and would have silently produced empty telemetry and a never-populated token-expiry field — no error, no log, just zero values. Recording them stops the next spec-driven change from "fixing" the code back to broken. Item 8 closes a documented open question rather than leaving it as a permanent caveat.
- **pinned-by:** `TestFleetDeploymentsAndTokens` (`internal/aura/fleet_v2beta1_test.go`) serves live-shaped fixtures for 1–3, 6 and 9 and asserts the decoded values; `TestIPFilterLifecycle` (`internal/aura/ipfilter_v2beta1_test.go`) covers the ip-filter envelope. `TestGetInstanceV2ReadsMultiDatabase` and `TestCreateInstanceV2LiveContract` (`internal/aura/instance_v2beta1_test.go`) serve the verbatim live payloads for 10–12; `TestMultiDatabaseOnlyIsTerminalNotAConflict` and `TestNewAPIError_AllObservedShapes` (`internal/aura/errors_v2beta1_shape_test.go`) cover 13. Items 4, 5, 7, 8 and 14 are **PROSE-ONLY — at risk**: an undocumented field's absence, a timestamp layout and a cross-version auth equivalence cannot be pinned by a stub server that we ourselves write.
- **enforcement:** unit test (live-shaped fixtures) + **PROSE-ONLY — at risk**. Re-verify with a read-only sweep whenever the Aura spec is re-diffed; the sweep needs only an API client ID/secret and issues GETs exclusively.

### id 85 — Aura v1 is the lifecycle foundation; v1beta5 REMOVES fields we use, and the v1 CMK list is a summary
- **scope:** `internal/aura/client.go` (`DefaultBaseURL`), `internal/aura/types.go`, `internal/aura/cmk.go`, `internal/aura/instances.go`, `internal/aura/snapshots.go`; `internal/controller/auracustomermanagedkey_controller.go`, `internal/controller/aurainstance_controller.go`, `internal/controller/aurasnapshot_controller.go`.
- **rule:** The Aura instance lifecycle stays on **v1 (GA)**. Do NOT "upgrade" the base URL to `v1beta5`: it is **not a superset of v1** — it *removes* `secondaries_count`, `cdc_enrichment_mode` and snapshot `exportable` entirely (zero occurrences in the v1beta5 spec, schemas and examples alike), and drops `/graph-analytics/sessions*`. Moving would silently delete working CRD fields. v2beta1 is also not a candidate for lifecycle: it still has no pause/resume/snapshot/overwrite/upgrade/CMK (it *did* gain instance `memory`+`storage` scaling via PATCH, so "v2beta1 can't scale" is no longer true — the rest of the gap is what keeps us on v1). Three further v1 landmines: (1) **`GET /customer-managed-keys` returns a SUMMARY** (`id`, `name`, `tenant_id` only — `CustomerManagedKeySummary`); never compare `key_id`/`region`/`cloud_provider`/`instance_type` against a list entry, they are always empty there. Narrow on `name`, then confirm with `GET /{id}`. (2) `CreateInstanceRequest.Memory` must never serialize as `""` — a CEL rule requires `memory` for every tier except `free-db`, and the field carries `omitempty` so free-db omits it rather than sending an empty string. (3) `secondaries_count` and `cdc_enrichment_mode` are **absent from v1's PATCH `properties`** but real: they are named in the endpoint description and its "Update Secondary Count" / "Update CDC Enrichment Mode" examples. Same for snapshot `exportable`, which v1 shows only in examples and v2beta1 declares as required. Keep all three.
- **why:** The CMK summary mismatch was a live duplicate-registration bug: adoption compared four fields the list endpoint never returns, so it could never match, and a lost `neo4j.com/external-cmk-id` annotation would re-register a second key against the same KMS key. The v1beta5 removals invert the usual intuition that a newer beta is additive, so "just bump the spec version" is a regression waiting to happen. The undeclared-but-real PATCH fields look like operator invention on a naive schema diff and have already been mistaken for such once.
- **pinned-by:** `TestAuraCMK_AdoptByName` and `TestAuraCMK_SameNameDifferentKeyIsRefused` (`internal/controller/aura_controllers_test.go`) pin summary-shaped list output + detail-confirmed adoption + refusal to duplicate on a name collision; the `fakeCMKAPI.ListCustomerManagedKeys` fake returns `CustomerManagedKeySummary` deliberately. The v1beta5 removals are **PROSE-ONLY — at risk**: no test can guard a spec we do not call.
- **enforcement:** unit test (CMK adoption) + CEL (instance `memory`) + **PROSE-ONLY — at risk** for the version-choice rule and the undeclared PATCH fields. Recorded from the 2026-07-30 three-spec re-diff (v1 / v1beta5 / v2beta1). **Beware when re-running that diff with PyYAML:** the v1 `cdc_enrichment_mode` enum is written as bare `OFF`, which YAML 1.1 parses as boolean `false`, making the operator's correct `OFF;DIFF;FULL` enum look like drift.

### id 84 — Composite-action steps must never self-gate on `failure()`
- **scope:** `.github/actions/collect-logs/action.yml` (all steps); the `Collect cluster logs` step in `.github/workflows/integration.yml` and `.github/workflows/integration-tests.yml`. Applies to any future composite action under `.github/actions/`.
- **rule:** A step *inside* a composite action MUST NOT use `failure()` / `success()` to detect that the **calling job** failed — inside a composite action those functions evaluate against that action's **own** step statuses, not the job's. Gate at the **caller** (`if: failure()` on the `uses:` step) and keep the composite's own steps `if: always()`. Do not "restore" the `if: failure()` that used to sit on the collect-logs steps.
- **why:** Both collect-logs steps carried `if: failure()`. On a red run nothing *inside* the action had failed yet, so `failure()` was false and **every step was skipped silently** — no `cluster-debug.log`, no uploaded artifact, and no way to see why a pod died. Verified on run 30304634581 (PR #314): the action's log group opened and closed in 0.2 ms *after* a 5-spec failure, and `integration-test-logs-2026.04-enterprise` was absent from the run's artifacts — leaving a crash-looping Neo4j (`rbac-shared-cluster-server-0/1`, 12 restarts) undiagnosable from CI alone. The failure mode is invisible: the action still *appears* in the job log, so the gap only shows up when someone goes looking for the logs.
- **pinned-by:** no automated guard — GitHub workflow/action YAML is not covered by the `unit-tests` job. Manual check (anchored so it matches real step conditions, not the prose/comments that *document* this rule): `grep -rnE '^[[:space:]]*if:[[:space:]]*failure\(\)' .github/actions/` must return **nothing** (0 today). The caller-side gate lives on the `uses: ./.github/actions/collect-logs` steps in both integration workflows.
- **enforcement:** convention + code review — **PROSE-ONLY — at risk**. Deliberately not added to `scripts/check-invariants.sh` (that guard is reserved for the 5 constitution invariants; this is CI-harness wiring). Fixed alongside the 2026.04 → 2026.06 CalVer anchor bump.

## Cross-cutting helpers referenced above

- **Condition helpers** (`internal/controller/conditions.go`): `SetReadyCondition` (~L65) is ONLY for the `Ready` condition type; use `SetNamedCondition` (~L88) for `ServersHealthy`/`DatabasesHealthy`/`PendingDependencies`. Pinned by `TestSetNamedCondition_Idempotent`.
- **Structured events** (`internal/controller/events.go`): event reasons (`EventReason*`) and condition type/reason constants (`ConditionType*`, `ConditionReason*`) are defined here. Use `corev1.EventTypeNormal` / `corev1.EventTypeWarning`, never raw reason strings.
- **Env-var ownership annotation** `neo4j.com/cluster-controller-env-vars` (`internal/controller/neo4jenterprisecluster_controller.go` `ownedEnvVarsAnnotation` ~L1156; see also `internal/controller/owned_keys.go`): the cluster controller records the env-var names it owns each reconcile so the next loop can enforce removals (`previously-owned ∖ desired`) via `mergeEnvVars` (~L1221) / `envVarsEqual` (~L1258) without disturbing foreign vars set by plugin/fleet/Aura controllers.
