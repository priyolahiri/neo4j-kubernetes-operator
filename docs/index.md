# Neo4j Kubernetes Operator

The declarative way to run Neo4j Enterprise on Kubernetes.

A Kubernetes operator that manages Neo4j Enterprise clusters, databases, users,
roles, backups, and plugins through Custom Resource Definitions. Every aspect of
a Neo4j deployment — from cluster topology to privilege grants — is expressed as
YAML and reconciled to its declared state.

!!! warning "Independent project — not affiliated with Neo4j, Inc."
    This is a personally maintained, community open-source project. It is **not**
    an official Neo4j product and is **not** affiliated with, endorsed by, or
    sponsored by Neo4j, Inc. "Neo4j" is a trademark of Neo4j, Inc.; it is used
    here only to describe the software this operator manages. You must hold your
    own valid Neo4j Enterprise license to run Neo4j Enterprise.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: production
spec:
  acceptLicenseAgreement: "eval"
  topology:
    servers: 3
  tls:
    mode: cert-manager
    issuerRef: { name: ca-cluster-issuer, kind: ClusterIssuer }
  monitoring:
    enabled: true
```

The manifest above provisions a three-server Neo4j Enterprise cluster with
cert-manager-issued TLS, RAFT-based primary election, and Prometheus metrics
enabled.

---

## Custom Resource Definitions

### Self-managed Neo4j

| Resource | Purpose |
|---|---|
| `Neo4jEnterpriseCluster` / `Neo4jEnterpriseStandalone` | Cluster and single-node deployments |
| `Neo4jDatabase` | Database lifecycle with topology, seed URIs (S3, GCS, Azure), and point-in-time recovery |
| `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding` | Declarative user, role, and privilege management with drift reconciliation |
| `Neo4jAuthRule` | Attribute-based access control (ABAC) — claims-to-roles mapping at OIDC authentication time (Neo4j 2026.03+) |
| `Neo4jPlugin` | Plugin installation for APOC, GDS, Bloom, GenAI, N10s, and GraphQL with automatic security configuration |
| `Neo4jBackup`, `Neo4jRestore` | FULL, DIFF, and AUTO backup types with point-in-time recovery |
| `Neo4jShardedDatabase` | Property sharding for horizontal scale (GA in Neo4j 2025.12 and later CalVer releases) |
| `Neo4jDatabaseAlias` | Database aliases — decouple the name applications connect to from the database it resolves to (Cypher has no `RENAME DATABASE`) |
| `Neo4jReplicaDatabase`, `Neo4jReplicaPromotion` | Cross-cluster replication — read-only replicas fed by a differential backup chain, with one-way promotion for disaster-recovery failover (Neo4j 2026.08+) |

### Neo4j Aura (cloud) — beta

The operator can also drive **Neo4j Aura** through the Aura API, alongside the
self-managed resources above. These are **beta / best-effort**: the instance
lifecycle runs on the Aura API **v1 (GA)**, while multi-database and console-RBAC
run on **v2beta1**, which Neo4j may change without a version bump.

| Resource | Purpose |
|---|---|
| `AuraProviderConfig` | Shared Aura API credentials plus default organization and project |
| `AuraInstance` | Aura instance lifecycle — create, resize, pause/resume, upgrade, delete |
| `AuraSnapshot`, `AuraRestore` | Instance-level snapshots and in-place restore |
| `AuraCustomerManagedKey` | Customer-managed encryption keys (CMK). **Unproven — never exercised against the live API** |
| `AuraIPFilter` | Organization-scoped network allowlists (v2beta1) |
| `AuraDatabase`, `AuraDatabaseBackup`, `AuraDatabaseRestore` | Per-database lifecycle and backups on an Aura instance (v2beta1). Requires an `AuraInstance` created with `multiDatabase: true` — Aura fixes that at creation and cannot convert an existing instance |
| `AuraOrganizationMember`, `AuraProjectMember`, `AuraInvite` | Aura **console** RBAC — organization/project roles and email invites (v2beta1) |

!!! note "Aura console RBAC is not in-database identity"

    `AuraOrganizationMember` / `AuraProjectMember` / `AuraInvite` govern who can
    use the Aura console and projects. In-database users, roles and privileges
    are `Neo4jUser` / `Neo4jRole` / `Neo4jRoleBinding`, which target
    self-managed clusters only.

[Browse the full API reference →](api_reference/neo4jenterprisecluster.md)

---

## Capabilities

**Drift reconciliation.** The operator continuously reconciles live Neo4j state
against the declared specification. Out-of-band changes — privilege revokes,
configuration edits, role drops — are detected and corrected on the next
reconciliation loop.

**Controller-side validation.** All validation runs inline within reconciliation
rather than via admission webhooks, eliminating webhook configuration overhead
and avoiding cluster-wide failure modes when the validator is unavailable.

**Live observability.** Operator status fields surface Neo4j cluster state
sourced from `SHOW SERVERS`, `SHOW DATABASES`, `SHOW USERS`, and
`SHOW ROLES` queries. Server health, database topology, user counts, and
privilege drift are visible via `kubectl describe`.

**Multi-version support.** Neo4j 5.26 LTS (the final SemVer release) and the
CalVer release line — 2025.x, 2026.x, and onward — are both supported, with
automatic version detection for Cypher 25 syntax and version-appropriate
configuration keys.

**Security-checklist coverage.** The operator addresses the
[Neo4j security checklist](https://neo4j.com/docs/operations-manual/current/security/checklist/)
defaults out of the box: strict-by-default intra-cluster mTLS,
`tls.key` mode `0440`, secure-by-default LDAP StartTLS for plain
`ldap://` hosts, JMX MBeans + CSV metrics export disabled, and a
typed `spec.audit` block for compliance-oriented logging. An opt-in
`spec.networkPolicy.enabled` emits an ingress policy that scopes
the backup port (6362) to operator-managed backup pods while leaving
client + Prometheus ports open.

---

## Installation

```bash
helm repo add neo4j-operator https://priyolahiri.github.io/neo4j-kubernetes-operator/charts
helm install neo4j-operator neo4j-operator/neo4j-operator \
  --namespace neo4j-operator-system --create-namespace
```

The operator is also published as an OCI artifact at
`oci://ghcr.io/priyolahiri/charts/neo4j-operator`.

For a step-by-step walkthrough, see [Getting Started](user_guide/getting_started.md).

---

## Documentation

| Section | Contents |
|---|---|
| [Getting Started](user_guide/getting_started.md) | Installation and first cluster deployment |
| [User Guide](user_guide/installation.md) | Configuration, TLS, networking, plugin management |
| [User & Role Management](user_guide/user_role_management.md) | Declarative RBAC end-to-end |
| [Backup & Restore](user_guide/guides/backup_restore.md) | Backup strategies and disaster recovery |
| [`kubectl-neo4j` CLI](user_guide/cli/index.md) | Validate manifests before applying, inspect status, open a shell, collect a support bundle |
| [GitOps Integration](gitops/README.md) | ArgoCD and Flux integration patterns |
| [API Reference](api_reference/neo4jenterprisecluster.md) | Complete CRD field documentation |
| [Architecture](developer_guide/architecture.md) | Operator internals and design |
| [Contributing](developer_guide/contributing.md) | Development environment and contribution workflow |

---

## Project Status

Development of this operator is LLM-driven, not merely LLM-assisted, so expect
bugs — including subtle, non-obvious ones. Independent validation is required
before production use. APIs and behaviour may change between releases.

Contributors are welcome — real-world usage is what will carry the project
forward to an acceptable maturity level. Issues and contributions:
[github.com/priyolahiri/neo4j-kubernetes-operator](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues).
