# Neo4j Enterprise Operator for Kubernetes - Product Requirements Document

**Status**: Work in Progress

This document outlines the product requirements and specifications for the Neo4j Enterprise Operator for Kubernetes.

## Overview

The Neo4j Enterprise Operator is a Kubernetes operator designed to manage Neo4j Enterprise Edition clusters in production environments. This document serves as the comprehensive specification for the operator's functionality, architecture, and requirements.

## Scope

This PRD covers:

- Functional requirements
- Technical specifications
- Architecture design
- Performance requirements
- Security considerations
- Operational requirements

## Target Audience

- Product managers
- Engineering teams
- DevOps engineers
- Enterprise customers
- Solution architects

## Product Vision

To provide a production-ready, enterprise-grade Kubernetes operator that simplifies the deployment, management, and scaling of Neo4j Enterprise clusters while maintaining high availability, security, and performance standards.

## Key Requirements

### Functional Requirements

1. **Cluster Management**
   - Deploy Neo4j Enterprise clusters
   - Manage cluster topology
   - Handle rolling updates
   - Support cluster scaling

2. **Data Management**
   - Database lifecycle management
   - Backup and restore operations
   - Data migration support
   - Schema management

3. **Security**
   - Authentication and authorization
   - TLS encryption
   - Certificate management
   - Role-based access control

4. **Monitoring and Observability**
   - Metrics collection
   - Health checks
   - Event logging
   - Performance monitoring

### Non-Functional Requirements

1. **Performance**
   - Sub-second response times for management operations
   - Support for clusters up to 100 nodes
   - Minimal resource overhead

2. **Reliability**
   - 99.9% availability SLA
   - Automatic failure recovery
   - Zero-downtime updates

3. **Security**
   - Enterprise-grade security standards
   - Compliance with industry regulations
   - Regular security audits

4. **Scalability**
   - Horizontal scaling support
   - Auto-scaling capabilities
   - Multi-region deployments

## Architecture

### High-Level Architecture

The operator follows the standard Kubernetes operator pattern with custom resource definitions (CRDs) and controllers that manage the lifecycle of Neo4j resources.

### Components

1. **Controller Manager**
   - Main operator process
   - Reconciliation loops
   - Event handling

2. **Custom Resources**
   - Neo4jEnterpriseCluster
   - Neo4jDatabase
   - Neo4jBackup
   - Neo4jRestore
   - Neo4jUser
   - Neo4jRole

3. **Supporting Components**
   - Webhooks for validation
   - Metrics collectors
   - Health check probes

## Implementation Plan

### Phase 1: Core Functionality
- Basic cluster deployment
- StatefulSet management
- Service configuration
- Basic monitoring

### Phase 2: Advanced Features
- Backup and restore
- User management
- Security enhancements
- Performance optimizations

### Phase 3: Enterprise Features
- Multi-region support
- Advanced monitoring
- Compliance features
- Integration ecosystem

## Success Metrics

- Customer adoption rate
- Time to deployment
- System reliability metrics
- Performance benchmarks
- Customer satisfaction scores

## Risk Assessment

### Technical Risks
- Kubernetes API changes
- Neo4j compatibility issues
- Performance bottlenecks

### Mitigation Strategies
- Comprehensive testing
- Version compatibility matrix
- Performance benchmarking
- Regular updates

## Conclusion

This PRD serves as the foundation for developing a world-class Neo4j Enterprise Operator that meets the needs of enterprise customers while maintaining high standards of quality, security, and performance.

---

### 1\.  Purpose & Scope 

Neo4j Enterprise already delivers ACID-compliant graph storage, RAFT clustering, online backups, granular security, and a rich query language—but **only** if each of those features is deployed and maintained correctly.

For organisations that run Neo4j **on their own infrastructure**—whether on-prem, in a regulated VPC, or inside a private cloud account—manually wiring those moving parts is error-prone and slow.

The Neo4j Enterprise Operator removes that friction by translating every runtime concern into Kubernetes-native objects (CRDs), providing enterprise-grade automation for self-managed Neo4j deployments.

| Self-Managed Pain | How the Operator Helps | Quantified Benefit |
| ----- | ----- | ----- |
| **Complex bootstrap** – hand-rolling StatefulSets, Services, headless discovery, init scripts | kubectl apply \-f Neo4jEnterpriseCluster.yaml creates primaries, secondaries, Services, and TLS in one step | **90 %** reduction in deployment time |
| **Day-2 drift** – clusters patched by scripts that differ per environment | Declarative spec \= immutable desired state; drift reconciled automatically | Zero config skew across dev → prod |
| **Patch & minor upgrades** with manual leader re-election and RAFT risk | Upgrade engine rolls secondaries first, then primaries, validating consensus between steps | Upgrades in minutes, no customer outage |
| **Backups to S3/GCS/Azure** require key management and cron jobs | Neo4jBackup CRD spawns pod-identity Job/CronJob; no static secrets | Passes CIS-AWS Benchmarks; declaratively manages backup configuration |
| **Least-privilege security** managed via ad-hoc Cypher files | Neo4jUser, Neo4jRole, Neo4jGrant CRDs store RBAC in Git | SOX-ready audit trail; one-click revoke |
| **Multiple teams/namespaces** need separate clusters | A single operator can watch *all* namespaces or a prefix rule (team-\*) | Single operator to match multiple namespaced deployments for multiple teams/workloads |

#### Why This Is Strategic for Self-Managed Customers

1. **Self-Service Velocity** – Teams can provision compliant graph databases without filing tickets, accelerating feature delivery.

2. **Operational Safety Net** – Built-in health checks, RAFT-aware rollouts, and automatic TLS rotation eliminate human error that causes outages.

3. **Regulatory Confidence** – Declarative RBAC, immutable manifests, and Kubernetes Events deliver the evidence trails auditors demand for SOC 2, ISO-27001, and HIPAA.

4. **Cloud portability—**The same CRDs work on AKS, EKS, GKE, OpenShift, or bare metal, which is critical for hybrid or sovereign-cloud strategies.

5. **Cost Efficiency**: One operator instance manages hundreds of namespaces, there is no per-team controller sprawl, and the compute and memory footprint is lower.

6. **Future-Proofing**—The operator road map (blue/green major upgrades, temporal RBAC) ensures self-managed customers stay aligned with Neo4j Cloud capabilities while keeping full infrastructure control.

In short, the Neo4j Enterprise Operator lets self-managed customers enjoy **cloud-service ergonomics**—speed, safety, compliance—while preserving the sovereignty and customizability of running Neo4j on their own Kubernetes clusters.

---

### 2\.  Goals / Non-Goals

| Goals – in scope for Operator | Why users care |
| ----- | ----- |
| **G-1** Simple, declarative deployment of Neo4j Enterprise clusters with primaries \+ secondaries via a Neo4jEnterpriseCluster CRD. | 90 % faster time-to-service; one YAML replaces a multi-page run-book. |
| **G-2** Zero-downtime *patch* & *minor* version upgrades orchestrated by the operator. | Eliminates on-call playbooks; protects SLAs. |
| **G-3** Cluster- and database-level backups & point-in-time restores via Neo4jBackup CRD; storage targets: PVC, S3, GCS, Azure Blob. | Automated backups without custom scripts and cron jobs maintenance |
| **G-4** Pod-identity integration for AWS IRSA, GCP Workload Identity, and Azure Pod-Managed Identity (no long-lived keys). | Passes CIS & SOC2 controls; simplifies credential rotation. |
| **G-5** Automatic TLS (cluster-internal \+ client) through Cert-Manager with seamless cert rotation. | "Encryption by default" with zero manual renewal tickets. |
| **G-6** Declarative **security objects**—Neo4jUser, Neo4jRole, Neo4jGrant—plus idempotent reconciliation & audit trail. | SOX-ready privilege management; Git-backed change history. |
| **G-7** Single operator can watch **single**, **prefix-filtered**, or **all** namespaces, with dynamic cache refresh. | One upgrade path across hundreds of team namespaces; lower resource use. |
| **G-8** Prometheus metrics & OpenTelemetry traces for clusters **and** operator internals. | Ready-made dashboards; root-cause tracing across control- and data-planes. |
| **G-9** Hardened, least-privilege RBAC manifests—no wildcard verbs, no escalate or impersonate. | Security teams can approve install unmodified; passes CIS benchmark. |
| **G-10** Helm chart with exhaustive, documented values (admin secret, watch mode, resources, etc.). | One-command install; declarative cluster customisation. |
| **G-11** Conversion-webhook-backed CRD evolution so future upgrades are non-breaking. | Enables continuous delivery without cluster downtime. |

| Non-Goals – intentionally out of scope | Rationale |
| ----- | ----- |
| **N-1** Support for Neo4j *Community* Edition. | Community edition lacks clustering & enterprise features required by the operator. |
| **N-2** Automated *major*\-version store upgrades (e.g., Neo4j 5 → 6). | Major upgrades may need offline migration; will be handled in a future blue/green feature. |
| **N-3** Full LDAP/Kerberos synchronisation service. | Operator consumes secrets/config but does not replicate external directories. |
| **N-4** Custom Neo4j plugins build/publish pipeline. | Packaging plugins is environment-specific; users can mount plugins via side-cars or init-containers. |
| **N-5** Automated cost optimisation/autoscaling of StatefulSets. | Policies vary by workload; users can combine with KEDA or HPA manually. |
| **N-6** Web UI for cluster management. | Focus on GitOps workflows and CLI tooling; UI adds complexity without significant value for enterprise users. |

### ---

### 3\.  Personas & Key Use-Cases

| Persona | Journey | Time Saved (estimated) |
| ----- | ----- | ----- |
| **Alex** – Platform Admin | Installs operator once (watch.prefix=team-); every team-\* namespace instantly supports Neo4j. | –3 days provisioning |
| **Sia** – Security Engineer | Git PR adds Neo4jRole \+ Neo4jGrant; audit trail via Kubernetes Events. | –90 % effort |
| **Dana** – Data Engineer | Adds DB & user CRDs to Helm chart. | Near-instant |
| **Olivia** – Business Analyst | Uses YAML templates to spin up test DB. | 5 min |
| **Ada** – Auditor | Exports Kubernetes Events for "Security Audit" compliance. | –100 % DBA queries |

### ---

### 4\.  Functional Requirements

| ID | Title | User Benefit | Technical / Acceptance Detail |
| ----- | ----- | ----- | ----- |
| **F-1** | **Neo4jEnterpriseCluster CRD** | One-file HA cluster | OpenAPI validated; fields image, topology, storage, tls, auth, service, backups; webhook defaults edition; Ready condition. |
| **F-2** | Online patch/minor upgrade | No outages | Rolls secondaries → non-leader primaries → leader; checks RAFT via CALL dbms.cluster.role. |
| **F-3** | Neo4jBackup CRD | Git & CLI DR plans | Creates Job/CronJob; streams with pod identity, retention policy, and status BackupSucceeded. |
| **F-4** | Cloud pod identities | No static keys | AWS IRSA, GCP WI, Azure MI; auto-create SA with annotations; webhook validation. |
| **F-5** | TLS via Cert-Manager | Encryption by default | Generates Certificate; hash label triggers restart / hot-reload. |
| **F-6** | External Auth (LDAP/Kerberos/SSO) | Enterprise SSO | Mounts secret with provider config; liveness auth probe. |
| **F-7** | Ingress & Service automation | Apps get the URL immediately | Headless Svc \+ optional L7 Ingress with sticky sessions for Bolt. |
| **F-8** | Online scale out/in | Rapid capacity shifts | Patches replicas; blocks primaries \< 3 or even. |
| **F-9** | Standard Status & Events | Single-pane health | Conditions & Kubernetes Events for every CRD. |
| **F-10** | Prom & OTEL | Dashboards & traces | Exporter side-car; custom gauges; OTEL spans reconcile, cypher.exec, proxy.forward. |
| **F-11** | Admin Secret execution | Central credential | Secret name configurable; mounted read-only. |
| **F-12** | Validation & defaulting webhooks | Fail-fast | \< 500 ms; odd primaries, username regex, priv-grammar. |
| **F-13** | Neo4jDatabase CRD | Safe multi-tenant graphs | CREATE/DROP DATABASE Cypher; status Ready. |
| **F-14** | Neo4jUser CRD | Git-auditable users | Username regex; secret password; role binding; mustChangePassword. |
| **F-15** | Neo4jRole CRD | Declarative roles | Inline privileges optional; uniqueness check. |
| **F-16** | Neo4jGrant CRD | Fine-grained privileges | Statements array; whenNotMatched policy. |
| **F-17** | Security reconcile order | Consistency | Roles → Grants → Users. |
| **F-18** | Idempotent Cypher diff | Re-apply safe | Hash annotation prevents flapping. |
| **F-19** | Multi-namespace modes | One operator for fleet | \`–watch-mode=single |
| **F-20** | Dynamic cache guard | OOM protection | Watches only CRDs, Certificates, StatefulSets, Jobs; RSS ≤ 200 MiB on 500 ns. |
| **F-21** | Hardened RBAC | Passes CIS | ClusterRole without wildcards; per-ns Role for leader-election. |

### ---

### 5\.  Non-Functional Requirements

| Category | Requirement | Target / Acceptance Criteria | Rationale |
| ----- | ----- | ----- | ----- |
| Availability | Operator control-plane high availability | *Operator Pods*: 1 \+ leader-election; a single Pod crash **must not** interrupt reconcile loops for longer than one reconcile interval (≤ 15 s). | Keeps day-2 automation running even during node failures. |
|  | Managed Neo4j clusters | During planned upgrade/scale actions triggered by the operator, recorded service downtime (Bolt/HTTP) **≤ 5s** per requestor. | Meets typical SLOs (99.95 %). |
| Performance & Scalability | Reconcile latency | 95-percentile **\< 1s** from CRD change to first controller action. | Ensures Git-Ops pipelines feel instant. |
|  | Memory footprint | RSS **≤ 200 MiB** while watching 500 namespaces, 50 clusters, 1,000 security CRDs. | Prevents "controller bloat" in multi-tenant platforms. |
|  | Backup throughput | ≥ 150 MB/s sustained streaming to object store (network permitting). | Satisfies large-dataset RPO targets. |
| Security | RBAC least-privilege | ClusterRole may access only resources; no \* verbs, no escalate, no impersonate. | Passes CIS 5.x and NIST 800-53 AC-6. |
|  | Secrets handling | • Admin secret is mounted **read-only**.• Provider secrets via CSI or tmpfs. | Minimises secret exposure risk. |
|  | Network | All inter-pod and client traffic is encrypted (TLS); network policies restrict traffic to the namespace and Ingress controller. | Defense-in-depth. |
| Reliability | Error handling | All controllers use exponential back-off (max 5 m) and set Condition=Degraded for fatal errors. | Surface problems to the monitoring stack. |
|  | Idempotency | Re-applying identical CRDs produces **zero** changes (hash annotation check). | Safe GitOps re-sync. |
| Observability | Metrics | Expose Prometheus metrics for:• Operator internals (controller\_runtime\_\*).• Custom (neo4j\_\*). | Enables SLO dashboards. |
|  | Tracing | OTEL spans for every reconcile path, Cypher execution, proxy call, and trace ID are injected into logs. | Fast root-cause analysis. |
|  | Logging | JSON structured logs, level-led (info/warn/error); no secrets logged; max 100 B per line average. | Log hygiene & cost control. |
| Maintainability | CRD evolution | Versioned (v1alpha2 → future) with conversion webhook; *no breaking field removals* in a minor operator release. | Allows rolling upgrades without cluster downtime. |
|  | Helm chart consistency | Chart changes follow SemVer; **major** bump on breaking values change. | Predictable upgrades for platform admins. |
| Portability | Kubernetes versions | Operator supports n-2 majors (≥ 1.27 currently) and passes conformance tests on EKS, GKE, AKS, and OpenShift. | Broad self-managed adoption. |
| Compliance & Audit | Audit trail | Changes to User/Role/Grant CRDs emit Kubernetes Events and are traceable via Git history for ≥ 1 year. | Satisfies SOX / ISO-27001 evidence requirements. |
| Documentation | Published docs | • CRD OpenAPI spec.• Helm README with value matrix. All are updated with each release. | Reduces onboarding friction. |

These non-functional requirements complement the functional feature set to ensure the operator is **robust, secure, performant, and maintainable** for enterprise, self-managed Neo4j deployments.

---

### 7\.  Core Workflows

This section describes the **day-to-day "happy-path" flows** that users and the operator execute.

For each flow, we list the **trigger**, the **actors**, the **ordered steps**, the **success signal**, and key **failure branches**.

| \# | Flow | Trigger / Actor | Detailed Steps | Success Criteria | Key Failure Handling |
| ----- | ----- | ----- | ----- | ----- | ----- |
| **CW-1** | **Cluster Provisioning** | *Actor* → Alex (YAML) | 1\. User creates Neo4jEnterpriseCluster manifest. 2\. Admission webhook defaults & validates.  3\. Operator reconciler:   a. Create TLS Certificate → secret.  b. Create headless and client Service.  c. Build \*-primary & \*-secondary StatefulSets.  d. Wait until primaries form RAFT quorum (CALL dbms.cluster.role).  4\. Set status conditions{Ready=True}.  5\. Prom gauge neo4j\_cluster\_replicas \> 0\. | Ready=True condition within SLA ( \< 2 min). | • Cert creation fails → Condition=Degraded \+ Event.• Primaries fail quorum (mis-sized storage) → StatefulSet error surfaces via Event. |
| **CW-2** | **Online Patch / Minor Upgrade** | *Actor* → Sam bumps .image.tag (Git PR) | 1\. Operator detects image diff.  2\. Scale-out guard: ensure all secondaries are Ready.  3\. **Rolling order:**  a. Update \*-secondary StatefulSet (partition=0).  b. Verify each secondary catches up (SHOW SERVERS).  c. Update non-leader primaries (ordinal ascending).  d. Update leader last (checks RAFT leadership via Bolt).  4\. status.conditions. UpgradeInProgress flips to False.  5\. Metrics log step durations. | No 5xx errors on Bolt/HTTP probes; UpgradeInProgress=False. | • Store migration needed → Operator dry-run fails, sets Blocked and aborts.• Pod fails readiness → rollback current step, Event escalated. |
| **CW-3** | **Online Scale Out / In** | Alex edits topology.secondaries (Git) | 1\. Reconciler patches \*-secondary replicas.  2\. Join: new pods start with SECONDARY role; RAFT automatically syncs.  3\. Drain: for scale-in operator checks consensus, then deletes the highest ordinal pod.  4\. Prom gauge reflects new replica count. | Desired replicas reached within HPA lag ( \< 90 s). | Scaling primaries to an even number blocked at the webhook with a clear error. |
| **CW-4** | **Nightly Backup** | Cron (Neo4jBackup.schedule) | 1\. Backup reconciler (00:00) creates Job.  2\. Job Pod uses cloud SA via IRSA/WI/MI.  3\. Executes neo4j-admin backup with streaming; writes checksum file.  4\. Pod exits 0; reconciler sets BackupSucceeded \+ timestamp.  5\. Metrics neo4j\_backup\_duration\_seconds observed. | Job Succeeded; status updated inside 10 s. | Non-zero exit or S3 403 → BackupFailed, retry next schedule; Event triggers alert. |
| **CW-5** | **Restore Drill** | Alex applies new Neo4jEnterpriseCluster with .spec.restoreFrom | 1\. Restore: init-container in primaries downloads backup via same cloud identity.  2\. Runs neo4j-admin load; integrity check.  3\. Cluster boots with restored data; Ready=True.  4\. Drill report logged to structured logs. | Ready=True; checksum matches source. | Integrity mismatch → Cluster remains Degraded; Events detail error. |
| **CW-6** | **Security Change (User / Role / Grant)** | Sia pushes Git PR | 1\. Webhook validates names & privilege grammar.  2\. **Ordering** by controller-runtime queue:  a. Role reconciler: CREATE ROLE / SET PRIVILEGE.  b. Grant reconciler: execute statements, respect whenNotMatched.  c. User reconciler: CREATE/ALTER USER; GRANT ROLE TO USER.  3\. Each controller stores checksum in the annotation. | All three CRDs Ready=True; audit events logged. | Duplicate role name → webhook reject (HTTP 400).Cypher error → Condition=Error, Event emitted. |
| **CW-7** | **Certificate Renewal** | Cert-Manager renews before expiry | 1\. Secret version bump triggers StatefulSet pod template hash change.  2\. Rolling restart **or** hot reload via Admin API. 3\. New cert served; Prom TLS expiry panel resets. | No failed client TLS handshakes; pods restart \< 60 s each. | Cert Manager down → Operator logs Warning; falls back to self-signed internal if configured. |
| **CW-8** | **Namespace Auto-Onboarding (Prefix Mode)** | New namespace team-foo created | 1\. Namespace informer detects ADDED.  2\. The prefix matcher adds ns to the dynamic cache.  3\. Clusters in new ns reconcile correctly within refresh interval (≤ 60 s). | Cluster in new ns reconciles correctly. | Cache OOM risk mitigated by CRD-only selectors; metric alerts if RSS \> 200 MiB. |
| **CW-9** | **Operator Upgrade** | Helm helm upgrade run | 1\. New Pods roll with surge 1; leader election hands off automatically.  2\. Conversion webhook (if CRD change) migrates objects on the fly.  3\. Controllers will resume reconciliation with zero missed events. | All previously Ready clusters are still Ready post-upgrade. | Leader election lease conflict → pod crash; Kubernetes restarts until the lease is acquired. |

These nine workflows cover **90 %+** of real-world operations for self-managed Neo4j Enterprise installations, ensuring repeatability and auditability through GitOps workflows.

---

### 8\.  Security Considerations

* Pods run as non-root (runAsUser/fsGroup 7474).

* End-to-end TLS; inter-server encryption defaults to REQUIRED.

* **Cloud identities**:

  * autoCreate: namespace-scoped SA with IRSA / WI / MI annotations; RBAC limited to backup/restore pods.

  * serviceAccountName: webhook validates that annotations match the requested provider.

* Secrets are projected via CSI or memory tmpfs.

---

### 9\.  Observability & Day-2 Ops

* Prom exporter sidecar on every pod.  
* Operator exposes controller-runtime metrics.  
* Grafana dashboards are shipped as JSON.  
* OTEL traces for Cypher commands and runs

---

### 10\.  Testing & QA

| Layer | Tests |
| ----- | ----- |
| CRD schema | OpenAPI \+ webhook validation |
| Controllers | EnvTest suites |
| E2E | Kind matrix {k8s 1.29,1.30}×{neo4j 5.26,calver}×{aws,gcp,azure} |
| DR drill | Hourly backup \+ random restore chaos |

---

### 11\.  Release & Versioning

* Operator SemVer; images signed with Cosign in ghcr.io/neo4j-operator.

* CRD version bump (v1alpha1→v1beta1) on breaking changes.

* Helm chart & Kustomize base provided.

---

### 12\.  Risks & Mitigations

| Risk | Mitigation |
| ----- | ----- |
| Long store migrations | Pre-flight dry-run; maintenance window gate |
| Misconfigured cloud identity | Webhook validation \+ IdentityInvalid status |
| Cert-Manager outage | Fallback to certificates outside cert-manager provided optionally in the CRD |
| StatefulSet volume resize limits | Feature-gate per CSI; doc manual path |

---

### 13\. Appendix: OpenAPI specs for CRDs

```
# ─────────────────────────────────────────────────────────────────────────────
# 1. Neo4jEnterpriseCluster
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jenterpriseclusters.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jEnterpriseCluster
    plural: neo4jenterpriseclusters
    singular: neo4jenterprisecluster
    shortNames: [necluster]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Primaries
      type: integer
      jsonPath: .spec.topology.primaries
    - name: Secondaries
      type: integer
      jsonPath: .spec.topology.secondaries
    - name: Ready
      type: string
      jsonPath: .status.conditions[?(@.type=="Ready")].status
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [image, topology, storage]
            properties:
              edition:
                type: string
                enum: [enterprise]
                default: enterprise
              image:
                type: object
                required: [repo, tag]
                properties:
                  repo: {type: string}
                  tag:  {type: string}
              topology:
                type: object
                required: [primaries, secondaries]
                properties:
                  primaries:
                    type: integer
                    minimum: 3
                    description: Must be odd to maintain quorum.
                  secondaries:
                    type: integer
                    minimum: 0
              storage:
                type: object
                required: [className, size]
                properties:
                  className: {type: string}
                  size: {type: string}
              tls:
                type: object
                properties:
                  mode: {type: string, enum: [cert-manager, disabled]}
                  issuerRef:
                    type: object
                    properties:
                      name: {type: string}
                      kind: {type: string, enum: [Issuer, ClusterIssuer]}
              auth:
                type: object
                properties:
                  provider: {type: string, enum: [native, ldap, kerberos, jwt]}
                  secretRef: {type: string}
              service:
                type: object
                properties:
                  ingress:
                    type: object
                    properties:
                      enabled: {type: boolean}
                      className: {type: string}
                      annotations:
                        type: object
                        additionalProperties: {type: string}
              backups:
                type: object
                properties:
                  defaultStorage:
                    $ref: '#/components/schemas/StorageLocation'
                  cloud:
                    $ref: '#/components/schemas/CloudBlock'
          status:
            type: object
            properties:
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            Condition:
              type: object
              required: [type, status]
              properties:
                type:   {type: string}
                status: {type: string}
                reason: {type: string}
                message:{type: string}
                lastTransitionTime: {type: string, format: date-time}
            StorageLocation:
              type: object
              required: [type]
              properties:
                type:   {type: string, enum: [s3, gcs, azure, pvc]}
                bucket: {type: string}
                path:   {type: string}
            CloudIdentity:
              type: object
              required: [provider]
              properties:
                provider: {type: string, enum: [aws, gcp, azure]}
                serviceAccountName: {type: string}
                autoCreate:
                  type: object
                  properties:
                    enabled:     {type: boolean}
                    annotations:
                      type: object
                      additionalProperties: {type: string}
            CloudBlock:
              type: object
              properties:
                provider: {$ref: '#/components/schemas/CloudIdentity/properties/provider'}
                identity: {$ref: '#/components/schemas/CloudIdentity'}

---
# ─────────────────────────────────────────────────────────────────────────────
# 2. Neo4jDatabase
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jdatabases.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jDatabase
    plural: neo4jdatabases
    singular: neo4jdatabase
    shortNames: [ndb]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Cluster
      type: string
      jsonPath: .spec.clusterRef
    - name: Ready
      type: string
      jsonPath: .status.conditions[?(@.type=="Ready")].status
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [clusterRef]
            properties:
              clusterRef: {type: string}
              options:
                type: object
                additionalProperties: {type: string}
          status:
            type: object
            properties:
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            Condition:
              type: object
              required: [type, status]
              properties:
                type: {type: string}
                status: {type: string}

---
# ─────────────────────────────────────────────────────────────────────────────
# 3. Neo4jBackup
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jbackups.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jBackup
    plural: neo4jbackups
    singular: neo4jbackup
    shortNames: [nbkp]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Target
      type: string
      jsonPath: .spec.target.kind
    - name: Schedule
      type: string
      jsonPath: .spec.schedule
    - name: LastRun
      type: string
      jsonPath: .status.lastRunTime
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [target, storage]
            properties:
              target:
                type: object
                required: [kind, name]
                properties:
                  kind: {type: string, enum: [Cluster, Database]}
                  name: {type: string}
              schedule: {type: string}
              storage:
                $ref: '#/components/schemas/StorageLocation'
              cloud:
                $ref: '#/components/schemas/CloudBlock'
              retention:
                type: object
                properties:
                  maxAge: {type: string}
          status:
            type: object
            properties:
              lastRunTime: {type: string, format: date-time}
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            Condition:
              type: object
              required: [type, status]
              properties:
                type:   {type: string}
                status: {type: string}
            StorageLocation:
              type: object
              required: [type]
              properties:
                type:   {type: string, enum: [s3, gcs, azure, pvc]}
                bucket: {type: string}
                path:   {type: string}
            CloudIdentity:
              type: object
              required: [provider]
              properties:
                provider: {type: string, enum: [aws, gcp, azure]}
                serviceAccountName: {type: string}
                autoCreate:
                  type: object
                  properties:
                    enabled: {type: boolean}
                    annotations:
                      type: object
                      additionalProperties: {type: string}
            CloudBlock:
              type: object
              properties:
                provider: {$ref: '#/components/schemas/CloudIdentity/properties/provider'}
                identity: {$ref: '#/components/schemas/CloudIdentity'}

---
# ─────────────────────────────────────────────────────────────────────────────
# 4. Neo4jUser
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jusers.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jUser
    plural: neo4jusers
    singular: neo4juser
    shortNames: [nuser]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Username
      type: string
      jsonPath: .spec.username
    - name: Roles
      type: string
      jsonPath: .spec.roles
    - name: Ready
      type: string
      jsonPath: .status.conditions[?(@.type=="Ready")].status
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [clusterRef, username, passwordSecret]
            properties:
              clusterRef: {type: string}
              username:
                type: string
                pattern: '^[a-z][a-z0-9_]{2,30}$'
              passwordSecret:
                type: object
                required: [name]
                properties:
                  name: {type: string}
                  key:  {type: string, default: password}
              roles:
                type: array
                items: {type: string}
              mustChangePassword: {type: boolean, default: false}
              suspended: {type: boolean, default: false}
          status:
            type: object
            properties:
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            Condition:
              type: object
              required: [type, status]
              properties:
                type:   {type: string}
                status: {type: string}

---
# ─────────────────────────────────────────────────────────────────────────────
# 5. Neo4jRole
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jroles.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jRole
    plural: neo4jroles
    singular: neo4jrole
    shortNames: [nrole]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Privileges
      type: integer
      jsonPath: .spec.privileges | length(@)
    - name: Ready
      type: string
      jsonPath: .status.conditions[?(@.type=="Ready")].status
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [clusterRef]
            properties:
              clusterRef: {type: string}
              privileges:
                type: array
                items: {$ref: '#/components/schemas/PrivilegeRule'}
          status:
            type: object
            properties:
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            PrivilegeRule:
              type: object
              required: [action, privilege, resource]
              properties:
                action:    {type: string, enum: [GRANT, DENY, REVOKE]}
                privilege: {type: string}
                resource:  {type: string}
                graph:     {type: string}
            Condition:
              type: object
              required: [type, status]
              properties:
                type:   {type: string}
                status: {type: string}

---
# ─────────────────────────────────────────────────────────────────────────────
# 6. Neo4jGrant
# ─────────────────────────────────────────────────────────────────────────────
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: neo4jgrants.neo4j.neo4j.com
spec:
  group: neo4j.neo4j.com
  names:
    kind: Neo4jGrant
    plural: neo4jgrants
    singular: neo4jgrant
    shortNames: [ngrant]
  scope: Namespaced
  versions:
  - name: v1alpha1
    served: true
    storage: true
    additionalPrinterColumns:
    - name: Target
      type: string
      jsonPath: .spec.target.kind
    - name: Stmts
      type: integer
      jsonPath: .spec.statements | length(@)
    - name: Ready
      type: string
      jsonPath: .status.conditions[?(@.type=="Ready")].status
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [clusterRef, target, statements]
            properties:
              clusterRef: {type: string}
              target:
                type: object
                required: [kind, name]
                properties:
                  kind: {type: string, enum: [User, Role]}
                  name: {type: string}
              statements:
                type: array
                items:
                  type: string
                  pattern: '^(GRANT|DENY|REVOKE) .+$'
              whenNotMatched:
                type: string
                enum: [error, ignore, replace]
                default: error
          status:
            type: object
            properties:
              conditions:
                type: array
                items: {$ref: '#/components/schemas/Condition'}
        components:
          schemas:
            Condition:
              type: object
              required: [type, status]
              properties:
                type:   {type: string}
                status: {type: string}
```

