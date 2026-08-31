# Design: `kubectl-neo4j` — a customer-facing CLI

> **Status:** Analysis only. Nothing is implemented. §8 proposes a first slice
> of one command.
> **Source:** this repository, read at commit `6afaf1c`; command-density counts
> measured across `docs/user_guide/`.
> **Scope of this document:** whether an accompanying CLI earns its place, who
> it is for, which commands justify themselves, and what it would cost. The
> audience is decided: **the customer operating a Neo4j deployment**, not Neo4j
> support or field engineering — that choice reorders everything in §4.
> **Headline:** the strongest case is not convenience, it is a structural gap.
> Invariant 1 forbids admission webhooks, so the operator's 29 validators only
> ever speak to a user *after* they apply, through status and events. A CLI
> closes that loop client-side **using the operator's own validation code**,
> which makes it the rare feature that is high-value and near-zero drift-risk
> at once. Everything else in §4 is ergonomics on top of that.

---

## 1. The problem

### 1.1 The docs are teaching a naming scheme by repetition

Manual command lines in the user-facing guides:

| Doc | `kubectl` / `cypher-shell` / `neo4j-admin` lines |
|---|---|
| `guides/troubleshooting.md` | 98 |
| `troubleshooting/split-brain-recovery.md` | 43 |
| `troubleshooting/backup_restore.md` | 43 |
| `guides/backup_restore.md` | 34 |

The count is not the interesting part — the *shape* is. Almost every one is
generic `kubectl` with placeholders the reader has to resolve from knowledge of
the operator's conventions: `<cluster-name>-server-0`, `<cluster-name>-client`,
`<cluster-name>-metrics`, `<cluster-name>-headless`. Three specific things the
docs currently ask a customer to get right unaided:

- **The operator's own namespace differs by install mode** — `neo4j-operator`
  for dev, `neo4j-operator-system` for integration and production. Every "check
  the operator logs" instruction hardcodes one of the two.
- **Cypher access needs a Secret round-trip first.** Extract the password, then
  `kubectl exec <pod> -c neo4j -- cypher-shell -u neo4j -p <password>`. Wrong
  container, or `bolt://` where TLS is enabled (which the operator *rejects*),
  produces an opaque failure.
- **Five status field declarations exist solely to be pasted into another
  resource's spec** — `Neo4jBackup.status.replicationPullURI`
  (`neo4jbackup_types.go:345`), `status.backupsPath` on both `Neo4jBackup`
  (`:401`) and `Neo4jShardedDatabase` (`neo4jshardeddatabase_types.go:254`),
  and `status.crossClusterReplication` / `status.internalAddresses` on
  `Neo4jEnterpriseCluster` (`:954`, `:966`).

That last point is the tell. `ConnectionExamples`
(`api/v1beta1/neo4jenterprisecluster_types.go:1386`) is already a status field
whose only purpose is to hand the user a `kubectl port-forward` command to
type. **The project has been building CLI ergonomics into status fields because
it had nowhere else to put them.**

### 1.2 The structural gap: no webhooks means no pre-apply feedback

This is the part that is not merely ergonomic.

`internal/validation/` holds **29 validators**. `ClusterValidator` alone
composes edition, topology, image, storage and TLS sub-validators, and exposes
`ValidateCreateWithWarnings` returning:

```go
type ClusterValidationResult struct {
	Errors   field.ErrorList
	Warnings []string
}
```

`field.ErrorList` carries a field path per error — precisely what a
command-line renderer wants.

Invariant 1 (**NO WEBHOOKS**) means none of this runs at admission. A customer's
loop today is: write YAML → apply → wait for reconcile → read `status` and
events → find the mistake → repeat. For a CRD surface of **26 kinds and 234
top-level spec fields**, that is a slow first hour and a recurring tax on every
spec change afterwards.

**21 of the 29 validators never touch the Kubernetes client at all**, so they
run fully offline against a YAML file. The 8 that do are the cross-reference
validators (`user`, `role`, `rolebinding`, `database`, `authrule`, `resource`,
`shardeddatabase`, `cluster`) and they need only a kubeconfig, not a running
operator. `ClusterValidator` touches the client in exactly one place
(`cluster_validator.go:212`, resolving the admin Secret).

A CLI therefore delivers webhook-grade feedback **without a webhook and without
re-implementing a single rule** — it exploits the invariant rather than
fighting it.

---

## 2. What this operator already provides

- **Validation with a CLI-shaped API.** 29 validators, an errors-plus-warnings
  result type, field paths on every error. Nothing new to write.
- **Rich status.** Phases, conditions, `status.diagnostics.servers[]` /
  `databases[]`, `ConnectionExamples`. A `status` command is a rendering
  problem, not a data-collection one.
- **A same-module import path.** `cmd/kubectl-neo4j` can import
  `internal/validation` and `internal/resources` directly (module
  `github.com/priyolahiri/neo4j-kubernetes-operator`), so the CLI and the
  operator share one definition of every name and rule. Drift becomes a compile
  error. **This is the single strongest argument for building it in this repo
  rather than as a separate project.**
- **An agent-facing interface already exists.** The MCP server
  (`api/v1beta1/mcp_types.go`, `docs/user_guide/guides/mcp_client_setup.md`)
  covers the LLM modality. A CLI is the human modality; they are complementary
  and would share helpers.

---

## 3. Costs and blockers

Ranked.

### B1 — There is no binary release pipeline *(the largest new surface)*

`.github/workflows/release.yml` builds **container images** with buildx for
`linux/amd64,linux/arm64`. It produces no binaries at all. A CLI needs
darwin/arm64, darwin/amd64, linux/amd64, linux/arm64 and windows/amd64,
checksums, and a signing story — a genuinely new release surface (goreleaser or
equivalent), not an increment on the existing one.

This is the cost most likely to be underestimated, because the *code* for a
first command is small and the *distribution* is not.

### B2 — `internal/` forbids the library story without an API commitment

The "customers can validate manifests in their own CI" pitch has two forms with
very different costs:

- **Invoke the binary** (`kubectl neo4j validate -f manifests/`) — free, works
  today, is what CI actually wants anyway.
- **Import the package** — impossible while validation lives under `internal/`,
  which Go forbids to out-of-module consumers. Promoting it to `pkg/validation`
  is not a file move: it is a public API surface this project would then owe
  compatibility on, forever, for code currently free to change shape every
  release.

**Recommendation: ship the binary story only, and say so explicitly.** Do not
promote to `pkg/` speculatively.

### B3 — A CLI is a second source of truth, in a codebase built around not having those

This project runs `check-drift`, `check-apiref-drift`, `check-crd-catalog` and
`check-knowledge-drift` precisely because hand-maintained parallel surfaces rot.
A CLI that re-encodes naming conventions, status semantics or validation rules
would be the largest such surface yet.

Mitigation is architectural and non-negotiable: **the CLI must consume the
operator's own packages, never restate them.** Name construction comes from
`internal/resources`; validation from `internal/validation`; types from
`api/v1beta1`. Any place the CLI needs a rule the operator does not export, the
fix is to export it, not to copy it.

### B4 — `--dry-run=server` already covers part of the claim

`kubectl apply --dry-run=server` already enforces the CRD's OpenAPI schema —
enums, ranges, required fields — and the CEL rules (for example the
`self == oldSelf` immutability transitions). A `validate` command adds only
what lives in `internal/validation` *beyond* the schema.

That remainder is substantial — cross-field topology rules, `serverRoles` index
and duplicate checks, TLS strictness and issuer-capability checks, rejected
config keys such as `dbms.default_database`, and the resource-floor warnings —
but it is **not** "everything", and the docs must not imply otherwise. Claiming
`validate` replaces server-side dry-run would be false and would be discovered
at exactly the wrong moment.

### B5 — Any mutating command can bypass a safety guard

The operator's write paths carry guards that exist because getting them wrong
loses data. `Neo4jReplicaPromotion` is a one-shot CR specifically so that
re-applying a manifest cannot un-promote a database, and its controller
re-reads `SHOW DATABASE ... YIELD type` before acting, so an out-of-band
promotion is detected rather than "corrected" by drop-and-recreate.

A CLI `promote` that **applies that CR** is safe. A CLI `promote` that calls
`dbms.promoteReplicaDatabase` itself bypasses the guard entirely.

**Hard rule, stated once and enforced in review:**

> The CLI may **create and read Custom Resources** and **read Kubernetes
> state**. It must **never** execute Cypher or `neo4j-admin` for any mutating
> operation. Where a mutation is wanted, the CLI's job is to author the CR that
> already models it.

`connect` / `cypher` are the deliberate exception and are read-through: they
hand the user an interactive session, they do not perform operations on the
user's behalf.

### B6 — Distribution to krew is its own review pipeline

Discoverability via `kubectl krew install neo4j` requires a PR to the krew-index
repository and its review cycle — structurally the same kind of external
submission as the OperatorHub community-operators flow this project already
maintains. Budget it as a separate, recurring task, not a one-off.

### B7 — Context and namespace handling is where CLIs usually rot

Kubeconfig precedence, `--context`, `--namespace`, in-cluster vs out, and
partial RBAC (a customer who can read `Neo4jEnterpriseCluster` but not Secrets)
are a large share of a CLI's real bug surface. Adopting `cli-runtime`'s
standard flag set rather than hand-rolling it removes most of this class, and
is the main reason to prefer the kubectl-plugin option in §5.

---

## 4. The commands, ranked for a customer

Audience decided: the customer operating the deployment. This reorders the list
materially — a support-bundle command, which would top a Neo4j-support-facing
ranking, drops to fifth.

### 4.1 `validate` — the structural gap (§1.2)

```
$ kubectl neo4j validate -f cluster.yaml
✗ spec.topology.serverRoles[1].serverIndex: 3 is out of range [0, 2]
✗ spec.config: dbms.default_database is rejected — deprecated; use dbms.setDefaultDatabase()
⚠ spec.resources.memory 1Gi is below the 1.5Gi Enterprise minimum — pods will OOMKill
```

Offline for 21 of 29 validators; `--context` enables the cross-reference 8.
Highest value, lowest drift risk, no write semantics, no cluster required.

### 4.2 `status` — the question customers ask most

One view across the 26 kinds in a namespace: what exists, what is `Ready`, what
is stuck and on which condition. Today this is `kubectl get` against 26 kinds
followed by `describe` on whichever looks wrong.

### 4.3 `connect` / `cypher` — the credential round-trip

Resolve the admin Secret, pick the right pod and container, choose `bolt://`
vs `bolt+s://` from `spec.tls`, and either exec or port-forward. Collapses the
most-repeated multi-step sequence in the docs, and removes the failure mode
where guessing the scheme produces an opaque TLS error.

### 4.4 `explain` — troubleshooting as code

Decode a phase or condition into the action to take. This is
`guides/troubleshooting.md` made executable, and it is the command most at risk
of B3 drift — every string it prints is a claim about operator behaviour. Build
it only after the first three have proven the shared-package discipline holds.

### 4.5 `support-bundle` — the escalation path

CR YAML, events, operator logs, pod logs, StatefulSet and PVC state,
`SHOW SERVERS` / `SHOW DATABASES` output, redacted, in one archive. Genuinely
valuable — a customer who cannot grant cluster access can still send one — but
it serves the escalation path rather than the daily loop, which is why it ranks
here for this audience and would rank first for the other.

---

## 5. Options

### (a) `kubectl` plugin — **recommended**

A binary named `kubectl-neo4j` on `PATH`, invoked as `kubectl neo4j …`. Inherits
kubeconfig, `--context` and `--namespace` handling from `cli-runtime` (B7),
matches the ecosystem convention customers already know, and is krew-installable
(B6). Runs standalone too, since the binary is directly executable.

### (b) Standalone binary (`neo4j-k8s`)

Everything in (a) minus the convention and the inherited flag handling. No
advantage identified.

### (c) `kubectl` plugin that shells out to `kubectl`

Avoids client-go entirely. Rejected: it inherits `kubectl`'s output formats as
a parsing surface, breaks under partial RBAC in ways that are hard to report
well, and forfeits the shared-package property in §2 that is the whole reason
to build this here.

### (d) Decline — improve the docs instead

The status quo, and the honest baseline. It is what the 98-line troubleshooting
guide already represents, and §1.2's structural gap is not something more prose
can close: no amount of documentation moves validation feedback from
after-apply to before-apply.

---

## 6. The shape of the first slice

`cmd/kubectl-neo4j/`, one command, `validate`. Chosen because it is the only
candidate that is simultaneously:

- the answer to a structural gap rather than a convenience (§1.2),
- implementable with **zero new domain logic** — it calls existing validators,
- free of write semantics, so B5 does not apply,
- runnable with no cluster at all for the majority path, so it is trivially
  testable, and
- immediately usable in a customer's own CI via the binary (B2).

It also functions as a test of the B3 mitigation: if `validate` cannot be built
without copying something out of `internal/`, that is early evidence the
shared-package discipline will not hold, and it is far cheaper to learn now.

---

## 7. Testing

- **Unit:** golden-file tests over a corpus of good and bad manifests, asserting
  the rendered output. Because the rules live in `internal/validation` and are
  already table-tested there, the CLI's own tests cover *rendering and exit
  codes*, not validation semantics — the split matters, or the same rule ends
  up tested twice and asserted differently.
- **Exit codes are API.** `0` clean, non-zero on errors, and a decision to make
  explicitly: whether warnings alone are non-zero (they should not be by
  default; a `--strict` flag is the conventional escape hatch for CI).
- **No cluster needed** for the offline path, which keeps this out of the
  integration lane entirely and off the one-Enterprise-deployment-at-a-time
  budget.
- **Cross-reference path** (the 8 client-touching validators) tests against
  envtest or a fake client, not a real Kind cluster.

---

## 8. Delivery order

1. **`validate`, offline only** (§6). One command, no cluster, no release
   pipeline yet — buildable and reviewable as a normal PR, with `go run` and
   `make` targets for local use.
2. **Binary release pipeline** (B1). Only once there is something worth
   shipping. goreleaser for the five platform targets plus checksums.
3. **`validate --context`** — the 8 cross-reference validators.
4. **`status`** (§4.2), then **`connect` / `cypher`** (§4.3).
5. **krew index submission** (B6), once the command set is stable enough that
   users installing it will not immediately want a newer one.
6. **`explain`** (§4.4) and **`support-bundle`** (§4.5), on evidence.

Deliberately *not* in the order: any mutating command beyond CR authoring
(B5), and any `pkg/` promotion (B2).

---

## 9. Open questions

**Q1 — Does `validate` earn its place against `--dry-run=server` in practice?**
B4 establishes the theoretical remainder is substantial. What is not known is
how often customers actually hit rules in that remainder versus schema rules
the API server already catches. The first slice is cheap enough to answer this
empirically rather than argue about it.

**Q2 — Who publishes the binaries, and under what support expectation?** A CLI
that customers install is a supported artifact in their eyes whether or not it
is labelled one. This is a support-posture question, not a technical one, and
it should be settled before B1 is built rather than after customers have it.

**Q3 — Does the CLI overlap the MCP server enough to matter?** Both exist to
make the operator legible; one to humans, one to agents. If `explain` and the
MCP server end up encoding the same troubleshooting knowledge in two places,
that is B3 in a new costume. Worth checking before §8 item 6, not before item 1.

**Q4 — Version skew.** A CLI validating manifests for an operator version other
than the one it was built against will eventually be wrong — a rule added in
v1.16 will not be enforced by a v1.14 CLI, and a removed one will be enforced
spuriously. Options are a version check against the running operator, a printed
"validated against operator vX.Y" banner, or accepting the skew silently. The
third is the default if nobody decides, and is the worst of the three.
