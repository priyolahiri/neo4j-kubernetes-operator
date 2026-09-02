# Design: `kubectl-neo4j` — a customer-facing CLI

> **Status:** Analysis only. Nothing is implemented. §8 proposes a first slice
> of one command.
> **Source:** this repository, read at commit `6afaf1c`; command-density counts
> measured across `docs/user_guide/`.
> **Scope of this document:** whether an accompanying CLI earns its place, who
> it is for, which commands justify themselves, and what it would cost. The
> audience is decided: **the customer operating a Neo4j deployment**, not Neo4j
> support or field engineering — that choice reorders everything in §4.
> **Update:** §9 Q2 is answered — binaries ship with the operator's existing
> release, under the operator's existing support statement. That revised B1
> down substantially (the release workflow already attaches assets by glob) and
> largely resolved Q4.
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

### B1 — Binaries need cross-compilation, but not a new pipeline *(revised down)*

**Corrected after Q2 was answered.** This blocker was first written as "the
largest new surface … a genuinely new release surface (goreleaser or
equivalent), not an increment." That was wrong, and the correction matters
because it was the main cost argument against building anything.

`.github/workflows/release.yml` builds container images with buildx
(`linux/amd64,linux/arm64`) — true — but it **already creates a GitHub Release
and attaches assets by glob**:

```yaml
- name: Create GitHub Release
  uses: softprops/action-gh-release@v3
  with:
    files: |
      release-artifacts/*
```

So publishing binaries is: add a cross-compile matrix step that writes into
`release-artifacts/`, and the existing glob attaches them. No new workflow, no
new release event, no goreleaser required.

What genuinely remains:

- five platform targets (darwin/arm64, darwin/amd64, linux/amd64, linux/arm64,
  windows/amd64), which is a `GOOS`/`GOARCH` matrix over one `go build`;
- a stable asset naming convention, because krew (B6) and any install script
  will depend on it — decide it once, before the first release, since renaming
  assets later breaks every pinned installer;
- checksums, and a decision on signing.

Real work, but bounded, and an increment on machinery that exists.

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

Q2's answer lowers this cost without removing it. The project's stated posture
is that "APIs and behaviour **may change between releases without notice**"
(`README.md`), so a `pkg/` promotion would not create the open-ended
compatibility debt it would in a project promising stability. It would still
create an import surface that people depend on in practice regardless of what
the README says.

**Recommendation stands: ship the binary story only, and say so explicitly.**
It is what CI actually wants, it costs nothing, and it can be revisited if
anyone ever asks for the library. Do not promote to `pkg/` speculatively.

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

**Scope of that rejection, clarified when `cypher` was built.** What is rejected
is shelling out to `kubectl` *as a data source* — parsing its output instead of
using the API. Handing it an **interactive session** parses nothing and forfeits
nothing, and `cypher` does exactly that: client-go resolves the pod, container
and Bolt scheme, then `kubectl exec -it` runs the session. Terminal raw mode,
window resize, signal forwarding and every kubeconfig auth plugin are already
solved there; reimplementing them via SPDY would be a few hundred lines of
fragile code for no capability we want. The cost — `kubectl` must be on PATH —
is definitional for a kubectl plugin and is reported plainly when it is not.

**The security argument that would have favoured SPDY does not apply**, because
neither approach needs to handle the password. `DB_USERNAME` / `DB_PASSWORD`
are already in the container via `secretKeyRef`, so the command references them
by name and the shell expands them inside the pod. The secret never reaches
this process, the user's shell history, or the Kubernetes audit log — which
records an exec request's command array verbatim, and is where a `-p <value>`
implementation would leak it.

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

### 6.1 What the prototype found

Built as `cmd/kubectl-neo4j`. **The B3 discipline held** — not one rule was
copied out of `internal/`; the command is a decoder, a dispatch table and a
renderer. Two findings changed the picture, one better than expected and one
worse.

**Offline coverage is six kinds, not two.** The first cut supported only
`Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`, on the assumption
that any validator whose constructor takes a `client.Client` needs a cluster.
That is not what the code does. Establishing it per kind rather than by
signature:

| Kind | Why it is safe with a nil client |
|---|---|
| `Neo4jEnterpriseStandalone`, `Neo4jBackup`, `Neo4jPlugin` | constructor takes no client |
| `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase` | constructor accepts a client, stores it, **never dereferences it** |
| `Neo4jEnterpriseCluster` | single client call is `ValidateAdminSecretPassword`, which returns early on nil |

Those are properties of code this command does not own, so they are pinned by
`TestOfflineValidatorsAreNilClientSafe`, which probes every kind in the
dispatch table and fails if the map and the probe list ever diverge. A client
call added to any of those validators becomes a failing test here rather than a
panic in a user's terminal.

The remaining kinds resolve `clusterRef`, Secrets or roles through the API
server. They are reported as *skipped*, never as failing — reporting "not
found" for a cluster that simply is not reachable would be a false error, which
is worse than not checking.

**Validation short-circuits, so `validate` may need more than one pass.**
`validateCluster` returns early when image validation fails
(`cluster_validator.go:170-174`, *"if image is invalid, other validations are
less meaningful"*), so a manifest with a bad image reports nothing about
storage, TLS, auth or `spec.config` until the image is fixed. Observed
directly: a manifest with both a malformed image and two rejected config keys
reported only the image and topology errors; correcting the image surfaced the
config errors on the next run.

This is the operator's own behaviour, faithfully reproduced, and the CLI should
**not** diverge from it — diverging is exactly the B3 failure this design is
built to avoid. But it is worse UX for a linter than for a reconciler, and the
honest options are recorded in §9 Q5 rather than papered over.

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

1. ~~**`validate`, offline only** (§6)~~ → **done.** Six kinds, no cluster
   required, `make build-cli` for local use.
2. ~~**Cross-compile matrix in `release.yml`** (revised B1)~~ → **done.** A
   `GOOS`/`GOARCH` matrix writing into `release-artifacts/`, which the existing
   release step already globs — no new workflow, as the revised B1 predicted.
   Five targets, tar.gz for unix and zip for Windows, one checksums file, with
   an assertion that fails the release rather than shipping a partial asset set.

   The asset naming convention is settled as
   `kubectl-neo4j_<version>_<os>_<arch>.<ext>` and is now pinned in three
   places — the release workflow, the release-notes template and the CLI guide.
   Because that string is a public contract that krew manifests and install
   scripts depend on, `scripts/check-cli-asset-names.sh` fails CI if the three
   ever disagree. This repo's own lesson about the CRD catalogue applies
   directly: two of three surfaces being right is the shape of a drift review
   miss.

   A CI cross-compile step (`ci.yml`) builds all five targets on every PR, so a
   unix-only import reaching `cmd/` fails a pull request rather than a tagged
   release.
3. ~~**`validate --context`** — the 8 cross-reference validators~~ → **done**,
   as `--connect` / `--context` / `--kubeconfig` / `--namespace`.

   Two corrections the work forced. First, it is **6** cross-reference
   validators, not 8: `resource` and `cluster` are sub-validators, not per-CRD
   entrypoints. Second, and more important, this document and the command's own
   output implied every unsupported kind merely needed a connection. **Of 26
   CRD kinds only 12 have operator-side validators at all** — the 12 Aura kinds,
   `Neo4jRestore` and `Neo4jReplicaPromotion` have none, and no amount of
   connecting will check them. The skip taxonomy now distinguishes "needs
   --connect" from "no validator exists; use --dry-run=server", because telling
   a user to connect for a check that does not exist sends them after nothing.

   Also surfaced `UserValidationResult.Pending` — the operator's own third
   outcome for a dependency that is not satisfiable *yet* rather than wrong. It
   renders distinctly and never affects the exit code, including under
   `--strict`. Dropping it, as the first implementation did, hid the difference
   between "wrong" and "not yet".
4. **`status`** (§4.2) → **done**. **`connect` / `cypher`** (§4.3) → **done**.

   `status` reads every kind generically — phase, ready, message — rather than
   through 26 typed switches, and takes its kind list from the registered
   scheme rather than a literal, so a new CRD appears without touching the
   command. Two reporting decisions carried over from `validate` deliberately:
   an unrecognised phase is **not** treated as a problem (the Aura kinds mirror
   an open vocabulary Neo4j can extend), and `Pending` messages are shown but
   marked `…` rather than `✗`, because "waiting for a Secret you have not
   created" is the line that says what to do next, not a failure.
5. **krew index submission** (B6), once the command set is stable enough that
   users installing it will not immediately want a newer one.
6. **`support-bundle`** (§4.5) → **done.** **`explain`** (§4.4) → **done.**

   `explain` was the command flagged in §4.4 as "most at risk of B3 drift —
   every string it prints is a claim about operator behaviour". That risk is
   addressed structurally rather than by care: the guidance map is **keyed off
   the operator's own exported condition constants**, so renaming or removing
   one stops this file compiling instead of leaving it explaining something
   that no longer exists. A test covers the reverse direction — a condition
   added without guidance fails here. Phases are covered only where the API
   package defines constants; phases that exist as inline literals in a
   controller are deliberately left out rather than copied in as strings, since
   a string copy is precisely the drift being avoided.

   An unrecognised phase is reported as unrecognised, naming the CLI's own
   version, rather than guessed at.

   `support-bundle`'s design centre is redaction, not collection. Secret values
   never ship; nor do `last-applied-configuration` annotations, which are a
   verbatim copy of a previous manifest and would re-introduce anything
   redacted elsewhere in the same object; nor do literal environment variables
   with sensitive-looking names. `valueFrom` references are deliberately KEPT —
   they hold no secret and are often the explanation for the failure being
   diagnosed. Every redaction is enumerated in `REDACTIONS.txt`, together with
   an explicit statement that redaction cannot cover the user's own
   `spec.config` or application log output.

7. **`diagnose`** (new; not in the original §4 ranking) → **done.**

   §4 ranked five commands and all five shipped, which surfaced what the
   ranking had missed: every one of them stops at the custom-resource boundary,
   while the failure taxonomy in `guides/troubleshooting.md` is dominated by
   the layer below it — pods that will not schedule, containers OOMKilled at
   1Gi, image pulls that fail, PVCs that never bind. `status` reports
   "Pending / not ready" for all of them and hands the user back to a 98-line
   runbook to work out which it was.

   The B3 answer is the same one `explain` used, transposed: anchor every rule
   to a fact the KUBERNETES API defines rather than a string this project made
   up. Where client-go exports a constant it is used (`PodScheduled`,
   `PodReasonUnschedulable`, `ClaimBound`), so an upstream rename fails the
   build. Three reasons have no constant because kubelet owns them —
   `CrashLoopBackOff`, `ImagePullBackOff`, `OOMKilled` — and those are matched
   as literals, with OOM *also* matched on exit code 137 so the most common
   Enterprise failure does not rest on a string alone.

   Workloads are found through the operator's own exported selectors, which
   required adding `resources.BackupJobSelector` and refactoring the
   controller's `backupLabels` onto it, with a contract test — the same
   producer/consumer discipline `cluster_selectors_test.go` already enforces
   for server pods.

   One reporting decision worth recording: a resource the operator has **never
   written status to** is reported explicitly. That silence has no other
   signal — nothing is broken, the CR simply sits there — and it is what a user
   sees when the operator is not running, lacks RBAC for the kind, or is
   namespace-scoped and not watching this namespace (#282).

8. **`preflight`** (new) → **done.** §1.2's insight applied a second time.

   `validate` moved the operator's SPEC rules from after-apply to before-apply.
   What it structurally cannot move is the class of failure that is not in the
   manifest at all: a StorageClass that does not exist, one that cannot expand,
   a credentials Secret missing a key, no node large enough for the pod. Those
   are cluster facts, and 21 of the 29 validators are deliberately offline.

   The scope line is the design: **shape, not reachability.** It reads
   Kubernetes objects and never contacts S3, GCS, Azure or a registry, and
   never runs a probe pod — which is what keeps it free of new images, new RBAC
   and any mutation. It is stated in the help text, the docs and on every run,
   because a clean result that implied "the backup will work" would be worse
   than no command.

   It replaces a ritual the troubleshooting guide documents today —
   `kubectl run backup-auth-check --image=amazon/aws-cli …`, in three vendor
   variants — which is only reached for *after* a backup has already failed.

   Cloud credential key names and the Job ServiceAccount names moved into
   `internal/resources` (`CloudCredentialKeys`, `CloudIdentityAnnotation`,
   `Backup`/`RestoreServiceAccountName`) rather than being restated in the CLI,
   pinned by a contract test asserting every key the Job builder actually wires
   — env var or projected volume item — is declared there.

9. **`export replica-database`** (new) → **done**, and much narrower than §1.1
   implied.

   §1.1 counted five status fields that exist solely to be pasted into another
   resource's spec and read that as a CLI-shaped gap. Re-reading the CRDs while
   building it showed the operator had **already closed most of it itself**:
   `source.upstreamBackupRef` resolves the pull URI live, and
   `source.upstreamClusterRef` resolves network addresses live. Generating a
   literal where a ref exists would be redundant *and worse* — the literal goes
   stale when the upstream changes.

   What no ref can do is cross a cluster boundary, since both resolve through a
   Get against their own API server; the CRD says as much, telling the user to
   "paste it from that CR's own status by hand" for an upstream on a different
   Kubernetes cluster. That paste is the entire scope of the command, and the
   docs point same-cluster users back at the refs.

   Two properties worth keeping if this pattern is extended: the manifest is
   the ONLY thing on stdout (notes go to stderr, so a redirect stays clean),
   and every manifest is run through the operator's own `ReplicaValidator`
   before it is printed — a generator that emitted something `validate` would
   reject would be worse than no generator.

10. **Docs routing** → **done.** A command nobody is shown has no value, and
    with krew declined (naming and identity, not effort) the docs *are* the
    discovery channel. `kubectl neo4j` appeared 7 times across
    `docs/user_guide/`, in 3 files, against roughly 300 manual `kubectl` /
    `cypher-shell` / `neo4j-admin` lines. The high-density runbooks now lead
    with the CLI at each problem heading and keep every raw command underneath
    as the by-hand path.

Deliberately *not* in the order: any mutating command beyond CR authoring
(B5), any `pkg/` promotion (B2), and a TUI. The TUI was considered and
declined: the state half duplicates `status` + `diagnose`, the metrics half
would open with empty graphs because a CLI process starts with no history
(Prometheus has the history, and the operator already ships that path), and it
would break the `run(args, stdout, stderr) int` shape that makes every command
here table-testable.

---

## 9. Open questions

**Q1 — Does `validate` earn its place against `--dry-run=server` in practice?**
B4 establishes the theoretical remainder is substantial. What is not known is
how often customers actually hit rules in that remainder versus schema rules
the API server already catches. The first slice is cheap enough to answer this
empirically rather than argue about it.

**Q2 — ANSWERED.** Binaries are published **as part of this operator's existing
release process**, on the same git tag, at the same cadence, and carry **the
same support guarantees as the operator project** — that is, `README.md`'s
statement verbatim: best-effort via GitHub issues, no official or commercial
support, no SLA, and APIs and behaviour may change between releases without
notice.

Three consequences, all of which make the CLI cheaper than this document
originally assumed:

1. **B1 shrinks to a cross-compile matrix** (see the revised B1) — the release
   workflow already attaches `release-artifacts/*` to a GitHub Release.
2. **Q4 largely dissolves.** Lockstep versioning means a CLI and an operator
   from the same tag agree by construction.
3. **The CLI inherits the operator's support statement rather than needing its
   own.** The docs must repeat it at the install instructions rather than
   leaving customers to infer that a downloadable binary is more supported than
   the operator it ships with.

**Q3 — ANSWERED: no overlap. The question rested on a wrong premise.**

This document assumed the MCP server exists "to make the operator legible … to
agents", and therefore that `explain` might duplicate it. That is not what it
is. `spec.mcp` deploys the **upstream `mcp/neo4j` image**, whose tools are
`read_neo4j_cypher`, `write_neo4j_cypher` and `get-schema`
(`docs/user_guide/guides/mcp_client_setup.md`): querying and analysing **data
inside a Neo4j database**. It knows nothing about operator phases, conditions
or Kubernetes state, and could not — this project does not author that image.

So the two do not overlap at all: `explain` decodes an operator-owned status
vocabulary; the MCP server queries user data in the graph. Nothing here blocked
`explain`, and the drift risk it was worried about lives entirely inside this
repo — addressed by keying the guidance off the operator's exported constants
(§8 item 6).

**Q5 — ANSWERED and fixed in the operator.** Raised by the prototype (§6.1):
`validateCluster`'s early return on image failure meant a fix-one-rerun loop.

Fixed where it belonged — the operator, not the CLI — by removing the early
return so every validator contributes. Two tests pin it, one of which was
verified to fail when the early return is reinstated rather than merely passing
today. The same broken manifest that previously reported 6 errors now reports
9, with the `spec.config` and `spec.storage` problems no longer hidden behind
the image check.

The CLI needed no change: it renders whatever the validators return, which is
precisely why not diverging (§3 B3) was the right call. The caveat in its help
text stays, because the operator still short-circuits in *other* validators and
the general warning remains true.

**Q4 — Version skew: mostly resolved by Q2, with one thing left to build.**
Lockstep release means `kubectl-neo4j vX.Y.Z` encodes operator `vX.Y.Z`'s rules
by construction, so the dangerous case narrows to one: **a customer running a
CLI from a different tag than the operator in their cluster.** A rule added in
v1.16 will not be enforced by a v1.14 CLI, and one removed in v1.16 will be
enforced spuriously by it.

Because both versions are now knowable — the CLI knows its own build version,
and the operator's version is readable from the cluster — the fix is cheap and
should be built with the first cluster-connected command:

- always print `validated against operator rules vX.Y.Z` (the CLI's own
  version), so offline output is never silently authoritative;
- when a `--context` is supplied, compare against the running operator and warn
  on mismatch.

Neither is needed for the offline first slice (§6), but the banner is, and it
costs nothing to add on day one.
