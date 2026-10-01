# `support-bundle`


Collects the diagnostic material for a Neo4j deployment into one archive — the roughly hundred documented `kubectl` invocations in the troubleshooting guide, in a single command.

```bash
kubectl neo4j support-bundle -n neo4j
kubectl neo4j support-bundle -n neo4j -o incident-4821.tar.gz
kubectl neo4j support-bundle -n neo4j --log-lines 5000   # default: tail 2000 lines per container log
```

What it gathers: every Neo4j custom resource, namespace events, per-pod status (including **last termination reason and exit code** — exit 137 is OOMKilled, the most common Enterprise failure on an under-provisioned cluster), container logs current and previous, and the operator's own logs — found by label wherever the operator runs, which is usually a different namespace from the one being diagnosed, and filed under `operator/<namespace>/<pod>/`. If no operator pod is visible, the archive says so — in `errors.txt` — rather than omitting it silently: that absence is itself worth reporting.

### What it will not collect

**Secret values never leave your cluster.** Secrets appear only as a list of names, types and *key names* — enough to diagnose "the Secret is missing the `password` key", which is a real and common cause, without shipping the value.

Three redactions are applied automatically:

| | Why |
|---|---|
| Secret values | The obvious one |
| `last-applied-configuration` annotations | A verbatim copy of a previous manifest, so it re-introduces anything redacted elsewhere in the same object |
| Literal environment variables with sensitive-looking names | A password typed directly into `spec.env` would otherwise be collected and mailed to a stranger |

Every redaction is listed in `REDACTIONS.txt` inside the archive, so the recipient can see *where* something was withheld rather than wondering whether a field was empty or censored.

!!! warning "Redaction is not a guarantee"
    It covers Secret values, last-applied annotations, and sensitive-looking literal env vars. It **cannot** know whether your own `spec.config`, connection strings, or application log output contain something private. The command says so on completion, and `REDACTIONS.txt` repeats it. **Review the archive before sharing it.**

Deliberately *not* redacted: `valueFrom` / `secretKeyRef` references. They contain no secret, and blanking them would destroy exactly the information that explains a misconfigured reference.

### Collection is best-effort by design

A bundle is most wanted when a cluster is unhealthy, so one unreadable resource must not abort the whole collection. A read that fails is skipped, not fatal — and it is **recorded**. `errors.txt` inside the archive lists every read that failed: what was being read and the error text, for example

```
- list events: events is forbidden: User "dev" cannot list resource "events" in namespace "neo4j"
- log of container neo4j in pod neo4j/prod-server-2: container "neo4j" in pod "prod-server-2" is waiting to start: ContainerCreating
- previous log of container neo4j in pod neo4j/prod-server-1: previous terminated container "neo4j" in pod "prod-server-1" not found
```

Everything listed is **missing** from the archive, so a section that is absent is never mistaken for one that was empty, and the recipient can see which permissions the collector lacked. The covered reads are each Neo4j kind's custom resources, events, pods, the Secret name listing, every container log (current, and previous where the container has restarted), and the operator's Deployment, pods and logs. A previous log is only expected for a container that has restarted, so its absence on one that never did is not listed. The file is always present — it says `(every read succeeded)` when nothing failed — and the command prints how many reads failed when it finishes.

Only the error text is recorded, never any data the read might have returned, so `errors.txt` cannot carry a value the redactions withhold.

## See also

- [Troubleshooting](../guides/troubleshooting.md) — diagnosing by hand, when you want to look rather than collect
- [`explain`](explain.md) — decode the conditions a bundle will contain
