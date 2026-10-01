# `status`


One view of every Neo4j resource in a namespace — what exists, what is healthy, and the message for anything that is not.

```bash
kubectl neo4j status                      # the current context's namespace
kubectl neo4j status -n neo4j
kubectl neo4j status --all-namespaces
kubectl neo4j status --problems           # only what needs attention
```

```
KIND                     NAME        PHASE     READY   AGE
Neo4jDatabase            analytics   Failed    -       5m
Neo4jEnterpriseCluster   prod        Ready     true    3h
Neo4jUser                reporting   Pending   -       2m

✗ Neo4jDatabase/analytics: topology requires 3 primaries, cluster has 2 servers
… Neo4jUser/reporting: waiting for password Secret "reporting-pw"
```

Without it, this is `kubectl get` against 27 kinds followed by `describe` on whichever looks wrong.

Four things worth knowing about how it reports:

- **Messages appear below the table, in full.** They are the part you act on, and squeezing them into a column would wrap them into noise.
- **`…` is not `✗`.** A `Pending` resource is not broken — it is waiting on something you have not created yet. Its message is still shown, because it is the line that tells you what to do next, but it is marked distinctly from a failure. This matches how `validate` separates "not yet" from "wrong".
- **An unrecognised phase is not treated as a problem.** The Aura kinds mirror Aura's own status vocabulary, which Neo4j can extend without a version bump; flagging every phase this binary predates would produce false alarms. This is the same reasoning behind the project's ArgoCD health checks.
- **`--problems` flags a fixed set of phases.** It lists the resources whose phase is `Failed`, `Error`, `Invalid`, `Degraded` or `Unknown`. `Invalid` is the operator rejecting the spec — nothing was created, and the message names the field — so it is shown, with that message, like any other failure. Everything in progress or deliberately idle (`Pending`, `Waiting`, `Scheduled`, `Running`, `Suspended`, …) and everything finished (`Ready`, `Completed`) is not counted, and neither are their messages: under `--problems` the message block below the table covers only the rows it shows. The `READY` column is informational; the verdict comes from the phase alone.

The kind list comes from the operator's registered API scheme rather than a hardcoded list, so a CRD added to the operator appears here automatically. Kinds you lack permission to read, or whose CRD is not installed, are skipped rather than failing the whole command.

Exit code is `0` on a successful query even when resources are unhealthy — mirroring `kubectl get`. If you want a health gate, run `--problems` and look for the `all N Neo4j resource(s) look healthy` line (it prints that, not nothing, when no row is flagged).
