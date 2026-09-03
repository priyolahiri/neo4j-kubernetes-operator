# `connect` and `cypher`


The most-repeated sequence in the troubleshooting guide is: find the pod, extract the password from a Secret, remember the container name, guess the right Bolt scheme. These two commands do the resolution for you.

```bash
kubectl neo4j cypher                    # the only deployment in the namespace
kubectl neo4j cypher prod -n neo4j      # a specific one
kubectl neo4j cypher prod -c "SHOW DATABASES"
```

`connect` prints the same resolution without executing anything — useful when you want the port-forward command, or to hand someone the details:

```
$ kubectl neo4j connect prod
Neo4jEnterpriseCluster/prod in namespace neo4j

In-cluster Bolt:
  bolt+s://prod-client.neo4j.svc.cluster.local:7687

From your machine:
  kubectl port-forward -n neo4j svc/prod-client 7687:7687 7474:7474
  then connect to bolt+s://localhost:7687
...
TLS is enabled: plain bolt:// is rejected by this deployment — use bolt+s://.
```

### Your password is never read, moved, or logged

This is worth stating precisely, because the obvious implementation gets it wrong.

The admin credentials are **already inside the pod** — the operator injects them via `secretKeyRef` as `DB_USERNAME` and `DB_PASSWORD`. So the command references them *by variable name* and lets the shell expand them in the container:

```bash
kubectl exec -n neo4j prod-server-0 -c neo4j -it --   sh -c 'cypher-shell -a bolt+s://localhost:7687 -u "$DB_USERNAME" -p "$DB_PASSWORD"'
```

The secret never leaves the pod. It is not in your shell history, not in `ps` output on either side, and — the one people forget — **not in the Kubernetes API audit log**, which records an exec request's command array verbatim. A version that read the Secret and passed `-p <value>` would leak it into all three.

### On a cluster the session is routed

A cluster session dials the client Service with the `neo4j://` routing scheme,
not `bolt://` on one server. That matters more than it looks: Neo4j's default
`neo4j` database has a single primary, so a session pinned to an arbitrary
server answers `Database neo4j not found` two times out of three on a perfectly
healthy three-server cluster. A standalone hosts everything on one server, so
it keeps the direct connection.

### It hands the session to `kubectl`

`cypher` resolves the target itself, then execs `kubectl` for the interactive part. That is deliberate: terminal raw mode, window resize, signal forwarding and every kubeconfig authentication plugin are already solved there, and reimplementing them would add a large amount of fragile code for no capability you want.

Consequence: **`kubectl` must be on your `PATH`.** Invoked as `kubectl neo4j cypher` it always is. If you run the binary standalone without kubectl installed, the command says so and points you at `connect` instead of failing obscurely.

### What it will not do

`cypher` passes *your* query through unchanged; the CLI never composes Cypher of its own. Operations the operator models as resources — creating a database, promoting a replica — belong in a CR, not in a shell command, so that they are declarative, auditable and reversible. `kubectl neo4j cypher -c "..."` is you running your own query, which is a different thing from the CLI deciding to mutate your database.

## See also

- [Authentication & Authorization](../guides/security.md) — how the admin credentials are managed
- [Troubleshooting](../guides/troubleshooting.md) — when the session itself will not open
