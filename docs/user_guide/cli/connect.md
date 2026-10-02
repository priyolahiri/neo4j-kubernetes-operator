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
  neo4j+s://prod-client.neo4j.svc.cluster.local:7687

From your machine:
  kubectl port-forward -n neo4j svc/prod-client 7687:7687 7474:7474
  then connect to neo4j+ssc://localhost:7687   (local testing only — see below)
...
TLS is enabled: plain bolt:// is rejected by this deployment — use neo4j+s://.
...
```

The scheme depends on what the deployment is: a **cluster** is addressed with the routing scheme (`neo4j://`, or `neo4j+s://` under TLS), a **standalone** with the direct one (`bolt://`, or `bolt+s://`). `connect` and `cypher` share the one function that decides this, so the address `connect` prints is the address `cypher` dials.

Through a port-forward `connect` tells you two things instead of leaving you to find them out:

- **TLS and `localhost`.** The certificates name the client Service and the pods, never `localhost`, so the verifying scheme (`neo4j+s://localhost:7687`) fails hostname verification against a port-forward. `connect` offers the `+ssc` variant for that — it encrypts but does **not** verify the server, so it is for local testing only — and points at the in-cluster address, which does verify. `kubectl neo4j cypher` uses that address.
- **A cluster behind a single port-forward.** `neo4j://` follows the routing table to the server that hosts the database, but the table lists servers by their in-cluster addresses, which one port-forward does not make reachable from your machine. From outside the cluster the session may not connect to the server it is routed to; `kubectl neo4j cypher` runs inside the cluster and does not have this problem.

### Your password is never read, moved, or logged

This is worth stating precisely, because the obvious implementation gets it wrong.

The admin credentials are **already inside the pod** — the operator injects them via `secretKeyRef` as `DB_USERNAME` and `DB_PASSWORD`. So the command references them *by variable name* and lets the shell expand them in the container:

```bash
kubectl exec -n neo4j prod-server-0 -c neo4j -it -- sh -c 'cypher-shell -a neo4j+s://prod-client.neo4j.svc.cluster.local:7687 -u "$DB_USERNAME" -p "$DB_PASSWORD"'
```

(`prod-server-0` stands for whichever server pod is Ready. On a TLS deployment the real command is wrapped to first build a throwaway truststore from the pod's `/ssl/ca.crt`, so the server certificate is verified rather than skipped; a standalone dials `bolt://` / `bolt+s://` instead of `neo4j://` / `neo4j+s://`.)

The secret never leaves the pod. It is not in your shell history, not in `ps` output on either side, and — the one people forget — **not in the Kubernetes API audit log**, which records an exec request's command array verbatim. A version that read the Secret and passed `-p <value>` would leak it into all three.

### The session is dialled at the client Service, not at localhost

Two reasons, and they are independent.

**Routing, on a cluster.** Neo4j's default `neo4j` database has a single
primary, so a session pinned to an arbitrary server answers `Database neo4j not
found` two times out of three on a perfectly healthy three-server cluster. Only
`neo4j://` follows the routing table to a server that hosts it. A standalone
needs no routing and keeps `bolt://`.

**Hostname verification, on either kind.** The operator's certificates carry
SANs for the client, internals and headless Services and for the pod FQDNs —
never `localhost`. Both `bolt+s://` and `neo4j+s://` verify the hostname, so a
session dialled at `localhost` cannot connect to a TLS deployment at all. The
client Service FQDN is a SAN on both Kinds.

### It hands the session to `kubectl`

`cypher` resolves the target itself, then execs `kubectl` for the interactive part. That is deliberate: terminal raw mode, window resize, signal forwarding and every kubeconfig authentication plugin are already solved there, and reimplementing them would add a large amount of fragile code for no capability you want.

Consequence: **`kubectl` must be on your `PATH`.** Invoked as `kubectl neo4j cypher` it always is. If you run the binary standalone without kubectl installed, the command says so and points you at `connect` instead of failing obscurely.

### What it will not do

`cypher` passes *your* query through unchanged; the CLI never composes Cypher of its own. Operations the operator models as resources — creating a database, promoting a replica — belong in a CR, not in a shell command, so that they are declarative, auditable and reversible. `kubectl neo4j cypher -c "..."` is you running your own query, which is a different thing from the CLI deciding to mutate your database.

## See also

- [Authentication & Authorization](../guides/security.md) — how the admin credentials are managed
- [Troubleshooting](../guides/troubleshooting.md) — when the session itself will not open
