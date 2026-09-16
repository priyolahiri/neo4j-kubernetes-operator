# `explain`


Turns a status condition, phase or reason into what it means and what to do about it — the troubleshooting guide, made executable.

```bash
kubectl neo4j explain Neo4jEnterpriseCluster/prod    # a live resource
kubectl neo4j explain ServersHealthy                 # a single term
kubectl neo4j explain CompositeDatabaseNameBlocked   # a condition reason
kubectl neo4j explain --list                         # everything it knows
```

```
$ kubectl neo4j explain ServersHealthy
ServersHealthy (condition)
  every server reported Enabled and Available by SHOW SERVERS.
  → When false, the message names the unhealthy servers. Check their pod logs for
    OOMKilled (exit 137) — Enterprise needs at least 1.5Gi.
```

Against a live resource it prints the phase, then every condition with its status, the operator's own message, and the guidance:

```
Neo4jEnterpriseCluster/prod in namespace neo4j

phase: Degraded
  ...

✗ ServersHealthy = False
    server prod-server-1 is not available
    every server reported Enabled and Available by SHOW SERVERS.
    → When false, ... Check their pod logs for OOMKilled (exit 137).
```

### Reasons, not just conditions

A condition tells you something is false; its **reason** tells you which of several unrelated causes it was. It is the most specific thing a status carries, and the least guessable from the string itself — so reasons are explained as their own kind of term, and are printed alongside each condition of a live resource.

```
$ kubectl neo4j explain CompositeDatabaseNameBlocked
CompositeDatabaseNameBlocked (reason)
  a dotted alias already occupies the composite's name, so the composite cannot be created.
  → Neo4j accepts CREATE ALIAS cineasts.latest even when no composite `cineasts`
    exists ... it permanently blocks the composite with 42N87. The server's own
    error never mentions ordering. The CR's message names the alias to drop ...
```

Only reasons that are *not* guessable get an entry. A reason that restates its condition needs none, and a list padded with those would rot without helping anyone.

### It admits what it does not know

The explanations describe the operator release this CLI was built from. A newer deployment can report conditions, phases or reasons it has never heard of, and in that case it says so — naming its own version — rather than inventing an explanation:

```
  (no guidance for this phase — it may be newer than this CLI, which carries v1.15.0 rules)
```

Guessing would be worse than admitting the gap, because a confident wrong answer during an incident costs more than no answer.

## See also

- [Troubleshooting](../guides/troubleshooting.md) — the long-form version of this knowledge
- [Composite databases](../guides/composite_databases.md) — the guide behind the composite reasons above
- [`status`](status.md) — see which resources have conditions worth explaining
