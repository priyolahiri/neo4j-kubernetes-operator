# OpenTelemetry

Get Neo4j's metrics and logs into an OpenTelemetry pipeline. Neo4j has no OTLP exporter on either supported line (5.26 or CalVer). It publishes metrics on a Prometheus endpoint and writes logs with Log4j. An OpenTelemetry Collector reads both, adds the Kubernetes metadata, and sends them on over OTLP.

The operator does its part with two `spec.monitoring` settings, on `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone` alike:

| Setting | What it does |
|---|---|
| `enabled: true` | Turns on Neo4j's Prometheus endpoint on port 2004 and adds `prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path` annotations to the Neo4j pods. |
| `logs.stdout: [...]` | Writes the listed logs (`query`, `security`, `debug`) to the container's standard output as JSON, besides their files under `/logs`. `neo4j.log` is on standard output already. |

```yaml
spec:
  monitoring:
    enabled: true
    logs:
      stdout: [query, security]
```

Turning `logs.stdout` on or off restarts each server once, since Neo4j reads its log configuration path only at startup. A `ConfigNeedsRestart` event names `server.logs.config`. Changing the list afterwards restarts nothing: Neo4j re-reads the file every 30 seconds, and the kubelet updates it in place, so the change takes effect usually within two minutes.

## The Collector

[`examples/observability/otel-collector.yaml`](https://github.com/priyolahiri/neo4j-kubernetes-operator/blob/main/examples/observability/otel-collector.yaml) is a Collector per node (a DaemonSet) using the `otel/opentelemetry-collector-contrib` image. It has been run against both lines, a 3-server CalVer cluster and a 5.26 standalone:

```bash
kubectl apply -f examples/observability/otel-collector.yaml
kubectl -n otel logs ds/otel-collector | grep -m3 'type: Str(query)'
```

It prints everything with the `debug` exporter. Replace that with your backend's OTLP exporter:

```yaml
exporters:
  otlphttp:
    endpoint: https://otlp.example.com
```

### Logs

The `filelog` receiver reads the `neo4j` container's log files on the node (`/var/log/pods/*/neo4j/*.log`):

- the `container` operator unwraps the runtime's framing and adds `k8s.namespace.name`, `k8s.pod.name` and `k8s.container.name`;
- the `json_parser` operator parses each JSON line into attributes, takes the record's timestamp from Neo4j's `time` field and its severity from `level`. Plain-text lines, `neo4j.log`, pass through as they are.

What each log carries:

| Log | Layout | Fields |
|---|---|---|
| `query` | `QueryLogJsonLayout` | `type` (`query` or `transaction`), `event` (`start`, `success`, `fail`), `database`, `query`, `queryParameters`, `elapsedTimeMs`, `planning`, `cpu`, `waiting`, `allocatedBytes`, `pageHits`, `pageFaults`, `authenticatedUser`, `executingUser`, `source`, `runtime`, `annotationData`, `errorInfo`, `transactionId`; CalVer adds `queryLang` |
| `security` | `StructuredJsonLayout` | `type` (`security`), `level`, `message`, `authenticatedUser`, `executingUser`, `database`, `source` |
| `debug` | `StructuredLayoutWithMessage` | `level`, `category` (the logging class), `message` |

The lines differ in small ways a pipeline should allow for. `annotationData` is a string on 5.26 and an object on CalVer. And `debug.log` itself stays plain text on 5.26 and JSON on CalVer, as Neo4j ships it. Its standard-output copy is JSON on both.

### Metrics

The `prometheus` receiver discovers pods and keeps those labelled `app.kubernetes.io/name: neo4j` and `app.kubernetes.io/managed-by: neo4j-operator` that carry the scrape annotation. Other pods can carry the same annotations, which is why it also filters on the operator's labels.

Neo4j turns its metric names into Prometheus names by replacing dots with underscores. For metrics about a database, it puts the database **in the name**: `neo4j_database_<db>_transaction_committed_total`. The receiver's `metric_relabel_configs` move it into a `database` attribute, giving one metric name per measurement across all databases:

```text
neo4j_database_neo4j_transaction_committed_total   ->  neo4j_database_transaction_committed_total{database="neo4j"}
```

The rule moves a name made of letters and digits only. A database name with a dash or dot, such as a property shard (`products-g000`), reaches Prometheus as underscores, so it cannot be told apart from the metric name that follows. Such metrics keep the database in their name rather than being split in the wrong place. If you set `spec.monitoring.metricsPrefix`, replace the leading `neo4j_` in the rule with your prefix.

Neo4j enables only a subset of its metrics by default (`server.metrics.filter`), and the subset differs between versions. Set `spec.monitoring.metricsFilter` to choose them, for example `"*"` for all.

## Things to know

- **The endpoint has no authentication or encryption.** The operator binds it to `0.0.0.0:2004` so it can be scraped. With `spec.networkPolicy` enabled, port 2004 stays open to any pod for the same reason. Keep it off the Internet: don't expose it through a Service of type `LoadBalancer` or an Ingress.
- **Query logs carry query text and parameters.** Shipping them widens who can read them. Consider `spec.monitoring.obfuscateLiterals: true`, and `db.logs.query.parameter_logging_enabled: "false"` in `spec.config`.
- **Query log volume.** With `monitoring.enabled: true`, the operator logs queries slower than `slowQueryThreshold` (default `5s`) at `queryLogLevel` (default `INFO`). With `monitoring.enabled: false`, Neo4j's own defaults apply, `VERBOSE` with a `0s` threshold, which logs every query at start and end.
- **Leave `debug.log` as it is.** Neo4j support reads that file. The operator writes the standard-output copy with an additional appender and does not change the file's format.
- **Your own log configuration.** `spec.config` may not set `server.logs.config` while a log is listed in `logs.stdout`; validation refuses the combination, naming the field.

## See also

- [Monitoring](monitoring.md) and [Prometheus & Grafana](prometheus-grafana-setup.md)
- [`MonitoringLogsSpec`](../../api_reference/neo4jenterprisecluster.md#monitoringlogsspec)
- Neo4j Operations Manual: [Logging](https://neo4j.com/docs/operations-manual/current/monitoring/logging/) and [Metrics](https://neo4j.com/docs/operations-manual/current/monitoring/metrics/) (CalVer); [Logging](https://neo4j.com/docs/operations-manual/5/monitoring/logging/) and [Metrics](https://neo4j.com/docs/operations-manual/5/monitoring/metrics/) (5.26)
