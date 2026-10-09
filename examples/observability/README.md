# Observability examples

## OpenTelemetry

[`otel-collector.yaml`](otel-collector.yaml) is an OpenTelemetry Collector,
one per node, that receives Neo4j's telemetry and exports it over OTLP:

- **Logs**: Neo4j's `query`, `security` and `debug` logs, which the operator
  writes to the pods' standard output as JSON when a deployment sets
  `spec.monitoring.logs.stdout`, read from the node's pod log files.
- **Metrics**: Neo4j's Prometheus endpoint on every pod the operator manages,
  which `spec.monitoring.enabled: true` turns on, with the database moved out
  of each per-database metric name into a `database` attribute.

```bash
kubectl apply -f examples/observability/otel-collector.yaml
kubectl -n otel logs ds/otel-collector | grep -m3 'type: Str(query)'
```

It prints what it receives with the `debug` exporter; replace that with your
backend's OTLP exporter. See the
[OpenTelemetry guide](../../docs/user_guide/guides/opentelemetry.md).
