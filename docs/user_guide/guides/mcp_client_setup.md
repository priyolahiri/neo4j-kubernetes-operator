# MCP Client Setup

This guide explains how to connect MCP clients (VSCode, Claude Desktop, curl) to a Neo4j MCP server deployed by the operator.

The operator uses the **`mcp/neo4j-cypher` image** from Docker Hub, which is published by the [Priyo Lahiri](https://neo4j.com/labs/) team (`neo4j-contrib/mcp-neo4j`).

> **Note:** `mcp/neo4j-cypher` is a **Priyo Lahiri** project. It is actively developed and maintained by the Neo4j Field GenAI team, but Neo4j does not provide SLAs or backwards-compatibility guarantees for it. If you are looking for the official supported product MCP server, see [github.com/neo4j/mcp](https://github.com/neo4j/mcp).

- **Image**: [`mcp/neo4j-cypher`](https://hub.docker.com/r/mcp/neo4j-cypher)
- **Default port**: `8000`
- **Default endpoint path**: `/mcp/`
- **Transports**: `http` (default) or `stdio`

## Before You Start

1. Deploy MCP using `spec.mcp` on either `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone`.
2. Install APOC with the `Neo4jPlugin` CRD (`get_neo4j_schema` requires APOC's schema inspection procedures).
3. Ensure the MCP Service is reachable from your client.

See [Configuration: MCP Server](../configuration.md#mcp-server) for deployment setup details.

## Minimal cluster example

```yaml
spec:
  mcp:
    enabled: true
    # image defaults to mcp/neo4j-cypher:latest — no need to specify
    readOnly: true   # disable write tools
```

The operator automatically injects `NEO4J_URL`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD` from the cluster admin secret.

## Choose a Transport

### HTTP (default)

Deploys a `Deployment` + `ClusterIP Service` (`<name>-mcp:8000`). Expose the service via Ingress, Route, or LoadBalancer. The MCP endpoint path is `/mcp/` (trailing slash required by the official image).

### STDIO (in-cluster only)

No Service is created. Use STDIO only when the MCP client runs inside the cluster (for example, a sidecar or Kubernetes Job) and can exec into the pod.

## HTTP Mode: Test with curl

```bash
# Inside-cluster test (works from any pod in the same namespace)
curl -X POST http://<name>-mcp.<namespace>.svc.cluster.local:8000/mcp/ \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":1}'
```

**Host validation:** The `mcp/neo4j-cypher` image validates the HTTP `Host` header against `NEO4J_MCP_SERVER_ALLOWED_HOSTS`. The server's own built-in default is `localhost,127.0.0.1`, which blocks all in-cluster Kubernetes requests (the `Host` header carries the service DNS name, not `localhost`). The operator overrides this to `*` by default, so in-cluster requests work out of the box.

When exposing the service externally (Ingress, LoadBalancer), set `spec.mcp.http.allowedHosts` to your domain to restrict access:

```yaml
spec:
  mcp:
    http:
      allowedHosts: "neo4j-mcp.example.com"
```

**Client authentication:** The image does not enforce HTTP-level client authentication. The `NEO4J_USERNAME` / `NEO4J_PASSWORD` credentials are used only to authenticate the MCP server's own connection *to Neo4j*, not to authenticate callers. Apply client authentication at the Ingress or via Kubernetes Network Policies.

## HTTP Mode: Expose via Ingress

```yaml
spec:
  mcp:
    enabled: true
    http:
      service:
        type: ClusterIP
        ingress:
          enabled: true
          host: neo4j-mcp.example.com
          className: nginx
          # TLS at the ingress level (official image does not terminate TLS):
          tlsSecretName: neo4j-mcp-tls
```

With an Ingress, reach the endpoint at `https://neo4j-mcp.example.com/mcp/`.

## VSCode

Create or edit `.vscode/mcp.json`:

```json
{
  "servers": {
    "neo4j": {
      "type": "http",
      "url": "http://<mcp-host>:8000/mcp/"
    }
  }
}
```

Replace `<mcp-host>` with the Ingress host or `kubectl port-forward` address.

## Claude Desktop

Edit `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "neo4j": {
      "type": "http",
      "url": "http://<mcp-host>:8000/mcp/"
    }
  }
}
```

## Advanced: custom port and path

```yaml
spec:
  mcp:
    enabled: true
    http:
      port: 9000
      path: /neo4j-mcp/
      allowedOrigins: "https://myapp.example.com"
      # When set, restricts which Host headers the server accepts.
      # The operator's default is "*" (allow any) for in-cluster access.
      # Tighten this when exposing externally:
      allowedHosts: "myapp.example.com,localhost"
      readTimeout: 60    # seconds
```

## Advanced: restrict to read-only queries

```yaml
spec:
  mcp:
    enabled: true
    readOnly: true   # disables write_neo4j_cypher tool
```

## Advanced: target a specific database

```yaml
spec:
  mcp:
    enabled: true
    database: movies    # default database for all queries
    schemaSampleSize: 500
    responseTokenLimit: 20000
```

## STDIO Mode: In-Cluster Usage

Configure STDIO and provide a secret with Neo4j credentials (defaults to the admin secret if `auth` is omitted):

```yaml
spec:
  mcp:
    enabled: true
    transport: stdio
    # Optional: override credentials (defaults to cluster admin secret)
    auth:
      secretName: my-readonly-user
      usernameKey: username
      passwordKey: password
```

Because no Service is created, STDIO is best for clients running inside the cluster. For desktop clients, use HTTP instead.

## Available Tools

| Tool | Description | Read-Only Safe |
|---|---|---|
| `get_neo4j_schema` | Returns nodes, properties, and relationships (uses APOC) | Yes |
| `read_neo4j_cypher` | Executes a read Cypher query | Yes |
| `write_neo4j_cypher` | Executes a write Cypher query | No (disabled when `readOnly: true`) |
