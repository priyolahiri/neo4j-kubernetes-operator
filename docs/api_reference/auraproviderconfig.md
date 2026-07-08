# AuraProviderConfig API Reference

The `AuraProviderConfig` Custom Resource Definition (CRD) holds Neo4j Aura API credentials and account-level defaults. Other Aura resources reference it via `providerConfigRef` so they can share one OAuth token cache and one Aura API rate-limit budget (25/125 req/min per credential).

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraProviderConfig`
- **Scope**: Namespaced
- **Short name**: `auracfg`
- **Purpose**: Central holder of Aura API OAuth client credentials + account defaults, referenced by other Aura resources via `providerConfigRef`.
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

| Field | Type | Description |
|---|---|---|
| `credentialsSecretRef` | `object` | **Required.** References the Kubernetes Secret holding the OAuth client credentials (client-credentials grant), in the same namespace. See [AuraCredentialsSecretRef](#auracredentialssecretref). |
| `defaultProjectId` | `string` | Default Aura project (the API `tenant_id`) used by resources that reference this config without their own `projectId`. |
| `defaultOrganizationId` | `string` | Default Aura organization ID. Only needed by v2beta1 resources such as [`AuraIPFilter`](auraipfilter.md) that use the hierarchical org/project API. |
| `baseUrl` | `string` | Overrides the Aura API base URL (default `https://api.neo4j.io/v1`). Intended for testing against a fake API server only; leave empty in production. |

### AuraCredentialsSecretRef

References a Kubernetes Secret holding the Aura API OAuth client credentials. The Secret can be populated by any means, including the External Secrets Operator.

| Field | Type | Description |
|---|---|---|
| `name` | `string` | **Required.** Name of the Secret holding the Aura API client credentials. |
| `clientIdKey` | `string` | Secret key holding the OAuth client ID. Default `clientId`. |
| `clientSecretKey` | `string` | Secret key holding the OAuth client secret. Default `clientSecret`. |

## Status

| Field | Type | Description |
|---|---|---|
| `conditions` | `[]metav1.Condition` | `Ready=True` once an access token was successfully obtained with the referenced credentials (reason `CredentialsValidated`). |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraProviderConfig
metadata:
  name: aura-account
  namespace: neo4j
spec:
  credentialsSecretRef:
    name: aura-api-credentials   # keys: clientId, clientSecret
  defaultProjectId: 00000000-0000-0000-0000-000000000000
```

Create the referenced Secret with the OAuth client-credentials grant:

```bash
kubectl create secret generic aura-api-credentials -n neo4j \
  --from-literal=clientId=<aura-oauth-client-id> \
  --from-literal=clientSecret=<aura-oauth-client-secret>
```

Other Aura resources then reference this config:

```yaml
spec:
  providerConfigRef:
    name: aura-account
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — Manage an Aura cloud instance
- [`AuraSnapshot`](aurasnapshot.md) — On-demand snapshot of an instance
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [`AuraIPFilter`](auraipfilter.md) — Manage a network IP filter (BETA)
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
