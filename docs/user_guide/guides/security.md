# Security

This guide explains how to secure your Neo4j Enterprise clusters using the features provided by the operator.

## Authentication

The operator provides first-class, typed configuration for external identity providers. You define providers via `spec.auth` and the operator generates the correct `neo4j.conf` entries automatically — no manual `dbms.security.*` keys required.

**Supported providers:**

*   **Native Neo4j authentication**: The default, managed via Kubernetes secrets.
*   **LDAP / Active Directory**: Full typed support — host, DN templates, group-to-role mapping, system account credentials (injected securely via env vars, never in ConfigMap).
*   **OIDC / SSO**: Multiple named providers (e.g., Okta + Azure AD simultaneously) with discovery URI, claims mapping, and group-to-role mapping.
*   **Kerberos**: For enterprise environments with Kerberos infrastructure.
*   **JWT**: Use JSON Web Tokens for authentication.

**Multi-provider support**: Neo4j evaluates providers in order, so you can configure `authenticationProviders: [ldap, native]` to try LDAP first with native as fallback.

**JVM TrustStore**: For LDAPS or OIDC with internal CAs, configure `spec.auth.trustStore` and the operator automatically creates an init container that builds a JKS truststore from your CA certificate.

For full configuration details and examples, see the [Security Best Practices](../security.md#authentication-configuration) guide and the [auth examples](../../../examples/clusters/auth-example.yaml).

## TLS

The operator makes it easy to enable TLS encryption for all communication to and from your Neo4j cluster. You can enable TLS by setting the `spec.tls.enabled` field to `true`.

The operator integrates with `cert-manager` to automatically provision and manage TLS certificates. This is the recommended approach for production environments. You can specify a `cert-manager` issuer to use for signing certificates.

## Network Policies

The operator can automatically create Kubernetes `NetworkPolicy` resources to restrict traffic to your Neo4j cluster. This helps to enforce a zero-trust security model by ensuring that only authorized applications can connect to the database.

## RBAC

The operator itself runs with a specific `ServiceAccount` that is bound to a `ClusterRole` with the minimum necessary permissions to do its job. For end-users, the operator also provides a set of `ClusterRoles` (`neo4j-viewer`, `neo4j-editor`) that administrators can bind to users or groups to grant them appropriate levels of access to the Neo4j custom resources.

## Pod Security Defaults

The operator now hardens all Neo4j data-plane pods (servers, centralized backup, standalone, restore jobs, and hook jobs) with strict security contexts by default:

- `runAsNonRoot: true` with UID/GID `7474` (Neo4j user) and matching `fsGroup`.
- `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `seccompProfile: RuntimeDefault`.
- Root filesystem remains writable to support Neo4j startup scripts and tmp usage; keep volumes for data/logs/plugins mounted as before.

These defaults are applied automatically; customization is not yet exposed via CRD fields. If you add sidecars, align their security contexts with these defaults.
