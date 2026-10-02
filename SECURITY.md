# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| The [latest release](https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/latest) | Yes |
| Any earlier release | No — upgrade to the latest release |

This is an independent, community-maintained project: security fixes are made
on a best-effort basis and land in the latest release. There is no commitment
to backport them to earlier releases, and no SLA for when a fix ships; the
acknowledgement and assessment targets below apply to every report. Which
**Neo4j** versions each operator release supports (and which are validated vs.
best-effort) is described in
[Supported Neo4j Versions](docs/user_guide/version_support.md).

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, report them via [GitHub Security Advisories](https://github.com/priyolahiri/neo4j-kubernetes-operator/security/advisories/new).

Include as much of the following as you can:

- Description of the vulnerability
- Steps to reproduce or proof of concept
- Affected versions
- Impact assessment (what an attacker could achieve)
- Any suggested fix (optional)

## What to Expect

- **Acknowledgement** within 3 business days
- **Initial assessment** within 7 business days
- **Fix timeline** communicated after assessment — critical vulnerabilities are prioritized for the next release
- **Credit** in the release notes (unless you prefer to remain anonymous)

## Scope

The following are in scope:

- Neo4j Kubernetes Operator code (`internal/`, `cmd/`, `api/`)
- Helm chart templates (`charts/neo4j-operator/`)
- OLM bundle manifests (`bundle/`)
- CI/CD workflows (`.github/workflows/`)
- Container images published to `ghcr.io/priyolahiri/neo4j-kubernetes-operator`

The following are out of scope:

- Neo4j database server itself (report to [Neo4j Security](https://neo4j.com/security/))
- Third-party dependencies (report upstream, but let us know if it affects this operator)
- Infrastructure hosting the repository (report to GitHub)

## Security Best Practices

See the [Security Guide](docs/user_guide/security.md) for recommendations on deploying Neo4j securely with this operator, including TLS, authentication, network policies, and encryption at rest.
