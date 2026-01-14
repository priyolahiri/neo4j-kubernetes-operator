# OpenShift OLM Packaging

This project includes make targets for building an Operator Lifecycle Manager (OLM) bundle and catalog for OpenShift. Use these steps on a workstation with Docker/Podman and operator-sdk/opm available (Makefile will download pinned versions).

## Build the bundle

```bash
# Generate bundle manifests from the current workspace
make bundle VERSION=0.0.1 IMG=ghcr.io/priyolahiri/neo4j-kubernetes-operator:latest

# Build and push bundle image
make bundle-build BUNDLE_IMG=quay.io/your-org/neo4j-operator-bundle:0.0.1
make bundle-push BUNDLE_IMG=quay.io/your-org/neo4j-operator-bundle:0.0.1
```

## Build/push catalog index

```bash
# Build catalog index containing the bundle
make catalog-build BUNDLE_IMGS=quay.io/your-org/neo4j-operator-bundle:0.0.1 \
  CATALOG_IMG=quay.io/your-org/neo4j-operator-catalog:0.0.1

make catalog-push CATALOG_IMG=quay.io/your-org/neo4j-operator-catalog:0.0.1
```

## Install via OLM on OpenShift

```bash
# Create CatalogSource in openshift-marketplace (edit image)
oc apply -f config/samples/olm/catalogsource.yaml

# Create operator namespace if not present
oc new-project neo4j-operator-system || true

# Create Subscription (edit channel/name if you changed defaults)
oc apply -f config/samples/olm/subscription.yaml
```

Default bundle channels can be set via `CHANNELS`/`DEFAULT_CHANNEL` when running `make bundle`.

## CI/OpenShift smoke (proposed)

- See `.github/workflows/openshift-olm-smoke.yml` for a self-hosted GitHub Actions job that uses OpenShift Local (crc): it starts CRC (requires `CRC_PULL_SECRET`, `CRC_KUBEADMIN_PASSWORD` secrets), builds/pushes the bundle and catalog to GHCR, and installs via the sample CatalogSource/Subscription. Runner must support virtualization (self-hosted).
- If you have a remote OpenShift, set `OCP_API`/`OCP_TOKEN` and reuse the same bundle+catalog steps without CRC.
