# Environment-Specific Deployment Overlays

This directory contains Kustomize overlays for different deployment environments, following Kubernetes best practices for configuration management.

## Structure

```
config/
├── overlays/
│   ├── dev/               # Development environment (local image, neo4j-operator-dev)
│   ├── prod/              # Production-like, local image (neo4j-operator-system)
│   ├── prod-registry/     # Production with the published ghcr.io image (neo4j-operator-system)
│   ├── ha/                # Two operator replicas with anti-affinity (neo4j-operator-system)
│   ├── integration-test/  # What `make test-integration` and CI deploy (neo4j-operator-system)
│   └── namespace-scoped/  # Namespace-scoped RBAC, no ClusterRole (neo4j-operator-dev)
├── default/           # Base configuration
└── manager/           # Core deployment manifests
```

## Usage

### Development Deployment
```bash
# Deploy to development environment
make deploy-dev

# Undeploy from development
make undeploy-dev

# Preview configuration
./bin/kustomize build config/overlays/dev
```

### Production Deployment
```bash
# Build a local image, load it into the Kind cluster, and deploy the prod overlay
make deploy-prod

# Deploy the published image from ghcr.io instead (prod-registry overlay)
make deploy-prod-registry

# Undeploy from production (keeps CRDs and the namespace)
make undeploy-prod

# Preview configuration
./bin/kustomize build config/overlays/prod
```

## Environment Differences

### Development Environment (`config/overlays/dev/`)

**Image Configuration:**
- Image: `neo4j-operator:dev`
- Built locally with `make docker-build`

**Manager Args:**
- `--mode=dev`

**Namespace:**
- `neo4j-operator-dev`

**Resource Configuration:**
- Uses base resource limits (development-friendly)

### Production Environment (`config/overlays/prod/`)

**Image Configuration:**
- Image: `neo4j-operator:latest`, built locally by `make deploy-prod` and loaded into Kind
- `VERSION` (Makefile variable) is only passed to the image build as a build-arg; it does not select a published version

**Production Configuration:**
- Single replica (leader election provides HA)
- Resource requests/limits optimized for production

**Namespace:**
- `neo4j-operator-system`

**Resource Configuration:**
- CPU: 100m requests, 1000m limits
- Memory: 256Mi requests, 1Gi limits

### Production, published image (`config/overlays/prod-registry/`)

Deployed with `make deploy-prod-registry`.

- Image: `ghcr.io/priyolahiri/neo4j-kubernetes-operator:latest`
- Namespace: `neo4j-operator-system`
- CPU: 100m requests, 500m limits; Memory: 64Mi requests, 128Mi limits

### Other overlays

- `ha/` — two operator replicas with preferred pod anti-affinity (200m/256Mi requests, 1 CPU/1Gi limits), published image.
- `integration-test/` — production mode with the `neo4j-operator:integration-test` image; used by `make test-integration` and the CI integration lanes.
- `namespace-scoped/` — namespace-scoped Role/RoleBinding instead of cluster-wide RBAC, `WATCH_NAMESPACE=neo4j-operator-dev`; deployed with `make deploy-namespace-scoped`.

## Benefits

1. **Environment Separation**: Clear separation between dev and prod configurations
2. **No Manual Edits**: No need to manually modify kustomization.yaml files
3. **Version Control**: All environment configurations tracked in Git
4. **CI/CD Ready**: Easily integrated with deployment pipelines
5. **Safety**: Prevents accidental production deployments with dev images

## Migration from Legacy Approach

**Old Way (Error-Prone):**
```bash
# Manually edit config/manager/kustomization.yaml
vim config/manager/kustomization.yaml
make deploy
```

**New Way (Environment-Safe):**
```bash
# Environment-specific deployment
make deploy-dev    # or make deploy-prod
```

## Customization

### Adding New Environments

1. Create new directory: `config/overlays/staging/`
2. Create `kustomization.yaml` with environment-specific settings
3. Add Makefile targets for deployment

### Overriding Configuration

Each overlay can include:
- Image overrides
- Environment variables
- Resource limits
- Namespace settings
- Additional patches

Example custom patch:
```yaml
patches:
- patch: |-
    - op: add
      path: /spec/template/spec/containers/0/env/-
      value:
        name: CUSTOM_SETTING
        value: "custom-value"
  target:
    kind: Deployment
    name: controller-manager
```

## Troubleshooting

### Common Issues

1. **Wrong deployment name in patches**: Use `controller-manager`, not `neo4j-operator-controller-manager`
2. **Missing base configuration**: Ensure `resources: - ../../default` is present
3. **Image pull issues**: Verify image names and tags are correct

### Validation

```bash
# Validate overlay syntax
./bin/kustomize build config/overlays/dev > /dev/null
./bin/kustomize build config/overlays/prod > /dev/null

# Check image references
./bin/kustomize build config/overlays/dev | grep "image:"
./bin/kustomize build config/overlays/prod | grep "image:"
```
