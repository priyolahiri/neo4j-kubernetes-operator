# Test Environment Cleanup and Sanity Checks

This document describes the aggressive cleanup and sanity check utilities implemented to ensure tests run in a clean environment without conflicts.

## Overview

The Neo4j Kubernetes Operator includes comprehensive utilities for:
- **Aggressive cleanup** of test resources
- **Sanity checks** to verify environment readiness
- **Conflict detection** to identify potential test interference
- **Automated setup** for clean test environments

## Components

### 1. Go Cleanup Utility (`test/utils/cleanup.go`)

A comprehensive Go package that provides:

- `SetupTestEnvironment()` - Complete test environment setup
- `AggressiveCleanup()` - Comprehensive resource cleanup
- `SanityCheck()` - Environment validation
- `DefaultCleanupOptions()` - Configurable cleanup behavior

#### Usage in Tests

```go
import "github.com/neo4j-labs/neo4j-kubernetes-operator/test/utils"

var _ = Describe("My Test Suite", func() {
    BeforeSuite(func() {
        By("Setting up test environment with aggressive cleanup")
        ctx = context.Background()

        // Perform aggressive cleanup and sanity checks
        utils.SetupTestEnvironment(ctx, k8sClient)
    })

    // ... test cases
})
```

#### Cleanup Options

```go
options := utils.CleanupOptions{
    DeleteNamespaces:    true,  // Delete test namespaces
    DeleteCRDs:          false, // Don't delete CRDs (shared)
    DeleteTestResources: true,  // Delete test-specific resources
    DeleteOrphanedPods:  true,  // Delete orphaned pods
    DeleteOrphanedPVCs:  true,  // Delete orphaned PVCs
    DeleteOrphanedJobs:  true,  // Delete orphaned jobs
    DeleteOrphanedSAs:   true,  // Delete orphaned service accounts
    ForceDelete:         true,  // Force deletion
    Timeout:             time.Minute * 5,
    LabelSelector:       "app.kubernetes.io/part-of=neo4j-operator-test",
}
```

### 2. Shell Cleanup Script (`scripts/test-cleanup.sh`)

A comprehensive bash script for environment cleanup and validation.

#### Usage

```bash
# Perform aggressive cleanup
./scripts/test-cleanup.sh cleanup

# Perform environment checks only
./scripts/test-cleanup.sh check

# Show help
./scripts/test-cleanup.sh help
```

#### Environment Variables

- `FORCE_CLEANUP=true` - Force deletion of resources
- `DELETE_NAMESPACES=true` - Delete test namespaces
- `DELETE_CRDS=false` - Delete CRDs (default: false)
- `CLEANUP_TIMEOUT=300` - Cleanup timeout in seconds
- `VERBOSE=false` - Verbose output

### 3. Test Runner Script (`scripts/run-tests.sh`)

A comprehensive test runner that includes cleanup and sanity checks.

#### Usage

```bash
# Run all tests with cleanup
./scripts/run-tests.sh

# Run specific test types
./scripts/run-tests.sh --test-type unit
./scripts/run-tests.sh --test-type integration
./scripts/run-tests.sh --test-type e2e
./scripts/run-tests.sh --test-type cloud

# Run tests in parallel
./scripts/run-tests.sh --test-type all --parallel

# Skip cleanup
./scripts/run-tests.sh --no-cleanup

# Skip coverage
./scripts/run-tests.sh --no-coverage
```

## Makefile Targets

The Makefile includes several targets for cleanup and testing:

```bash
# Cleanup targets
make test-cleanup      # Perform aggressive cleanup
make test-check        # Perform sanity checks
make test-setup        # Setup clean environment

# Test runner targets
make test-runner       # Run all tests with cleanup
make test-runner-unit  # Run unit tests with cleanup
make test-runner-integration  # Run integration tests with cleanup
make test-runner-e2e   # Run e2e tests with cleanup
make test-runner-parallel  # Run all tests in parallel with cleanup
```

## GitHub Workflows Integration

The cleanup utilities are integrated into GitHub workflows:

### CI Workflow (`.github/workflows/ci.yml`)

```yaml
- name: Environment cleanup and sanity checks
  run: |
    echo "Performing aggressive environment cleanup and sanity checks..."

    # Check if we're in a Kubernetes environment
    if command -v kubectl &> /dev/null; then
      echo "Kubernetes environment detected, running aggressive cleanup..."
      if [ -f "scripts/test-cleanup.sh" ]; then
        chmod +x scripts/test-cleanup.sh
        export FORCE_CLEANUP=true
        export DELETE_NAMESPACES=true
        export VERBOSE=true
        ./scripts/test-cleanup.sh cleanup
      fi
    fi

    # Clean up any local test artifacts
    rm -rf test-results/ coverage/ logs/ tmp/
```

### Static Analysis Workflow (`.github/workflows/static-analysis.yml`)

```yaml
- name: Environment cleanup and sanity checks
  run: |
    echo "Performing environment cleanup and sanity checks..."

    # Check if we're in a Kubernetes environment
    if command -v kubectl &> /dev/null; then
      echo "Kubernetes environment detected, running cleanup..."
      if [ -f "scripts/test-cleanup.sh" ]; then
        chmod +x scripts/test-cleanup.sh
        ./scripts/test-cleanup.sh check
      fi
    fi

    # Clean up any local test artifacts
    rm -rf test-results/ coverage/ logs/ tmp/
```

## What Gets Cleaned Up

### Neo4j Custom Resources
- `Neo4jEnterpriseCluster`
- `Neo4jBackup`
- `Neo4jRestore`
- `Neo4jUser`
- `Neo4jRole`
- `Neo4jGrant`
- `Neo4jDatabase`
- `Neo4jPlugin`

### Kubernetes Resources
- StatefulSets with `app.kubernetes.io/part-of=neo4j-operator`
- Jobs with `app.kubernetes.io/part-of=neo4j-operator`
- Pods with `app.kubernetes.io/part-of=neo4j-operator`
- PVCs with `app.kubernetes.io/part-of=neo4j-operator`
- ServiceAccounts with `app.kubernetes.io/part-of=neo4j-operator`

### Test Namespaces
- Namespaces starting with `test-`
- Namespaces starting with `gke-`
- Namespaces starting with `aks-`
- Namespaces starting with `eks-`

### Local Artifacts
- `test-results/` directory
- `coverage/` directory
- `logs/` directory
- `tmp/` directory
- `bin/` directory

## Sanity Checks

### Cluster Health
- Verify kubectl connectivity
- Check for ready nodes
- Verify storage classes availability

### CRD Availability
- Verify all required CRDs are installed
- Check CRD API availability

### Resource Conflicts
- Detect existing Neo4j resources
- Identify conflicting test namespaces
- Warn about potential interference

## Best Practices

### 1. Always Use Cleanup in Tests

```go
BeforeSuite(func() {
    utils.SetupTestEnvironment(ctx, k8sClient)
})
```

### 2. Use Appropriate Cleanup Options

```go
// For shared environments (CI/CD)
options := utils.DefaultCleanupOptions()
options.DeleteCRDs = false  // Don't delete shared CRDs

// For isolated environments
options := utils.DefaultCleanupOptions()
options.DeleteCRDs = true   // Safe to delete CRDs
```

### 3. Handle Cleanup Failures Gracefully

```go
if err := utils.AggressiveCleanup(ctx, k8sClient, options); err != nil {
    // Log warning but don't fail tests
    fmt.Printf("Warning: Cleanup failed: %v\n", err)
}
```

### 4. Use Timeouts Appropriately

```go
options := utils.DefaultCleanupOptions()
options.Timeout = time.Minute * 10  // Longer timeout for large clusters
```

## Troubleshooting

### Common Issues

1. **Cleanup Timeout**
   - Increase `CLEANUP_TIMEOUT` environment variable
   - Check for stuck resources with finalizers

2. **Permission Denied**
   - Ensure proper RBAC permissions
   - Check service account permissions

3. **Resource Not Found**
   - Resources may have been deleted by other processes
   - This is usually not an error

4. **CRD Not Available**
   - Ensure CRDs are installed before running tests
   - Check operator deployment status

### Debug Mode

Enable verbose output for debugging:

```bash
export VERBOSE=true
./scripts/test-cleanup.sh cleanup
```

### Manual Cleanup

If automated cleanup fails, manual cleanup can be performed:

```bash
# Delete all Neo4j resources
kubectl delete neo4jenterprisecluster --all --all-namespaces
kubectl delete neo4jbackup --all --all-namespaces
kubectl delete neo4jrestore --all --all-namespaces
# ... etc

# Delete test namespaces
kubectl get namespaces | grep -E "^(test-|gke-|aks-|eks-)" | awk '{print $1}' | xargs kubectl delete namespace
```

## Security Considerations

- Cleanup scripts use force deletion for stuck resources
- Service accounts and secrets are cleaned up
- PVCs are deleted to prevent storage leaks
- Namespaces are deleted to ensure complete cleanup

## Performance Considerations

- Cleanup operations are batched where possible
- Timeouts prevent infinite waiting
- Parallel deletion is used for independent resources
- Verification ensures cleanup completion
