# GitHub Actions Workflows

This directory contains streamlined GitHub Actions workflows for the Neo4j Kubernetes Operator with reusable composite actions to eliminate duplication.

## 🔄 Workflows

### 🧪 ci.yml - Main CI Pipeline
**Triggers:** Push to main/develop, Pull Requests, Manual dispatch
**Purpose:** Streamlined CI with automatic unit tests and optional integration tests

**Jobs:**
1. **Unit Tests** - ✅ Always run on all pushes/PRs (fast, no cluster required)
2. **Integration Tests** - ⏭️ Optional, on-demand only (memory-intensive, requires explicit trigger)
3. **CI Summary** - Status summary with integration test trigger instructions

**Integration Test Triggers (Optional):**
- **Manual**: Use "Run workflow" button in GitHub Actions with "Run integration tests" checked
- **PR Label**: Add `run-integration-tests` label to pull request
- **Commit Message**: Include `[run-integration]` in commit message

**Features:**
- ⚡ Fast feedback with automatic unit tests
- 💾 Resource savings - integration tests only run when explicitly requested
- 🎛️ Configurable Neo4j version for integration tests
- 📊 Clear status reporting with trigger instructions
- 🧹 Automatic cluster cleanup

### 🚀 release.yml - Release Pipeline
**Triggers:** Git tags (v*.*.*), Manual dispatch
**Purpose:** Automated release builds and GitHub releases

**Jobs:**
1. **Validate Release** - Run tests and build validation
2. **Build and Push** - Multi-arch container builds to ghcr.io
3. **Create Release** - GitHub release with manifests and release notes

**Features:**
- Multi-architecture support (amd64, arm64)
- Automated manifest bundling
- Generated release notes
- Container image publishing to GitHub Container Registry

## 🧩 Reusable Actions

### setup-go
**Location:** `.github/actions/setup-go/action.yml`
**Purpose:** Standardized Go environment setup with caching

**Features:**
- Go installation with configurable version
- Optimized Go module caching
- Automatic dependency downloads

### setup-k8s
**Location:** `.github/actions/setup-k8s/action.yml`
**Purpose:** Kubernetes testing environment setup

**Features:**
- Kind cluster creation with configurable version
- kubectl installation
- CRD installation and operator setup
- Cluster readiness verification

### collect-logs
**Location:** `.github/actions/collect-logs/action.yml`
**Purpose:** Comprehensive log collection for debugging

**Features:**
- Cluster state information
- Operator logs
- Event collection
- Automatic artifact upload on failure

## 📋 Environment Variables

The workflows use centralized environment variables for consistency:

```yaml
env:
  GO_VERSION: '1.22'          # Go version for all jobs
  KIND_VERSION: 'v0.20.0'     # Kind version for integration tests
  REGISTRY: ghcr.io           # Container registry
```

## 🔧 Usage

### Running Tests Locally
```bash
# Unit tests (fast)
make test-unit

# Integration tests (requires cluster)
make test-cluster
make test-integration
make test-cluster-delete
```

### Triggering Workflows

**Unit Tests:** ✅ Run automatically on every push/PR

**Integration Tests (Optional - On-demand only):**
- **Manual Trigger**: Go to Actions → CI → "Run workflow" → Check "Run integration tests"
- **PR Label**: Add `run-integration-tests` label to any pull request
- **Commit Message**: Include `[run-integration]` anywhere in commit message
- **Configuration**: Optionally specify Neo4j version (default: `5.26-enterprise`)

**Full E2E Tests:**
- **Manual Trigger**: Actions → "Full Integration Tests (E2E)" → "Run workflow"
- **Features**: Complete operator deployment testing with custom image tags

**Release:**
- Push git tag: `git tag v1.0.0 && git push origin v1.0.0`
- Manual: Use GitHub Actions UI with custom tag

## 🎯 Optimization Benefits

### Before Optimization
- 4 separate workflow files
- ~150 lines of duplicated setup code
- Inconsistent caching strategies
- Hardcoded versions scattered throughout

### After Optimization
- 2 consolidated workflow files
- 3 reusable composite actions
- 60% reduction in total workflow code
- Centralized version management
- Consistent caching and setup patterns

### Performance Improvements
- **Faster builds** through optimized caching
- **Reduced redundancy** with shared actions
- **Better maintainability** with centralized configuration
- **Consistent environments** across all jobs

## 🚀 Development Commands

```bash
# Environment setup
make dev-cluster              # Create dev cluster with cert-manager
make manifests generate       # Code generation

# Testing
make test-unit               # Unit tests only
make test-integration        # Integration tests (requires cluster)

# Cleanup
make dev-cluster-clean       # Clean operator resources
make dev-cluster-delete      # Delete dev cluster
```

## 🔍 Troubleshooting

### Workflow Failures
1. Check the **Actions** tab for detailed logs
2. Look for artifacts containing test results and logs
3. Integration test failures will include cluster logs

### Common Issues
- **Integration tests timeout:** Usually cert-manager deployment issues
- **Unit test failures:** Check for Go version compatibility
- **Release failures:** Verify tag format and permissions

### Debugging Commands
```bash
# Check workflow status
gh run list --workflow=ci.yml

# View specific run
gh run view <run-id>

# Download artifacts
gh run download <run-id>
```
