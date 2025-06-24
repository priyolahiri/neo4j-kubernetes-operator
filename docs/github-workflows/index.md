# GitHub Workflows Documentation

This directory contains comprehensive documentation for the GitHub workflows used in the Neo4j Kubernetes Operator project.

## Overview

The Neo4j Kubernetes Operator uses several GitHub workflows for CI/CD, testing, and deployment. These workflows are designed with reliability, security, and maintainability in mind.

## Documentation Index

### 📋 [README.md](README.md)
Comprehensive guide covering all aspects of the GitHub workflows:
- Workflow overview and architecture
- Cluster setup and connectivity
- Required secrets and environment variables
- Workflow dependencies and relationships
- Fail-fast mechanisms
- Troubleshooting guide
- Security considerations
- Best practices

### 🔧 [workflow-improvements-summary.md](workflow-improvements-summary.md)
Detailed summary of recent improvements made to the workflows:
- Enhanced cluster creation and configuration
- Comprehensive connectivity checks
- Fail-fast mechanisms implementation
- Improved secrets management
- New reusable tools and scripts
- Testing procedures
- Future enhancement plans

## Workflow Files

The actual workflow files are located in `.github/workflows/`:

- **`ci.yml`** - Main continuous integration pipeline
- **`static-analysis.yml`** - Code quality and security checks
- **`openshift-certification.yml`** - OpenShift compatibility testing
- **`cluster-test.yml`** - Reusable cluster testing workflow

## Quick Start

### For Developers
1. Read the [README.md](README.md) for workflow overview
2. Review [workflow-improvements-summary.md](workflow-improvements-summary.md) for recent changes
3. Check the troubleshooting section for common issues

### For Contributors
1. Understand the workflow architecture from [README.md](README.md)
2. Review security considerations and best practices
3. Test workflows locally using the provided scripts

### For Maintainers
1. Review the workflow dependencies and relationships
2. Understand the fail-fast mechanisms and error handling
3. Monitor workflow performance and reliability metrics

## Related Resources

- **Cluster Configuration**: `hack/kind-config.yaml`
- **Validation Scripts**: `scripts/validate-cluster.sh`
- **Makefile Targets**: See `Makefile` for cluster validation targets
- **Development Setup**: `hack/setup-dev.sh`

## Support

For issues related to GitHub workflows:
1. Check the troubleshooting section in [README.md](README.md)
2. Review the workflow logs for specific error messages
3. Test workflows locally using the provided validation scripts
4. Open an issue with detailed error information and context
