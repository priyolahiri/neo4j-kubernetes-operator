#!/bin/bash

# Comprehensive Test Runner Script
# This script performs aggressive cleanup, sanity checks, and runs tests

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
CLEANUP_TIMEOUT=${CLEANUP_TIMEOUT:-300}
FORCE_CLEANUP=${FORCE_CLEANUP:-true}
DELETE_NAMESPACES=${DELETE_NAMESPACES:-true}
VERBOSE=${VERBOSE:-false}
TEST_TYPE=${TEST_TYPE:-all}
PARALLEL=${PARALLEL:-false}
COVERAGE=${COVERAGE:-true}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --test-type)
            TEST_TYPE="$2"
            shift 2
            ;;
        --no-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        --no-coverage)
            COVERAGE=false
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --test-type TYPE    Test type: all, unit, integration, e2e, cloud (default: all)"
            echo "  --no-cleanup        Skip environment cleanup"
            echo "  --no-coverage       Skip coverage generation"
            echo "  --parallel          Run tests in parallel"
            echo "  --verbose           Verbose output"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  CLEANUP_TIMEOUT     - Cleanup timeout in seconds (default: 300)"
            echo "  FORCE_CLEANUP       - Force deletion (default: true)"
            echo "  DELETE_NAMESPACES   - Delete test namespaces (default: true)"
            echo "  VERBOSE             - Verbose output (default: false)"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use '$0 --help' for usage information"
            exit 1
            ;;
    esac
done

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if Go is installed
    if ! command -v go &> /dev/null; then
        log_error "Go is not installed or not in PATH"
        exit 1
    fi

    # Check Go version
    local go_version=$(go version | awk '{print $3}' | sed 's/go//')
    log_info "Go version: $go_version"

    # Check if make is available
    if ! command -v make &> /dev/null; then
        log_error "make is not installed or not in PATH"
        exit 1
    fi

    # Check if we're in the right directory
    if [ ! -f "go.mod" ] || [ ! -f "Makefile" ]; then
        log_error "Not in a Go project directory (missing go.mod or Makefile)"
        exit 1
    fi

    log_success "Prerequisites check passed"
}

# Perform environment cleanup
perform_cleanup() {
    if [ "${SKIP_CLEANUP:-false}" = "true" ]; then
        log_warning "Skipping environment cleanup (--no-cleanup specified)"
        return
    fi

    log_info "Performing aggressive environment cleanup..."

    # Check if we're in a Kubernetes environment
    if command -v kubectl &> /dev/null; then
        log_info "Kubernetes environment detected, running cleanup..."
        if [ -f "scripts/test-cleanup.sh" ]; then
            chmod +x scripts/test-cleanup.sh
            export FORCE_CLEANUP="$FORCE_CLEANUP"
            export DELETE_NAMESPACES="$DELETE_NAMESPACES"
            export VERBOSE="$VERBOSE"
            export CLEANUP_TIMEOUT="$CLEANUP_TIMEOUT"
            ./scripts/test-cleanup.sh cleanup
        else
            log_warning "Cleanup script not found, using make target"
            make test-cleanup || log_warning "Cleanup failed, continuing anyway"
        fi
    else
        log_info "No Kubernetes environment detected, skipping cleanup"
    fi

    # Clean up local artifacts
    log_info "Cleaning up local test artifacts..."
    rm -rf test-results/ coverage/ logs/ tmp/ bin/

    log_success "Environment cleanup completed"
}

# Run specific test type
run_test_type() {
    local test_type="$1"

    case "$test_type" in
        "unit")
            log_info "Running unit tests..."
            if [ "$COVERAGE" = "true" ]; then
                make test
            else
                go test -v -race ./...
            fi
            ;;
        "integration")
            log_info "Running integration tests..."
            if [ "$COVERAGE" = "true" ]; then
                make test-integration
            else
                go test -v -race ./test/integration/...
            fi
            ;;
        "e2e")
            log_info "Running e2e tests..."
            if [ "$COVERAGE" = "true" ]; then
                make test-e2e
            else
                go test -v -race ./test/e2e/...
            fi
            ;;
        "cloud")
            log_info "Running cloud tests..."
            # Run cloud tests if available
            if [ -d "test/cloud" ]; then
                for cloud_dir in test/cloud/*/; do
                    if [ -d "$cloud_dir" ]; then
                        cloud_name=$(basename "$cloud_dir")
                        log_info "Running $cloud_name cloud tests..."
                        go test -v -race "$cloud_dir"...
                    fi
                done
            else
                log_warning "Cloud test directory not found"
            fi
            ;;
        "all")
            log_info "Running all tests..."
            if [ "$PARALLEL" = "true" ]; then
                log_info "Running tests in parallel..."
                # Run different test types in parallel
                make test &
                make test-integration &
                make test-e2e &
                wait
            else
                make test
                make test-integration
                make test-e2e
            fi
            ;;
        *)
            log_error "Unknown test type: $test_type"
            exit 1
            ;;
    esac
}

# Generate coverage report
generate_coverage_report() {
    if [ "$COVERAGE" != "true" ]; then
        log_info "Skipping coverage report generation"
        return
    fi

    log_info "Generating coverage report..."

    # Check if coverage files exist
    local coverage_files=()
    for file in coverage*.out; do
        if [ -f "$file" ]; then
            coverage_files+=("$file")
        fi
    done

    if [ ${#coverage_files[@]} -eq 0 ]; then
        log_warning "No coverage files found"
        return
    fi

    # Generate HTML coverage report
    for file in "${coverage_files[@]}"; do
        local html_file="${file%.out}.html"
        log_info "Generating HTML coverage for $file..."
        go tool cover -html="$file" -o="$html_file"
        log_success "Coverage report generated: $html_file"
    done

    # Generate combined coverage report
    if [ ${#coverage_files[@]} -gt 1 ]; then
        log_info "Generating combined coverage report..."
        # This would require additional tools like gocovmerge
        log_warning "Combined coverage report not implemented yet"
    fi
}

# Main function
main() {
    log_info "Starting comprehensive test run..."
    log_info "Configuration:"
    log_info "  TEST_TYPE: $TEST_TYPE"
    log_info "  COVERAGE: $COVERAGE"
    log_info "  PARALLEL: $PARALLEL"
    log_info "  VERBOSE: $VERBOSE"
    log_info "  SKIP_CLEANUP: ${SKIP_CLEANUP:-false}"

    # Check prerequisites
    check_prerequisites

    # Perform cleanup
    perform_cleanup

    # Install dependencies
    log_info "Installing dependencies..."
    make install-tools
    make manifests

    # Run tests
    log_info "Running tests..."
    run_test_type "$TEST_TYPE"

    # Generate coverage report
    generate_coverage_report

    log_success "Test run completed successfully!"
}

# Run main function
main "$@"
