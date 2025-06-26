#!/bin/bash

# Neo4j Operator Test Runner Script
# This script provides a unified interface for running all types of tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
TEST_TYPE="all"
VERBOSE=false
PARALLEL=false
COVERAGE=false
CLEANUP=true
TIMEOUT="10m"
SKIP_SETUP=false
SKIP_CLEANUP=false
FAIL_FAST=false
RETAIN_LOGS=false
TEST_NAMESPACE="neo4j-operator-system"

# Test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print verbose output
verbose() {
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "${BLUE}[VERBOSE] $1${NC}"
    fi
}

# Function to print test results
print_test_results() {
    echo ""
    print_status $PURPLE "📊 Test Results Summary"
    print_status $PURPLE "======================"
    print_status $GREEN "✅ Passed: $PASSED_TESTS"
    print_status $RED "❌ Failed: $FAILED_TESTS"
    print_status $YELLOW "⏭️  Skipped: $SKIPPED_TESTS"
    print_status $CYAN "📈 Total: $TOTAL_TESTS"

    if [[ $FAILED_TESTS -gt 0 ]]; then
        print_status $RED "❌ Some tests failed!"
        return 1
    else
        print_status $GREEN "🎉 All tests passed!"
        return 0
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS] [TEST_TYPE]"
    echo ""
    echo "Test Types:"
    echo "  unit         - Run unit tests only"
    echo "  integration  - Run integration tests only"
    echo "  e2e          - Run end-to-end tests only"
    echo "  all          - Run all tests (default)"
    echo "  simple       - Run simple integration tests"
    echo "  smoke        - Run smoke tests"
    echo ""
    echo "Options:"
    echo "  -v, --verbose        - Enable verbose output"
    echo "  -p, --parallel       - Run tests in parallel"
    echo "  -c, --coverage       - Generate coverage reports"
    echo "  --no-cleanup         - Skip cleanup after tests"
    echo "  --no-setup           - Skip test environment setup"
    echo "  --fail-fast          - Stop on first failure"
    echo "  --retain-logs        - Keep test logs after completion"
    echo "  --timeout DURATION   - Set test timeout (default: 10m)"
    echo "  --namespace NAME     - Set test namespace (default: neo4j-operator-system)"
    echo "  -h, --help           - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run all tests"
    echo "  $0 integration               # Run integration tests only"
    echo "  $0 --coverage --verbose      # Run all tests with coverage and verbose output"
    echo "  $0 simple --no-cleanup       # Run simple tests without cleanup"
    echo "  $0 e2e --timeout 30m         # Run e2e tests with 30 minute timeout"
}

# Function to setup test environment
setup_test_environment() {
    if [[ "$SKIP_SETUP" == "true" ]]; then
        print_status $YELLOW "⏭️  Skipping test environment setup"
        return 0
    fi

    print_status $BLUE "🚀 Setting up test environment..."

    # Run the setup script
    if ! "$SCRIPT_DIR/setup-test-environment.sh" setup; then
        print_status $RED "❌ Failed to setup test environment"
        return 1
    fi

    print_status $GREEN "✅ Test environment setup completed"
}

# Function to cleanup test environment
cleanup_test_environment() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        print_status $YELLOW "⏭️  Skipping test environment cleanup"
        return 0
    fi

    print_status $BLUE "🧹 Cleaning up test environment..."

    # Run the cleanup script
    if ! "$SCRIPT_DIR/setup-test-environment.sh" cleanup; then
        print_status $YELLOW "⚠️  Cleanup had some issues, but continuing..."
    fi

    print_status $GREEN "✅ Test environment cleanup completed"
}

# Function to run unit tests
run_unit_tests() {
    print_status $BLUE "🧪 Running unit tests..."

    local start_time=$(date +%s)
    local test_args=()

    if [[ "$VERBOSE" == "true" ]]; then
        test_args+=("-v")
    fi

    if [[ "$COVERAGE" == "true" ]]; then
        test_args+=("-coverprofile=coverage/coverage-unit.out")
        test_args+=("-covermode=atomic")
    fi

    if [[ "$FAIL_FAST" == "true" ]]; then
        test_args+=("-failfast")
    fi

    # Run unit tests
    if go test -timeout="$TIMEOUT" "${test_args[@]}" ./... 2>&1 | tee -a logs/unit-tests.log; then
        print_status $GREEN "✅ Unit tests passed"
        ((PASSED_TESTS++))
    else
        print_status $RED "❌ Unit tests failed"
        ((FAILED_TESTS++))
        return 1
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    verbose "Unit tests completed in ${duration}s"

    ((TOTAL_TESTS++))
}

# Function to run integration tests
run_integration_tests() {
    print_status $BLUE "🔗 Running integration tests with webhooks and cert-manager..."

    local start_time=$(date +%s)

    # Run the integration tests with webhooks
    if go test -v -timeout="$TIMEOUT" ./test/integration/... 2>&1 | tee -a logs/integration-tests.log; then
        print_status $GREEN "✅ Integration tests passed"
        ((PASSED_TESTS++))
    else
        print_status $RED "❌ Integration tests failed"
        ((FAILED_TESTS++))
        return 1
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    verbose "Integration tests completed in ${duration}s"

    ((TOTAL_TESTS++))
}

# Function to run simple integration tests
run_simple_integration_tests() {
    print_status $BLUE "🔗 Running simple integration tests with webhooks..."

    local start_time=$(date +%s)

    # Run simple integration tests with webhooks
    if go test -v -timeout="$TIMEOUT" ./test/integration/... -ginkgo.focus="Simple" 2>&1 | tee -a logs/simple-integration-tests.log; then
        print_status $GREEN "✅ Simple integration tests passed"
        ((PASSED_TESTS++))
    else
        print_status $RED "❌ Simple integration tests failed"
        ((FAILED_TESTS++))
        return 1
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    verbose "Simple integration tests completed in ${duration}s"

    ((TOTAL_TESTS++))
}

# Function to run e2e tests
run_e2e_tests() {
    print_status $BLUE "🌐 Running end-to-end tests with webhooks and cert-manager..."

    local start_time=$(date +%s)

    # Check if ginkgo is available
    if ! command -v ginkgo &> /dev/null; then
        print_status $YELLOW "📦 Installing ginkgo..."
        go install github.com/onsi/ginkgo/v2/ginkgo@latest
    fi

    # Set up ginkgo arguments
    local ginkgo_args=()

    if [[ "$VERBOSE" == "true" ]]; then
        ginkgo_args+=("-v")
    fi

    if [[ "$FAIL_FAST" == "true" ]]; then
        ginkgo_args+=("-fail-fast")
    fi

    if [[ "$COVERAGE" == "true" ]]; then
        ginkgo_args+=("-coverprofile=coverage/coverage-e2e.out")
    fi

    # Run e2e tests
    if ginkgo -timeout="$TIMEOUT" "${ginkgo_args[@]}" ./test/e2e/... 2>&1 | tee -a logs/e2e-tests.log; then
        print_status $GREEN "✅ E2E tests passed"
        ((PASSED_TESTS++))
    else
        print_status $RED "❌ E2E tests failed"
        ((FAILED_TESTS++))
        return 1
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    verbose "E2E tests completed in ${duration}s"

    ((TOTAL_TESTS++))
}

# Function to run smoke tests
run_smoke_tests() {
    print_status $BLUE "💨 Running smoke tests with webhooks..."

    local start_time=$(date +%s)

    # Run basic functionality tests with webhooks
    if go test -v -timeout="$TIMEOUT" ./test/integration/... -ginkgo.focus="Smoke" 2>&1 | tee -a logs/smoke-tests.log; then
        print_status $GREEN "✅ Smoke tests passed"
        ((PASSED_TESTS++))
    else
        print_status $RED "❌ Smoke tests failed"
        ((FAILED_TESTS++))
        return 1
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    verbose "Smoke tests completed in ${duration}s"

    ((TOTAL_TESTS++))
}

# Function to generate coverage report
generate_coverage_report() {
    if [[ "$COVERAGE" != "true" ]]; then
        return 0
    fi

    print_status $BLUE "📊 Generating coverage report..."

    # Merge coverage files if multiple exist
    if [[ -f "coverage/coverage-unit.out" ]] && [[ -f "coverage/coverage-integration.out" ]]; then
        echo "mode: atomic" > coverage/coverage-combined.out
        tail -n +2 coverage/coverage-unit.out >> coverage/coverage-combined.out
        tail -n +2 coverage/coverage-integration.out >> coverage/coverage-combined.out

        # Generate HTML report
        go tool cover -html=coverage/coverage-combined.out -o coverage/coverage.html

        print_status $GREEN "✅ Coverage report generated: coverage/coverage.html"
    elif [[ -f "coverage/coverage-unit.out" ]]; then
        go tool cover -html=coverage/coverage-unit.out -o coverage/coverage.html
        print_status $GREEN "✅ Coverage report generated: coverage/coverage.html"
    elif [[ -f "coverage/coverage-integration.out" ]]; then
        go tool cover -html=coverage/coverage-integration.out -o coverage/coverage.html
        print_status $GREEN "✅ Coverage report generated: coverage/coverage.html"
    fi

    # Show coverage summary
    if [[ -f "coverage/coverage-combined.out" ]]; then
        go tool cover -func=coverage/coverage-combined.out | tail -1
    fi
}

# Function to cleanup logs
cleanup_logs() {
    if [[ "$RETAIN_LOGS" == "true" ]]; then
        print_status $YELLOW "📁 Retaining test logs"
        return 0
    fi

    print_status $BLUE "🧹 Cleaning up test logs..."
    rm -f logs/*.log
    print_status $GREEN "✅ Test logs cleaned up"
}

# Function to handle signals
cleanup_on_exit() {
    print_status $YELLOW "🛑 Received interrupt signal, cleaning up..."
    cleanup_test_environment
    cleanup_logs
    print_test_results
    exit 1
}

# Main function
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            unit|integration|e2e|all|simple|smoke)
                TEST_TYPE="$1"
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -p|--parallel)
                PARALLEL=true
                shift
                ;;
            -c|--coverage)
                COVERAGE=true
                shift
                ;;
            --no-cleanup)
                SKIP_CLEANUP=true
                shift
                ;;
            --no-setup)
                SKIP_SETUP=true
                shift
                ;;
            --fail-fast)
                FAIL_FAST=true
                shift
                ;;
            --retain-logs)
                RETAIN_LOGS=true
                shift
                ;;
            --timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            --namespace)
                TEST_NAMESPACE="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done

    # Set up signal handlers
    trap cleanup_on_exit INT TERM

    # Change to project root
    cd "$PROJECT_ROOT"

    # Create necessary directories
    mkdir -p logs coverage test-results

    print_status $PURPLE "🚀 Neo4j Operator Test Runner"
    print_status $PURPLE "=========================="
    print_status $CYAN "Test Type: $TEST_TYPE"
    print_status $CYAN "Verbose: $VERBOSE"
    print_status $CYAN "Coverage: $COVERAGE"
    print_status $CYAN "Timeout: $TIMEOUT"
    print_status $CYAN "Namespace: $TEST_NAMESPACE"
    echo ""

    # Setup test environment
    if ! setup_test_environment; then
        print_status $RED "❌ Failed to setup test environment"
        exit 1
    fi

    # Run tests based on type
    local test_failed=false

    case "$TEST_TYPE" in
        unit)
            if ! run_unit_tests; then
                test_failed=true
            fi
            ;;
        integration)
            if ! run_integration_tests; then
                test_failed=true
            fi
            ;;
        e2e)
            if ! run_e2e_tests; then
                test_failed=true
            fi
            ;;
        simple)
            if ! run_simple_integration_tests; then
                test_failed=true
            fi
            ;;
        smoke)
            if ! run_smoke_tests; then
                test_failed=true
            fi
            ;;
        all)
            # Run all test types
            if ! run_unit_tests; then
                test_failed=true
            fi

            if ! run_simple_integration_tests; then
                test_failed=true
            fi

            if ! run_integration_tests; then
                test_failed=true
            fi

            if ! run_e2e_tests; then
                test_failed=true
            fi
            ;;
        *)
            print_status $RED "❌ Unknown test type: $TEST_TYPE"
            show_usage
            exit 1
            ;;
    esac

    # Generate coverage report
    generate_coverage_report

    # Cleanup
    cleanup_test_environment
    cleanup_logs

    # Print results
    print_test_results

    # Exit with appropriate code
    if [[ "$test_failed" == "true" ]]; then
        exit 1
    else
        exit 0
    fi
}

# Run main function with all arguments
main "$@"
