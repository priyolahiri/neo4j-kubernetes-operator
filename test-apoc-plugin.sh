#!/bin/bash
set -e

echo "Testing APOC plugin installation locally..."

# Set environment for local testing
export CI=""
export GITHUB_ACTIONS=""
export NEO4J_IMAGE_TAG="5.26.0-enterprise"

# Run the specific test with Ginkgo
cd test/integration

echo "Running APOC plugin test..."
ginkgo run -v --focus "Should install APOC plugin on Neo4jEnterpriseCluster" . 2>&1 | tee ../../apoc-test.log

echo "Test completed. Check apoc-test.log for details."
