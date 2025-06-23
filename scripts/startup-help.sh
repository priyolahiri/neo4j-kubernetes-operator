#!/bin/bash

# Neo4j Operator Development Startup Help
# This script shows all available startup options and their characteristics

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}"
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    Neo4j Operator Development Startup Options                ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BLUE}🚀 Available Startup Modes:${NC}"
echo

echo -e "${GREEN}1. ULTRA-FAST MODE (Recommended for daily development)${NC}"
echo -e "   ${YELLOW}Command:${NC} make dev-start-minimal"
echo -e "   ${YELLOW}Startup Time:${NC} 1-3 seconds ⚡"
echo -e "   ${YELLOW}Controllers:${NC} 1 (cluster only)"
echo -e "   ${YELLOW}Namespace:${NC} default only"
echo -e "   ${YELLOW}Caching:${NC} No informer caching (direct API calls)"
echo -e "   ${YELLOW}Use Case:${NC} Daily development, quick testing"
echo -e "   ${YELLOW}Ports:${NC} :8084 (metrics), :8085 (health)"
echo

echo -e "${GREEN}2. FAST MODE (Good balance)${NC}"
echo -e "   ${YELLOW}Command:${NC} make dev-start-fast"
echo -e "   ${YELLOW}Startup Time:${NC} 5-10 seconds"
echo -e "   ${YELLOW}Controllers:${NC} Configurable (default: cluster)"
echo -e "   ${YELLOW}Namespace:${NC} default only"
echo -e "   ${YELLOW}Caching:${NC} Lazy informer caching"
echo -e "   ${YELLOW}Use Case:${NC} Feature development, testing"
echo -e "   ${YELLOW}Ports:${NC} :8082 (metrics), :8083 (health)"
echo

echo -e "${GREEN}3. ENHANCED MODE (Original with optimizations)${NC}"
echo -e "   ${YELLOW}Command:${NC} make dev-start"
echo -e "   ${YELLOW}Startup Time:${NC} 15-25 seconds"
echo -e "   ${YELLOW}Controllers:${NC} 1 (cluster only)"
echo -e "   ${YELLOW}Namespace:${NC} default only"
echo -e "   ${YELLOW}Caching:${NC} Selective resource caching"
echo -e "   ${YELLOW}Use Case:${NC} Enhanced original workflow"
echo -e "   ${YELLOW}Ports:${NC} :8080 (metrics), :8081 (health)"
echo

echo -e "${PURPLE}4. ORIGINAL MODE (Full functionality)${NC}"
echo -e "   ${YELLOW}Command:${NC} go run cmd/main.go --enable-webhooks=false"
echo -e "   ${YELLOW}Startup Time:${NC} 60+ seconds"
echo -e "   ${YELLOW}Controllers:${NC} 8 (all controllers)"
echo -e "   ${YELLOW}Namespace:${NC} all namespaces"
echo -e "   ${YELLOW}Caching:${NC} Full informer caching"
echo -e "   ${YELLOW}Use Case:${NC} Full feature testing"
echo -e "   ${YELLOW}Ports:${NC} :8080 (metrics), :8081 (health)"
echo

echo -e "${BLUE}💡 Quick Start Recommendations:${NC}"
echo
echo -e "${GREEN}For most development:${NC}"
echo -e "   make dev-start-minimal"
echo
echo -e "${GREEN}For testing multiple controllers:${NC}"
echo -e "   make dev-start-fast"
echo
echo -e "${GREEN}For full operator testing:${NC}"
echo -e "   go run cmd/main.go --enable-webhooks=false"
echo

echo -e "${BLUE}🔧 Customization Examples:${NC}"
echo
echo -e "${YELLOW}Ultra-fast mode (bypasses all caching):${NC}"
echo "   go run cmd/main.go --mode=minimal --ultra-fast --enable-webhooks=false"
echo
echo -e "${YELLOW}Fast mode with multiple controllers:${NC}"
echo "   go run cmd/main.go --mode=dev --controllers=cluster,database,backup"
echo
echo -e "${YELLOW}Development mode with no caching:${NC}"
echo "   go run cmd/main.go --mode=dev --cache-strategy=none --enable-webhooks=false"
echo
echo -e "${YELLOW}Minimal mode with custom namespace:${NC}"
echo "   go run cmd/main.go --mode=minimal --namespace=my-namespace"
echo
echo -e "${YELLOW}Custom ports (if conflicts):${NC}"
echo "   go run cmd/main.go --mode=dev --metrics-bind-address=:9082 --health-probe-bind-address=:9083"
echo

echo -e "${BLUE}🏥 Health Checks:${NC}"
echo
echo "   Minimal mode:  curl http://localhost:8085/healthz"
echo "   Fast mode:     curl http://localhost:8083/healthz"
echo "   Enhanced mode: curl http://localhost:8081/healthz"
echo

echo -e "${BLUE}📊 Metrics:${NC}"
echo
echo "   Minimal mode:  curl http://localhost:8084/metrics"
echo "   Fast mode:     curl http://localhost:8082/metrics"
echo "   Enhanced mode: curl http://localhost:8080/metrics"
echo

echo -e "${BLUE}🐛 Troubleshooting:${NC}"
echo
echo -e "${YELLOW}Port conflicts:${NC}"
echo "   lsof -i :8080 :8081 :8082 :8083 :8084 :8085"
echo "   kill <PID>"
echo
echo -e "${YELLOW}Still slow startup:${NC}"
echo "   kubectl cluster-info  # Check cluster connectivity"
echo "   make dev-start-minimal  # Use fastest mode"
echo
echo -e "${YELLOW}Need specific controllers:${NC}"
echo "   go run cmd/main.go --mode=dev --controllers=cluster,database"
echo

echo -e "${RED}⚠️  Production Warning:${NC}"
echo "   These optimized modes are for DEVELOPMENT ONLY"
echo "   Always use the original cmd/main.go for production"
echo

echo -e "${CYAN}For detailed documentation, see: docs/development/startup-optimization.md${NC}"
echo
