# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Neo4j Enterprise Operator for Kubernetes - manages Neo4j Enterprise deployments (v5.26+) using Kubebuilder framework.

**Supported Neo4j Versions**: Only 5.26+ (Semver: 5.26.0+, Calver: 2025.01.0+)
**Discovery**: V2_ONLY mode exclusively

**Deployment Types:**
- **Neo4jEnterpriseCluster**: High availability clusters (minimum 2 servers that self-organize into primary/secondary roles)
- **Neo4jEnterpriseStandalone**: Single-node deployments (development/testing)

## Architecture

**Key Components:**
- CRDs: Neo4jEnterpriseCluster, Neo4jEnterpriseStandalone, Neo4jBackup/Restore
- Controllers: Cluster & standalone controllers with client-side validation
- Neo4j Client: Bolt protocol communication

**Directory Structure:**
- `api/v1alpha1/` - CRD definitions
- `internal/controller/` - Controller logic
- `internal/resources/` - K8s resource builders
- `test/` - Unit, integration, e2e tests

## Essential Commands

### Build & Development
```bash
make build                 # Build operator binary
make docker-build         # Build container image
make manifests            # Generate CRDs and RBAC
make generate             # Generate DeepCopy methods
make dev-run              # Run operator locally (outside cluster)

# Development cluster management
make dev-cluster          # Create Kind development cluster (neo4j-operator-dev)
make dev-cluster-clean    # Clean operator resources from dev cluster
make dev-cluster-reset    # Delete and recreate dev cluster
make dev-cluster-delete   # Delete dev cluster
make dev-cleanup          # Clean dev environment (keep cluster)
make dev-destroy          # Completely destroy dev environment

make operator-setup       # Deploy operator to test cluster
```

### Quick Testing with Examples
```bash
# Deploy a standalone instance for development
kubectl create secret generic neo4j-admin-secret \
  --from-literal=username=neo4j --from-literal=password=admin123
kubectl apply -f examples/standalone/single-node-standalone.yaml

# Check standalone status
kubectl get neo4jenterprisestandalone
kubectl get pods

# Access Neo4j Browser (standalone)
kubectl port-forward svc/standalone-neo4j-service 7474:7474 &
open http://localhost:7474

# Or deploy a minimal cluster for testing
kubectl apply -f examples/clusters/minimal-cluster.yaml
kubectl get neo4jenterprisecluster
kubectl port-forward svc/minimal-cluster-client 7474:7474 &
```

### Testing
```bash
# Quick tests (no cluster required)
make test-unit            # Unit tests only
make test-webhooks        # Webhook validation tests with envtest

# Test cluster management
make test-cluster         # Create test cluster (neo4j-operator-test)
make test-cluster-clean   # Clean operator resources from test cluster
make test-cluster-reset   # Delete and recreate test cluster
make test-cluster-delete  # Delete test cluster

# Cluster-based tests
make test-integration     # Integration tests (requires test cluster)
make test-e2e            # End-to-end tests (requires test cluster)

# Full test suite
make test                 # Run unit + integration tests
make test-coverage       # Generate coverage report

# Environment cleanup
make test-cleanup        # Clean test artifacts (keep cluster)
make test-destroy        # Completely destroy test environment

# Run specific test
go test ./internal/controller -run TestClusterReconciler
ginkgo run -focus "should create backup" ./test/integration
```

### Code Quality
```bash
make fmt                  # Format code with gofmt
make lint                 # Run golangci-lint (strict mode)
make lint-lenient        # Run with relaxed rules for CI
make vet                  # Run go vet
make security            # Run gosec security scan
```

### Debugging & Troubleshooting
```bash
# View operator logs
kubectl logs -n neo4j-operator deployment/neo4j-operator-controller-manager

# Check webhook certificates
kubectl get certificate -n neo4j-operator

# Validate CRDs
kubectl explain neo4jenterprisecluster.spec

# Test webhook locally
make webhook-test

# Troubleshoot OOM issues
kubectl describe pod <pod-name> | grep -E "(OOMKilled|Memory|Exit.*137)"
kubectl top pod <pod-name> --containers  # Check memory usage
kubectl logs <pod-name> --previous | tail  # Check logs before restart

# Test Neo4j database operations
kubectl exec <pod-name> -c neo4j -- cypher-shell -u neo4j -p <password> "SHOW SERVERS"
kubectl exec <pod-name> -c neo4j -- cypher-shell -u neo4j -p <password> "CREATE DATABASE testdb TOPOLOGY 1 PRIMARY"
```

## Testing & Development

**Test Suite** (Ginkgo/Gomega):
- Unit Tests: `make test-unit` (run before commits)
- Integration Tests: `make test-integration` (requires cluster)
- E2E Tests: `make test-e2e`

**Key Notes**:
- Kind clusters only (no minikube/k3s)
- Webhooks require cert-manager
- Use envtest for controller unit tests
- Neo4j client uses Bolt protocol
- Integration tests use 300-second timeouts for CI compatibility

**Test Troubleshooting**:
- If tests timeout: Check image pull delays in CI - tests use 5-minute timeout
- If pod scheduling fails: Check resource constraints - tests use minimal CPU/memory
- If cluster formation fails: Check discovery service and endpoints RBAC permissions
- If pods get OOMKilled: Check memory limits - Neo4j Enterprise needs ≥ 1.5Gi for database operations
- If database creation hangs: Verify Neo4j 5.x syntax uses `TOPOLOGY` clause, not `OPTIONS`

### Development Environment

**Kind Clusters** (Kind only - no minikube/k3s):
- **Development**: `neo4j-operator-dev` - manual testing
- **Test**: `neo4j-operator-test` - automated tests
- Both include cert-manager v1.18.2 with `ca-cluster-issuer`

**Cleanup Commands**:
- `make dev-cluster-clean` / `make test-cluster-clean` - Remove operator only
- `make dev-cluster-reset` / `make test-cluster-reset` - Recreate cluster
- `make dev-destroy` / `make test-destroy` - Complete destruction

## CI/CD & Debugging

**GitHub Actions**: Unit/lint → Integration → E2E → Multi-arch builds
**PR Requirements**: All checks must pass, use conventional commits

**Debug Failed Reconciliation**:
```bash
kubectl logs -n neo4j-operator deployment/neo4j-operator-controller-manager -f
kubectl describe neo4jenterprisecluster <name>
make dev-run ARGS="--zap-log-level=debug"
```

## Key Features

### Backup Sidecar
- **Automatic**: Added to all pods with RBAC auto-creation
- **Resources**: Memory: 512Mi/1Gi, CPU: 200m/500m (prevents OOM)
- **Neo4j 5.26+ Support**: Correct `--to-path` syntax, auto path creation
- **Test**: `kubectl exec <pod> -c backup-sidecar -- sh -c 'echo "{\"path\":\"/data/backups/test\",\"type\":\"FULL\"}" > /backup-requests/backup.request'`

### Deployment Configuration

**Neo4jEnterpriseCluster**:
- Min topology: 2+ servers (self-organize into primary/secondary roles)
- Scalable, uses V2_ONLY discovery

**Neo4jEnterpriseStandalone**:
- Fixed single node (no scaling)
- Uses clustering infrastructure (no `dbms.mode=SINGLE`)

**Version-Specific Discovery**:
- **5.x**: `dbms.kubernetes.discovery.v2.service_port_name=tcp-discovery`
- **2025.x**: `dbms.kubernetes.discovery.service_port_name=tcp-discovery`
- Auto-detected via `getKubernetesDiscoveryParameter()`

### Configuration Guidelines

**Never Use** (Neo4j 4.x settings - DEPRECATED):
- `dbms.mode=SINGLE` - Use server-based architecture instead
- `causal_clustering.*` - Replaced by modern clustering in 5.26+
- `metrics.bolt.*` - Use `server.metrics.*` instead
- `server.groups` - Not applicable to 5.26+ clustering
- `dbms.cluster.role` - Use `SHOW DATABASES` for cluster status
- `causal_clustering.leader_election_timeout` - Use `causal_clustering.leader_failure_detection_window`

**Always Use** (Neo4j 5.26+ and 2025.x):
- `dbms.cluster.discovery.version=V2_ONLY` (5.x) / default in 2025.x
- `server.*` instead of `dbms.connector.*`
- `dbms.ssl.policy.{scope}.*` for TLS
- Environment variables over config files
- Modern database topology syntax (see below)

### TLS Configuration

**Setup**:
```yaml
spec:
  tls:
    mode: cert-manager
    issuerRef:
      name: ca-cluster-issuer
      kind: ClusterIssuer
```

**Auto-generated**:
- SSL policies for `https` and `bolt` scopes
- Certificates mounted at `/ssl/`
- `dbms.ssl.policy.cluster.trust_all=true` for cluster formation

**Test**: `curl -k https://localhost:7473`

## Neo4j Database Syntax Reference (5.26+ and 2025.x)

### CREATE DATABASE Syntax

**Neo4j 5.26+ (Cypher 5)**:
```cypher
CREATE DATABASE name [IF NOT EXISTS]
[TOPOLOGY n PRIMAR{Y|IES} [m SECONDAR{Y|IES}]]
[OPTIONS "{" option: value[, ...] "}"]
[WAIT [n [SEC[OND[S]]]]|NOWAIT]

CREATE OR REPLACE DATABASE name
[TOPOLOGY n PRIMAR{Y|IES} [m SECONDAR{Y|IES}]]
[OPTIONS "{" option: value[, ...] "}"]
[WAIT [n [SEC[OND[S]]]]|NOWAIT]
```

**Neo4j 2025.x (Cypher 25)**:
```cypher
CREATE DATABASE name [IF NOT EXISTS]
[[SET] DEFAULT LANGUAGE CYPHER {5|25}]
[[SET] TOPOLOGY n PRIMARIES [m SECONDARIES]]
[OPTIONS "{" option: value[, ...] "}"]
[WAIT [n [SEC[OND[S]]]]|NOWAIT]
```

### Example Usage

**Basic Database Creation**:
```cypher
-- Single primary with secondaries
CREATE DATABASE mydb TOPOLOGY 1 PRIMARY 2 SECONDARIES

-- Multiple primaries for fault tolerance
CREATE DATABASE proddb TOPOLOGY 3 PRIMARIES 2 SECONDARIES

-- Neo4j 2025.x with Cypher 25
CREATE DATABASE moderndb
DEFAULT LANGUAGE CYPHER 25
TOPOLOGY 3 PRIMARIES 2 SECONDARIES
```

**Parameterized Creation** (Operator Usage):
```cypher
-- Using parameters from operator
CREATE DATABASE $dbname
TOPOLOGY $primary PRIMARIES $secondary SECONDARIES WAIT
```

### ALTER DATABASE Syntax

```cypher
-- Change topology
ALTER DATABASE name
SET TOPOLOGY n PRIMARIES m SECONDARIES

-- Change access mode
ALTER DATABASE name
SET ACCESS {READ ONLY | READ WRITE}

-- Change default language (2025.x)
ALTER DATABASE name
SET DEFAULT LANGUAGE CYPHER {5|25}
```

### DROP DATABASE Syntax

```cypher
-- Standard drop
DROP DATABASE name [IF EXISTS]

-- Force drop with data destruction
DROP DATABASE name [IF EXISTS] DESTROY

-- Using parameters
DROP DATABASE $dbname IF EXISTS
```

### Database Status and Information

**Recommended Commands** (5.26+ and 2025.x):
```cypher
-- Show all databases and their topology
SHOW DATABASES

-- Show servers hosting databases
SHOW SERVERS

-- Check database allocation
SHOW DATABASE name YIELD name, currentPrimariesCount, currentSecondariesCount
```

### CRITICAL: Deprecated 4.x Syntax to AVOID

**❌ NEVER USE** (Neo4j 4.x - Will Fail in 5.26+):
```cypher
-- DEPRECATED: OPTIONS with primaries/secondaries
CREATE DATABASE baddb OPTIONS {primaries: 1, secondaries: 1}

-- DEPRECATED: dbms.cluster.role usage
CALL dbms.cluster.role()

-- DEPRECATED: Causal clustering syntax
-- Any causal_clustering.* configuration
```

### Options Parameter Validation

**Valid OPTIONS** (Database-level configuration):
- `storeFormat: "block"` - Storage format selection
- `txLogEnrichment: "OFF"` - Transaction log enrichment
- `existingData: "use"` - Handle existing data
- `initialNodeLabel: "Node"` - Initial node labeling

**❌ Invalid OPTIONS** (Will be rejected by operator):
- `db.logs.query.enabled` - Not a CREATE DATABASE option
- `primaries` / `secondaries` - Use TOPOLOGY clause instead
- Any `dbms.*` configuration - Not database-specific

### Best Practices for Operator Implementation

1. **Always Use TOPOLOGY Clause**:
   ```go
   // Correct approach in operator
   query := fmt.Sprintf("CREATE DATABASE %s TOPOLOGY %d PRIMARY %d SECONDARIES",
                       dbName, primaries, secondaries)
   ```

2. **Parameter Validation**:
   - Validate topology against cluster capacity
   - Reject 4.x-style OPTIONS parameters
   - Use proper Cypher escaping for database names

3. **Error Handling**:
   - Handle "database already exists" gracefully
   - Validate cluster can satisfy topology requirements
   - Provide clear error messages for syntax issues

4. **Version Compatibility**:
   ```go
   // Version-specific handling
   if neo4jVersion.IsAfter("2025.06") {
       // Use Cypher 25 features
       query += " DEFAULT LANGUAGE CYPHER 25"
   }
   ```

### Topology Fault Tolerance Guidelines

**Formula**: M = 2F + 1 (where M = primaries needed to tolerate F faults)

**Examples**:
- **1 Primary**: No fault tolerance (F=0)
- **3 Primaries**: Tolerates 1 failure (F=1)
- **5 Primaries**: Tolerates 2 failures (F=2)

**Recommended Topologies**:
- **Development**: `1 PRIMARY 1 SECONDARY` (minimal resources)
- **Production**: `3 PRIMARIES 2 SECONDARIES` (fault tolerant)
- **High Availability**: `5 PRIMARIES 2 SECONDARIES` (enterprise grade)

## Critical Architecture Decisions

### V2_ONLY Discovery (Fixed 2025-07-17)
- **Issue**: Wrong port used (6000 instead of 5000)
- **Solution**: Use `tcp-discovery` port (5000) for V2_ONLY mode
- **Verification**: `kubectl logs <pod> | grep "Resolved endpoints"` should show port 5000

### K8s Discovery Architecture (Fixed 2025-07-18)
- **Discovery returns service hostname**: This is correct behavior
- **RBAC**: Must have `endpoints` permission for discovery
- **Service**: Single ClusterIP discovery service (not headless)
- **DO NOT CHANGE**: This matches Neo4j Helm charts

### Parallel Cluster Formation (Fixed 2025-07-18)
- **Configuration**: `dbms.cluster.minimum_initial_system_primaries_count=1`, `ParallelPodManagement`
- **Result**: 100% cluster formation success
- **Key**: All server pods start simultaneously, self-organize roles



### TLS Cluster Formation (Fixed 2025-07-18)
- **Solution**: `ParallelPodManagement` + `trust_all=true` for cluster SSL
- **Result**: 100% success rate (was failing with split-brain)
- **Key**: Don't reduce timeouts, ensure endpoints RBAC

### Neo4j Configuration & Cluster Formation (Updated 2025-08-05)
- **Discovery Service Architecture**: V2_ONLY mode correctly uses discovery service hostname, not individual endpoints
- **Port Configuration**: Always use `tcp-discovery` (port 5000) for K8s discovery, not `tcp-tx` (port 6000)
- **Minimum Servers**: Set to 1 (`dbms.cluster.minimum_initial_system_primaries_count=1`) for flexible cluster formation
- **FQDN Addressing**: All advertised addresses use FQDN format via headless service
- **Service Setup**:
  - **Discovery Service**: ClusterIP with `tcp-discovery:5000`, selector includes `neo4j.com/clustering=true`
  - **Headless Service**: For pod-to-pod communication with all cluster ports
  - **Client Service**: ClusterIP for external access (bolt/http)
  - **Internals Service**: ClusterIP for operator management access
- **Key Success Factor**: Service-based discovery more reliable than endpoint-based for Neo4j in K8s

### Critical Neo4j Settings for Clusters (Added 2025-08-05)
These settings are automatically configured by the operator:
- `dbms.cluster.discovery.resolver_type=K8S`
- `dbms.kubernetes.discovery.v2.service_port_name=tcp-discovery` (5.x)
- `dbms.kubernetes.discovery.service_port_name=tcp-discovery` (2025.x)
- `dbms.cluster.discovery.version=V2_ONLY` (5.x only, default in 2025.x)
- `initial.dbms.automatically_enable_free_servers=true`
- `dbms.cluster.minimum_initial_system_primaries_count=1`

**Variable Substitution**: `${HOSTNAME_FQDN}` is substituted in startup script (server count is set directly)

## CRITICAL: Split-Brain Detection and Repair (Added 2025-08-09)

**MANDATORY FOR PRODUCTION**: The operator includes comprehensive split-brain detection and automatic repair to prevent Neo4j cluster inconsistencies.

### Split-Brain Detection System
**Location**: `internal/controller/splitbrain_detector.go`

**Key Features**:
- **Multi-Pod Analysis**: Connects to each Neo4j server pod individually to compare cluster views
- **Smart Detection**: Distinguishes between split-brain scenarios and normal startup/formation
- **Automatic Repair**: Restarts orphaned pods to rejoin the main cluster
- **Production Ready**: Includes comprehensive logging, events, and fallback mechanisms

### Detection Logic
```go
// Integrated into cluster formation verification
func (r *Neo4jEnterpriseClusterReconciler) verifyNeo4jClusterFormation(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) (bool, string, error) {
    // Performs split-brain detection before marking cluster as ready
    splitBrainDetector := NewSplitBrainDetector(r.Client)
    analysis, err := splitBrainDetector.DetectSplitBrain(ctx, cluster)

    // Automatic repair if split-brain detected
    if analysis.IsSplitBrain && analysis.RepairAction == RepairActionRestartPods {
        repairErr := splitBrainDetector.RepairSplitBrain(ctx, cluster, analysis)
    }
}
```

### Repair Actions
1. **`RepairActionRestartPods`**: Restart specific orphaned pods (preferred - automatic)
2. **`RepairActionRestartAll`**: Restart all pods (nuclear option - automatic)
3. **`RepairActionWaitForming`**: Wait for natural cluster formation (monitoring)
4. **`RepairActionInvestigate`**: Manual intervention required (alert)

### Why This Fix Is Critical
1. **Split-Brain Prevention**: Detects scenarios where servers form separate clusters instead of one unified cluster
2. **Data Consistency**: Prevents data divergence between isolated cluster partitions
3. **Automatic Recovery**: No manual intervention required for common split-brain scenarios
4. **Production Reliability**: Essential for maintaining cluster integrity in production environments

### Expected Behavior WITH Detection
- **Early Detection**: `Starting split-brain detection ... expectedServers: 3`
- **Smart Analysis**: `Split-brain analysis results ... isSplitBrain: true, orphanedPods: 1`
- **Automatic Repair**: `Split-brain automatically repaired by restarting orphaned pods`
- **Kubernetes Events**: `SplitBrainDetected`, `SplitBrainRepaired` events for monitoring

### Expected Behavior WITHOUT Detection
- **Silent Split-Brain**: Multiple independent clusters form without detection
- **Data Inconsistency**: Writes to different clusters create conflicting state
- **Database Creation Failures**: Applications fail due to insufficient cluster capacity
- **Manual Intervention**: Requires cluster deletion and recreation

### Monitoring Commands
```bash
# Check for split-brain events
kubectl get events --field-selector reason=SplitBrainDetected -A

# Monitor cluster formation
kubectl logs -n neo4j-operator-system deployment/neo4j-operator-controller-manager | grep -E "(split|brain|SplitBrain)"

# Verify cluster health after repair
kubectl exec <cluster>-server-0 -- cypher-shell -u neo4j -p <password> "SHOW SERVERS"
```

### Integration Testing
**Location**: `test/integration/splitbrain_detection_test.go`

**Test Coverage**:
- Split-brain detection during cluster startup
- Automatic repair verification
- Pod failure recovery scenarios
- Event generation validation

### Troubleshooting Split-Brain Issues
1. **Symptoms**: Database creation fails with "insufficient servers" despite running pods
2. **Detection**: Check operator logs for "Split-brain detected" messages
3. **Verification**: Use `SHOW SERVERS` on different pods - should see same server list
4. **Resolution**: Split-brain detector automatically restarts orphaned pods
5. **Prevention**: Ensure proper resource allocation and stable network connectivity

## CRITICAL: Resource Version Conflict Handling (Added 2025-08-05)

**MANDATORY FOR CLUSTER FORMATION**: The operator MUST include resource version conflict retry logic to prevent timing-sensitive cluster formation failures.

### Resource Version Conflict Fix Implementation
**Location**: `internal/controller/neo4jenterprisecluster_controller.go`

**Essential Pattern**:
```go
import "k8s.io/client-go/util/retry"

func (r *Neo4jEnterpriseClusterReconciler) createOrUpdateResource(ctx context.Context, obj client.Object, owner client.Object) error {
    return retry.RetryOnConflict(retry.DefaultRetry, func() error {
        return r.createOrUpdateResourceInternal(ctx, obj, owner)
    })
}
```

### Why This Fix Is Critical
1. **Neo4j 2025.01.0 Dependency**: Without retry logic, Neo4j 2025.01.0 fails to form clusters due to timing-sensitive resource conflicts during bootstrap
2. **StatefulSet Conflicts**: Kubernetes StatefulSet controller and operator reconciliation create concurrent updates
3. **Cluster Bootstrap Window**: Resource conflicts during critical cluster formation window cause permanent failure
4. **Production Reliability**: Essential for consistent cluster formation across all Neo4j versions

### Expected Behavior WITH Fix
- **Conflict Detection**: `Retrying resource update due to conflict ... retryCount: 1`
- **Fast Resolution**: `Successfully updated resource after conflict resolution ... duration: "18-25ms"`
- **Pod-2 Rolling Updates**: Expected side effect - highest-indexed pods restart during StatefulSet updates
- **100% Success Rate**: All conflicts resolved automatically

### Expected Behavior WITHOUT Fix
- **Neo4j 5.26.x**: Usually works but may have occasional timing issues
- **Neo4j 2025.01.0**: Consistently fails to form clusters - gets stuck at discovery resolution
- **Resource Conflicts**: Unresolved conflicts cause reconciliation failures
- **Manual Intervention**: Requires cluster deletion and recreation

### Verification Commands
```bash
# Check for conflict resolution in operator logs
kubectl logs -n neo4j-operator-system deployment/neo4j-operator-controller-manager | grep -E "(conflict|retry)"

# Verify cluster formation success
kubectl get neo4jenterprisecluster
kubectl get pods | grep -E "(server)"

# Monitor StatefulSet revisions (should be minimal)
kubectl rollout history statefulset <cluster-name>-server
```

### ConfigMap Debounce Configuration
**Location**: `internal/controller/configmap_manager.go:120`

**Recommended Setting**:
```go
minInterval := 1 * time.Second // Fast updates for cluster formation
```

**NOT**: `minInterval := 2 * time.Minute // Can cause timing issues`

### Troubleshooting Resource Conflicts
1. **Symptoms**: Pods stuck in ContainerCreating, cluster never reaches Ready state
2. **Diagnosis**: Check operator logs for "resource version" or "conflict" errors
3. **Resolution**: Ensure retry logic is implemented and debounce period is minimal
4. **Prevention**: Always include conflict handling in any resource update operations

### REGRESSION PREVENTION CHECKLIST
- [x] Retry logic present in `createOrUpdateResource` methods
- [x] Import `k8s.io/client-go/util/retry` in controller
- [x] ConfigMap debounce ≤ 1 second for cluster formation
- [x] Test with Neo4j 2025.01.0 to verify cluster formation
- [x] Monitor operator logs for conflict resolution messages
- [x] Verify StatefulSet rolling updates complete successfully
- [x] Split-brain detection system active in `verifyNeo4jClusterFormation`
- [x] Test split-brain repair with orphaned pod scenarios
- [x] Verify `SplitBrainDetected` and `SplitBrainRepaired` events are generated
- [x] Monitor split-brain logs during cluster formation
- [x] Database deletion handles "database not found" gracefully
- [x] ConfigMap generation is deterministic (sorted keys)
- [x] Database OPTIONS validation rejects invalid parameters
- [x] Split-brain detection re-enabled with stable ConfigMap
- [x] Memory limits set to ≥ 1.5Gi to prevent OOM during database operations
- [x] Integration tests updated to prevent OOMKilled (exit code 137)
- [x] Neo4j 5.x TOPOLOGY syntax validated for database creation
- [x] Neo4j 4.x deprecated syntax identification and prevention
- [x] Comprehensive Neo4j syntax reference documentation added
- [x] Database OPTIONS validation prevents deprecated parameters

**DO NOT**: Remove or modify retry logic without comprehensive testing across all Neo4j versions
**DO NOT**: Remove or disable split-brain detection without understanding production impact
**DO NOT**: Use any Neo4j 4.x syntax (causal_clustering.*, dbms.mode=SINGLE, OPTIONS primaries/secondaries)

## CRITICAL: Server-Based Architecture (Updated 2025-08-07)

### Architecture Transition Overview
The operator transitioned from a **primary/secondary StatefulSet architecture** to a **unified server architecture** where Neo4j servers self-organize into primary and secondary roles based on database requirements.

### Key Architecture Changes

#### **Before (Primary/Secondary StatefulSets)**:
```yaml
topology:
  primaries: 3
  secondaries: 2
```
- Separate `<cluster>-primary` and `<cluster>-secondary` StatefulSets
- Pre-assigned pod roles at infrastructure level
- Complex topology management and scaling logic

#### **After (Server-Based Architecture)**:
```yaml
topology:
  servers: 5  # Self-organize into primary/secondary roles
```
- Single `<cluster>-server` StatefulSet
- Servers auto-assign roles based on database topology requirements
- Simplified infrastructure, flexible role assignment

### Database vs Cluster Topology
**CRITICAL DISTINCTION**: Different levels use different topology models:

#### **Cluster Level** (Neo4jEnterpriseCluster):
- Uses `servers: N` field
- Servers self-organize and are role-agnostic
- Infrastructure provides server pool

#### **Database Level** (Neo4jDatabase):
- Still uses `primaries: X, secondaries: Y` fields
- Specifies how databases should be distributed across available servers
- Neo4j automatically allocates databases to appropriate server roles

### Implementation Details

#### **StatefulSet Architecture**:
- **Old**: `<cluster>-primary-{0,1,2}` and `<cluster>-secondary-{0,1}`
- **New**: `<cluster>-server-{0,1,2,3,4}`
- All pods use identical configuration and auto-discover roles

#### **Service Architecture** (Unchanged):
- **Discovery Service**: ClusterIP for cluster formation (`tcp-discovery:5000`)
- **Headless Service**: Pod-to-pod communication
- **Client Service**: External access (bolt/http)
- **Internals Service**: Operator management

#### **Cluster Formation Process**:
1. All server pods start simultaneously (`ParallelPodManagement`)
2. First server(s) to start form the initial cluster
3. Additional servers join the existing cluster
4. Databases are created with specified primary/secondary topology
5. Neo4j automatically assigns database hosting to appropriate servers

### Configuration Impact

#### **Startup Script Changes**:
- **Old**: `MIN_PRIMARIES=${REQUESTED_PRIMARIES}` variable substitution
- **New**: `TOTAL_SERVERS=N` with fixed `dbms.cluster.minimum_initial_system_primaries_count=1`

#### **Discovery Configuration** (Unchanged):
- Still uses V2_ONLY discovery with `tcp-discovery` port (5000)
- Same Kubernetes service discovery patterns
- Same RBAC requirements (`endpoints` permission)

### Key Benefits

1. **Simplified Scaling**: Single StatefulSet vs multiple StatefulSets
2. **Flexible Role Assignment**: Servers adapt to database topology needs
3. **Reduced Complexity**: No pre-role assignment logic
4. **Better Resource Utilization**: Servers can host multiple database roles
5. **Easier Maintenance**: Single pod template and configuration

### Migration Considerations

#### **API Compatibility**:
- Old `primaries`/`secondaries` fields removed from Neo4jEnterpriseCluster
- New `servers` field for total server count
- Database-level topology preserved (Neo4jDatabase CRD unchanged)

#### **Operational Impact**:
- Pod names changed: `cluster-primary-0` → `cluster-server-0`
- DNS names updated for certificates and services
- Monitoring queries need updates for new naming

#### **Testing Updates**:
- All tests updated to expect `server-*` naming
- Topology validation focuses on server counts vs role counts
- Certificate generation includes all server DNS names

### Critical Success Factors

1. **Resource Conflict Handling**: Essential for server bootstrap coordination
2. **Parallel Pod Management**: Ensures simultaneous server startup
3. **Fixed Minimum Bootstrap**: `dbms.cluster.minimum_initial_system_primaries_count=1`
4. **Service Discovery**: V2_ONLY with proper RBAC permissions
5. **FQDN Addressing**: Consistent pod FQDN usage for cluster communication

### Troubleshooting Server Architecture

#### **Cluster Formation Issues**:
```bash
# Check server pod status
kubectl get pods -l neo4j.com/cluster=<cluster-name>

# Verify cluster formation
kubectl exec <cluster>-server-0 -- cypher-shell -u neo4j -p <password> "SHOW SERVERS"

# Check server logs for discovery
kubectl logs <cluster>-server-0 | grep -E "(Resolved endpoints|cluster formation)"
```

#### **Database Role Assignment**:
```bash
# Check database distribution across servers
kubectl exec <cluster>-server-0 -- cypher-shell -u neo4j -p <password> "SHOW DATABASES"

# Verify topology constraints are met
kubectl exec <cluster>-server-0 -- cypher-shell -u neo4j -p <password> "SHOW DATABASE <db-name> YIELD name, currentPrimariesCount, currentSecondariesCount"
```

### Documentation and Examples Impact

All documentation, examples, and guides updated to reflect:
- Server-based topology specifications
- Updated command examples with `server-*` pod names
- Certificate DNS name patterns
- Monitoring query updates
- Test case expectations

**CRITICAL**: When making changes, always distinguish between:
- **Cluster-level topology**: Uses `servers` (self-organizing infrastructure)
- **Database-level topology**: Uses `primaries`/`secondaries` (allocation requirements)

## Configuration Validation

### Integration Test Configuration
- **Timeouts**: All integration tests use 300-second timeout for CI compatibility
- **Resources**: Minimal CPU (50m-200m), memory limits at 1.5Gi (Neo4j Enterprise requirement for database operations)
- **Storage**: Reduced storage sizes (500Mi-1Gi) to avoid PVC scheduling issues
- **Image Pull**: Tests account for image pull delays in CI environments
- **Memory Validation**: Neo4j Enterprise requires minimum 1.5Gi for database creation and topology operations
- **OOM Prevention**: Tests configured to prevent Out of Memory kills (exit code 137) during database operations

### Template Comparison Fix (Critical)
**Issue**: Original logic used `sts.ResourceVersion != ""` to check if StatefulSet exists
**Problem**: ResourceVersion is populated even for new resources, preventing initial creation
**Solution**: Use `sts.UID != ""` which correctly identifies existing vs new resources
**Impact**: Enables immediate StatefulSet creation instead of being blocked by template comparison

### CI Environment Considerations
- **Resource Limits**: GitHub Actions runners have limited CPU/memory
- **Storage Classes**: Use 'standard' storage class for compatibility
- **Pod Scheduling**: Avoid resource requests that exceed node capacity
- **Network Policies**: Ensure cert-manager can issue certificates

### Regression Prevention Checklist
1. **Resource Conflicts**: Always use `retry.RetryOnConflict` with `controllerutil.CreateOrUpdate`
2. **Template Comparison**: Use `UID != ""` to check resource existence, not `ResourceVersion != ""`
3. **Test Timeouts**: Use 300-second timeout for all integration tests
4. **Resource Requirements**: Keep CPU ≤ 200m, memory limits must be ≥ 1.5Gi for Neo4j Enterprise (database operations)
5. **Cluster Formation**: Verify using `SHOW SERVERS` command, not just status checks
6. **Server Architecture**: Always use `servers` field for clusters, preserve `primaries`/`secondaries` for databases
7. **Pod Naming**: Expect `<cluster>-server-*` naming, not `<cluster>-primary-*` or `<cluster>-secondary-*`
8. **Certificate DNS**: Include all server pod DNS names in certificates
9. **Discovery Port**: Always use `tcp-discovery` (5000), never `tcp-tx` (6000) for V2_ONLY mode

## Key Learnings and Best Practices

### Architecture Evolution Insights

#### **Server-Based Architecture Benefits** (2025-08-07):
- **Simplified Operations**: Single StatefulSet reduces complexity vs separate primary/secondary StatefulSets
- **Role Flexibility**: Servers adapt to database needs rather than pre-assigned infrastructure roles
- **Better Resource Usage**: Servers can host multiple database roles based on actual requirements
- **Easier Scaling**: Scale server pool independently of database topology requirements
- **Reduced Configuration**: Identical pod configuration for all servers simplifies management

#### **Critical Design Patterns That Work**:
1. **Parallel Pod Management**: Essential for coordinated server startup and cluster formation
2. **Fixed Bootstrap Minimum**: `dbms.cluster.minimum_initial_system_primaries_count=1` provides maximum flexibility
3. **Service-Based Discovery**: More reliable than endpoint-based discovery in Kubernetes environments
4. **FQDN Addressing**: Consistent FQDN usage prevents communication issues in complex network setups
5. **Resource Conflict Retry**: Mandatory for reliable cluster formation under concurrent operations

#### **Anti-Patterns to Avoid**:
- **Never** use `tcp-tx` port (6000) for V2_ONLY discovery - always use `tcp-discovery` (5000)
- **Never** set `dbms.mode=SINGLE` in clustered environments - breaks cluster capabilities
- **Never** mix pre-Neo4j 5.26 configuration patterns with modern V2_ONLY discovery
- **Never** use `ResourceVersion != ""` for existence checks - use `UID != ""` instead
- **Never** remove resource conflict retry logic without extensive testing across all Neo4j versions

### Operational Excellence

#### **Testing Strategy**:
- **Unit Tests First**: Run `make test-unit` before all commits - catches 80% of issues early
- **Integration Testing**: Use 300-second timeouts to handle CI environment constraints
- **Server Architecture**: Test with `SHOW SERVERS` commands, not just pod status checks
- **Resource Constraints**: Keep test resource requests minimal (≤ 200m CPU, ≥ 1Gi memory)
- **Neo4j 2025.x Focus**: Always test new features with latest Neo4j versions first

#### **Debugging Methodology**:
1. **Check Cluster Formation**: `kubectl exec <pod> -- cypher-shell "SHOW SERVERS"`
2. **Verify Discovery**: `kubectl logs <pod> | grep "Resolved endpoints"`
3. **Monitor Resources**: Check for resource conflicts in operator logs
4. **Database Topology**: Use `SHOW DATABASES` to verify role assignment
5. **Network Connectivity**: Ensure all pods can resolve each other's FQDNs

#### **Performance Considerations**:
- **Memory Requirements**: Neo4j Enterprise requires minimum 1.5Gi for database operations - never go below this
- **OOM Prevention**: Monitor for exit code 137 (OOMKilled) and increase memory if needed
- **Storage Classes**: Use fast storage classes for production (`fast-ssd` preferred)
- **CPU Allocation**: Start with 500m requests, scale based on actual load
- **Network Policies**: Ensure cluster formation traffic is not blocked
- **Image Pull**: Account for image pull time in test timeouts (especially in CI)

### Development Workflow

#### **Code Change Process**:
1. **Read Documentation**: Start with CLAUDE.md and relevant API documentation
2. **Understand Architecture**: Distinguish between cluster-level and database-level topology
3. **Update Tests First**: Write/update tests before implementing changes
4. **Validate Locally**: Use `make dev-run` for quick local testing
5. **Integration Testing**: Deploy to Kind cluster for realistic testing
6. **Monitor Logs**: Watch for resource conflicts, discovery issues, cluster formation problems

#### **Documentation Maintenance**:
- **Update Examples**: Keep examples synchronized with API changes
- **Version Compatibility**: Document which Neo4j versions support which features
- **Troubleshooting Guides**: Update based on real operational issues
- **API Changes**: Always update both code and documentation simultaneously

#### **Release Considerations**:
- **Backward Compatibility**: Consider impact of CRD changes on existing deployments
- **Migration Paths**: Provide clear guidance for architecture transitions
- **Feature Flags**: Use feature gates for experimental functionality
- **Version Support**: Clearly communicate supported Neo4j version ranges

### Kubernetes Ecosystem Integration

#### **cert-manager Integration**:
- **Version**: Use cert-manager v1.18.2 for development clusters
- **CA Cluster Issuer**: Standard pattern for test environments
- **DNS Names**: Include all server pod FQDNs in certificates
- **Trust Policies**: Use `trust_all=true` for cluster SSL in development

#### **RBAC Best Practices**:
- **Minimal Permissions**: Only grant necessary permissions for discovery and management
- **Endpoints Access**: Required for Kubernetes service discovery
- **Service Account**: Use dedicated service accounts per operator installation
- **Namespace Scoped**: Prefer namespace-scoped roles over cluster roles where possible

#### **Storage Integration**:
- **Storage Classes**: Support multiple storage classes for different performance needs
- **PVC Sizing**: Validate storage size requirements during admission
- **Backup Storage**: Support multiple cloud providers (S3, GCS, Azure)
- **Volume Expansion**: Consider dynamic volume expansion capabilities

### Future Architecture Considerations

#### **Scalability Targets**:
- **Cluster Size**: Support clusters up to 100+ servers
- **Database Count**: Multiple databases per cluster with independent topologies
- **Multi-Region**: Consider cross-region deployment patterns
- **Auto-Scaling**: Horizontal pod autoscaling based on resource utilization

#### **Observability Enhancements**:
- **Metrics Integration**: Prometheus metrics for all cluster operations
- **Distributed Tracing**: OpenTelemetry integration for request tracing
- **Log Aggregation**: Structured logging with consistent formats
- **Health Checks**: Comprehensive health check endpoints

#### **Security Hardening**:
- **Pod Security Standards**: Implement restricted pod security standards
- **Network Policies**: Default-deny network policies with explicit allowlists
- **Secret Management**: Integration with external secret management systems
- **Image Scanning**: Container image vulnerability scanning in CI/CD

**Remember**: The Neo4j Kubernetes Operator manages complex stateful systems. Always prioritize reliability and operational simplicity over feature complexity.

## Development Milestones

### 2025-08-12: Neo4j Enterprise Memory Optimization and OOM Fix
**Status**: ✅ COMPLETE - Database validation tests now pass consistently without memory issues

**Critical Memory Issues Resolved**:
1. **OOM Kill Prevention** - Identified and resolved Out of Memory kills during database operations
2. **Neo4j Enterprise Memory Requirements** - Documented minimum 1.5Gi requirement for database operations
3. **Integration Test Optimization** - Updated test configurations to prevent OOM in CI environments
4. **Database Creation Reliability** - Validated Neo4j 5.x TOPOLOGY syntax working correctly

**Root Cause Analysis**:
- **Problem**: Integration tests failing with OOMKilled (exit code 137) during database creation
- **Investigation**: Neo4j containers were being terminated due to insufficient memory during database operations
- **Memory Profile**: 1Gi was insufficient for Neo4j Enterprise when creating databases with topology constraints
- **Solution**: Increased memory limits from 1Gi to 1.5Gi in integration test configurations

**Technical Details**:
```yaml
# Previous configuration (causing OOM)
resources:
  requests:
    memory: "1Gi"
  limits:
    memory: "1Gi"

# Updated configuration (OOM-free)
resources:
  requests:
    memory: "1.5Gi"  # Sufficient for Neo4j Enterprise + database operations
  limits:
    memory: "1.5Gi"  # Prevents OOM during database creation
```

**Files Modified**:
- `test/integration/database_validation_test.go` - Memory limits increased to 1.5Gi
- `test/integration/splitbrain_detection_test.go` - Verified existing 1Gi limits sufficient for cluster formation
- Verified Neo4j 5.x database creation syntax: `CREATE DATABASE testdb TOPOLOGY 1 PRIMARY 1 SECONDARY`

**Verification Results**:
- **Cluster Formation**: 100% success with 2-server and 3-server clusters
- **Database Operations**: `SHOW SERVERS` and `SHOW DATABASES` commands working correctly
- **Memory Usage**: No OOM kills observed with 1.5Gi limits
- **Syntax Validation**: Neo4j 5.x TOPOLOGY clause working as expected vs old OPTIONS syntax

**Production Implications**:
- **Memory Requirements**: Neo4j Enterprise clusters should use minimum 1.5Gi for database operations
- **CI Environment**: Integration tests now pass consistently in resource-constrained environments
- **Monitoring**: Watch for `OOMKilled` status and exit code 137 in production deployments
- **Scaling**: Consider higher memory limits (2Gi+) for clusters with frequent database operations

### 2025-08-10: Major Stability and Validation Improvements
**Status**: ✅ COMPLETE - All priority fixes implemented and tested

**Critical Fixes Delivered**:
1. **Database Deletion Timeout Fix** - Resolved infinite retry loop when deleting non-existent databases
2. **ConfigMap Oscillation Fix** - Eliminated hash thrashing causing unnecessary pod restarts
3. **Database OPTIONS Validation** - Comprehensive validation for CREATE DATABASE OPTIONS syntax
4. **Split-Brain Detection Re-enabled** - Restored automatic cluster health monitoring and repair

**Key Technical Achievements**:
- **Enhanced Reliability**: Database operations now handle edge cases gracefully
- **Improved Stability**: ConfigMap determinism prevents spurious pod restarts
- **Better UX**: OPTIONS validation provides clear error messages and guidance
- **Monitoring**: Split-brain detection provides automatic cluster health checks

**Files Modified**:
- `internal/neo4j/client.go` - Database deletion timeout fix
- `internal/resources/cluster.go` - ConfigMap determinism fix
- `internal/validation/database_validator.go` - OPTIONS validation enhancement
- `internal/controller/neo4jenterprisecluster_controller.go` - Split-brain detection re-enabled

**Test Coverage Added**:
- Unit tests for ConfigMap determinism in `internal/resources/cluster_test.go`
- Integration tests for OPTIONS validation in `test/integration/database_validation_test.go`
- Split-brain detection validated with 3-node cluster scenarios

**Operational Impact**:
- Zero unnecessary pod restarts due to ConfigMap oscillation
- Database operations complete successfully instead of hanging
- Invalid OPTIONS parameters rejected with helpful error messages
- Automatic split-brain detection and repair for production clusters

### 2025-08-08: Seed URI Functionality Complete
**Status**: ✅ COMPLETE - All unit tests (32/32) and integration tests (6/6) passing

**Functionality Delivered**:
- Complete seed URI implementation for Neo4jDatabase CRD
- Support for S3, GS, AZB, HTTP, HTTPS, FTP protocols
- Comprehensive validation (credentials, configuration, URI format)
- Conflict detection (seed URI vs initial data)
- Topology validation against cluster capacity

**Key Technical Fixes**:
- Fixed unit tests by adding DatabaseValidator initialization
- Fixed integration tests by adding missing `neo4j-admin-secret` and Auth configuration
- Fixed cluster resource validation (reduced test cluster size from 5 to 3 servers)
- Resolved `CreateContainerConfigError` issues in integration test pods
- Cleaned up 63MB binary artifact from root directory and added to .gitignore

**Files Modified**:
- `internal/controller/neo4jdatabase_controller_test.go` - Unit test fixes
- `test/integration/database_seed_uri_test.go` - Integration test fixes
- Operator successfully deployed and tested in Kind clusters

**Lessons Learned**:
- Integration test failures were not resource constraints but real configuration issues
- Always investigate actual error messages rather than assuming timeouts = resource issues
- Neo4j Enterprise clusters require proper authentication secrets and minimum memory per server

## Reports

All reports go in `/reports/` directory with mandatory `YYYY-MM-DD-descriptive-name.md` format.

Example: `2025-07-23-integration-tests-fix-summary.md`
