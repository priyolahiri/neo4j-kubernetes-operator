/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//
// Neo4jShardedDatabase Controller Implementation
//
// This controller manages Neo4j property-sharded databases using Neo4j 2025.12+ Cypher 25 syntax.
//
// Key Implementation Details:
//
// 1. Database Creation Approach:
//    - Uses single Cypher 25 CREATE DATABASE command (not separate shard creation)
//    - Command format: CYPHER 25 CREATE DATABASE `name` IF NOT EXISTS
//                      SET DEFAULT LANGUAGE CYPHER 25
//                      SET GRAPH SHARD { TOPOLOGY n PRIMARIES m SECONDARIES }
//                      SET PROPERTY SHARDS { COUNT n TOPOLOGY m REPLICAS }
//                      OPTIONS { seedURI: ..., seedConfig: ..., seedSourceDatabase: ..., seedRestoreUntil: ..., txLogEnrichment: ... } WAIT
//
// 2. Status Verification:
//    - Uses SHOW DATABASES command to verify graph shard creation
//    - Avoids non-existent SHOW SHARDED DATABASES command
//    - Looks for {database}-g000 shard to confirm successful creation
//
// 3. Resource Requirements (Updated 2025-09-07):
//    - Minimum: 4GB memory per server (reduced from 12-16GB)
//    - Recommended: 8GB memory per server for production workloads
//    - CPU: 2+ cores per server for cross-shard query performance
//    - Servers: 1+ server (3+ recommended for HA graph shard primaries)
//
// 4. Error Handling:
//    - Graceful degradation when virtual database not immediately visible
//    - Retry logic for Neo4j client operations
//    - Comprehensive status reporting and event recording
//

package controller

import (
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
	corev1 "k8s.io/api/core/v1"
)

// shardedNeo4jClient is the part of the Neo4j client the sharded database
// reconcile uses. It exists so the branches that depend on what the server
// answers — whether the family already exists, what CREATE returns — can be
// driven without a Bolt server; production always supplies a *neo4j.Client.
type shardedNeo4jClient interface {
	GetDatabases(ctx context.Context) ([]neo4j.DatabaseInfo, error)
	ExecuteCypher(ctx context.Context, databaseName, statement string) error
	ExecuteCypherWithParams(ctx context.Context, databaseName, statement string, params map[string]any) error
	PrepareCloudCredentialsForShardedDatabase(ctx context.Context, k8sClient client.Client, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) error
	Close() error
}

// Neo4jShardedDatabaseReconciler reconciles a Neo4jShardedDatabase object
type Neo4jShardedDatabaseReconciler struct {
	client.Client
	Scheme                   *runtime.Scheme
	Recorder                 record.EventRecorder
	MaxConcurrentReconciles  int
	RequeueAfter             time.Duration
	ShardedDatabaseValidator *validation.ShardedDatabaseValidator

	// APIReader reads straight from the API server, bypassing the informer
	// cache. Each pass reads the sharded database through it: the pass decides
	// from the status the previous pass wrote — its phase, ShardingReady,
	// LastDestructiveRestoreGeneration — and the cache can still hold a copy
	// from before that write when the write's own watch event starts the pass
	// (knowledge rule 94). nil falls back to the cached client.
	APIReader client.Reader

	// newNeo4jClient builds the Neo4j client for the referenced cluster. nil
	// selects the real one; tests substitute a scripted server.
	newNeo4jClient func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error)
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jshardeddatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jshardeddatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jshardeddatabases/finalizers,verbs=update
// PVC seed proxy resources (F5): the operator creates a busybox httpd
// Deployment + ClusterIP Service per Neo4jShardedDatabase whose seedBackupRef
// resolves to a PVC-backed backup. Owner reference on the sharded DB CR
// handles GC on delete, so no explicit delete verb is needed.
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Neo4jShardedDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jshardeddatabase", req.NamespacedName)
	logger.Info("Starting reconciliation of Neo4jShardedDatabase")

	// Fetch the Neo4jShardedDatabase instance, as the API server has it. From
	// the cache, a pass started by the Ready write could read the phase from
	// before it ("Validating"), write "Creating" over Ready and announce
	// ShardedDatabaseReady a second time in the same second (v1.22.0
	// journey); a stale ShardingReady would also re-resolve a consumed seed.
	var shardedDatabase neo4jv1beta1.Neo4jShardedDatabase
	if err := r.reader().Get(ctx, req.NamespacedName, &shardedDatabase); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Neo4jShardedDatabase resource not found, likely deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Neo4jShardedDatabase")
		return ctrl.Result{}, err
	}

	// Initialize status if needed
	if shardedDatabase.Status.Phase == "" {
		logger.Info("Initializing Neo4jShardedDatabase status")
		if err := r.updateStatus(ctx, &shardedDatabase, "Validating", "Validating sharded database configuration", nil); err != nil {
			logger.Error(err, "Failed to initialize status")
			return ctrl.Result{}, err
		}
	}

	// Validate the sharded database configuration
	if err := r.validateShardedDatabase(ctx, &shardedDatabase); err != nil {
		// One prefix, here: the validator returns the errors alone.
		msg := fmt.Sprintf("Validation failed: %v", err)
		failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed", msg, nil)
		if statusErr != nil {
			logger.Error(statusErr, "Failed to update status after validation failure")
		}
		// Logged as the event is announced: once per cause, not as an ERROR
		// on every requeue (knowledge ids 136, 197).
		logRefusal(logger, failed, "Validation failed", err)
		// Announced when the failure starts or its message changes, not on
		// every RequeueAfter while the spec stays invalid: the persisted phase
		// and message say whether this pass is news (knowledge rule 136).
		if failed {
			r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
		}
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Update status to show validation passed
	if shardedDatabase.Status.Phase == "Validating" {
		if err := r.updateStatus(ctx, &shardedDatabase, "Creating", "Configuration validated, preparing to create sharded database", nil); err != nil {
			logger.Error(err, "Failed to update status after validation")
			return ctrl.Result{}, err
		}
	}

	// An older Neo4jShardedDatabase that gives this name on this cluster owns
	// it: Neo4j lower-cases the family's names, so both would adopt one
	// family (operations.md id 217).
	duplicate, err := shardedDatabaseNames.duplicate(ctx, r.Client, &shardedDatabase)
	if err != nil {
		return ctrl.Result{}, err
	}
	if duplicate != "" {
		failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed", duplicate, nil)
		if statusErr != nil {
			logger.Error(statusErr, "Failed to update status for a duplicate name")
		}
		// Announced when it starts, not on every pass (knowledge rule 136).
		if failed {
			r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonDuplicateName, duplicate)
		}
		return ctrl.Result{}, nil
	}

	// Get the referenced cluster. A cluster that does not exist (yet) is a
	// wait, as for a Neo4jDatabase: Pending, announced once, and the sharded
	// database is created once the cluster is there. Any other read error is
	// transient and retried with backoff (knowledge rule 145).
	cluster, err := r.getReferencedCluster(ctx, &shardedDatabase)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to get referenced cluster")
			return ctrl.Result{}, err
		}
		msg := fmt.Sprintf("Cluster not found: Neo4jEnterpriseCluster %q does not exist in namespace %q; waiting for it",
			shardedDatabase.Spec.ClusterRef, shardedDatabase.Namespace)
		logger.Info("Referenced cluster not found; waiting", "cluster", shardedDatabase.Spec.ClusterRef)
		waiting, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Pending", msg, nil)
		if statusErr != nil {
			logger.Error(statusErr, "Failed to update status while waiting for the cluster")
		}
		if waiting {
			r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonClusterNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Verify cluster supports property sharding. Capability (spec/version)
	// and readiness (cluster still Forming) are DIFFERENT answers: the
	// shipped examples apply the cluster and the sharded DB in one file, so
	// every user reconciles this CR minutes before the cluster is Ready —
	// reporting that as Failed/"does not support property sharding" sent
	// them debugging the wrong thing. Not-ready waits in Waiting; only a
	// genuine capability gap is Failed.
	if !r.clusterSupportsPropertySharding(cluster) {
		if cluster.Spec.PropertySharding == nil || !cluster.Spec.PropertySharding.Enabled {
			err := fmt.Errorf("cluster %s does not support property sharding (requires Neo4j 2025.12+ and propertySharding.enabled=true)", cluster.Name)
			failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed", err.Error(), nil)
			if statusErr != nil {
				logger.Error(statusErr, "Failed to update status after cluster capability check")
			}
			logRefusal(logger, failed, "Cluster does not support property sharding", err)
			// Announced when the failure starts, not on every RequeueAfter
			// while the cluster stays without sharding (knowledge rule 145).
			if failed {
				r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonClusterNotReady, err.Error())
			}
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		msg := fmt.Sprintf("waiting for cluster %s to become Ready with property sharding operational (phase: %s)", cluster.Name, cluster.Status.Phase)
		logger.Info("Cluster not ready for property sharding yet; waiting", "cluster", cluster.Name, "phase", cluster.Status.Phase)
		// A sharded database Ready at its generation stays Ready while its
		// cluster rolls or restarts, with ClusterNotReady=True; anything else
		// waits in Waiting (knowledge rule 183).
		r.waitForCluster(ctx, &shardedDatabase, ConditionReasonClusterNotReady, msg)
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Create Neo4j client for the cluster
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		if targetAway(err) {
			// The cluster reads Ready but does not answer: the rest of its
			// outage, not a failure of this database (knowledge rule 183).
			return r.clusterUnreachable(ctx, &shardedDatabase, err)
		}
		logger.Error(err, "Failed to create Neo4j client")
		failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed", fmt.Sprintf("Failed to create Neo4j client: %v", err), nil)
		if statusErr != nil {
			logger.Error(statusErr, "Failed to update status after client creation failure")
		}
		// Retried with backoff; the same failure is announced once (rule 145).
		if failed {
			r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonClientCreationFailed, err.Error())
		}
		return ctrl.Result{}, err
	}
	defer neo4jClient.Close()

	// Phase 2 seed-from-backup: if spec.seedBackupRef is set, resolve it into
	// a concrete seedURI before building the CREATE DATABASE Cypher. Errors
	// from the resolver have two meanings:
	//   - ErrBackupNotReady (backup exists but has no Succeeded run yet) →
	//     transient; route to Pending + requeue. Mirrors CLAUDE.md rule 72
	//     for the restore controller.
	//   - Anything else (missing CR, PVC storage, unsupported type) →
	//     permanent; route to Failed.
	// Seed resolution only matters while the seed can still be CONSUMED:
	// initial creation (not ShardingReady yet) or a pending destructive
	// replaceExisting re-trigger (rule 64). Once the sharded DB is Ready,
	// resolving again would re-create the PVC seed proxy that the Ready
	// transition just tore down — oscillating Ready→Pending and re-exposing
	// the backup PVC on every periodic reconcile (#224 review).
	seedConsumable := shardedSeedConsumable(&shardedDatabase)
	var resolved *ResolvedShardedSeed
	var seedErr error
	if seedConsumable {
		resolved, seedErr = r.resolveShardedSeed(ctx, &shardedDatabase)
	}
	if resolved != nil || seedErr != nil {
		if seedErr != nil {
			if stderrors.Is(seedErr, ErrBackupNotReady) {
				logger.Info("seedBackupRef target has no Succeeded run yet, requeuing",
					"seedBackupRef", shardedDatabase.Spec.SeedBackupRef, "error", seedErr.Error())
				// Once when the wait starts, not on every pass: repeated every
				// RequeueAfter it exhausted the object's event budget, and
				// SeedCredsAutoInherited never reached the API (v1.20.0 journey).
				waiting, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Pending",
					fmt.Sprintf("Waiting for Neo4jBackup %q to produce a Succeeded run", shardedDatabase.Spec.SeedBackupRef), nil)
				if statusErr != nil {
					logger.Error(statusErr, "Failed to update status to Pending")
				}
				if waiting {
					r.Recorder.Event(&shardedDatabase, corev1.EventTypeNormal, EventReasonSeedBackupPending,
						"Waiting for referenced Neo4jBackup to complete a successful run")
				}
				// The seed waits ask nothing of the cluster (knowledge rule 210).
				r.clusterAnswered(ctx, &shardedDatabase)
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed",
				fmt.Sprintf("seedBackupRef resolution failed: %v", seedErr), nil)
			if statusErr != nil {
				logger.Error(statusErr, "Failed to update status to Failed")
			}
			logRefusal(logger.WithValues("seedBackupRef", shardedDatabase.Spec.SeedBackupRef), failed,
				"Failed to resolve seedBackupRef", seedErr)
			// Announced when the failure starts or its message changes, not
			// on every RequeueAfter (knowledge rule 145).
			if failed {
				r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonSeedBackupResolutionFailed, seedErr.Error())
			}
			r.clusterAnswered(ctx, &shardedDatabase)
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		// Populate the in-memory spec so downstream Cypher builders use the
		// resolved URI(s) without needing to know about SeedBackupRef. The
		// CR is NOT Updated — only the in-memory copy is mutated for this
		// reconcile. URI vs PerShardURIs is mutually exclusive: cloud
		// backups populate the single URI; PVC backups populate the
		// per-shard map served by the operator-managed seed proxy.
		switch {
		case resolved.URI != "":
			shardedDatabase.Spec.SeedURI = resolved.URI
			logger.Info("Resolved seedBackupRef to cloud seedURI",
				"seedBackupRef", shardedDatabase.Spec.SeedBackupRef, "seedURI", resolved.URI)
		case len(resolved.PerShardURIs) > 0:
			if !resolved.ProxyAvailable {
				logger.Info("PVC seed proxy not yet Ready; requeuing",
					"seedBackupRef", shardedDatabase.Spec.SeedBackupRef)
				waiting, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Pending",
					"Waiting for PVC seed proxy to become Ready", nil)
				if statusErr != nil {
					logger.Error(statusErr, "Failed to update status to Pending")
				}
				if waiting {
					r.Recorder.Event(&shardedDatabase, corev1.EventTypeNormal, EventReasonSeedProxyStarting,
						"Waiting for backup-seed-proxy Deployment to become Ready")
				}
				r.clusterAnswered(ctx, &shardedDatabase)
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			shardedDatabase.Spec.SeedURIs = resolved.PerShardURIs
			// Clear SeedURI so the Cypher options builder uses seedURIs map.
			shardedDatabase.Spec.SeedURI = ""
			// seedSourceDatabase is incompatible with the seedURIs map form
			// — Neo4j rejects CREATE DATABASE with "OPTIONS specify
			// 'seedSourceDatabase' expecting 'seedURI' to be present" when
			// both are emitted. The per-shard URL map carries the source
			// shard identity in its key (target-shard-name → source-shard
			// URL), so seedSourceDatabase would just be redundant anyway.
			// Clear it in-memory on the PVC path.
			shardedDatabase.Spec.SeedSourceDatabase = ""
			logger.Info("Resolved seedBackupRef to PVC per-shard URIs",
				"seedBackupRef", shardedDatabase.Spec.SeedBackupRef, "shardCount", len(resolved.PerShardURIs))
		}

		// Phase 2b: the server pods fetch a cloud seed themselves.
		if res, wait := r.ensureClusterSeedConfig(ctx, &shardedDatabase, cluster, resolved); wait {
			r.clusterAnswered(ctx, &shardedDatabase)
			return res, nil
		}
	}

	// Create or update sharded database
	destructive, err := r.reconcileShardedDatabase(ctx, &shardedDatabase, neo4jClient)
	if err != nil {
		if targetAway(err) {
			// The cluster went away mid-pass (knowledge rule 183).
			return r.clusterUnreachable(ctx, &shardedDatabase, err)
		}
		logger.Error(err, "Failed to reconcile sharded database")
		failed, statusErr := r.transitionStatus(ctx, &shardedDatabase, "Failed", fmt.Sprintf("Reconcile failed: %v", err), nil)
		if statusErr != nil {
			logger.Error(statusErr, "Failed to update status after reconcile failure")
		}
		// Retried with backoff; the same failure is announced once (rule 145).
		if failed {
			r.Recorder.Event(&shardedDatabase, corev1.EventTypeWarning, EventReasonReconcileFailed, err.Error())
		}
		return ctrl.Result{}, err
	}

	// Update status to Ready if everything succeeded. When the destructive
	// path fired AND succeeded, also stamp Status.LastDestructiveRestoreGeneration
	// so the next reconcile at the same generation skips the destructive
	// branch (otherwise the controller would re-drop on every poll cycle
	// since IF EXISTS makes the drop idempotent).
	ready := true
	// Only a cluster that served the pass serves this database: written once
	// its statements succeeded, not on the connection alone — a cluster that
	// lost its majority accepts the connection and refuses the statements,
	// and writing it after the connection restarted the outage's clock on
	// every pass (knowledge rules 172, 210).
	r.clusterAnswered(ctx, &shardedDatabase)
	// Seeding is complete once the sharded DB is operational — tear down the
	// PVC seed proxy stack (if any) so the backup PVC stops being served
	// cluster-wide (#219). Idempotent NotFound no-op after the first pass.
	if err := r.teardownPVCSeedProxy(ctx, &shardedDatabase); err != nil {
		logger.Error(err, "Failed to tear down seed proxy after sharded DB became Ready (non-fatal)")
	}
	becameReady, err := r.transitionStatus(ctx, &shardedDatabase, "Ready", "Sharded database is operational", &ready)
	if err != nil {
		logger.Error(err, "Failed to update status to Ready")
		return ctrl.Result{}, err
	}
	if destructive {
		if err := r.recordDestructiveRestoreGeneration(ctx, &shardedDatabase); err != nil {
			// Non-fatal: re-running drop+create at the same generation is
			// safe-but-wasteful; the user can still recover by editing the
			// CR. Log and continue.
			logger.Error(err, "Failed to record LastDestructiveRestoreGeneration; reconciler may re-drop on next poll")
		}
	}

	// Announced when the database becomes Ready, or is Ready again after a
	// destructive recreate — not on every pass, which spent the object's
	// event budget on a steady state.
	if becameReady || destructive {
		r.Recorder.Event(&shardedDatabase, corev1.EventTypeNormal, EventReasonShardedDatabaseReady, "Sharded database is ready and operational")
	}
	logger.Info("Successfully reconciled Neo4jShardedDatabase")

	if shardFamilySettling(&shardedDatabase.Status, time.Now()) {
		return ctrl.Result{RequeueAfter: shardSettleRequeue}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil // Periodic reconciliation
}

// validateShardedDatabase performs comprehensive validation of the sharded database spec
func (r *Neo4jShardedDatabaseReconciler) validateShardedDatabase(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) error {
	if r.ShardedDatabaseValidator != nil {
		return r.ShardedDatabaseValidator.ValidateShardedDatabase(ctx, shardedDB)
	}

	// Basic validation if validator not available
	if shardedDB.Spec.ClusterRef == "" {
		return fmt.Errorf("clusterRef is required")
	}
	if shardedDB.Spec.Name == "" {
		return fmt.Errorf("database name is required")
	}
	if shardedDB.Spec.PropertySharding.PropertyShards < 1 {
		return fmt.Errorf("propertyShards must be at least 1")
	}
	if shardedDB.Spec.DefaultCypherLanguage != "25" {
		return fmt.Errorf("defaultCypherLanguage must be '25' for property sharding")
	}
	if shardedDB.Spec.PropertySharding.PropertyShardTopology.Replicas < 1 {
		return fmt.Errorf("propertyShardTopology.replicas must be at least 1")
	}
	if shardedDB.Spec.SeedURI != "" && len(shardedDB.Spec.SeedURIs) > 0 {
		return fmt.Errorf("seedURI and seedURIs cannot be specified together")
	}
	if shardedDB.Spec.SeedSourceDatabase != "" && shardedDB.Spec.SeedURI == "" && len(shardedDB.Spec.SeedURIs) == 0 {
		return fmt.Errorf("seedSourceDatabase requires seedURI or seedURIs")
	}
	if shardedDB.Spec.SeedCredentials != nil && shardedDB.Spec.SeedURI == "" && len(shardedDB.Spec.SeedURIs) == 0 {
		return fmt.Errorf("seedCredentials requires seedURI or seedURIs")
	}

	return nil
}

// getReferencedCluster retrieves the Neo4jEnterpriseCluster referenced by the sharded database
func (r *Neo4jShardedDatabaseReconciler) getReferencedCluster(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) (*neo4jv1beta1.Neo4jEnterpriseCluster, error) {
	var cluster neo4jv1beta1.Neo4jEnterpriseCluster
	clusterKey := types.NamespacedName{
		Name:      shardedDB.Spec.ClusterRef,
		Namespace: shardedDB.Namespace,
	}

	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		return nil, fmt.Errorf("failed to get cluster %s: %w", shardedDB.Spec.ClusterRef, err)
	}

	return &cluster, nil
}

// clusterSupportsPropertySharding verifies that the cluster supports property sharding
func (r *Neo4jShardedDatabaseReconciler) clusterSupportsPropertySharding(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	// Check if property sharding is enabled on the cluster
	if cluster.Spec.PropertySharding == nil || !cluster.Spec.PropertySharding.Enabled {
		return false
	}

	// Check if cluster is ready (or Degraded but serving, #444) and property
	// sharding is operational
	if !clusterAcceptsWork(cluster) {
		return false
	}

	if cluster.Status.PropertyShardingReady == nil || !*cluster.Status.PropertyShardingReady {
		return false
	}

	return true
}

// createNeo4jClient creates a Neo4j client for the specified cluster
func (r *Neo4jShardedDatabaseReconciler) createNeo4jClient(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
	if r.newNeo4jClient != nil {
		return r.newNeo4jClient(cluster)
	}
	// Use the same pattern as the database controller
	return neo4j.NewClientForEnterprise(cluster, r.Client, getClusterAdminSecretName(cluster))
}

// reconcileShardedDatabase handles the creation and management of sharded
// databases. Returns destructive=true when the Phase 2c
// replaceExisting+force path fired at the current spec generation; the
// caller is expected to record that generation in
// Status.LastDestructiveRestoreGeneration so future reconciles at the
// same generation skip the destructive branch.
func (r *Neo4jShardedDatabaseReconciler) reconcileShardedDatabase(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, client shardedNeo4jClient) (destructive bool, err error) {
	logger := log.FromContext(ctx)

	// Wait for Neo4j to be ready for database operations. The SHOW DATABASES
	// that proves it also says whether the family exists already.
	databases, err := r.waitForNeo4jReadiness(ctx, client, readinessAttemptsFor(shardedDB))
	if err != nil {
		return false, fmt.Errorf("Neo4j not ready for database operations: %w", err)
	}

	// Phase 2c: destructive drop-and-recreate path. Validator already
	// ensured replaceExisting+force pair correctly and that a seed source
	// is present. Generation guard: only run DROP if the current spec
	// generation hasn't already been destructively restored — without this
	// the controller would re-drop on every reconcile (and re-seed from
	// the backup), since IF EXISTS keeps the DROP idempotent. The guard
	// records Generation on Status.LastDestructiveRestoreGeneration once
	// the drop+create cycle succeeds; any subsequent reconcile at the same
	// generation skips the destructive branch and falls through to the
	// standard create path.
	destructive = destructiveRestorePending(shardedDB)
	if destructive {
		if dropErr := r.dropShardedDatabaseIfExists(ctx, shardedDB, client); dropErr != nil {
			return false, fmt.Errorf("failed to DROP DATABASE %q for replaceExisting: %w", shardedDB.Spec.Name, dropErr)
		}
		logger.Info("Dropped existing sharded database for replaceExisting", "database", shardedDB.Spec.Name)
		r.Recorder.Event(shardedDB, corev1.EventTypeNormal, EventReasonShardedDatabaseDropped,
			fmt.Sprintf("Dropped existing sharded database %q before recreating from seed", shardedDB.Spec.Name))
	}

	if !shardCreateNeeded(shardedDB, destructive, databases) {
		logger.V(1).Info("Sharded database already exists; not creating it again", "database", shardedDB.Spec.Name)
	} else {
		// Create the sharded database using Cypher 25 syntax in a single command
		if (shardedDB.Spec.SeedURI != "" || len(shardedDB.Spec.SeedURIs) > 0) && shardedDB.Spec.SeedCredentials != nil {
			if credErr := client.PrepareCloudCredentialsForShardedDatabase(ctx, r.Client, shardedDB); credErr != nil {
				return destructive, fmt.Errorf("failed to prepare cloud credentials: %w", credErr)
			}
		}

		if createErr := r.createShardedDatabase(ctx, shardedDB, client); createErr != nil {
			return destructive, fmt.Errorf("failed to create sharded database: %w", createErr)
		}
	}

	// Update shard status
	if statusErr := r.updateShardStatus(ctx, shardedDB, client, destructive); statusErr != nil {
		// Non-fatal error, continue. A cluster that cannot serve for now is
		// not an ERROR (knowledge rule 210).
		if targetAway(statusErr) {
			logger.Info("Could not read the shard status; continuing", "error", statusErr.Error())
		} else {
			logger.Error(statusErr, "Failed to update shard status, continuing")
		}
	}

	return destructive, nil
}

// shardCreateNeeded decides whether this pass sends CREATE DATABASE, from the
// readiness SHOW DATABASES listing and the CR's persisted status.
//
// CREATE used to run on every pass. With IF NOT EXISTS that is a no-op on an
// existing name, but a DDL statement against the system database on every
// pass for the life of the CR. Without IF NOT EXISTS, which replaceExisting
// requires, Neo4j refuses CREATE on an existing name, so every pass after a
// destructive restore failed with "already exists" and turned the CR Failed.
//
// So CREATE is sent when:
//   - this is the destructive pass, which has just dropped the family (the
//     listing predates the drop);
//   - the family is not listed;
//   - the family is listed, ifNotExists is false, and this CR did not create
//     it. ifNotExists: false means "create, do not adopt", and Neo4j's
//     refusal is the answer it asks for. The validator does not tie it to
//     replaceExisting, so it can meet a family made outside the operator.
//
// "This CR created it" is status.virtualDatabase naming spec.name: the
// reconcile writes it from SHOW DATABASES only after its own CREATE succeeded
// (or IF NOT EXISTS adopted the family), and for spec.name (compared ignoring
// case, as Neo4j compares database names). Status
// survives an operator restart. creationTime and shardingReady are not tied
// to a name, and spec.name can change.
func shardCreateNeeded(sd *neo4jv1beta1.Neo4jShardedDatabase, destructive bool, databases []neo4j.DatabaseInfo) bool {
	if destructive || !shardFamilyExists(sd.Spec.Name, databases) {
		return true
	}
	if sd.Spec.IfNotExistsEffective() {
		return false
	}
	// Case-insensitive, as Neo4j compares database names (the validator
	// admits capitals in spec.name; SHOW DATABASES lists them lower-cased).
	recorded := sd.Status.VirtualDatabase != nil && strings.EqualFold(sd.Status.VirtualDatabase.Name, sd.Spec.Name)
	return !recorded
}

// dropShardedDatabaseIfExists runs `CYPHER 25 DROP DATABASE name IF EXISTS
// DESTROY DATA WAIT` against the system database, idempotently. Used by the
// Phase 2c replaceExisting+force path before re-creating from seed.
//
// IF EXISTS makes the call a no-op when the logical sharded database isn't
// present, so the helper is safe to invoke on every reconcile while the
// caller still has replaceExisting=true. The WAIT clause blocks until the
// drop completes (including all per-shard databases the sharded DB owns)
// so the subsequent CREATE doesn't race against in-flight cleanup.
//
// CYPHER 25 prefix matches createShardedDatabase's invocation — the rest
// of the sharded DB Cypher path is Cypher 25 so the prefix stays
// consistent (CLAUDE.md rule 30 territory).
func (r *Neo4jShardedDatabaseReconciler) dropShardedDatabaseIfExists(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, client shardedNeo4jClient) error {
	query := neo4j.Cypher25(fmt.Sprintf("DROP DATABASE `%s` IF EXISTS DESTROY DATA WAIT", neo4j.EscapeBackticks(shardedDB.Spec.Name)))
	logger := log.FromContext(ctx).WithValues("database", shardedDB.Spec.Name, "query", query)
	logger.Info("Executing destructive DROP DATABASE for replaceExisting")
	return client.ExecuteCypher(ctx, "system", query)
}

// createShardedDatabase creates the sharded database using Cypher 25 syntax
// buildCreateShardedDatabaseCypher renders the sharded CREATE DATABASE and its
// parameters. Separate from createShardedDatabase so the statement can be
// tested without a server.
func buildCreateShardedDatabaseCypher(shardedDB *neo4jv1beta1.Neo4jShardedDatabase) (string, map[string]any, error) {
	var query strings.Builder

	// CREATE DATABASE; the Cypher 25 directive is applied once the statement is built
	fmt.Fprintf(&query, "CREATE DATABASE `%s`", neo4j.EscapeBackticks(shardedDB.Spec.Name))

	// Add IF NOT EXISTS if specified
	if shardedDB.Spec.IfNotExistsEffective() {
		query.WriteString(" IF NOT EXISTS")
	}

	// Always set the family's language. Shard sub-databases inherit the
	// PARENT's default language, not the server's (measured on 2026.06.0), and
	// the server no longer forces CYPHER_25 for sharding (design §5.9) — so
	// without this clause a sharded family would take whatever
	// serverDefaultCypherLanguage says. The CRD admits only "25".
	lang := shardedDB.Spec.DefaultCypherLanguage
	if lang == "" {
		lang = "25"
	}
	fmt.Fprintf(&query, " SET DEFAULT LANGUAGE CYPHER %s", lang)

	// Add graph shard topology
	graphShard := shardedDB.Spec.PropertySharding.GraphShard
	fmt.Fprintf(&query, " SET GRAPH SHARD { TOPOLOGY %d", graphShard.Primaries)
	if graphShard.Primaries == 1 {
		query.WriteString(" PRIMARY")
	} else {
		query.WriteString(" PRIMARIES")
	}

	if graphShard.Secondaries > 0 {
		fmt.Fprintf(&query, " %d", graphShard.Secondaries)
		if graphShard.Secondaries == 1 {
			query.WriteString(" SECONDARY")
		} else {
			query.WriteString(" SECONDARIES")
		}
	}
	query.WriteString(" }")

	// Add property shards configuration
	propertyShards := shardedDB.Spec.PropertySharding.PropertyShards
	propertyTopology := shardedDB.Spec.PropertySharding.PropertyShardTopology

	fmt.Fprintf(&query, " SET PROPERTY SHARDS { COUNT %d", propertyShards)

	// Property shards use replicas, not primaries/secondaries
	replicas := propertyTopology.Replicas
	if replicas > 0 {
		fmt.Fprintf(&query, " TOPOLOGY %d", replicas)
		if replicas == 1 {
			query.WriteString(" REPLICA")
		} else {
			query.WriteString(" REPLICAS")
		}
	}
	query.WriteString(" }")

	// Add supported options for sharded databases. Values are bound as driver
	// parameters (params) so seed URIs / config can't inject Cypher (#170).
	options, params, err := buildShardedDatabaseOptions(shardedDB)
	if err != nil {
		return "", nil, err
	}
	if options != "" {
		query.WriteString(" OPTIONS { ")
		query.WriteString(options)
		query.WriteString(" }")
	}

	// Add WAIT if specified
	if shardedDB.Spec.Wait {
		query.WriteString(" WAIT")
	}

	// Cypher 25 syntax end to end (SET GRAPH SHARD, SET PROPERTY SHARDS).
	queryStr := neo4j.Cypher25(query.String())
	return queryStr, params, nil
}

func (r *Neo4jShardedDatabaseReconciler) createShardedDatabase(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, client shardedNeo4jClient) error {
	logger := log.FromContext(ctx).WithValues("database", shardedDB.Spec.Name)

	// Build the Cypher 25 CREATE DATABASE command for property sharding
	// Format: CREATE DATABASE name [IF NOT EXISTS]
	//         SET DEFAULT LANGUAGE CYPHER 25
	//         SET GRAPH SHARD { TOPOLOGY n PRIMARIES m SECONDARIES }
	//         SET PROPERTY SHARDS { COUNT n TOPOLOGY m REPLICAS }
	//         OPTIONS { seedURI: ..., seedConfig: ..., seedSourceDatabase: ..., seedRestoreUntil: ..., txLogEnrichment: ... }

	queryStr, params, err := buildCreateShardedDatabaseCypher(shardedDB)
	if err != nil {
		return err
	}
	logger.Info("Executing sharded database creation", "query", queryStr)

	// Execute the command with retry logic
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Execute against system database for DDL commands (params bind the
		// user-supplied seed values — see buildShardedDatabaseOptions).
		err := client.ExecuteCypherWithParams(ctx, "system", queryStr, params)
		if err == nil {
			logger.Info("Successfully created sharded database")
			return nil
		}

		// Check if database already exists (not an error if IfNotExists is true)
		if shardedDB.Spec.IfNotExistsEffective() && strings.Contains(err.Error(), "already exists") {
			logger.Info("Database already exists, continuing")
			return nil
		}

		// Check for transient errors
		if attempt < maxRetries-1 && isTransientError(err) {
			delay := time.Duration(attempt+1) * time.Second
			logger.V(1).Info("Transient error, retrying", "error", err, "attempt", attempt+1, "delay", delay)
			time.Sleep(delay)
			continue
		}

		return fmt.Errorf("failed to create sharded database: %w", err)
	}

	return fmt.Errorf("failed to create sharded database after %d attempts", maxRetries)
}

// buildShardedDatabaseOptions builds the inner OPTIONS entries for a sharded
// CREATE DATABASE plus the driver parameters they reference. Every user-supplied
// value (seedURI, per-shard seedURIs, seedSourceDatabase, seedConfig,
// seedRestoreUntil, txLogEnrichment) is bound as a parameter so it cannot inject
// Cypher (issue #170). Only the per-shard seedURI map KEYS are interpolated —
// backtick-escaped, and constrained by the validator. seedConfig is the
// documented comma-separated string and seedRestoreUntil follows the documented
// integer/`datetime()` forms, matching the standard seed path.
func buildShardedDatabaseOptions(shardedDB *neo4jv1beta1.Neo4jShardedDatabase) (string, map[string]any, error) {
	var options []string
	params := map[string]any{}

	if shardedDB.Spec.SeedURI != "" && len(shardedDB.Spec.SeedURIs) > 0 {
		return "", nil, fmt.Errorf("seedURI and seedURIs cannot be specified together")
	}

	if shardedDB.Spec.SeedURI != "" {
		options = append(options, "seedURI: $seed_uri")
		params["seed_uri"] = shardedDB.Spec.SeedURI
	}

	if len(shardedDB.Spec.SeedURIs) > 0 {
		keys := make([]string, 0, len(shardedDB.Spec.SeedURIs))
		for key := range shardedDB.Spec.SeedURIs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		entries := make([]string, 0, len(keys))
		for i, key := range keys {
			p := fmt.Sprintf("seed_uri_%d", i)
			// A key is a shard name, which Neo4j matches exactly against
			// the lower-cased names it derives (shardSeedKey): a spec's
			// `Products-g000` was refused as missing.
			entries = append(entries, fmt.Sprintf("`%s`: $%s", neo4j.EscapeBackticks(strings.ToLower(key)), p))
			params[p] = shardedDB.Spec.SeedURIs[key]
		}
		options = append(options, "seedURI: { "+strings.Join(entries, ", ")+" }")
	}

	if shardedDB.Spec.SeedSourceDatabase != "" {
		options = append(options, "seedSourceDatabase: $seed_source_database")
		params["seed_source_database"] = shardedDB.Spec.SeedSourceDatabase
	}

	if shardedDB.Spec.SeedConfig != nil {
		if restoreUntil := shardedDB.Spec.SeedConfig.RestoreUntil; restoreUntil != "" {
			if txid, ok := strings.CutPrefix(restoreUntil, "txId:"); ok {
				// The sharded validator (isValidRestoreUntilTxID) rejects a txId
				// that doesn't fit int64, so ParseInt succeeds for any spec that
				// reached here; the err==nil guard never silently drops it.
				if n, err := strconv.ParseInt(txid, 10, 64); err == nil {
					options = append(options, "seedRestoreUntil: $seed_restore_until")
					params["seed_restore_until"] = n
				}
			} else {
				options = append(options, "seedRestoreUntil: datetime($seed_restore_until)")
				params["seed_restore_until"] = restoreUntil
			}
		}

		if cfg := neo4j.SerializeSeedConfig(shardedDB.Spec.SeedConfig.Config); cfg != "" {
			options = append(options, "seedConfig: $seed_config")
			params["seed_config"] = cfg
		}
	}

	if shardedDB.Spec.TxLogEnrichment != "" {
		options = append(options, "txLogEnrichment: $tx_log_enrichment")
		params["tx_log_enrichment"] = shardedDB.Spec.TxLogEnrichment
	}

	return strings.Join(options, ", "), params, nil
}

// isTransientError checks if an error is transient and can be retried
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "unavailable") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "transient") ||
		strings.Contains(errMsg, "connection")
}

// updateShardStatus records the shard-level state of the sharded database
// (status.graphShard, propertyShards, virtualDatabase and creationTime) from
// one SHOW DATABASES pass. recreated is true when this reconcile dropped and
// recreated the database (replaceExisting + force).
//
// Best-effort: the caller logs and continues on error, because shard state is
// informational. A shard that is missing or not online yet is reported as
// observed, never turned into a failure here -- that can be a transient state
// during startup. The write is skipped when nothing changed (see
// applyShardStatus).
func (r *Neo4jShardedDatabaseReconciler) updateShardStatus(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, client shardedNeo4jClient, recreated bool) error {
	logger := log.FromContext(ctx).WithValues("database", shardedDB.Spec.Name)

	databases, err := client.GetDatabases(ctx)
	if err != nil {
		logger.Error(err, "Failed to get database information")
		return fmt.Errorf("failed to get database information: %w", err)
	}

	obs := observeShardFamily(shardedDB.Spec.Name, databases)
	switch {
	case obs.GraphShard == nil:
		logger.Info("Graph shard not found yet", "expectedName", shardedDB.Spec.Name+"-g000")
	case !obs.GraphShard.Ready:
		logger.Info("Graph shard exists but is not ready yet", "database", obs.GraphShard.Name, "state", obs.GraphShard.State)
	default:
		logger.V(1).Info("Graph shard is ready", "database", obs.GraphShard.Name)
	}

	if err := r.applyShardStatus(ctx, shardedDB, obs, recreated); err != nil {
		return fmt.Errorf("failed to record shard status: %w", err)
	}
	mirrorShardObservation(shardedDB, obs, recreated, time.Now())
	return nil
}

// updateStatus updates the sharded database status
// reader is where a pass reads its sharded database: the API server when an
// APIReader is wired, else the cache.
func (r *Neo4jShardedDatabaseReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *Neo4jShardedDatabaseReconciler) updateStatus(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, phase, message string, shardingReady *bool) error {
	_, err := r.transitionStatus(ctx, shardedDB, phase, message, shardingReady)
	return err
}

// transitionStatus is updateStatus that also reports whether the write
// changed the persisted phase or message — a state transition — so a caller
// can emit the matching event once, on the transition, not on every pass. The
// answer comes from the attempt whose write succeeded, which optimistic
// concurrency guarantees read the current object.
func (r *Neo4jShardedDatabaseReconciler) transitionStatus(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, phase, message string, shardingReady *bool) (bool, error) {
	transitioned := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		transitioned = false
		// Fetch latest version
		var latest neo4jv1beta1.Neo4jShardedDatabase
		if err := r.Get(ctx, client.ObjectKeyFromObject(shardedDB), &latest); err != nil {
			return err
		}
		changed := latest.Status.Phase != phase || latest.Status.Message != message

		// Update status
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation

		if shardingReady != nil {
			latest.Status.ShardingReady = shardingReady
		}

		// Update Ready condition using standard helper
		condStatus, condReason := PhaseToConditionStatus(phase)
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, condStatus, condReason, message)

		if err := r.Status().Update(ctx, &latest); err != nil {
			return err
		}
		transitioned = changed
		// The pass goes on from its own copy, so it gets the status it just
		// wrote: from the copy it read, the first pass of a new CR still had
		// no phase after writing "Validating", and never wrote "Creating"
		// (#549).
		latest.Status.DeepCopyInto(&shardedDB.Status)
		return nil
	})
	return transitioned && err == nil, err
}

// waitForCluster records that the cluster cannot serve the sharded database —
// reason ClusterNotReady (not Ready, or property sharding not operational) or
// ClusterUnreachable (reads Ready, does not answer): the ClusterNotReady
// condition, and phase Waiting unless the database is already Ready at its
// generation, which it stays. The shards live on the cluster and do not change
// while it rolls or restarts, and the cluster announces its own state; writing
// Waiting over Ready made the pass that found the cluster back a transition
// into Ready, announced as ShardedDatabaseReady on every rollout of the cluster
// (#565; knowledge rule 183). The status is written only when it changes, and
// the pass's copy takes it. It reports lasting when an unreachable cluster has
// outlasted targetUnreachableGrace.
func (r *Neo4jShardedDatabaseReconciler) waitForCluster(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, reason, msg string) (lasting bool) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		lasting = false
		var latest neo4jv1beta1.Neo4jShardedDatabase
		if err := r.Get(ctx, client.ObjectKeyFromObject(shardedDB), &latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		var keepsReady bool
		lasting, keepsReady = noteTargetWait(&latest.Status.Conditions, latest.Status.Phase, latest.Generation,
			neo4jv1beta1.PhaseReady, reason, msg, time.Now())
		if !lasting && !keepsReady {
			latest.Status.Phase = neo4jv1beta1.PhaseWaiting
			latest.Status.Message = msg
			condStatus, condReason := PhaseToConditionStatus(neo4jv1beta1.PhaseWaiting)
			SetReadyCondition(&latest.Status.Conditions, latest.Generation, condStatus, condReason, msg)
		}
		latest.Status.ObservedGeneration = latest.Generation
		if !apiequality.Semantic.DeepEqual(*before, latest.Status) {
			if err := r.Status().Update(ctx, &latest); err != nil {
				return err
			}
		}
		latest.Status.DeepCopyInto(&shardedDB.Status)
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to record the cluster wait on Neo4jShardedDatabase")
		return false
	}
	return lasting
}

// clusterUnreachable handles a cluster that reads Ready but cannot be reached,
// or cannot serve the statement for now (targetAway: it lost its majority;
// knowledge rule 210):
// waitForCluster with reason ClusterUnreachable, logged at info and retried on
// the fixed interval, not as an error. Only an outage that outlasts
// targetUnreachableGrace is this database's failure: Failed,
// ClientCreationFailed, announced once (knowledge rules 172 and 183).
func (r *Neo4jShardedDatabaseReconciler) clusterUnreachable(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, err error) (ctrl.Result, error) {
	if r.waitForCluster(ctx, shardedDB, ConditionReasonClusterUnreachable, targetAwayMessage(shardedDB.Spec.ClusterRef, err)) {
		msg := fmt.Sprintf("Failed to connect to Neo4j for more than %s: %v", targetUnreachableGrace, err)
		failed, statusErr := r.transitionStatus(ctx, shardedDB, neo4jv1beta1.PhaseFailed, msg, nil)
		if statusErr != nil {
			log.FromContext(ctx).Error(statusErr, "Failed to update status after a lasting outage")
		}
		if failed {
			r.Recorder.Event(shardedDB, corev1.EventTypeWarning, EventReasonClientCreationFailed, msg)
		}
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}
	log.FromContext(ctx).Info("Cluster reads Ready but cannot be reached; retrying",
		"clusterRef", shardedDB.Spec.ClusterRef, "error", err.Error())
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

// clusterAnswered writes ClusterNotReady=False once the cluster served the
// pass — its statements answered, not only the connection, since a cluster
// that lost its majority takes the connection and refuses the statements
// (knowledge rule 210) — when that changes the condition; the pass's copy
// takes it.
func (r *Neo4jShardedDatabaseReconciler) clusterAnswered(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest neo4jv1beta1.Neo4jShardedDatabase
		if err := r.Get(ctx, client.ObjectKeyFromObject(shardedDB), &latest); err != nil {
			return err
		}
		if !targetAnswered(&latest.Status.Conditions, latest.Generation) {
			return nil
		}
		if err := r.Status().Update(ctx, &latest); err != nil {
			return err
		}
		latest.Status.DeepCopyInto(&shardedDB.Status)
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to record that the cluster answered on Neo4jShardedDatabase")
	}
}

// recordDestructiveRestoreGeneration stamps Status.LastDestructiveRestoreGeneration
// with the current spec generation after a Phase 2c destructive restore
// completes successfully. Used as a guard against re-triggering the
// drop-and-recreate cycle on every reconcile while
// spec.replaceExisting=true (which is the steady state after a destructive
// restore — users typically leave the field set rather than racing to
// unset it after Ready). Done as a separate Status.Update from the Ready
// transition so a single retry-on-conflict cycle covers it; failure here
// is non-fatal because a re-trigger is just a wasteful drop+create at
// the same generation, not data loss.
func (r *Neo4jShardedDatabaseReconciler) recordDestructiveRestoreGeneration(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jShardedDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(shardedDB), latest); err != nil {
			return err
		}
		latest.Status.LastDestructiveRestoreGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

// readinessAttempts is how many times, a second apart, waitForNeo4jReadiness
// asks a cluster that has just formed for its databases.
const readinessAttempts = 30

// readinessAttemptsFor is how many times the pass asks. The in-pass retries
// are for a cluster that has just formed. A database Ready at its generation,
// or one already waiting out its cluster (ClusterNotReady=True), asks once: a
// cluster that cannot serve it for now — one that lost its majority still
// reads Ready — is waited out by requeue, not by holding the worker thirty
// seconds a pass (knowledge rule 210).
func readinessAttemptsFor(shardedDB *neo4jv1beta1.Neo4jShardedDatabase) int {
	if readyAtGeneration(neo4jv1beta1.PhaseReady, shardedDB.Status.Phase, shardedDB.Status.Conditions, shardedDB.Generation) {
		return 1
	}
	if c := findCondition(shardedDB.Status.Conditions, ConditionTypeClusterNotReady); c != nil && c.Status == metav1.ConditionTrue {
		return 1
	}
	return readinessAttempts
}

// waitForNeo4jReadiness waits for Neo4j to be ready for database operations,
// asking at most maxRetries times a second apart, and returns the SHOW
// DATABASES listing that proved it.
func (r *Neo4jShardedDatabaseReconciler) waitForNeo4jReadiness(ctx context.Context, client shardedNeo4jClient, maxRetries int) ([]neo4j.DatabaseInfo, error) {
	logger := log.FromContext(ctx)

	// Check if we can execute basic queries (system database readiness)
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		logger.V(1).Info("Checking Neo4j readiness", "attempt", i+1, "maxRetries", maxRetries)

		// Try to get databases (this will fail if system database is not ready)
		databases, err := client.GetDatabases(ctx)
		if err == nil {
			logger.Info("Neo4j is ready for database operations")
			return databases, nil
		}
		lastErr = err

		// Check if this is a transient error we should retry (simple string check)
		errMsg := strings.ToLower(err.Error())
		isTransient := strings.Contains(errMsg, "unavailable") ||
			strings.Contains(errMsg, "timeout") ||
			strings.Contains(errMsg, "transient") ||
			strings.Contains(errMsg, "connection") ||
			strings.Contains(errMsg, "routing")

		if isTransient {
			if i == maxRetries-1 {
				break // the last attempt: nothing to wait for
			}
			logger.V(1).Info("Neo4j not ready yet, retrying", "error", err.Error())
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		} else {
			// Non-transient error, fail immediately
			return nil, fmt.Errorf("Neo4j readiness check failed with non-transient error: %w", err)
		}
	}

	// The last error is wrapped, so the caller can tell a cluster that cannot
	// serve for now (targetAway: it lost its majority) from a failure
	// (knowledge rule 210).
	return nil, fmt.Errorf("timeout waiting for Neo4j to become ready for database operations after %d attempts: %w", maxRetries, lastErr)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Neo4jShardedDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrentReconciles := r.MaxConcurrentReconciles
	if maxConcurrentReconciles <= 0 {
		maxConcurrentReconciles = 1
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jShardedDatabase{}).
		// The other CRs of a name, when one comes, goes or is renamed
		// (operations.md id 217).
		Watches(&neo4jv1beta1.Neo4jShardedDatabase{}, shardedDatabaseNames.othersOfTheName(mgr.GetClient()),
			shardedDatabaseNames.onClaimChange()).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		Complete(r)
}

// shardedSeedConsumable reports whether the sharded database's seed can still be
// CONSUMED, i.e. whether the reconcile should resolve it at all: during initial
// creation (not ShardingReady yet) or while a destructive replaceExisting
// re-trigger is pending (rule 64). Once the database is Ready, resolving again
// would re-create the PVC seed proxy that the Ready transition just tore down,
// oscillating Ready→Pending and re-exposing the backup PVC on every periodic
// reconcile (#224 review). It is a function of its own so a test can pin the
// real predicate rather than a copy of it.
func shardedSeedConsumable(sd *neo4jv1beta1.Neo4jShardedDatabase) bool {
	return sd.Status.ShardingReady == nil || !*sd.Status.ShardingReady ||
		(sd.Spec.ReplaceExisting && sd.Status.LastDestructiveRestoreGeneration < sd.Generation)
}

// destructiveRestorePending reports whether the drop-and-recreate (replaceExisting
// + force) path should run for this generation of the spec. It is the gate of
// knowledge rule 64: the controller stamps Status.LastDestructiveRestoreGeneration
// once the cycle succeeds, so a later reconcile at the same generation falls
// through to the standard create path instead of re-dropping the database and
// re-seeding it on every poll. Re-triggering means changing the spec (which bumps
// the generation). A function of its own so a test pins the real predicate.
func destructiveRestorePending(sd *neo4jv1beta1.Neo4jShardedDatabase) bool {
	return sd.Spec.ReplaceExisting && sd.Spec.Force &&
		sd.Status.LastDestructiveRestoreGeneration < sd.Generation
}
