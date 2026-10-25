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

package controller

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"golang.org/x/time/rate"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/metrics"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jEnterpriseClusterReconciler reconciles a Neo4jEnterpriseCluster object
type Neo4jEnterpriseClusterReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	Recorder           record.EventRecorder
	RequeueAfter       time.Duration
	TopologyScheduler  *TopologyScheduler
	Validator          *validation.ClusterValidator
	ConfigMapManager   *ConfigMapManager
	SplitBrainDetector *SplitBrainDetector
	// ServerUnavailableGrace is how long a formed cluster may run short of a
	// server before its phase turns Degraded (#444); zero uses
	// DefaultServerUnavailableGrace. Set from --server-unavailable-grace.
	ServerUnavailableGrace time.Duration
	// ReadyPollInterval is how long a Ready cluster waits for its next pass
	// when nothing in Kubernetes changes; zero uses RequeueAfter. Set from
	// --ready-poll-interval.
	ReadyPollInterval time.Duration
	// SplitBrainCheckInterval is how long a Ready cluster whose server pods
	// have not changed goes between full split-brain checks; zero checks on
	// every pass. Set from --split-brain-check-interval.
	SplitBrainCheckInterval time.Duration
	// MaxConcurrentReconciles is how many clusters are reconciled at once; a
	// cluster is never reconciled twice at once. Zero means one. Set from
	// --cluster-max-concurrent-reconciles.
	MaxConcurrentReconciles int
	// ReconcileTimeout bounds one reconcile of one cluster, so a pass stuck on
	// a server that stopped answering mid-query cannot hold a worker; zero
	// means no bound. Set from --cluster-reconcile-timeout.
	ReconcileTimeout time.Duration
	// splitBrainChecks records, per cluster (key ns/name), the last full
	// split-brain check that found the cluster whole (splitBrainCheck).
	splitBrainChecks sync.Map
	// RestartPendingChecker reads each server's settings for the RestartPending
	// condition (#468). nil uses Bolt; tests inject a fake.
	RestartPendingChecker *restartPendingChecker
	restartPendingOnce    sync.Once
	// TLSReloadDeps replaces the network in TLS-reload tests (#469); nil in
	// production.
	TLSReloadDeps *tlsReloadDeps
	// UpgradeGates replaces the network in rolling-upgrade tests (#488); nil
	// in production.
	UpgradeGates *upgradeGates
	// Ignore, when set, makes Reconcile leave alone every cluster it returns
	// true for. The envtest suite uses it for clusters that stand in for a
	// Ready cluster in backup and restore specs: envtest runs no pods, so this
	// controller would rightly take them out of Ready. nil in production.
	Ignore func(*neo4jv1beta1.Neo4jEnterpriseCluster) bool
	// fleetFailures dedupes the AuraFleetManagementFailed event so a
	// registration that keeps failing the same way is announced once, not on
	// every reconcile.
	fleetFailures fleetFailureTracker
	// fleetPluginPatchFailures remembers, per deployment, the last
	// AuraFleetManagementPluginPatchFailed it announced, so a patch that keeps
	// failing the same way is announced once (operations.md id 145).
	fleetPluginPatchFailures lastAnnounced
	// FleetClientFactory injects a fake Aura Fleet Manager client in tests. nil in
	// production, where the shared credential-keyed client is used.
	FleetClientFactory auraFleetClientFactory
	// deferredUpgradeTargets dedupes the UpgradeDeferred event per cluster
	// (key ns/name → heldImageChange) so a held image change doesn't emit an
	// event on every Forming-phase reconcile (#262 review), and is announced
	// only once it outlasts the pass that first held it
	// (announceHeldImageChange). The target a scale-down drain holds back is
	// under drainDeferralKey (#527; announceUpgradeDeferredByDrain).
	deferredUpgradeTargets sync.Map
	// connectivityFailures tracks consecutive can't-connect reconciles per
	// cluster (key ns/name → *connectivityFailureStreak) so a persistent
	// outage is distinguishable from a transient blip in logs and events
	// (#263 forensics). Cleared on the first successful connection.
	connectivityFailures sync.Map
	// upgradeDiagnostics records, per cluster (key ns/name), when diagnostics
	// were last collected during an upgrade (collectDiagnosticsDuringUpgrade).
	upgradeDiagnostics sync.Map
	// availabilityAnnounced records, per cluster (key ns/name), which
	// availability transition — Degraded or QuorumLost — has had its Warning
	// event, so a reconcile that reads the cluster from a cache that has not
	// seen the new phase yet does not announce it twice (#444). Cleared when
	// every server is available again.
	availabilityAnnounced sync.Map
	// serverStatefulSetWrites records, per cluster (key ns/name), the UID and
	// generation of the server StatefulSet as the last pass left it
	// (writtenGeneration), so a cached read that predates the write is a
	// rollout in flight, not the rolled-out template it replaced.
	serverStatefulSetWrites sync.Map
	// deletedServerStatefulSets keeps the server StatefulSet a storage
	// expansion orphan-deleted until the pass that creates it again, which
	// takes its template (statefulset_recreate.go, operations.md id 171).
	deletedServerStatefulSets deletedStatefulSets
	// APIReader reads what the cache does not hold (the ControllerRevision a
	// StatefulSet created again takes its template from after an operator
	// restart). nil uses Client.
	APIReader client.Reader

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings
}

// connectivityFailureStreak records when a cluster's Bolt connectivity first
// failed and how many consecutive reconciles have failed since. The Warning
// event fires exactly once per streak, at count == threshold.
type connectivityFailureStreak struct {
	since time.Time
	count int
	// announced records that this streak's ConnectivityDegraded event has
	// fired, so it fires once per streak however long the streak runs.
	announced bool
}

// dueForEvent reports, once per streak, that it has become persistent enough
// for a ConnectivityDegraded Warning: at least connectivityFailureEventThreshold
// consecutive failures AND connectivityFailureEventMinDuration since the first.
//
// Both, because the count alone does not measure time. During a new cluster's
// formation pod events drive reconciles far faster than the 30s requeue, so ten
// failures came inside the 91 seconds the cluster took to form, and every
// healthy new cluster carried a ConnectivityDegraded warning that `kubectl
// neo4j diagnose` reported long after (v1.18.0 journey).
func (s *connectivityFailureStreak) dueForEvent(now time.Time) bool {
	if s.announced || s.count < connectivityFailureEventThreshold ||
		now.Sub(s.since) < connectivityFailureEventMinDuration {
		return false
	}
	s.announced = true
	return true
}

const (
	// ClusterFinalizer is the finalizer for Neo4j enterprise clusters
	ClusterFinalizer = "neo4j.neo4j.com/cluster-finalizer"
	// DefaultAdminSecretName is the default name for admin credentials
	DefaultAdminSecretName = "neo4j-admin-secret"
)

//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraproviderconfigs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch;create;update;patch;delete
// Endpoints permission is CRITICAL for Neo4j Kubernetes discovery to resolve pod IPs behind services
// Roles and RoleBindings: only to delete the legacy {cluster}-discovery ones
// (deleteLegacyDiscoveryObjects); the operator creates none.
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cert-manager.io,resources=issuers,verbs=get;list;watch
//+kubebuilder:rbac:groups=cert-manager.io,resources=clusterissuers,verbs=get;list;watch
//+kubebuilder:rbac:groups=external-secrets.io,resources=externalsecrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=external-secrets.io,resources=secretstores,verbs=get;list;watch
//+kubebuilder:rbac:groups=external-secrets.io,resources=clustersecretstores,verbs=get;list;watch
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;prometheusrules,verbs=get;list;watch;create;update;patch;delete

// isStrictPeerValidationEnabled reports whether the cluster wants strict
// intra-cluster TLS peer validation. The CRD field defaults to true; an
// explicit false opts the cluster into the legacy debugging-only posture.
func isStrictPeerValidationEnabled(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	if cluster.Spec.TLS == nil || cluster.Spec.TLS.Mode != "cert-manager" {
		return false
	}
	if cluster.Spec.TLS.StrictPeerValidation == nil {
		return true
	}
	return *cluster.Spec.TLS.StrictPeerValidation
}

// errTLSSecretPending is the sentinel returned by verifyTLSSecretHasCA when
// the cert-manager Secret has not yet been issued. The reconciler treats
// this as "wait for cert-manager, don't proceed to STS emission" — surfacing
// a clear Initializing status rather than the misleading Failed status that
// would result from treating it as a hard preflight error.
//
// Used with errors.Is so callers can distinguish "still bootstrapping" from
// "issuer is permanently mis-configured."
var errTLSSecretPending = goerrors.New("cert-manager Secret has not yet been issued")

// verifyTLSSecretHasCA fetches the cert-manager-issued Secret and returns:
//   - errTLSSecretPending if the Secret does not yet exist (cert-manager
//     is still working; the reconcile must NOT proceed to emit the strict
//     STS template, because that template references a Secret with a
//     required ca.crt key that doesn't exist yet — the Pod would get
//     stuck in CreateContainerConfigError).
//   - A descriptive error if the Secret exists but ca.crt is missing or
//     empty (the issuer doesn't populate it; permanent until the user
//     either fixes the issuer or opts out of strict peer validation).
//   - nil if the Secret exists and has a non-empty ca.crt — safe to emit
//     strict-mode resources.
func (r *Neo4jEnterpriseClusterReconciler) verifyTLSSecretHasCA(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	secretName := fmt.Sprintf("%s-tls-secret", cluster.Name)
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, secret); err != nil {
		if errors.IsNotFound(err) {
			return errTLSSecretPending
		}
		return fmt.Errorf("strict peer validation preflight: failed to read Secret %s/%s: %w", cluster.Namespace, secretName, err)
	}
	if ca, ok := secret.Data["ca.crt"]; !ok || len(ca) == 0 {
		issuerName := ""
		if cluster.Spec.TLS != nil && cluster.Spec.TLS.IssuerRef != nil {
			issuerName = cluster.Spec.TLS.IssuerRef.Name
		}
		// Enumerate the keys that ARE present so debug output makes it
		// obvious whether the Secret is genuinely missing ca.crt (issuer
		// bug) or is in some other partial state we hadn't considered.
		// Previously the failure was opaque: "Failed != Ready" with no
		// hint as to which keys cert-manager populated.
		presentKeys := make([]string, 0, len(secret.Data))
		for k, v := range secret.Data {
			presentKeys = append(presentKeys, fmt.Sprintf("%s(%dB)", k, len(v))) //nolint:perfsprint // composite "key(NB)" debug string; Sprintf is clearer than strconv here
		}
		return fmt.Errorf("strict peer validation requires Secret %s/%s to expose a non-empty ca.crt key, but the cert-manager-issued Secret has keys=[%s]. The issuer %q likely does not populate ca.crt (some external issuers don't), or cert-manager has not finished issuing. Either fix the issuer to include the CA in its Secret output, or set spec.tls.strictPeerValidation=false on this cluster to opt into the legacy trust_all=true posture",
			cluster.Namespace, secretName, strings.Join(presentKeys, ","), issuerName)
	}
	return nil
}

// recordValidationWarnings turns the validator's advisory output into Warning
// events. Topology warnings keep their TopologyWarning reason; warnings about
// spec fields the schema accepts but nothing reads are ValidationWarning, the
// reason every other kind uses for the same thing. Neither blocks the reconcile,
// and each is announced when it starts to apply, not on every pass.
func (r *Neo4jEnterpriseClusterReconciler) recordValidationWarnings(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, result validation.ClusterValidationResult) {
	r.advisoryWarnings.record(r.Recorder, cluster, EventReasonTopologyWarning, result.Warnings)
	r.advisoryWarnings.record(r.Recorder, cluster, EventReasonValidationWarning, result.NoEffectWarnings)
}

// recordZoneDiscovery surfaces a degraded zone discovery so it's visible via
// `kubectl describe`, not just operator logs. Spread still applies via the zone
// label key; only AZ enumeration was skipped (#202). It stays true for as long
// as the install is namespace-scoped, so it is an advisory warning: announced
// when it starts to apply and refreshed every advisoryWarningRefresh, not on
// every pass (knowledge rules 102 and 145).
func (r *Neo4jEnterpriseClusterReconciler) recordZoneDiscovery(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, placement *TopologyPlacement) {
	var warnings []string
	if placement.ZoneDiscoveryDegraded {
		warnings = append(warnings,
			"Availability-zone auto-discovery unavailable (cannot list cluster-scoped nodes; expected in namespace-scoped installs). Applying best-effort zone spread via the topology key — set spec.topology.availabilityZones to enumerate zones explicitly.")
	}
	r.advisoryWarnings.record(r.Recorder, cluster, EventReasonTopologyZoneDiscoveryDegraded, warnings)
}

// clusterEverAccepted reports whether a reconcile of this cluster has ever got
// past spec validation. Reconcile adds the finalizer only once validation and
// the StorageClass check have passed, and it stays until deletion. No status
// field can say this: phase is "Failed" both for a cluster refused at creation
// and for one whose later edit was refused, and every status write stamps
// observedGeneration, a refusal's included.
func clusterEverAccepted(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	return controllerutil.ContainsFinalizer(cluster, ClusterFinalizer)
}

func (r *Neo4jEnterpriseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Neo4jEnterpriseCluster instance
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Neo4jEnterpriseCluster resource not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Neo4jEnterpriseCluster")
		return ctrl.Result{}, err
	}
	if r.Ignore != nil && r.Ignore(cluster) {
		return ctrl.Result{}, nil
	}

	// Handle deletion — before the reconcile metrics are set up: the pass
	// that releases the finalizer withdraws the cluster's series (Forget),
	// and recording this pass afterwards would export them again.
	if cluster.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, cluster)
	}

	reconcileStart := time.Now()
	reconcileM := metrics.NewReconcileMetrics(cluster.Name, cluster.Namespace)
	defer func() {
		success := cluster.Status.Phase == "Ready"
		reconcileM.RecordReconcile(ctx, "cluster", time.Since(reconcileStart), success)
	}()

	// Apply defaults and validate the cluster
	if r.Validator != nil {
		// The defaults go on this pass's copy of the cluster only. Nothing
		// writes this copy back: the operator patches what it changes on the
		// object and never Updates it, so the stored spec stays the one the
		// user applied (operations.md id 150).
		r.Validator.ApplyDefaults(ctx, cluster)

		// One set of rules on every pass. There is no webhook, so there is no
		// previous spec to compare an edit with — reading the object again
		// returns the same spec, which is what the update-only rules used to
		// compare with itself (operations.md id 139). The rules that depend on
		// what the cluster already runs read it from status (status.version).
		result := r.Validator.ValidateWithWarnings(ctx, cluster)

		// Emit warnings as events
		r.recordValidationWarnings(cluster, result)

		if len(result.Errors) > 0 {
			// The errors alone: the status and the event below say "validation
			// failed" themselves, and the prefix here doubled it.
			err := result.Errors.ToAggregate()
			// What a refusal is called follows whether any pass has accepted
			// the cluster: "Update validation failed" on a first apply sent
			// people looking for an edit they never made.
			event, status := "Cluster update validation failed", "Update validation failed"
			if !clusterEverAccepted(cluster) {
				event, status = "Cluster validation failed", "Validation failed"
			}
			// Announced — and logged at info — when the failure starts or its
			// message changes, not on every RequeueAfter while the spec stays
			// invalid (knowledge rule 136).
			announced := r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("%s: %v", status, err))
			logRefusal(logger, announced, event, err)
			if announced {
				r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonValidationFailed, "%s: %v", event, err)
			}
			// A spec the user must fix is not a reconcile error: it is
			// already logged, evented and in status, and returning it
			// would make controller-runtime ignore RequeueAfter and
			// retry on error backoff instead. The same holds for the
			// validation returns below.
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}

		// Validate server role hints for both create and update
		roleHintErrors := resources.ValidateServerRoleHints(cluster)
		if len(roleHintErrors) > 0 {
			// Announced when the failure starts or its message changes, not
			// on every RequeueAfter while it stays (knowledge rule 145).
			announced := r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Server role validation failed: %v", roleHintErrors))
			logRefusal(logger, announced, "Server role validation failed", fmt.Errorf("%s", strings.Join(roleHintErrors, "; ")))
			if announced {
				for _, roleError := range roleHintErrors {
					r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonServerRoleFailed, "Server role hint validation failed: %s", roleError)
				}
			}
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}

	// Validate property sharding configuration if enabled
	if cluster.Spec.PropertySharding != nil && cluster.Spec.PropertySharding.Enabled {
		if err := r.validatePropertyShardingConfiguration(ctx, cluster); err != nil {
			announced := r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Property sharding validation failed: %v", err))
			logRefusal(logger, announced, "Property sharding validation failed", err)
			if announced {
				r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonPropertyShardingFailed, "Property sharding validation failed: %v", err)
			}
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}

	// Verify the requested StorageClass exists before creating the StatefulSet.
	// A misnamed class (e.g. "standard" on AKS, which ships "managed-csi") would
	// otherwise leave every server pod Pending indefinitely with no operator-level
	// signal. An empty className is allowed and inherits the cluster default.
	if exists, err := storageClassExists(ctx, r.Client, cluster.Spec.Storage.ClassName); err != nil {
		logger.Error(err, "Failed to look up StorageClass", "storageClass", cluster.Spec.Storage.ClassName)
		return ctrl.Result{}, err
	} else if !exists {
		msg := fmt.Sprintf("StorageClass %q not found; create it or set spec.storage.className to an existing class (or leave it empty to use the cluster default)", cluster.Spec.Storage.ClassName)
		announced := r.updateClusterStatus(ctx, cluster, "Failed", msg)
		logRefusal(logger, announced, "StorageClass not found", fmt.Errorf("%s", msg))
		if announced {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonStorageClassNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Add finalizer if not present. A patch of metadata.finalizers alone:
	// this copy of the cluster carries ApplyDefaults' defaults, and an Update
	// wrote them, and the rest of the spec as the Go types encode it, into the
	// stored object (operations.md id 150).
	if !controllerutil.ContainsFinalizer(cluster, ClusterFinalizer) {
		if err := addFinalizer(ctx, r.Client, cluster, ClusterFinalizer); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// A held upgrade: record what its servers have started on (an image that
	// was not pullable may be now), and drop the record of one nothing started
	// on that spec.image.tag no longer names (operations.md id 149).
	if upgradeHeld(cluster) {
		r.recordStartedTarget(ctx, cluster)
		r.forgetAbandonedUpgrade(ctx, cluster)
	}

	// Check if this is an upgrade scenario. A mid-flight state machine
	// (upgradeStateMachineActive) keeps driving regardless of image drift or
	// cluster phase: the normal path below would stomp the partition freeze
	// (line ~838 applies the builder's UpdateStrategy) and unfreeze a staged
	// roll. isUpgradeRequired starts a NEW upgrade on image drift, and takes
	// a Paused or Failed one up again; until then the normal path below runs
	// with the partition that upgrade left (upgradeHeld, operations.md id 149).
	if upgradeStateMachineActive(cluster) || r.isUpgradeRequired(ctx, cluster) {
		res, err := r.handleRollingUpgrade(ctx, cluster)
		// Only the read-only half of the normal pass keeps running (id 131).
		r.collectDiagnosticsDuringUpgrade(ctx, cluster)
		return res, err
	}

	// #262: image drift while the cluster is NOT Ready must never reach the
	// normal path below — its builders would stomp the new image into the
	// StatefulSet as a plain config change, performing the version change as
	// a bare roll with none of the upgrade machine's gating (no partition
	// staging, no per-pod SHOW SERVERS verification, no version stamping).
	// Pin the in-memory spec to the image the StatefulSet currently runs;
	// once the cluster reaches Ready, isUpgradeRequired fires on the drift
	// and the state machine performs the upgrade properly.
	r.holdImageDriftUntilReady(ctx, cluster)
	// Likewise a change of spec.topology.servers, while a Paused or Failed
	// upgrade keeps a partition: servers added then would start on its target,
	// and a drain would run mid-upgrade (operations.md id 163).
	r.holdTopologyWhileUpgradeHolds(ctx, cluster)

	// Neo4jEnterpriseCluster is always multi-node from the start
	// (minimum 1 primary + 1 secondary OR 2+ primaries)
	// No need for single-node to multi-node transition logic

	// Set phase to "Initializing" only on the very first reconcile (no phase set yet).
	// Never regress an established phase (Forming, Ready, etc.) back to Initializing.
	if cluster.Status.Phase == "" {
		_ = r.updateClusterStatus(ctx, cluster, "Initializing", "Starting cluster reconciliation")
	}

	// Create Certificate if cert-manager is enabled
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == "cert-manager" {
		certificate := resources.BuildCertificateForEnterprise(cluster)
		if certificate != nil {
			if err := r.createOrUpdateResource(ctx, certificate, cluster); err != nil {
				logger.Error(err, "Failed to create Certificate")
				_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create Certificate: %v", err))
				return ctrl.Result{}, err
			}
		}

		// Strict peer validation requires the cert-manager Secret to
		// expose a ca.crt key (the issuer's CA bundle). Most issuers
		// populate it (CA, ACME, Vault); some external issuers do not.
		// We must NOT proceed to ConfigMap + STS emission until we've
		// verified the Secret is ready, because the strict-mode STS
		// template requires ca.crt in its volume projection (KeyToPath
		// has no per-item optional flag — the kubelet would refuse to
		// mount the volume if the key is missing).
		if isStrictPeerValidationEnabled(cluster) {
			if err := r.verifyTLSSecretHasCA(ctx, cluster); err != nil {
				if goerrors.Is(err, errTLSSecretPending) {
					// Bootstrap path: cert-manager hasn't issued the Secret
					// yet. Block downstream ConfigMap + STS emission so the
					// strict-mode template (which requires ca.crt in its
					// Secret items projection) never lands before the
					// Secret is verified.
					//
					// Phase handling honours the "never regress an
					// established phase to Initializing" policy documented
					// just above. Only surface Initializing on the
					// first-reconcile path (empty phase); for established
					// clusters in Forming/Ready/etc., requeue silently and
					// leave the phase intact. A subsequent reconcile will
					// proceed normally once cert-manager finishes.
					if cluster.Status.Phase == "" || cluster.Status.Phase == "Initializing" {
						_ = r.updateClusterStatus(ctx, cluster, "Initializing",
							fmt.Sprintf("Waiting for cert-manager to issue Secret %s-tls-secret (strict peer validation)", cluster.Name))
					} else {
						logger.Info("Strict peer validation preflight: cert-manager Secret not yet available; requeueing",
							"secret", fmt.Sprintf("%s-tls-secret", cluster.Name),
							"phase", cluster.Status.Phase)
					}
					return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
				}
				logger.Error(err, "Strict peer validation preflight failed")
				_ = r.updateClusterStatus(ctx, cluster, "Failed", err.Error())
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
		}
	}

	// Create External Secrets if enabled
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.ExternalSecrets != nil && cluster.Spec.TLS.ExternalSecrets.Enabled {
		if err := r.createExternalSecretForTLS(ctx, cluster); err != nil {
			logger.Error(err, "Failed to create TLS ExternalSecret")
			_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create TLS ExternalSecret: %v", err))
			return ctrl.Result{}, err
		}
	}

	if cluster.Spec.Auth != nil && cluster.Spec.Auth.ExternalSecrets != nil && cluster.Spec.Auth.ExternalSecrets.Enabled {
		if err := r.createExternalSecretForAuth(ctx, cluster); err != nil {
			logger.Error(err, "Failed to create Auth ExternalSecret")
			_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create Auth ExternalSecret: %v", err))
			return ctrl.Result{}, err
		}
	}

	// Reconcile the network-mode CCDR exposure proxy BEFORE the server
	// ConfigMap, so a freshly-observed load balancer hostname is already on
	// cluster.Status when the startup script renders
	// server.cluster.advertised_address below.
	ccdrStatus, err := r.reconcileCrossClusterReplicationProxy(ctx, cluster)
	if err != nil {
		logger.Error(err, "Failed to reconcile CCDR proxy")
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to reconcile CCDR proxy: %v", err))
		return ctrl.Result{}, err
	}
	cluster.Status.CrossClusterReplication = ccdrStatus

	// Resolve the server default Cypher language before the ConfigMap that
	// renders it (spec.serverDefaultCypherLanguage; server_cypher_language.go).
	if err := r.stampClusterCypherLanguage(ctx, cluster); err != nil {
		logger.Error(err, "Failed to resolve the server default Cypher language")
		return ctrl.Result{}, err
	}

	// Reconcile ConfigMap with immediate updates and pod restarts
	if err := r.ConfigMapManager.ReconcileConfigMap(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile ConfigMap")
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to reconcile ConfigMap: %v", err))
		return ctrl.Result{}, err
	}

	// The ServiceAccount the server pods run under (it carries
	// spec.podServiceAccountAnnotations for cloud workload identity). LIST
	// discovery needs no Kubernetes API access, so it is bound to no Role.
	serviceAccount := resources.BuildDiscoveryServiceAccountForEnterprise(cluster)
	if err := r.createOrUpdateResource(ctx, serviceAccount, cluster); err != nil {
		logger.Error(err, "Failed to create the server ServiceAccount", "serviceAccount", serviceAccount.Name)
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create ServiceAccount %s: %v", serviceAccount.Name, err))
		return ctrl.Result{}, err
	}
	if err := r.deleteLegacyDiscoveryObjects(ctx, cluster); err != nil {
		// Not fatal: nothing uses them, and the next pass retries.
		logger.V(1).Info("Could not delete the legacy discovery Role, RoleBinding or Service", "error", err.Error())
	}

	// Create Services
	services := []*corev1.Service{
		resources.BuildHeadlessServiceForEnterprise(cluster),  // Headless service for StatefulSet
		resources.BuildInternalsServiceForEnterprise(cluster), // Internals service for client connections
		resources.BuildClientServiceForEnterprise(cluster),    // Client service for external access
		resources.BuildMetricsServiceForEnterprise(cluster),   // Metrics service for Prometheus scraping
	}

	// Filter out nil services (e.g., secondary service when secondaries = 0)
	var validServices []*corev1.Service
	for _, service := range services {
		if service != nil {
			validServices = append(validServices, service)
		}
	}
	services = validServices
	for _, service := range services {
		if err := r.createOrUpdateResource(ctx, service, cluster); err != nil {
			logger.Error(err, "Failed to create Service", "service", service.Name)
			_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create Service %s: %v", service.Name, err))
			return ctrl.Result{}, err
		}
	}

	// Create Ingress if configured
	if cluster.Spec.Service != nil && cluster.Spec.Service.Ingress != nil && cluster.Spec.Service.Ingress.Enabled {
		ingress := resources.BuildIngressForEnterprise(cluster)
		if ingress != nil {
			if err := r.createOrUpdateResource(ctx, ingress, cluster); err != nil {
				logger.Error(err, "Failed to create Ingress")
				_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create Ingress: %v", err))
				return ctrl.Result{}, err
			}
		}
	}

	// Create NetworkPolicy if enabled (issue #128 gap #2 — restricts
	// port 6362 ingress to operator-managed backup pods). Effective only
	// on CNIs that enforce NetworkPolicy (Calico/Cilium/Antrea/Weave);
	// safe no-op on others. Build returns nil when disabled so this
	// branch trivially short-circuits without a feature-flag dance.
	if np := resources.BuildNetworkPolicyForEnterprise(cluster); np != nil {
		if err := r.createOrUpdateResource(ctx, np, cluster); err != nil {
			logger.Error(err, "Failed to create NetworkPolicy")
			_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create NetworkPolicy: %v", err))
			return ctrl.Result{}, err
		}
	}

	// Create Route if configured (OpenShift)
	if err := r.reconcileRoute(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile Route")
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to reconcile Route: %v", err))
		return ctrl.Result{}, err
	}

	// Reconcile MCP resources if enabled
	if err := r.reconcileMCP(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile MCP resources")
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to reconcile MCP resources: %v", err))
		return ctrl.Result{}, err
	}

	// Reconcile Aura Fleet Management registration if enabled
	if cluster.Spec.AuraFleetManagement != nil && cluster.Spec.AuraFleetManagement.Enabled {
		// Phase 0 — operator-driven onboarding: register the Fleet Manager
		// deployment and mint its token into a Secret, so the Phase 2 registration
		// below has something to read. No-op unless spec.provision is set, and
		// itself non-fatal (it reports through status, never returns an error).
		_ = r.newFleetProvisioner().reconcileAuraFleetProvision(ctx, cluster)

		if err := r.reconcileAuraFleetManagement(ctx, cluster); err != nil {
			// Fleet management registration failures are non-fatal: the cluster is operational,
			// only the Aura monitoring registration failed. Log and surface via status.
			logger.Error(err, "Failed to reconcile Aura Fleet Management registration")
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonAuraFleetFailed,
				"Aura Fleet Management registration failed: %v", err)
		}
	}

	// Calculate topology placement if topology scheduler is available
	var topologyPlacement *TopologyPlacement
	if r.TopologyScheduler != nil {
		placement, err := r.TopologyScheduler.CalculateTopologyPlacement(ctx, cluster)
		if err != nil {
			logger.Error(err, "Failed to calculate topology placement")
			// Retried with backoff; the same failure is announced once
			// (knowledge rule 145).
			if r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to calculate topology placement: %v", err)) {
				r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonTopologyPlacementFailed, fmt.Sprintf("Failed to calculate topology placement: %v", err))
			}
			return ctrl.Result{}, err
		}
		topologyPlacement = placement
		logger.Info("Calculated topology placement",
			"useTopologySpread", placement.UseTopologySpread,
			"useAntiAffinity", placement.UseAntiAffinity,
			"zones", len(placement.AvailabilityZones),
			"enforceDistribution", placement.EnforceDistribution)

		// Announced when the zones change, not on every pass (#527).
		if msg, news := r.TopologyScheduler.placementAnnouncement(cluster, placement); news {
			r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonTopologyPlacementCalc, msg)
		}
		r.recordZoneDiscovery(cluster, placement)
	}

	// Check if PVC storage expansion is needed before creating/updating StatefulSets.
	// If expansion is needed, PVCs are patched and the StatefulSet is orphan-deleted,
	// then we requeue so the next reconcile recreates it with updated VolumeClaimTemplates.
	if requeue, err := r.reconcileStorageExpansion(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile storage expansion")
		return ctrl.Result{}, err
	} else if requeue {
		logger.Info("Storage expansion completed, requeueing to recreate StatefulSet")
		// The delete has not gone through yet (waitStatefulSetGone): its watch
		// events, or this requeue, start the pass that creates it again.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Create single StatefulSet for all servers
	serverStatefulSet := resources.BuildServerStatefulSetForEnterprise(cluster)

	// Apply topology constraints to the server StatefulSet
	if r.TopologyScheduler != nil && topologyPlacement != nil {
		if err := r.TopologyScheduler.ApplyTopologyConstraints(ctx, serverStatefulSet, cluster, topologyPlacement); err != nil {
			logger.Error(err, "Failed to apply topology constraints to server StatefulSet")
			_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to apply topology constraints to server StatefulSet: %v", err))
			return ctrl.Result{}, err
		}
	}

	// Drive the scale-down drain BEFORE applying the StatefulSet. When a
	// scale-down is pending it sets ScaleDownDrainingAnnotation (in-memory +
	// persisted), which the apply below honours to HOLD replicas until the
	// removed servers are deallocated + dropped (#173). Non-fatal — on
	// connect/query errors it keeps holding and retries next reconcile.
	if err := r.reconcileScaleDownDrain(ctx, cluster); err != nil {
		logger.Error(err, "Scale-down drain step failed (non-fatal)")
	}

	if err := r.createOrUpdateResource(ctx, serverStatefulSet, cluster); err != nil {
		logger.Error(err, "Failed to create server StatefulSet")
		_ = r.updateClusterStatus(ctx, cluster, "Failed", fmt.Sprintf("Failed to create server StatefulSet: %v", err))
		return ctrl.Result{}, err
	}
	// The rollout checks below read the StatefulSet from the cache, which may
	// not have this write yet (serverTemplateWriteUnobserved).
	r.noteServerStatefulSetWrite(cluster, serverStatefulSet)

	// Handle Query Performance Monitoring
	if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Enabled {
		queryMonitor := NewQueryMonitor(r.Client, r.Scheme)
		if err := queryMonitor.ReconcileMonitoring(ctx, cluster); err != nil {
			logger.Error(err, "Failed to reconcile query monitoring")
			// Don't fail the entire reconciliation for monitoring issues
			logger.Info("Query monitoring setup failed, continuing with cluster reconciliation")
		}
	}

	// Collect live diagnostics once the cluster has formed — including while it
	// is short of a server, which is when ServersHealthy and the server_health
	// metric matter most (#444). Before formation there is nothing to report.
	// Diagnostics are collected by default (monitoring nil or monitoring.enabled=true).
	// Only skipped when monitoring is explicitly disabled.
	monitoringDisabled := cluster.Spec.Monitoring != nil && !cluster.Spec.Monitoring.Enabled
	if !monitoringDisabled && clusterHasFormed(cluster) {
		neo4jDiagClient, diagClientErr := r.createNeo4jClient(ctx, cluster)
		if diagClientErr != nil {
			logger.V(1).Info("Skipping diagnostics collection: could not create Neo4j client", "error", diagClientErr)
		} else {
			defer neo4jDiagClient.Close()
			diagMonitor := NewQueryMonitor(r.Client, r.Scheme)
			if diagErr := diagMonitor.CollectDiagnostics(ctx, cluster, neo4jDiagClient); diagErr != nil {
				logger.Error(diagErr, "Failed to collect cluster diagnostics (non-fatal)")
			}
		}
	}

	// Name the servers that still have to restart to pick up a setting the
	// operator applies at the next restart (#468). Best-effort, never fatal.
	r.reconcileRestartPending(ctx, cluster)

	// Plugin management is now handled by the separate Neo4jPlugin CRD and controller

	// Verify Neo4j cluster formation before marking as Ready. Formation does
	// not fail, it waits: every way the check can come back short is a
	// formationCheck the shortfall assessment reads.
	formation := r.verifyNeo4jClusterFormation(ctx, cluster)

	// Not every server is available. A cluster that has formed and is still
	// serving stays Ready (then turns Degraded); anything else is Forming.
	if !formation.formed {
		r.reconcileServerShortfall(ctx, cluster, formation, formation.message)
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Never declare Ready while the server StatefulSet is mid-rollout: a
	// half-rolled (possibly mixed-version) cluster that answers Bolt is
	// converging, not Ready (#262). Outside a rollout this is a pod whose
	// readiness probe lags Neo4j after a restart, which the shortfall
	// assessment treats like any other missing server.
	if rolled, detail := r.serverStatefulSetFullyRolled(ctx, cluster); !rolled {
		r.reconcileServerShortfall(ctx, cluster, formation, "Waiting for every server pod to be updated and ready: "+detail)
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	formedConditions := []metav1.Condition{
		clusterFormedCondition(),
		allServersAvailableCondition(cluster.Spec.Topology.Servers),
	}
	r.availabilityAnnounced.Delete(cluster.Namespace + "/" + cluster.Name)

	// Update status to "Ready" only if cluster formation is verified
	// Note: Split-brain detection is already performed in verifyNeo4jClusterFormation
	statusChanged := r.recordClusterReady(ctx, cluster, formation.servers, formedConditions...)

	// Scale-down draining (server cordon→deallocate→drop) is driven earlier in
	// the reconcile by reconcileScaleDownDrain, which owns the
	// ServersPendingDrain condition + events and holds replicas until the drain
	// completes (#173).

	// Update property sharding readiness status if enabled
	if cluster.Spec.PropertySharding != nil && cluster.Spec.PropertySharding.Enabled {
		if err := r.updatePropertyShardingStatus(ctx, cluster, true); err != nil {
			logger.Error(err, "Failed to update property sharding status")
			// Don't fail the reconciliation for status update issues
		}
	}

	// Only create event if status actually changed
	if statusChanged {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonClusterReady, "Neo4j Enterprise cluster is ready")
	}

	// Reload a renewed TLS certificate on the running servers (#469).
	requeue := readyPollInterval(r.ReadyPollInterval, r.RequeueAfter)
	if d := r.reconcileClusterTLSReload(ctx, cluster); d > 0 && (requeue == 0 || d < requeue) {
		requeue = d
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reconcileClusterTLSReload reloads a renewed TLS certificate on every running
// server (#469); see reconcileTLSReload.
func (r *Neo4jEnterpriseClusterReconciler) reconcileClusterTLSReload(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) time.Duration {
	certManager := cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == resources.CertManagerMode
	if !resources.TLSReloadApplies(cluster.Spec.Image.Tag, certManager) {
		return 0
	}
	return reconcileTLSReload(ctx, r.Client, r.Recorder, cluster, cluster.Name+"-tls-secret", cluster.Name+"-server", r.TLSReloadDeps,
		func(_ context.Context, pod string) (tlsReloadConn, error) {
			return dialClusterPod(r.Client, cluster, pod)
		})
}

func (r *Neo4jEnterpriseClusterReconciler) handleDeletion(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(cluster, ClusterFinalizer) {
		logger.Info("Finalizer not present, nothing to do", "finalizers", cluster.Finalizers, "deletionTimestamp", cluster.DeletionTimestamp)
		return ctrl.Result{}, nil
	}

	// Unregister the Aura Fleet Manager deployment when provisioning asked for it
	// (deletionPolicy: Delete). Non-fatal and hooked into the existing finalizer
	// rather than adding a second one — a stuck Aura API must not block deletion.
	r.newFleetProvisioner().deprovisionAuraFleet(ctx, cluster)
	r.fleetFailures.clear(client.ObjectKeyFromObject(cluster))
	r.fleetPluginPatchFailures.forget(client.ObjectKeyFromObject(cluster).String())
	r.availabilityAnnounced.Delete(clusterKey(cluster))
	r.splitBrainChecks.Delete(clusterKey(cluster))
	r.upgradeDiagnostics.Delete(clusterKey(cluster))
	r.serverStatefulSetWrites.Delete(clusterKey(cluster))
	r.deletedServerStatefulSets.forget(cluster.Namespace, cluster.Name+"-server")
	r.forgetDeferredUpgrades(cluster)
	if r.TopologyScheduler != nil {
		r.TopologyScheduler.forgetCluster(cluster)
	}
	forgetServerAddresses(cluster)
	neo4jclient.ForgetCluster(cluster.Namespace, cluster.Name)

	// Clean up PVCs if retention policy is Delete (default behavior)
	retentionPolicy := cluster.Spec.Storage.RetentionPolicy
	if retentionPolicy == "" || retentionPolicy == "Delete" {
		logger.Info("Cleaning up PVCs due to Delete retention policy", "retentionPolicy", retentionPolicy)
		if err := r.cleanupPVCs(ctx, cluster); err != nil {
			logger.Error(err, "Failed to cleanup PVCs")
			return ctrl.Result{}, err
		}
		logger.Info("Successfully cleaned up PVCs")
	} else {
		logger.Info("Retaining PVCs due to Retain retention policy", "retentionPolicy", retentionPolicy)
	}

	// Explicitly delete StatefulSets to avoid slow GC with blockOwnerDeletion
	serverStsName := fmt.Sprintf("%s-server", cluster.Name)
	serverSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: serverStsName, Namespace: cluster.Namespace}, serverSts); err == nil {
		logger.Info("Deleting server StatefulSet", "name", serverStsName)
		if err := r.Delete(ctx, serverSts); err != nil {
			logger.Error(err, "Failed to delete server StatefulSet")
		}
	}
	// cert-manager leaves the Secret it issued behind when the Certificate is
	// garbage-collected (#479). Not worth blocking deletion over.
	if err := deleteIssuedTLSSecret(ctx, r.Client, cluster.Namespace, cluster.Name, cluster.Name+"-tls"); err != nil {
		logger.Error(err, "Failed to delete the TLS Secret issued for this cluster")
	}

	logger.Info("Removing finalizer from cluster", "finalizers", cluster.Finalizers, "deletionTimestamp", cluster.DeletionTimestamp)
	// A cluster already gone — a pass that read it before an earlier one
	// released the finalizer — is released: removeFinalizer returns nil (#562
	// re-walk: an ERROR and a "Reconciler error" on every cluster deletion;
	// knowledge rule 189).
	if err := removeFinalizer(ctx, r.Client, cluster, ClusterFinalizer); err != nil {
		logger.Error(err, "Failed to update cluster after removing finalizer", "finalizers", cluster.Finalizers, "deletionTimestamp", cluster.DeletionTimestamp)
		return ctrl.Result{}, err
	}
	logger.Info("Successfully removed finalizer and updated cluster", "finalizers", cluster.Finalizers, "deletionTimestamp", cluster.DeletionTimestamp)
	// The cluster is gone; so are its metric series.
	metrics.NewClusterMetrics(cluster.Name, cluster.Namespace).Forget()
	return ctrl.Result{}, nil
}

func (r *Neo4jEnterpriseClusterReconciler) cleanupPVCs(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	// List PVCs that belong to this cluster
	pvcList := &corev1.PersistentVolumeClaimList{}
	labelSelector := client.MatchingLabels(resources.PVCSelectorByInstance(cluster.Name))

	if err := r.List(ctx, pvcList, client.InNamespace(cluster.Namespace), labelSelector); err != nil {
		return fmt.Errorf("failed to list PVCs for cluster %s: %w", cluster.Name, err)
	}

	logger.Info("Found PVCs to delete", "count", len(pvcList.Items), "cluster", cluster.Name)

	// Delete each PVC
	for _, pvc := range pvcList.Items {
		logger.Info("Deleting PVC", "pvc", pvc.Name, "cluster", cluster.Name)
		if err := r.Delete(ctx, &pvc); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("failed to delete PVC %s: %w", pvc.Name, err)
			}
			logger.Info("PVC already deleted", "pvc", pvc.Name)
		} else {
			logger.Info("Successfully deleted PVC", "pvc", pvc.Name)
		}
	}

	return nil
}

// CreateOrUpdateResource is exported for testing
func (r *Neo4jEnterpriseClusterReconciler) CreateOrUpdateResource(ctx context.Context, obj client.Object, owner client.Object) error {
	return r.createOrUpdateResource(ctx, obj, owner)
}

// replicasReconciliationPaused reports whether the cluster controller should
// leave sts.Spec.Replicas alone for this owner. Today the only pause signal
// is the restore-in-progress annotation set by Neo4jRestoreReconciler (issue
// #117), but extracting the check makes the gate testable in isolation and
// gives a single place to add new pause signals (e.g. operator-wide
// maintenance mode) without touching the resource builder.
func replicasReconciliationPaused(owner client.Object) bool {
	if owner == nil {
		return false
	}
	_, paused := owner.GetAnnotations()[RestoreInProgressAnnotation]
	return paused
}

func (r *Neo4jEnterpriseClusterReconciler) createOrUpdateResource(ctx context.Context, obj client.Object, owner client.Object) error {
	logger := log.FromContext(ctx)

	// Initialize metrics
	conflictMetrics := metrics.NewConflictMetrics()
	startTime := time.Now()

	// Every attempt starts from the desired object. CreateOrUpdate reads the
	// live object into obj, so after a conflict obj holds what the previous
	// attempt read: a retry that took it as "desired" applied a stale
	// StatefulSet template and recorded every env var in it as cluster-owned —
	// NEO4J_PLUGINS included, when the conflict was the plugin controller's
	// patch — and the next pass then removed the plugin's variable.
	desired := obj.DeepCopyObject()

	// Use retry logic to handle resource version conflicts
	retryCount := 0
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if retryCount > 0 {
			logger.Info("Retrying resource update due to conflict",
				"resource", fmt.Sprintf("%T", obj),
				"name", obj.GetName(),
				"retryCount", retryCount)
			// Record conflict metric
			conflictMetrics.RecordConflict(fmt.Sprintf("%T", obj), obj.GetNamespace())
			reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(desired.DeepCopyObject()).Elem())
		}
		retryCount++
		return r.createOrUpdateResourceInternal(ctx, obj, owner)
	})

	// Record metrics if we had conflicts
	if retryCount > 1 {
		retryDuration := time.Since(startTime)
		conflictMetrics.RecordConflictRetry(fmt.Sprintf("%T", obj), obj.GetNamespace(), retryCount-1, retryDuration)

		if err != nil && errors.IsConflict(err) {
			logger.Error(err, "Failed to update resource after retries due to conflict",
				"resource", fmt.Sprintf("%T", obj),
				"name", obj.GetName(),
				"finalRetryCount", retryCount)
		} else {
			logger.Info("Successfully updated resource after conflict resolution",
				"resource", fmt.Sprintf("%T", obj),
				"name", obj.GetName(),
				"totalRetries", retryCount-1,
				"duration", retryDuration)
		}
	}

	return err
}

// createOrUpdateResourceInternal performs the actual create or update operation with resource conflict handling
//
// This function implements critical resource version conflict resolution that eliminates the Pod-2 restart
// pattern observed during Neo4j cluster formation. Key improvements:
//
// 1. Proper resource existence detection using UID instead of ResourceVersion (UID is empty for new resources)
// 2. Template comparison logic that prevents unnecessary pod restarts during cluster formation
// 3. Selective template updates only when changes are significant enough to warrant pod disruption
//
// The template comparison is essential for Neo4j cluster stability - without it, resource version conflicts
// during reconciliation loops would cause the highest-indexed pods (Pod-2) to restart repeatedly, disrupting
// cluster formation especially for Neo4j 2025.01.0 which is more sensitive to discovery timing.
func (r *Neo4jEnterpriseClusterReconciler) createOrUpdateResourceInternal(ctx context.Context, obj client.Object, owner client.Object) error {
	// Set owner reference
	if err := controllerutil.SetControllerReference(owner, obj, r.Scheme); err != nil {
		return err
	}

	// Non-StatefulSet resources whose spec must stay in sync on a live cluster.
	// These previously flowed through CreateOrUpdate below with a
	// StatefulSet-only mutate closure, so the closure was a no-op for them and
	// spec edits (service type/annotations/loadBalancerIP, ingress host,
	// network-policy rules, certificate DNS names) were silently dropped on
	// existing clusters. Handle them with an explicit get-then-update.
	// RBAC and the MCP Deployment intentionally stay on the create-only path
	// (RoleBinding.RoleRef and Deployment.Spec.Selector are immutable and rarely
	// change for this operator).
	switch obj.(type) {
	case *corev1.Service, *networkingv1.NetworkPolicy, *networkingv1.Ingress, *certmanagerv1.Certificate, *corev1.ServiceAccount:
		return r.reconcileMutableResource(ctx, obj)
	}

	// Capture the desired spec if this is a StatefulSet
	var desiredSpec appsv1.StatefulSetSpec
	if sts, ok := obj.(*appsv1.StatefulSet); ok {
		desiredSpec = *sts.Spec.DeepCopy()
	}

	logger := log.FromContext(ctx)
	logger.Info("Starting CreateOrUpdate operation",
		"resource", fmt.Sprintf("%T", obj),
		"name", obj.GetName(),
		"namespace", obj.GetNamespace())

	restored := false
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if sts, ok := obj.(*appsv1.StatefulSet); ok {
			// The second step of a server StatefulSet created again in two
			// steps after a storage expansion, and nothing else, until it has
			// run: a write of the spec's template before it would make that the
			// update revision the servers below the partition are restored to
			// (restoreHeldTemplate, operations.md id 171).
			if ownerCluster, isCluster := owner.(*neo4jv1beta1.Neo4jEnterpriseCluster); isCluster && sts.UID != "" {
				if _, pending := restorePending(sts); pending {
					restored = r.restoreHeldTemplate(ctx, sts, ownerCluster, desiredSpec.Template)
					return nil
				}
			}
			// Check if this is an update (object already exists in cluster)
			// CRITICAL FIX: Use UID to determine if this is an existing object, not ResourceVersion
			// ResourceVersion can be populated even for new resources during CreateOrUpdate operations,
			// but UID is only set for resources that actually exist in the cluster
			if sts.UID != "" {
				// This is an existing StatefulSet, only update allowed mutable fields
				// Note: sts already contains the current state from CreateOrUpdate
				originalMeta := sts.ObjectMeta.DeepCopy()
				originalStatus := sts.Status.DeepCopy()

				// Apply desired spec — but yield on replicas while a
				// Neo4jRestore is coordinating a scale-down → restore →
				// scale-up cycle on this cluster (issue #117). Without
				// this gate the cluster controller races the restore
				// controller every reconcile and the scale-to-0 never
				// sticks. Only the replicas line is skipped; everything
				// else (services, ConfigMap, certs, template updates)
				// continues to reconcile normally.
				// Hold replicas during a restore coordination (#117) AND while a
				// scale-down drain is in progress (#173) — in the latter case the
				// removed pods must stay running until their servers are
				// deallocated + dropped, then a later reconcile lowers replicas.
				if !replicasReconciliationPaused(owner) && !scaleDownDrainInProgress(owner) {
					sts.Spec.Replicas = desiredSpec.Replicas
				}
				// The rolling-upgrade state machine owns RollingUpdate.Partition
				// (#174) while it is mid-flight and while its upgrade is Paused
				// or Failed: preserve the live partition, so a normal-path apply
				// can neither unfreeze a staged roll nor restart the servers a
				// stopped upgrade has not rolled (operations.md id 149). While
				// the machine is active this is defense-in-depth (the upgrade
				// branch returns before this path, but races on stale reads
				// happen); once it has stopped, the normal path runs and this is
				// what holds the servers.
				livePartition := (*int32)(nil)
				if sts.Spec.UpdateStrategy.RollingUpdate != nil {
					livePartition = sts.Spec.UpdateStrategy.RollingUpdate.Partition
				}
				sts.Spec.UpdateStrategy = desiredSpec.UpdateStrategy
				if ownerCluster, ok := owner.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok &&
					(upgradeStateMachineActive(ownerCluster) || upgradeHeld(ownerCluster)) && livePartition != nil &&
					sts.Spec.UpdateStrategy.RollingUpdate != nil &&
					sts.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
					sts.Spec.UpdateStrategy.RollingUpdate.Partition = livePartition
				}
				sts.Spec.PersistentVolumeClaimRetentionPolicy = desiredSpec.PersistentVolumeClaimRetentionPolicy
				sts.Spec.MinReadySeconds = desiredSpec.MinReadySeconds
				sts.Spec.Ordinals = desiredSpec.Ordinals

				// Preserve original metadata and status
				sts.ObjectMeta = *originalMeta
				sts.Status = *originalStatus

				// For template updates, check if changes are significant to avoid unnecessary pod restarts
				// This is the core mechanism that prevents Pod-2 restart patterns during cluster formation
				if sts.Spec.Selector != nil {
					// Copy the desired template but ensure labels match the existing selector
					updatedTemplate := desiredSpec.Template.DeepCopy()
					if updatedTemplate.Labels == nil {
						updatedTemplate.Labels = make(map[string]string)
					}
					// Ensure all selector labels are present in the template
					for key, value := range sts.Spec.Selector.MatchLabels {
						updatedTemplate.Labels[key] = value
					}

					// Check if template update is significant enough to warrant pod restarts
					// This prevents resource version conflicts from causing unnecessary pod disruption
					if r.isTemplateChangeSignificant(ctx, sts.Spec.Template, *updatedTemplate, sts) {
						logger := log.FromContext(ctx)
						logger.Info("Applying significant StatefulSet template changes",
							"statefulSet", sts.Name,
							"namespace", sts.Namespace)

						// Smart env-var merge: preserve foreign vars (added by
						// plugin / fleet controllers), drop previously-owned-but-
						// no-longer-desired (e.g. user removed a spec.config key),
						// apply desired. The wholesale `Template = updatedTemplate`
						// without this would clobber plugin / fleet env vars and
						// trigger cross-controller oscillation. See envVarsEqual
						// docs and mergeEnvVars for the design.
						owned := readOwnedTemplateItems(sts, sts.Spec.Template)
						currentEnv := []corev1.EnvVar{}
						if len(sts.Spec.Template.Spec.Containers) > 0 {
							currentEnv = sts.Spec.Template.Spec.Containers[0].Env
						}
						desiredEnv := []corev1.EnvVar{}
						if len(updatedTemplate.Spec.Containers) > 0 {
							desiredEnv = updatedTemplate.Spec.Containers[0].Env
						}
						merged := mergeEnvVars(currentEnv, desiredEnv, owned.env)
						// Init containers and volumes the same way, by name: the
						// plugin controller's VerifiedDownload init container and
						// its auth/CA volumes are not the cluster's, and replacing
						// the list wholesale dropped them, so the servers restarted
						// without the plugin's JAR (operations.md id 141).
						currentInit := sts.Spec.Template.Spec.InitContainers
						currentVolumes := sts.Spec.Template.Spec.Volumes

						// Capture foreign pod-template annotations before the
						// wholesale replace so the config-restart/config-hash stamps
						// and any mesh/plugin pod annotations survive (only env vars
						// are merged otherwise). Mirrors the standalone controller.
						livePodAnnotations := sts.Spec.Template.Annotations
						// A copy: a struct assignment shares the container array,
						// so writing the merged env into the live template wrote
						// it into updatedTemplate too, and the record and hash
						// below then took the plugin's variables for the
						// cluster's — the next pass removed them (operations.md
						// id 155).
						sts.Spec.Template = *updatedTemplate.DeepCopy()
						if len(sts.Spec.Template.Spec.Containers) > 0 {
							sts.Spec.Template.Spec.Containers[0].Env = merged
						}
						sts.Spec.Template.Spec.InitContainers = mergeOwnedByName(
							currentInit, updatedTemplate.Spec.InitContainers, owned.initContainers, containerNameOf)
						sts.Spec.Template.Spec.Volumes = mergeOwnedByName(
							currentVolumes, updatedTemplate.Spec.Volumes, owned.volumes, volumeNameOf)
						sts.Spec.Template.Annotations = mergePodTemplateAnnotations(livePodAnnotations, updatedTemplate.Annotations)
						writeOwnedTemplateItems(sts, *updatedTemplate)
						// Record the desired template hash so the next stable
						// reconcile can detect drift in fields the field-by-field
						// check omits. updatedTemplate keeps cluster-owned env only
						// (the foreign-merge above wrote to sts, not updatedTemplate).
						setClusterTemplateHash(sts, *updatedTemplate)
					} else {
						logger := log.FromContext(ctx)
						logger.V(1).Info("Skipping StatefulSet template update - no significant changes detected",
							"statefulSet", sts.Name,
							"namespace", sts.Namespace)
						// Keep existing template to prevent unnecessary pod restarts.
						// The owned-env-vars annotation does NOT need updating here:
						// if no template change was applied, the previously-recorded
						// owned set is still accurate by definition.
					}
				} else {
					// If no selector exists, just use the desired template
					// (preserving foreign pod-template annotations as above).
					livePodAnnotations := sts.Spec.Template.Annotations
					sts.Spec.Template = desiredSpec.Template
					sts.Spec.Template.Annotations = mergePodTemplateAnnotations(livePodAnnotations, desiredSpec.Template.Annotations)
					writeOwnedTemplateItems(sts, desiredSpec.Template)
					setClusterTemplateHash(sts, desiredSpec.Template)
				}
			} else {
				// This is a new StatefulSet, use the desired spec as-is
				logger := log.FromContext(ctx)
				logger.V(1).Info("Creating new StatefulSet with full template",
					"statefulSet", sts.Name,
					"namespace", sts.Namespace)
				sts.Spec = desiredSpec
				ownerCluster, isCluster := owner.(*neo4jv1beta1.Neo4jEnterpriseCluster)
				// Created again after a storage expansion deleted it: as it was —
				// its template with the plugins' and other controllers' writes,
				// replica count, strategy and records — with the new claim
				// templates, so no pod restarts (operations.md id 171).
				var recreate *recreateSource
				if isCluster {
					recreate = r.deletedServerStatefulSets.source(ctx, r.Client, r.apiReader(), sts.Namespace, sts.Name,
						ownerCluster.UID, map[string]string{"neo4j.com/cluster": ownerCluster.Name, "neo4j.com/clustering": "true"})
				}
				if recreate != nil {
					recreate.applyTo(sts)
					logger.Info("Creating the server StatefulSet again as it was, with the new volume claim templates",
						"statefulSet", sts.Name, "from", recreate.from)
				}
				// Created again while an upgrade is held (a storage expansion
				// deletes the StatefulSet and orphans its pods): start with the
				// upgrade's partition, or the new StatefulSet would restart
				// every server below it onto its template (operations.md id 149).
				if isCluster && sts.Spec.UpdateStrategy.RollingUpdate != nil && sts.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
					sts.Spec.UpdateStrategy.RollingUpdate.Partition = partitionForNewServerStatefulSet(ownerCluster, sts.Spec.Replicas)
				}
				// Servers below the partition that run another revision keep it:
				// created first on their template, frozen, the held template
				// written back once the StatefulSet controller has made theirs
				// the current revision (restoreHeldTemplate, id 171).
				twoStep := recreate != nil && recreate.holdBelowPartition(ctx, r.apiReader(), sts)
				if twoStep {
					logger.Info("Creating the server StatefulSet again on the template the servers below the partition run, frozen; the held template is written back once its current revision is recorded",
						"statefulSet", sts.Name, "currentRevision", recreate.currentRevision, "updateRevision", recreate.updateRevision)
				}
				switch {
				case recreate == nil:
					writeOwnedTemplateItems(sts, desiredSpec.Template)
					// Stamp on create as well as on update. The stamp is what lets a
					// cluster that never finishes forming still accept a corrected
					// template (see isTemplateChangeSignificant); a StatefulSet that
					// only ever got stamped once it went stable could not be repaired
					// before it got there, which is precisely when it needs to be.
					setClusterTemplateHash(sts, desiredSpec.Template)
				case recreate.annotations == nil:
					// Read from a revision: what the operator renders is recorded,
					// and the template hash when the template carries it, as on
					// any write of it (a two-step create stamps it in its second
					// step, with the held template).
					writeOwnedTemplateItems(sts, desiredSpec.Template)
					if !twoStep {
						r.stampTemplateHashIfApplied(ctx, sts, desiredSpec.Template)
					}
				}
				// A cluster with Aura Fleet Management starts its servers with
				// the plugin. Merged here, into the StatefulSet being created and
				// after the record of the env the cluster owns and its template
				// hash, NEO4J_PLUGINS stays out of the static template and out of
				// that record, as every pass's fleet merge leaves it. Merged by
				// the next pass instead, it rolled every server of a new cluster
				// during its first formation (operations.md id 175). A template
				// recreated as it was already carries it; the merge is idempotent.
				// Not into the first step of a two-step create: its template is the
				// revision the servers below the partition run, exactly.
				if isCluster && !twoStep && sts.Name == ownerCluster.Name+"-server" && clusterInstallsFleetManagement(ownerCluster) {
					sts.Spec.Template = *sts.Spec.Template.DeepCopy()
					if _, err := addFleetManagementPlugin(&sts.Spec.Template); err != nil {
						return fmt.Errorf("%w in StatefulSet %s", err, sts.Name)
					}
				}
			}
		}
		return nil
	})
	if sts, ok := obj.(*appsv1.StatefulSet); ok && err == nil {
		_, pending := restorePending(sts)
		if (op == controllerutil.OperationResultCreated && !pending) || restored {
			r.deletedServerStatefulSets.forget(sts.Namespace, sts.Name)
		}
	}

	return err
}

// apiReader is APIReader, or Client when none is set.
func (r *Neo4jEnterpriseClusterReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// isTemplateChangeSignificant determines if StatefulSet template changes warrant pod restarts
// This prevents unnecessary pod restarts during cluster formation due to resource version conflicts
func (r *Neo4jEnterpriseClusterReconciler) isTemplateChangeSignificant(ctx context.Context, currentTemplate, desiredTemplate corev1.PodTemplateSpec, sts *appsv1.StatefulSet) bool {
	logger := log.FromContext(ctx)

	// During initial cluster formation (not all pods ready), be more conservative about template updates
	if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
		logger.V(1).Info("StatefulSet still forming - being conservative about template updates",
			"statefulSet", sts.Name,
			"readyReplicas", sts.Status.ReadyReplicas,
			"desiredReplicas", *sts.Spec.Replicas)

		// Critical changes always apply. So does a change in what the operator
		// WANTS: if the desired template's hash differs from the one we last
		// stamped, the rendering changed since we applied it — the user edited
		// the CR, or an operator upgrade renders it differently — and that is
		// intent, not the reconcile churn this branch exists to damp.
		//
		// Without this a cluster that cannot finish forming BECAUSE of its
		// template was unrepairable: the pods never go ready, so every reconcile
		// takes this branch, and every non-critical fix (volumes, env, init
		// containers — hasCriticalTemplateChanges covers none of them) was
		// dropped forever. Found on a cross-cluster TLS walk, where a peer-CA
		// mount made server-1 crash-loop and the corrected spec never reached
		// the StatefulSet; the only way out was to delete it by hand.
		if r.hasCriticalTemplateChanges(currentTemplate, desiredTemplate) {
			return true
		}
		// Only the stamp can tell intent from churn, so a StatefulSet that has
		// never been stamped keeps the old critical-only behaviour rather than
		// reading "no stamp" as "changed" and rolling a cluster that is merely
		// still coming up.
		stamped, ok := sts.Annotations[clusterTemplateHashAnnotation]
		return ok && podTemplateSpecHash(desiredTemplate) != stamped
	}

	// For a stable cluster a change is significant if EITHER:
	//   - a cluster-owned core field drifted from desired (env/volumes/init
	//     containers/image/resources/securityContext/SA — foreign-tolerant via
	//     envVarsEqual, initContainersEqual and volumesEqual so plugin/fleet/
	//     Aura-owned env vars and a plugin's init container don't trip it), OR
	//   - the desired template's hash differs from the one we last applied.
	// The hash closes the fields the field-by-field check omits (nodeSelector,
	// affinity, tolerations, probes, resource requests, volume sources,
	// initContainer args/env). It is computed over the *desired* template
	// (cluster-owned items only) and compared to a stored annotation, never to
	// the live template — so foreign env vars, init containers, volumes and the
	// config-restart annotation never enter it and cannot cause oscillation
	// against the ownership protocol. A missing annotation (first reconcile
	// after an operator upgrade) reads as changed, converging once.
	if r.hasSignificantTemplateChanges(currentTemplate, desiredTemplate, readOwnedTemplateItems(sts, currentTemplate)) {
		return true
	}
	return podTemplateSpecHash(desiredTemplate) != sts.Annotations[clusterTemplateHashAnnotation]
}

// hasCriticalTemplateChanges checks for changes that are essential during cluster formation
func (r *Neo4jEnterpriseClusterReconciler) hasCriticalTemplateChanges(current, desired corev1.PodTemplateSpec) bool {
	// Check for image changes (critical)
	if len(current.Spec.Containers) != len(desired.Spec.Containers) {
		return true
	}

	for i, currentContainer := range current.Spec.Containers {
		if i >= len(desired.Spec.Containers) {
			return true
		}
		desiredContainer := desired.Spec.Containers[i]

		// Image changes are critical
		if currentContainer.Image != desiredContainer.Image {
			return true
		}

		// Resource limit changes are critical
		if !r.resourcesEqual(currentContainer.Resources, desiredContainer.Resources) {
			return true
		}

		// Security context changes are critical
		if !r.containerSecurityContextEqual(currentContainer.SecurityContext, desiredContainer.SecurityContext) {
			return true
		}
	}

	// Check for security context changes (critical)
	if !r.securityContextEqual(current.Spec.SecurityContext, desired.Spec.SecurityContext) {
		return true
	}

	// Check for service account changes (critical for RBAC)
	if current.Spec.ServiceAccountName != desired.Spec.ServiceAccountName {
		return true
	}

	// No critical changes found
	return false
}

// hasSignificantTemplateChanges checks for any meaningful changes in stable clusters.
// previousOwned is what this controller recorded as owned on the previous
// reconcile (read from the StatefulSet's annotations by the caller): the env
// vars, init containers and volumes it rendered. envVarsEqual,
// initContainersEqual and volumesEqual use it to tell a removal from the
// controller's own desired set from an item another controller added.
func (r *Neo4jEnterpriseClusterReconciler) hasSignificantTemplateChanges(current, desired corev1.PodTemplateSpec, previousOwned ownedTemplateItems) bool {
	// For stable clusters, allow more changes including:
	// - Environment variable updates
	// - Volume mount changes
	// - Label/annotation updates (beyond selector labels)
	// - Init container changes

	// First check critical changes
	if r.hasCriticalTemplateChanges(current, desired) {
		return true
	}

	// Check for environment variable changes
	if len(current.Spec.Containers) > 0 && len(desired.Spec.Containers) > 0 {
		if !r.envVarsEqual(current.Spec.Containers[0].Env, desired.Spec.Containers[0].Env, previousOwned.env) {
			return true
		}
	}

	// Check for volume changes
	if !r.volumesEqual(current.Spec.Volumes, desired.Spec.Volumes, previousOwned.volumes) {
		return true
	}

	// Check for init container changes
	if !r.initContainersEqual(current.Spec.InitContainers, desired.Spec.InitContainers, previousOwned.initContainers) {
		return true
	}

	return false
}

// Helper functions for template comparison
func (r *Neo4jEnterpriseClusterReconciler) resourcesEqual(current, desired corev1.ResourceRequirements) bool {
	// Compare CPU and memory limits/requests
	currentCPU := current.Limits.Cpu()
	desiredCPU := desired.Limits.Cpu()
	if (currentCPU == nil) != (desiredCPU == nil) || (currentCPU != nil && !currentCPU.Equal(*desiredCPU)) {
		return false
	}

	currentMem := current.Limits.Memory()
	desiredMem := desired.Limits.Memory()
	if (currentMem == nil) != (desiredMem == nil) || (currentMem != nil && !currentMem.Equal(*desiredMem)) {
		return false
	}

	return true
}

func (r *Neo4jEnterpriseClusterReconciler) securityContextEqual(current, desired *corev1.PodSecurityContext) bool {
	return equality.Semantic.DeepEqual(current, desired)
}

func (r *Neo4jEnterpriseClusterReconciler) containerSecurityContextEqual(current, desired *corev1.SecurityContext) bool {
	return equality.Semantic.DeepEqual(current, desired)
}

// ownedEnvVarsAnnotation is written to the StatefulSet by the cluster
// controller and lists the env-var names that this controller considered
// "owned" on the most recent reconcile. The list lets a subsequent reconcile
// distinguish a removal-from-desired (name in previous-owned, absent from
// desired) from a foreign env var added by another controller (name not in
// previous-owned and not in desired) so that removals can be applied without
// disturbing plugin / fleet additions. Names are stored sorted, comma-
// separated. Same shape as kubectl's last-applied-configuration tracking.
const ownedEnvVarsAnnotation = "neo4j.com/cluster-controller-env-vars"

// clusterTemplateHashAnnotation stores the hash of the desired pod template the
// cluster controller last applied. A stable-phase reconcile treats the template
// as changed when this differs from the current desired hash, which catches the
// fields hasSignificantTemplateChanges does not compare (nodeSelector, affinity,
// tolerations, probes, resource requests, volume sources, initContainer
// args/env). See isTemplateChangeSignificant.
const clusterTemplateHashAnnotation = "neo4j.com/cluster-template-hash"

// setClusterTemplateHash records the hash of the just-applied desired pod
// template on the StatefulSet's annotations.
func setClusterTemplateHash(sts *appsv1.StatefulSet, desired corev1.PodTemplateSpec) {
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[clusterTemplateHashAnnotation] = podTemplateSpecHash(desired)
}

// readOwnedEnvVarNames returns the set of env-var names this controller
// recorded as owned on the StatefulSet's previous reconcile. An empty set is
// returned when the annotation is absent (first reconcile after upgrade or
// brand-new StatefulSet) — the caller must treat that as "no removals to
// reconcile this round" so the upgrade path is non-disruptive.
func readOwnedEnvVarNames(sts *appsv1.StatefulSet) map[string]struct{} {
	out := map[string]struct{}{}
	raw := sts.Annotations[ownedEnvVarsAnnotation]
	if raw == "" {
		return out
	}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

// writeOwnedEnvVarNames records the controller-owned env-var names on the
// StatefulSet annotation. Names are extracted from desired and stored sorted
// for stable diffs.
func writeOwnedEnvVarNames(sts *appsv1.StatefulSet, desired []corev1.EnvVar) {
	names := make([]string, 0, len(desired))
	for _, e := range desired {
		names = append(names, e.Name)
	}
	sort.Strings(names)
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[ownedEnvVarsAnnotation] = strings.Join(names, ",")
}

// ownedInitContainersAnnotation and ownedVolumesAnnotation do for the server
// pod template's init containers and volumes what ownedEnvVarsAnnotation does
// for env vars: they list, sorted and comma-separated, the names the cluster
// controller rendered on its last apply, so the next one drops an item it no
// longer renders (the truststore init container and its volumes once
// spec.trustedCASecrets is cleared) and keeps one it never rendered (the
// plugin controller's VerifiedDownload init container and its auth/CA
// volumes). The standalone controller keeps the same record under its own
// names (standaloneOwnedInitContainersAnnotation).
const (
	ownedInitContainersAnnotation = "neo4j.com/cluster-controller-init-containers"
	ownedVolumesAnnotation        = "neo4j.com/cluster-controller-volumes"
)

// ownedTemplateItems is what the cluster controller rendered into the server
// pod template on its last apply, by name: the neo4j container's env vars, the
// init containers and the volumes.
type ownedTemplateItems struct {
	env            map[string]struct{}
	initContainers map[string]struct{}
	volumes        map[string]struct{}
}

// readOwnedTemplateItems reads the record writeOwnedTemplateItems left on sts;
// live is its pod template.
//
// A StatefulSet an earlier operator wrote has the env record but not the other
// two. That operator replaced the init containers and volumes wholesale on
// every apply, so every one on live is its own except what the plugin
// controller added since: the init containers it lists in its
// neo4j.com/plugin-init-containers pod-template annotation, and the volumes
// only those mount. Reading the missing record as "owns nothing" instead would
// keep for ever an init container the cluster stops rendering — the truststore
// one, still reading a CA Secret the user then deletes.
func readOwnedTemplateItems(sts *appsv1.StatefulSet, live corev1.PodTemplateSpec) ownedTemplateItems {
	owned := ownedTemplateItems{env: readOwnedEnvVarNames(sts)}

	raw, recorded := sts.Annotations[ownedInitContainersAnnotation]
	if recorded {
		owned.initContainers = splitCSVSet(raw)
	} else {
		plugins := parsePluginInitOwned(live.Annotations[PluginInitContainersAnnotation])
		owned.initContainers = map[string]struct{}{}
		for _, c := range live.Spec.InitContainers {
			if _, isPlugin := plugins[c.Name]; !isPlugin {
				owned.initContainers[c.Name] = struct{}{}
			}
		}
	}

	raw, recorded = sts.Annotations[ownedVolumesAnnotation]
	if recorded {
		owned.volumes = splitCSVSet(raw)
		return owned
	}
	usedByOwn, usedByPlugin := map[string]struct{}{}, map[string]struct{}{}
	for _, c := range live.Spec.Containers {
		for _, m := range c.VolumeMounts {
			usedByOwn[m.Name] = struct{}{}
		}
	}
	for _, c := range live.Spec.InitContainers {
		used := usedByPlugin
		if _, ours := owned.initContainers[c.Name]; ours {
			used = usedByOwn
		}
		for _, m := range c.VolumeMounts {
			used[m.Name] = struct{}{}
		}
	}
	owned.volumes = map[string]struct{}{}
	for _, v := range live.Spec.Volumes {
		_, own := usedByOwn[v.Name]
		_, plugin := usedByPlugin[v.Name]
		if own || !plugin {
			owned.volumes[v.Name] = struct{}{}
		}
	}
	return owned
}

// writeOwnedTemplateItems records what the cluster controller renders into the
// server pod template — desired, never the merged live template, or another
// controller's items would be recorded as the cluster's and removed by the next
// apply (operations.md id 134).
func writeOwnedTemplateItems(sts *appsv1.StatefulSet, desired corev1.PodTemplateSpec) {
	var env []corev1.EnvVar
	if len(desired.Spec.Containers) > 0 {
		env = desired.Spec.Containers[0].Env
	}
	writeOwnedEnvVarNames(sts, env)
	sts.Annotations[ownedInitContainersAnnotation] = sortedNamesCSV(desired.Spec.InitContainers, containerNameOf)
	sts.Annotations[ownedVolumesAnnotation] = sortedNamesCSV(desired.Spec.Volumes, volumeNameOf)
}

func containerNameOf(c corev1.Container) string { return c.Name }

func volumeNameOf(v corev1.Volume) string { return v.Name }

// withoutForeign returns current less the items another writer added: those
// neither desired nor previously owned. What is left is what the cluster
// controller renders, plus anything it rendered before and no longer does.
func withoutForeign[T any](current, desired []T, previousOwned map[string]struct{}, nameOf func(T) string) []T {
	desiredNames := make(map[string]struct{}, len(desired))
	for _, d := range desired {
		desiredNames[nameOf(d)] = struct{}{}
	}
	out := make([]T, 0, len(current))
	for _, c := range current {
		n := nameOf(c)
		_, inDesired := desiredNames[n]
		_, owned := previousOwned[n]
		if inDesired || owned {
			out = append(out, c)
		}
	}
	return out
}

// mergeEnvVars produces the env-var slice the cluster controller should write
// to the StatefulSet container after deciding to apply a template change:
//
//   - desired: applied (added or value-overwritten by name).
//   - previousOwned ∖ desired: removed from current (a key the controller
//     used to own and no longer wants — the case the old subset check missed).
//   - current ∖ previousOwned ∖ desired: preserved (foreign env vars added by
//     the plugin / fleet / Aura controllers, which the cluster controller
//     must not clobber to avoid cross-controller oscillation).
//
// Output order: foreign vars first (in their original relative order), then
// desired. Stable across reconciles for unchanged input.
func mergeEnvVars(current, desired []corev1.EnvVar, previousOwned map[string]struct{}) []corev1.EnvVar {
	desiredNames := make(map[string]struct{}, len(desired))
	for _, e := range desired {
		desiredNames[e.Name] = struct{}{}
	}
	final := make([]corev1.EnvVar, 0, len(current)+len(desired))
	for _, e := range current {
		if _, owned := previousOwned[e.Name]; owned {
			continue // owned-by-us — desired is the source of truth below.
		}
		if _, inDesired := desiredNames[e.Name]; inDesired {
			continue // in desired — desired wins below.
		}
		final = append(final, e)
	}
	final = append(final, desired...)
	return final
}

// envVarsEqual returns true when no env-var change is needed. It performs a
// subset check (every env var in desired must be present in current with the
// correct value) plus a removal check against previousOwned: a name in
// previousOwned that is no longer in desired but is still in current means
// the controller needs to apply a removal.
//
// The subset (rather than strict equality) on the desired side is intentional.
// The Neo4jPlugin controller (and reconcileAuraFleetManagement) live-patches
// the StatefulSet to add env vars (NEO4J_PLUGINS, NEO4J_APOC_*, fleet token
// vars) that are not part of the cluster controller's desired template. A
// strict count check would make those additions look like a "significant
// change", causing the cluster controller to overwrite the StatefulSet on
// every reconcile and creating an infinite oscillation between the
// controllers. The previousOwned-driven removal check closes the corollary
// gap that was tracked as a known limitation prior to this fix: removals
// from the cluster controller's own desired set are now detected and applied,
// but only for names this controller previously claimed. Names it never
// owned (foreign vars added by plugin/fleet) are still left alone.
func (r *Neo4jEnterpriseClusterReconciler) envVarsEqual(current, desired []corev1.EnvVar, previousOwned map[string]struct{}) bool {
	currentMap := make(map[string]corev1.EnvVar)
	for _, env := range current {
		currentMap[env.Name] = env
	}

	desiredNames := make(map[string]struct{}, len(desired))
	for _, env := range desired {
		desiredNames[env.Name] = struct{}{}
		currentEnv, exists := currentMap[env.Name]
		if !exists {
			// A desired env var is absent from current — needs update.
			return false
		}
		if currentEnv.Value != env.Value {
			// Desired env var present but with wrong value — needs update.
			return false
		}
		if !r.envVarSourceEqual(currentEnv.ValueFrom, env.ValueFrom) {
			// SecretKeyRef / ConfigMapKeyRef / FieldRef mismatch — needs update.
			return false
		}
	}

	// Removal check: a name in previousOwned that is no longer in desired
	// but is still in current means the controller used to manage it,
	// has dropped it from desired (e.g. user removed a spec.config key),
	// and needs to apply the removal.
	for name := range previousOwned {
		if _, inDesired := desiredNames[name]; inDesired {
			continue
		}
		if _, stillPresent := currentMap[name]; stillPresent {
			return false
		}
	}

	return true
}

// envVarSourceEqual compares two EnvVarSource pointers for the purposes of template
// change detection. Nil == Nil, and non-nil sources are compared structurally.
func (r *Neo4jEnterpriseClusterReconciler) envVarSourceEqual(a, b *corev1.EnvVarSource) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil) != (b == nil) {
		return false
	}
	return equality.Semantic.DeepEqual(a, b)
}

// volumesEqual reports whether the volumes the cluster renders are on the live
// template, by name. A volume another writer added (neither desired nor in
// previousOwned — a VerifiedDownload plugin's auth or CA volume) is not a
// change; one the cluster rendered before and no longer does is. Counting
// every live volume made a plugin's volume a "significant change" on every
// pass of a stable cluster, and the apply then removed it.
func (r *Neo4jEnterpriseClusterReconciler) volumesEqual(current, desired []corev1.Volume, previousOwned map[string]struct{}) bool {
	current = withoutForeign(current, desired, previousOwned, volumeNameOf)
	if len(current) != len(desired) {
		return false
	}

	// Create maps for easier comparison
	currentMap := make(map[string]corev1.Volume)
	for _, vol := range current {
		currentMap[vol.Name] = vol
	}

	for _, vol := range desired {
		if _, exists := currentMap[vol.Name]; !exists {
			return false
		}
		// Note: We're doing basic name comparison here. For more sophisticated
		// comparison, we could compare volume sources, but this is sufficient
		// for our use case during cluster formation
	}

	return true
}

// initContainersEqual reports whether the init containers the cluster renders
// are on the live template, in order, with their images. One another writer
// added (neither desired nor in previousOwned — a VerifiedDownload plugin's
// plugin-init-<name>) is not a change, wherever it sits; one the cluster
// rendered before and no longer does is.
func (r *Neo4jEnterpriseClusterReconciler) initContainersEqual(current, desired []corev1.Container, previousOwned map[string]struct{}) bool {
	current = withoutForeign(current, desired, previousOwned, containerNameOf)
	if len(current) != len(desired) {
		return false
	}

	for i, currentContainer := range current {
		if i >= len(desired) {
			return false
		}
		desiredContainer := desired[i]

		// Compare names and images
		if currentContainer.Name != desiredContainer.Name ||
			currentContainer.Image != desiredContainer.Image {
			return false
		}
	}

	return true
}

func (r *Neo4jEnterpriseClusterReconciler) updateClusterStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, phase, message string) bool {
	return r.updateClusterStatusWithVersion(ctx, cluster, phase, message, "")
}

// updateClusterStatusWithVersion is updateClusterStatus plus an optional
// status.version write, folded into the SAME refetch + RetryOnConflict closure.
// When version != "" it is written onto the freshly-Get'd `latest` object —
// never on the caller's in-memory `cluster`, whose resourceVersion may already
// be stale from an earlier status write in the same reconcile (the rolling-
// upgrade completion path did exactly that, so the bare follow-up
// Status().Update conflicted and the version bump was silently dropped — #207).
//
// extra conditions (ClusterFormed, Degraded) are upserted in the same write.
// They force a write when they differ but do not count towards the returned
// "changed", which callers read as "the phase, message or Ready condition
// moved" and announce with an event.
func (r *Neo4jEnterpriseClusterReconciler) updateClusterStatusWithVersion(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, phase, message, version string, extra ...metav1.Condition) bool {
	return r.updateClusterStatusRecording(ctx, cluster, phase, message, statusRecord{version: version}, extra...)
}

// statusRecord is what a cluster status write records besides the phase. An
// empty version, or a nil pointer, leaves that field as it is.
type statusRecord struct {
	// version is status.version, the image tag every server runs.
	version string
	// runningVersion is status.runningVersion: what the servers report for a
	// status.version without a patch, "" to clear it (operations.md id 153).
	runningVersion *string
	// resolvedTargetVersion is status.upgradeStatus.resolvedTargetVersion,
	// written only when there is an upgrade record.
	resolvedTargetVersion *string
	// outcome, when set, receives what became of the two above in the write
	// that succeeded.
	outcome *reportedVersionsOutcome
}

// reportedVersionsOutcome is what a status write did with the versions the
// servers report (statusRecord).
type reportedVersionsOutcome struct {
	// sent: the write carried a change of them.
	sent bool
	// dropped: the stored object came back without one of them — the
	// installed CRD does not have the field (this release's CRDs are not
	// applied), and the API server pruned it.
	dropped bool
}

// updateClusterStatusRecording is updateClusterStatusWithVersion with the
// versions the servers report (statusRecord), in the same write.
//
// status.observedGeneration, and the conditions' observedGeneration, is the
// generation this pass read at its start and acted on (cluster.Generation),
// not the one the write re-reads: a spec changed while the pass ran has not
// been acted on, and claiming it had read as "nothing pending" while a
// rollout of it was about to start.
func (r *Neo4jEnterpriseClusterReconciler) updateClusterStatusRecording(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, phase, message string, rec statusRecord, extra ...metav1.Condition) bool {
	return r.writeClusterStatus(ctx, cluster, phase, message, rec, extra...).changed
}

// clusterStatusWrite is what a cluster status write did.
type clusterStatusWrite struct {
	// changed: the phase, the message or the Ready condition moved — what a
	// caller announces with an event.
	changed bool
	// before is the status the write replaced (or found already right), read
	// in the attempt that succeeded, so it is what the API server held and
	// not this pass's copy, which the informer cache may have handed over
	// from before the last pass's write. nil when the write failed.
	before *neo4jv1beta1.Neo4jEnterpriseClusterStatus
}

// writeClusterStatus is updateClusterStatusRecording, reporting what the write
// replaced as well as whether it changed something. A caller that announces
// entering a phase reads it from before (operations.md id 136).
func (r *Neo4jEnterpriseClusterReconciler) writeClusterStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, phase, message string, rec statusRecord, extra ...metav1.Condition) clusterStatusWrite {
	logger := log.FromContext(ctx)
	statusChanged := false
	var before *neo4jv1beta1.Neo4jEnterpriseClusterStatus
	version := rec.version
	actedOn := cluster.Generation

	update := func() error {
		// Get latest version
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		before = latest.Status.DeepCopy()
		if actedOn == 0 {
			// An object built in memory rather than read from the API server.
			actedOn = latest.Generation
		}
		if rec.outcome != nil {
			*rec.outcome = reportedVersionsOutcome{} // this attempt's
		}

		// Check if status is already exactly what we want
		readyBool := (phase == "Ready")
		statusNeedsUpdate := latest.Status.Phase != phase || latest.Status.Message != message ||
			latest.Status.Ready != readyBool || latest.Status.ObservedGeneration != actedOn ||
			(version != "" && latest.Status.Version != version)
		// The versions the servers report are written when they change; they
		// do not count as a state change to announce.
		versionsNeedUpdate := (rec.runningVersion != nil && latest.Status.RunningVersion != *rec.runningVersion) ||
			(rec.resolvedTargetVersion != nil && latest.Status.UpgradeStatus != nil &&
				latest.Status.UpgradeStatus.ResolvedTargetVersion != *rec.resolvedTargetVersion)

		// Determine standard condition status and reason
		condStatus, condReason := PhaseToConditionStatus(phase)
		conditionNeedsUpdate := true

		// Check existing condition against standardized values
		for _, cond := range latest.Status.Conditions {
			if cond.Type == ConditionTypeReady {
				if cond.Status == condStatus && cond.Reason == condReason && cond.Message == message {
					conditionNeedsUpdate = false
				}
				break
			}
		}

		extraNeedsUpdate := false
		for _, want := range extra {
			if have := findCondition(latest.Status.Conditions, want.Type); have == nil ||
				have.Status != want.Status || have.Reason != want.Reason || have.Message != want.Message {
				extraNeedsUpdate = true
			}
		}

		// If neither status nor condition needs update, skip entirely
		if !statusNeedsUpdate && !conditionNeedsUpdate && !extraNeedsUpdate && !versionsNeedUpdate {
			logger.V(1).Info("Status and condition already correct, skipping update",
				"phase", phase, "message", message)
			statusChanged = false
			return nil
		}

		// Log what we're updating
		logger.V(1).Info("Updating cluster status",
			"phase", phase, "message", message,
			"statusNeedsUpdate", statusNeedsUpdate,
			"conditionNeedsUpdate", conditionNeedsUpdate)

		// Update status fields
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.Ready = readyBool
		latest.Status.ObservedGeneration = actedOn
		if version != "" {
			latest.Status.Version = version
		}
		if rec.runningVersion != nil {
			latest.Status.RunningVersion = *rec.runningVersion
		}
		if rec.resolvedTargetVersion != nil && latest.Status.UpgradeStatus != nil {
			latest.Status.UpgradeStatus.ResolvedTargetVersion = *rec.resolvedTargetVersion
		}

		// Populate connection endpoints + connection examples. Cluster
		// previously left status.endpoints unset; surfacing these lets users
		// `kubectl get neo4jenterprisecluster -o jsonpath='{.status.endpoints.connectionExamples.boltURI}'`
		// to copy a working connection string. Service-type and external-IP
		// resolution mirrors what the connection_helper helpers already do
		// for the standalone path.
		latest.Status.Endpoints = r.buildClusterEndpoints(ctx, cluster)

		// Ready-to-paste in-cluster address list for a same-Kubernetes-cluster
		// downstream Neo4jReplicaDatabase (source.upstreamClusterRef reads this
		// directly). Pure function of spec — needs no proxy, no LoadBalancer,
		// always populated, same as Endpoints above.
		latest.Status.InternalAddresses = resources.ServerClusterAddresses(cluster)

		// Replica count (desired vs ready). Pulls Ready directly off the
		// server StatefulSet status — the controller-runtime cache already
		// has the StatefulSet, so this is a free lookup.
		latest.Status.Replicas = r.buildReplicaStatus(ctx, cluster)

		// Update Ready condition using standard helper
		SetReadyCondition(&latest.Status.Conditions, actedOn, condStatus, condReason, message)
		for _, c := range extra {
			SetNamedCondition(&latest.Status.Conditions, c.Type, actedOn, c.Status, c.Reason, c.Message)
		}

		// Record Prometheus phase metric on every phase transition
		clusterM := metrics.NewClusterMetrics(cluster.Name, cluster.Namespace)
		clusterM.RecordClusterPhase(phase)
		switch phase {
		case "Ready":
			clusterM.RecordClusterHealth(true)
			var ready int32
			if latest.Status.Replicas != nil {
				ready = latest.Status.Replicas.Ready
			}
			clusterM.RecordClusterReplicas(cluster.Spec.Topology.Servers, ready)
		case "Failed", "Degraded":
			clusterM.RecordClusterHealth(false)
		case "Forming":
			clusterM.RecordClusterHealth(false)
		}

		statusChanged = statusNeedsUpdate || conditionNeedsUpdate
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		if rec.outcome != nil {
			// The update returns the object as stored: without a field the
			// installed CRD lacks.
			*rec.outcome = reportedVersionsOutcome{
				sent: versionsNeedUpdate,
				dropped: (rec.runningVersion != nil && latest.Status.RunningVersion != *rec.runningVersion) ||
					(rec.resolvedTargetVersion != nil && latest.Status.UpgradeStatus != nil &&
						latest.Status.UpgradeStatus.ResolvedTargetVersion != *rec.resolvedTargetVersion),
			}
		}
		return nil
	}

	err := retry.RetryOnConflict(retry.DefaultBackoff, update)
	if err != nil {
		logger.Error(err, "Failed to update cluster status")
		return clusterStatusWrite{}
	}
	return clusterStatusWrite{changed: statusChanged, before: before}
}

// buildReplicaStatus reports the desired vs ready server count by reading
// the live `{cluster}-server` StatefulSet's status.readyReplicas. Returns
// nil if the StatefulSet doesn't exist yet (early in reconcile, before the
// resource builder has run) — better than reporting fictional zeros.
func (r *Neo4jEnterpriseClusterReconciler) buildReplicaStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *neo4jv1beta1.ReplicaStatus {
	sts := &appsv1.StatefulSet{}
	stsKey := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      fmt.Sprintf("%s-server", cluster.Name),
	}
	if err := r.Get(ctx, stsKey, sts); err != nil {
		return nil
	}
	return &neo4jv1beta1.ReplicaStatus{
		Servers: cluster.Spec.Topology.Servers,
		Ready:   sts.Status.ReadyReplicas,
	}
}

// buildClusterEndpoints assembles the EndpointStatus surfaced via
// status.endpoints. Returns the in-cluster {cluster}-client Service URLs
// scheme-adjusted for TLS, plus the connection-example helper output keyed
// off the configured Service type. External-IP resolution looks at the live
// Service status and substitutes a `<external-ip>` placeholder when no IP
// has been assigned yet.
func (r *Neo4jEnterpriseClusterReconciler) buildClusterEndpoints(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *neo4jv1beta1.EndpointStatus {
	hasTLS := cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == "cert-manager"
	boltScheme := "bolt"
	if hasTLS {
		boltScheme = "bolt+s"
	}

	serviceType := corev1.ServiceTypeClusterIP
	if cluster.Spec.Service != nil && cluster.Spec.Service.Type != "" {
		serviceType = corev1.ServiceType(cluster.Spec.Service.Type)
	}
	externalIP := clusterServiceExternalIP(ctx, r.Client, cluster)

	return &neo4jv1beta1.EndpointStatus{
		Bolt:  fmt.Sprintf("%s://%s-client.%s.svc.cluster.local:7687", boltScheme, cluster.Name, cluster.Namespace),
		HTTP:  fmt.Sprintf("http://%s-client.%s.svc.cluster.local:7474", cluster.Name, cluster.Namespace),
		HTTPS: fmt.Sprintf("https://%s-client.%s.svc.cluster.local:7473", cluster.Name, cluster.Namespace),
		Internal: &neo4jv1beta1.InternalEndpoints{
			Headless: fmt.Sprintf("%s-headless.%s.svc.cluster.local", cluster.Name, cluster.Namespace),
			Client:   fmt.Sprintf("%s-client.%s.svc.cluster.local", cluster.Name, cluster.Namespace),
		},
		ConnectionExamples: GenerateConnectionExamples(cluster.Name, cluster.Namespace, serviceType, externalIP, hasTLS),
	}
}

// createExternalSecretForTLS creates an ExternalSecret resource for TLS certificates
func (r *Neo4jEnterpriseClusterReconciler) createExternalSecretForTLS(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	esData := resources.BuildExternalSecretForTLS(cluster)
	if esData == nil {
		return nil
	}

	// Convert map to unstructured object
	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(esData)

	return r.createOrUpdateUnstructuredResource(ctx, obj, cluster)
}

// createExternalSecretForAuth creates an ExternalSecret resource for authentication secrets
func (r *Neo4jEnterpriseClusterReconciler) createExternalSecretForAuth(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	esData := resources.BuildExternalSecretForAuth(cluster)
	if esData == nil {
		return nil
	}

	// Convert map to unstructured object
	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(esData)

	return r.createOrUpdateUnstructuredResource(ctx, obj, cluster)
}

// reconcileMutableResource creates desired if absent, otherwise updates the
// live object's mutable fields from desired and writes it back — preserving the
// live ResourceVersion and any server-assigned immutable fields. Each Update is
// gated on an actual drift so unchanged resources don't churn ResourceVersion.
func (r *Neo4jEnterpriseClusterReconciler) reconcileMutableResource(ctx context.Context, desired client.Object) error {
	logger := log.FromContext(ctx)
	existing, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("object %T is not a client.Object", desired)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, desired)
		}
		return err
	}

	switch d := desired.(type) {
	case *corev1.Service:
		e, ok := existing.(*corev1.Service)
		if !ok {
			return fmt.Errorf("existing object %T does not match desired *corev1.Service", existing)
		}
		if applyDesiredServiceFields(e, d) {
			logger.Info("Updating Service", "name", e.Name)
			return r.Update(ctx, e)
		}
	case *networkingv1.NetworkPolicy:
		e, ok := existing.(*networkingv1.NetworkPolicy)
		if !ok {
			return fmt.Errorf("existing object %T does not match desired *networkingv1.NetworkPolicy", existing)
		}
		changed := false
		if !equality.Semantic.DeepEqual(e.Spec, d.Spec) {
			e.Spec = d.Spec
			changed = true
		}
		if applyOwnedMetadata(e, d.Annotations, d.Labels) {
			changed = true
		}
		if changed {
			logger.Info("Updating NetworkPolicy", "name", e.Name)
			return r.Update(ctx, e)
		}
	case *networkingv1.Ingress:
		e, ok := existing.(*networkingv1.Ingress)
		if !ok {
			return fmt.Errorf("existing object %T does not match desired *networkingv1.Ingress", existing)
		}
		changed := false
		if !equality.Semantic.DeepEqual(e.Spec, d.Spec) {
			e.Spec = d.Spec
			changed = true
		}
		if applyOwnedMetadata(e, d.Annotations, d.Labels) {
			changed = true
		}
		if changed {
			logger.Info("Updating Ingress", "name", e.Name)
			return r.Update(ctx, e)
		}
	case *certmanagerv1.Certificate:
		e, ok := existing.(*certmanagerv1.Certificate)
		if !ok {
			return fmt.Errorf("existing object %T does not match desired *certmanagerv1.Certificate", existing)
		}
		changed := false
		if !equality.Semantic.DeepEqual(e.Spec, d.Spec) {
			e.Spec = d.Spec
			changed = true
		}
		if applyOwnedMetadata(e, d.Annotations, d.Labels) {
			changed = true
		}
		if changed {
			logger.Info("Updating Certificate", "name", e.Name)
			return r.Update(ctx, e)
		}
	case *corev1.ServiceAccount:
		// The discovery SA carries spec.podServiceAccountAnnotations (cloud
		// Workload Identity). Merge additively: the operator owns only those
		// keys (removals honored via the owned-keys bookkeeping) and preserves
		// annotations added out-of-band — e.g. an IRSA annotation a user set
		// manually before this field existed, or another controller's.
		e, ok := existing.(*corev1.ServiceAccount)
		if !ok {
			return fmt.Errorf("expected *corev1.ServiceAccount, got %T", existing)
		}
		if applyOwnedMetadata(e, d.Annotations, d.Labels) {
			logger.Info("Updating ServiceAccount annotations", "name", e.Name)
			return r.Update(ctx, e)
		}
	}
	return nil
}

// applyDesiredServiceFields copies the mutable fields of desired onto existing,
// preserving API-server-assigned immutable fields (ClusterIP/ClusterIPs/
// IPFamilies/IPFamilyPolicy/HealthCheckNodePort) so the Update is not rejected,
// and carrying over allocated NodePorts the desired spec leaves unset. Returns
// true when any field changed.
func applyDesiredServiceFields(existing, desired *corev1.Service) bool {
	changed := false
	if existing.Spec.Type != desired.Spec.Type {
		existing.Spec.Type = desired.Spec.Type
		changed = true
	}
	if mergedPorts := mergeServicePorts(existing.Spec.Ports, desired.Spec.Ports); !equality.Semantic.DeepEqual(existing.Spec.Ports, mergedPorts) {
		existing.Spec.Ports = mergedPorts
		changed = true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Selector = desired.Spec.Selector
		changed = true
	}
	if existing.Spec.LoadBalancerIP != desired.Spec.LoadBalancerIP {
		existing.Spec.LoadBalancerIP = desired.Spec.LoadBalancerIP
		changed = true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.LoadBalancerSourceRanges, desired.Spec.LoadBalancerSourceRanges) {
		existing.Spec.LoadBalancerSourceRanges = desired.Spec.LoadBalancerSourceRanges
		changed = true
	}
	if existing.Spec.ExternalTrafficPolicy != desired.Spec.ExternalTrafficPolicy {
		existing.Spec.ExternalTrafficPolicy = desired.Spec.ExternalTrafficPolicy
		changed = true
	}
	if existing.Spec.SessionAffinity != desired.Spec.SessionAffinity {
		existing.Spec.SessionAffinity = desired.Spec.SessionAffinity
		changed = true
	}
	if existing.Spec.PublishNotReadyAddresses != desired.Spec.PublishNotReadyAddresses {
		existing.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		changed = true
	}
	if applyOwnedMetadata(existing, desired.Annotations, desired.Labels) {
		changed = true
	}
	return changed
}

// mergeServicePorts returns desired ports with allocated NodePorts carried over
// from existing for matching ports (by Name, else by Port) when desired leaves
// NodePort unset — so a ClusterIP→NodePort/LoadBalancer transition or a routine
// reconcile doesn't ask the API to reassign node ports.
func mergeServicePorts(existing, desired []corev1.ServicePort) []corev1.ServicePort {
	byName := make(map[string]int32, len(existing))
	byPort := make(map[int32]int32, len(existing))
	for _, p := range existing {
		if p.NodePort != 0 {
			if p.Name != "" {
				byName[p.Name] = p.NodePort
			}
			byPort[p.Port] = p.NodePort
		}
	}
	out := make([]corev1.ServicePort, len(desired))
	copy(out, desired)
	for i := range out {
		if out[i].NodePort != 0 {
			continue
		}
		if np, ok := byName[out[i].Name]; ok && out[i].Name != "" {
			out[i].NodePort = np
		} else if np, ok := byPort[out[i].Port]; ok {
			out[i].NodePort = np
		}
	}
	return out
}

// createOrUpdateUnstructuredResource handles unstructured resources like ExternalSecrets
func (r *Neo4jEnterpriseClusterReconciler) createOrUpdateUnstructuredResource(ctx context.Context, obj *unstructured.Unstructured, owner client.Object) error {
	// Set owner reference
	if err := controllerutil.SetControllerReference(owner, obj, r.Scheme); err != nil {
		return err
	}

	// Try to get the existing resource
	key := client.ObjectKeyFromObject(obj)
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())

	err := r.Get(ctx, key, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create the resource
			return r.Create(ctx, obj)
		}
		return err
	}

	// Update the resource
	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// isUpgradeRequired checks if an image upgrade is needed
func (r *Neo4jEnterpriseClusterReconciler) isUpgradeRequired(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	// Skip upgrade check if cluster is not ready — including a cluster that
	// is still Ready while a server is down (#444): rolling the others then
	// could cost it its majority. holdImageDriftUntilReady keeps the image
	// change held until every server is back. The one exception: a held
	// upgrade whose restarted servers cannot start on its target, and a new
	// tag — only the machine's write of it brings them back, and it restarts
	// nothing that is up (heldUpgradeRepair, operations.md id 162).
	if cluster.Status.Phase != "Ready" || serversUnavailableSince(cluster) != nil {
		return r.heldUpgradeRepair(ctx, cluster) != nil
	}

	// "Paused" is an explicit manual hold (autoPauseOnFailure) — never
	// auto-resumed. It goes on when the user changes spec.image.tag (the drift
	// below) or turns autoPauseOnFailure off (the retry at the end); until
	// then the servers it has not rolled stay where they are (upgradeHeld,
	// operations.md id 149).
	if upgradeWaitsForUser(cluster) {
		return false
	}
	// Mid-flight phases (Staging/Rolling/Stabilizing/Verifying and the legacy
	// "InProgress" marker) are handled by upgradeStateMachineActive at the
	// Reconcile entry — this function only decides whether to START a new
	// upgrade from image drift.

	// Check the unified server StatefulSet for image drift
	serverSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, serverSts); err != nil {
		return false // StatefulSet doesn't exist yet
	}

	// Compare current image with desired image
	if len(serverSts.Spec.Template.Spec.Containers) == 0 {
		return false // StatefulSet has no containers defined
	}
	currentImage := serverSts.Spec.Template.Spec.Containers[0].Image
	desiredImage := fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	if currentImage != desiredImage {
		// An attempt at this image that a check refused before it staged
		// anything is tried again once upgradeStartRetry has passed, not on
		// every pass: the normal pass runs in between, with the change held
		// (refuseUpgradeStart). Retried on every pass, it starved the normal
		// pass; failed and then made Ready again, it flapped (#538).
		return !startRefusedRecently(cluster.Status.UpgradeStatus, cluster.Spec.Image.Tag, time.Now())
	}

	// Failed-upgrade retry: once Staging has run, the template already carries
	// the target image, so the drift check above can never re-fire — a Failed
	// upgrade (e.g. a transient post-upgrade verification failure) would stay
	// Failed forever with status.version never stamped. Re-enter while the
	// spec still wants the same target the failed attempt was driving toward
	// and the version was never verified. Level-based retry; users opt out via
	// upgradeStrategy.autoPauseOnFailure (a Paused upgrade that still waits
	// for the user returned above; one whose autoPauseOnFailure has since
	// been turned off is retried here like a Failed one). The partition the
	// failed attempt left has held the servers it had not rolled, so the
	// retry rolls them through the machine's gates (operations.md id 149).
	us := cluster.Status.UpgradeStatus
	if !upgradeHeld(cluster) || us.TargetVersion != cluster.Spec.Image.Tag ||
		cluster.Status.Version == cluster.Spec.Image.Tag {
		return false
	}
	// The retry waits until every server is available, as for a Paused
	// upgrade: the normal pass runs meanwhile and records the shortfall — the
	// Degraded condition, then, past the grace, the Degraded phase — which the
	// check at the top then reads. The status cannot say so on its own: the
	// upgrade stopped on a cluster that read Ready with every server available,
	// and a retry on every pass kept the normal pass from running at all, so a
	// server that could not start on the target stayed hidden behind
	// Degraded=False "All 3 servers are available" (the walk of be1e0a6f,
	// after #565 kept a stopped upgrade's cluster Ready). A server pod
	// that is not Ready is one (the retry's first gate,
	// validateStatefulSetsReady, would only wait for it); a server the
	// pre-upgrade check finds unavailable refuses the retry, which is not tried
	// again for upgradeStartRetry, so the normal pass runs in between and
	// records it.
	if serverSts.Spec.Replicas == nil || serverSts.Status.ReadyReplicas < *serverSts.Spec.Replicas {
		return false
	}
	return !startRefusedRecently(us, cluster.Spec.Image.Tag, time.Now())
}

// handleRollingUpgrade dispatches one state-machine step per reconcile (#174).
// Each step is non-blocking (single observation + at most one mutation +
// RequeueAfter), so an upgrading cluster never holds a worker other clusters
// are waiting for. The phases and
// step implementations live in rolling_upgrade_statemachine.go.
func (r *Neo4jEnterpriseClusterReconciler) handleRollingUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("rolling-upgrade-handler")

	// Every cluster upgrades through this machine, whatever
	// spec.upgradeStrategy.strategy says. Recreate used to return here without
	// starting it: the "regular reconciliation" it claimed to fall back to never
	// ran (Reconcile returns what this returns), so a Ready cluster kept its old
	// image and no other spec change was applied while the tag differed. Neo4j
	// documents only rolling upgrades for a cluster, so Recreate is accepted
	// with a ValidationWarning and does what RollingUpgrade does
	// (operations.md id 144).

	phase := ""
	if cluster.Status.UpgradeStatus != nil {
		phase = cluster.Status.UpgradeStatus.Phase
	}

	// Target changed mid-flight (user edited the tag again): re-stage from ANY
	// active phase, not just Rolling — a tag change during Stabilizing or
	// Verifying would otherwise verify the new spec tag against servers
	// running the old target, burning a full deadline cycle into Failed before
	// converging. Staging is idempotent; already-on-target pods re-verify
	// instantly during the walk. (LegacyInProgress is excluded: it routes to
	// Staging below regardless, and its TargetVersion may predate the field.)
	if upgradeStateMachineActive(cluster) && phase != upgradePhaseLegacyInProgress &&
		cluster.Status.UpgradeStatus.TargetVersion != cluster.Spec.Image.Tag {
		// What the current target started a server on, read before it is left
		// behind; and the new tag held to it, since validation may have read the
		// record before that server started (operations.md id 149). A refused
		// tag waits: the next pass's validation reports it.
		r.recordStartedTarget(ctx, cluster)
		if errs := validation.NewUpgradeValidator().ValidateTagAgainstRunning(cluster); len(errs) > 0 {
			logger.Info("Not re-staging: the new target is below what a server has started on", "reason", errs.ToAggregate().Error())
			return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
		}
		logger.Info("Upgrade target changed mid-flight; re-staging",
			"previousTarget", cluster.Status.UpgradeStatus.TargetVersion, "newTarget", cluster.Spec.Image.Tag)
		now := metav1.Now()
		newTarget := cluster.Spec.Image.Tag
		// Announced from the write that records the new target, once per
		// target (operations.md id 136): a pass whose cached copy still names
		// the target being left writes the same record again and says nothing.
		var announce bool
		var previous, during string
		if err := r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
			announce, previous, during = us.TargetVersion != newTarget, us.TargetVersion, us.Phase
			us.Phase = upgradePhaseStaging
			us.TargetVersion = newTarget
			us.StepStartTime = &now
			us.CurrentStep = "Re-staging: upgrade target changed mid-flight"
			us.Message = us.CurrentStep
		}); err != nil {
			logger.Error(err, "Failed to persist re-staging transition")
		} else {
			r.noteUpgradeInClusterMessage(ctx, cluster)
			if announce {
				r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeRetargeted,
					"Rolling upgrade retargeted from %s to %s during %s; the new target is staged", previous, newTarget, during)
			}
		}
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	}

	switch phase {
	case upgradePhaseStaging:
		return r.stepUpgradeStaging(ctx, cluster)
	case upgradePhaseRolling:
		return r.stepUpgradeRolling(ctx, cluster)
	case upgradePhaseStabilizing:
		return r.stepUpgradeStabilizing(ctx, cluster)
	case upgradePhaseVerifying:
		return r.stepUpgradeVerifying(ctx, cluster)
	case upgradePhaseLegacyInProgress:
		// An older operator version persisted its blocking-loop marker and was
		// interrupted. Resume as Staging: it is idempotent (re-applies the
		// target template, re-freezes the partition) and already-rolled pods
		// re-verify instantly during the walk.
		logger.Info("Resuming interrupted legacy upgrade as Staging")
		now := metav1.Now()
		if err := r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
			us.Phase = upgradePhaseStaging
			us.StepStartTime = &now
			us.CurrentStep = "Resuming interrupted upgrade"
		}); err != nil {
			logger.Error(err, "Failed to persist legacy upgrade resume")
		}
		return ctrl.Result{RequeueAfter: upgradeTransitionRequeue}, nil
	default:
		// No active state — image drift detected by isUpgradeRequired.
		return r.startRollingUpgrade(ctx, cluster)
	}
}

// createNeo4jClient creates a Neo4j client for cluster operations
func (r *Neo4jEnterpriseClusterReconciler) createNeo4jClient(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jclient.Client, error) {
	// Get admin credentials
	adminSecretName := DefaultAdminSecretName
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.AdminSecret != "" {
		adminSecretName = cluster.Spec.Auth.AdminSecret
	}

	// Create Neo4j client
	neo4jClient, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, adminSecretName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Neo4j client: %w", err)
	}

	return neo4jClient, nil
}

// formationCheck is the result of verifyNeo4jClusterFormation.
type formationCheck struct {
	formed  bool
	message string
	// servers is SHOW SERVERS as seen through the client Service by the
	// connectivity test; nil when Neo4j could not be reached. It is what a
	// cluster that is not fully formed is assessed on (#444).
	servers []neo4jclient.ServerInfo
	// splitBrain is set when the shortfall is a detected split-brain, which
	// stays Forming rather than being treated as a missing server.
	splitBrain bool
}

// verifyNeo4jClusterFormation checks if Neo4j cluster formation is complete and detects split-brain scenarios
func (r *Neo4jEnterpriseClusterReconciler) verifyNeo4jClusterFormation(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) formationCheck {
	logger := log.FromContext(ctx)

	// Skip verification for single-server clusters (always formed)
	expectedServers := int(cluster.Spec.Topology.Servers)
	if expectedServers == 1 {
		return formationCheck{formed: true, message: "Single server cluster - formation complete"}
	}

	// First, check if Neo4j is ready to accept connections using legacy check
	// Only run split-brain detection if we can connect to Neo4j
	canConnect := false
	var connectError error
	var testError error
	var servers []neo4jclient.ServerInfo
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err == nil {
		// Test the connection by trying to get server list
		list, testErr := neo4jClient.GetServerList(ctx)
		neo4jClient.Close()
		servers = list
		if testErr == nil {
			canConnect = true
		} else {
			testError = testErr
		}
	} else {
		connectError = err
	}

	if !canConnect {
		// Neo4j is not ready to accept connections yet, wait for it.
		// Log the resolved target and the consecutive-failure streak so a
		// persistent outage (#263) is diagnosable from operator logs alone:
		// a streak of 1-2 during a rolling restart is expected; a streak
		// climbing for minutes against a settled cluster is the incident.
		streak := r.recordConnectivityFailure(cluster)
		// A member that never starts looks identical to a slow one from the Bolt
		// side, so name the pod-level cause when there is one. Without this the
		// only signal is ConnectivityDegraded ("connection refused"), which points
		// at networking — a crash-looping container (bad JVM flags, OOM, image
		// problem) reads as a hung reconcile and sends people to the wrong place.
		podIssues := r.diagnoseUnhealthyMembers(ctx, cluster)
		logger.Info("Neo4j not ready to accept connections, waiting...",
			"targetURI", neo4jclient.ConnectionURIForEnterprise(cluster),
			"clientError", connectError, "testError", testError,
			"consecutiveFailures", streak.count,
			"failingSince", streak.since.Format(time.RFC3339),
			"memberPods", podIssues)
		if r.Recorder != nil && streak.dueForEvent(time.Now()) {
			msg := fmt.Sprintf("Operator has failed to reach Neo4j at %s for %d consecutive reconciles (since %s); last error: %v",
				neo4jclient.ConnectionURIForEnterprise(cluster), streak.count,
				streak.since.Format(time.RFC3339), firstNonNilErr(testError, connectError))
			if podIssues != "" {
				msg += "; " + podIssues
			}
			r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonConnectivityDegraded, msg)
		}
		if podIssues != "" {
			// Surface it on the CR too — this is the string `kubectl describe`
			// and `kubectl get` show, and it is where a user looks first.
			return formationCheck{message: "Waiting for Neo4j to accept connections; " + podIssues}
		}
		return formationCheck{message: "Waiting for Neo4j to accept connections"}
	}
	if servers == nil {
		// Connected, and SHOW SERVERS returned no rows: an empty list, not
		// "could not query".
		servers = []neo4jclient.ServerInfo{}
	}
	r.clearConnectivityFailures(ctx, cluster)

	// Use the reconciler's SplitBrainDetector if available, otherwise create a new one
	splitBrainDetector := r.SplitBrainDetector
	if splitBrainDetector == nil {
		splitBrainDetector = NewSplitBrainDetector(r.Client)
	}

	// A Ready cluster that is whole through the client Service, and whose
	// server pods have not changed since a full check found it whole, waits
	// for --split-brain-check-interval between full checks: each one opens a
	// Bolt connection to every server.
	skip, podsFingerprint := r.skipSplitBrainCheck(ctx, cluster, splitBrainDetector, servers)
	if skip {
		available := countAvailableServers(servers)
		logger.V(1).Info("Skipping split-brain detection: a recent check found the cluster whole and no server pod has changed",
			"available", available, "expected", expectedServers, "interval", r.SplitBrainCheckInterval)
		return formationCheck{formed: true, message: fmt.Sprintf("Cluster formation complete: %d/%d servers available", available, expectedServers), servers: servers}
	}
	// Only a check that finds the cluster whole is recorded below; any other
	// outcome leaves nothing, so the next pass checks again.
	r.splitBrainChecks.Delete(clusterKey(cluster))

	// Now perform split-brain detection since Neo4j is responsive
	logger.Info("Neo4j is responsive, performing split-brain detection")
	analysis, err := splitBrainDetector.DetectSplitBrain(ctx, cluster)
	if err != nil {
		logger.Error(err, "Failed to perform split-brain detection, falling back to legacy check")
		// Fall back to legacy cluster formation check
		isFormed, message := r.legacyClusterFormationCheck(ctx, cluster, expectedServers)
		logger.Info("Legacy cluster formation check result",
			"isFormed", isFormed, "message", message)
		return formationCheck{formed: isFormed, message: message, servers: servers}
	}

	logger.Info("Split-brain analysis results",
		"isSplitBrain", analysis.IsSplitBrain,
		"repairAction", analysis.RepairAction,
		"orphanedPods", len(analysis.OrphanedPods),
		"errorMessage", analysis.ErrorMessage)

	// Handle split-brain scenarios
	if analysis.IsSplitBrain {
		logger.Info("Split-brain detected in Neo4j cluster",
			"orphanedPods", analysis.OrphanedPods,
			"repairAction", analysis.RepairAction)

		// Record event about split-brain detection
		splitBrainMsg := r.reportSplitBrainDetected(cluster, analysis)

		// Attempt automatic repair if configured
		if analysis.RepairAction == RepairActionRestartPods {
			logger.Info("Attempting automatic split-brain repair by restarting orphaned pods",
				"orphanedPods", analysis.OrphanedPods)

			repairErr := splitBrainDetector.RepairSplitBrain(ctx, cluster, analysis)
			if repairErr != nil {
				logger.Error(repairErr, "Failed to repair split-brain automatically")
				r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonSplitBrainRepairFailed,
					"Automatic split-brain repair failed: %v", repairErr)
				return formationCheck{message: fmt.Sprintf("Split-brain repair failed: %v", repairErr), servers: servers, splitBrain: true}
			}

			r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonSplitBrainRepaired,
				"Split-brain automatically repaired by restarting orphaned pods")

			// After repair, cluster needs time to reform
			return formationCheck{message: "Split-brain repaired, waiting for cluster reformation", servers: servers, splitBrain: true}
		}

		// For other repair actions, report but don't auto-repair
		return formationCheck{message: splitBrainMsg, servers: servers, splitBrain: true}
	}

	// If no split-brain, check if cluster formation is complete
	if analysis.RepairAction == RepairActionWaitForming {
		return formationCheck{message: analysis.ErrorMessage, servers: servers}
	}

	// Verify we have the expected number of servers across all views
	if len(analysis.ClusterViews) > 0 && analysis.LargestCluster.ConnectionError == nil {
		availableServers := 0
		for _, server := range analysis.LargestCluster.Servers {
			if server.State == "Enabled" && server.Health == "Available" {
				availableServers++
			}
		}

		if availableServers >= expectedServers {
			r.recordCleanSplitBrainCheck(cluster, podsFingerprint, time.Now())
			return formationCheck{formed: true, message: fmt.Sprintf("Cluster formation complete: %d/%d servers available", availableServers, expectedServers), servers: servers}
		}
		return formationCheck{message: fmt.Sprintf("Cluster forming: %d/%d servers available", availableServers, expectedServers), servers: servers}
	}

	// Fall back to legacy cluster formation check if split-brain analysis was inconclusive
	logger.Info("Falling back to legacy cluster formation check after split-brain analysis")
	isFormed, message := r.legacyClusterFormationCheck(ctx, cluster, expectedServers)
	logger.Info("Final legacy cluster formation check result",
		"isFormed", isFormed, "message", message)
	return formationCheck{formed: isFormed, message: message, servers: servers}
}

// reconcileServerShortfall sets the status of a cluster that is not fully
// formed: not every server is available, or the server StatefulSet is not
// fully rolled. detail says which, and is carried into the messages (#444).
func (r *Neo4jEnterpriseClusterReconciler) reconcileServerShortfall(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, formation formationCheck, detail string) {
	formed := clusterHasFormed(cluster)
	in := shortfallInput{
		Formed:           formed,
		SplitBrain:       formation.splitBrain,
		Servers:          withKnownAddresses(cluster, formation.servers),
		Expected:         int(cluster.Spec.Topology.Servers),
		UnavailableSince: serversUnavailableSince(cluster),
		Now:              time.Now(),
		Grace:            r.serverUnavailableGrace(),
	}
	if formed {
		in.RolloutInFlight = r.serverRolloutInFlight(ctx, cluster)
		if formation.servers == nil {
			in.ReadyServerPods = r.readyServerPods(ctx, cluster)
			in.UnreachableSince = r.unreachableSince(cluster)
		}
	}
	a := assessServerShortfall(in)
	log.FromContext(ctx).V(1).Info("Cluster is not fully formed",
		"verdict", a.Verdict, "detail", detail, "available", a.Available, "expected", a.Expected,
		"formedBefore", formed, "rolloutInFlight", in.RolloutInFlight, "splitBrain", in.SplitBrain)

	// A formed cluster short of a server is an outage, not formation: its
	// messages say what Neo4j and Kubernetes report about the servers. detail
	// comes from the formation check ("Cluster formation in progress: 2/3
	// servers visible") or the rollout check ("Waiting for every server pod
	// to be updated…"), which describe neither, and both appeared while the
	// phase read Ready (#553). It stays the message of a cluster that is
	// Forming.
	var shortfall string
	switch a.Verdict {
	case verdictQuorumLost, verdictWithinGrace, verdictDegraded:
		shortfall = outageDetail(a, r.notReadyServerPods(ctx, cluster))
	}
	switch a.Verdict {
	case verdictUnmeasured:
		// Nothing is known to have changed, so nothing is written; the next
		// pass asks Neo4j again.

	case verdictQuorumLost:
		if r.firstAnnouncement(cluster, a.Verdict, false) {
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonClusterQuorumLost,
				"Neo4j cluster has lost a majority of its servers (%s); resources that depend on it pause until a majority is back", shortfall)
		}
		_ = r.updateClusterStatusWithVersion(ctx, cluster, neo4jv1beta1.PhaseForming,
			"Waiting for a majority of servers: "+shortfall, "",
			clusterFormedCondition(), serversUnavailableCondition(shortfall))

	case verdictWithinGrace:
		// Still serving: the phase, the Ready condition and every dependent
		// carry on. Only the Degraded condition and ServersHealthy say so.
		_ = r.updateClusterStatusWithVersion(ctx, cluster, neo4jv1beta1.PhaseReady,
			"Neo4j cluster is serving. "+shortfall, "",
			clusterFormedCondition(), serversUnavailableCondition(shortfall))

	case verdictDegraded:
		if r.firstAnnouncement(cluster, a.Verdict, cluster.Status.Phase == neo4jv1beta1.PhaseDegraded) {
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, EventReasonClusterDegraded,
				"Neo4j cluster has been short of servers for longer than %s: %s", in.Grace, shortfall)
		}
		_ = r.updateClusterStatusWithVersion(ctx, cluster, neo4jv1beta1.PhaseDegraded,
			fmt.Sprintf("Neo4j cluster is serving but degraded for longer than %s. %s", in.Grace, shortfall), "",
			clusterFormedCondition(), serversUnavailableCondition(shortfall))

	default:
		// First formation, a rollout or scale-up, a split-brain, or nothing
		// measurable. "Formation started" is only true the first time, and is
		// announced by the write that takes a cluster that never formed into
		// Forming, judged from what that write replaced. This pass's copy can
		// come from a cache that has not seen the last pass's write of Forming,
		// and judged from it every new cluster announced formation twice
		// (#553; operations.md id 136).
		w := r.writeClusterStatus(ctx, cluster, neo4jv1beta1.PhaseForming, detail, statusRecord{})
		if w.before != nil && w.before.Phase != neo4jv1beta1.PhaseForming &&
			!clusterHasFormed(&neo4jv1beta1.Neo4jEnterpriseCluster{Status: *w.before}) {
			r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonClusterFormationStarted,
				"Neo4j cluster formation started")
		}
	}
}

// reportSplitBrainDetected emits the SplitBrainDetected Warning event, counts
// the detection in metrics, and returns the text to use as the cluster's status
// message. The text comes from splitBrainMessage so the "Split-brain detected"
// prefix the detector already wrote is not repeated.
func (r *Neo4jEnterpriseClusterReconciler) reportSplitBrainDetected(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, analysis *SplitBrainAnalysis) string {
	msg := splitBrainMessage(analysis)
	r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonSplitBrainDetected, msg)
	metrics.RecordSplitBrainDetected(cluster.Name, cluster.Namespace)
	return msg
}

// legacyClusterFormationCheck performs the original cluster formation
// verification. It reports whether the cluster has formed and what to say if
// not; it has no error to return, because every way it can come back short —
// no connection, no server list — is the cluster not formed yet.
func (r *Neo4jEnterpriseClusterReconciler) legacyClusterFormationCheck(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, expectedServers int) (bool, string) {
	logger := log.FromContext(ctx)

	// Create Neo4j client to check cluster status
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		// If we can't connect to Neo4j yet, cluster is still forming
		return false, "Waiting for Neo4j to accept connections"
	}
	defer func() {
		if closeErr := neo4jClient.Close(); closeErr != nil {
			logger.Error(closeErr, "Failed to close Neo4j client")
		}
	}()

	// Check cluster formation using SHOW SERVERS with stability verification
	for attempt := 1; attempt <= 3; attempt++ {
		servers, err := neo4jClient.GetServerList(ctx)
		if err != nil {
			logger.Info("Cluster formation check failed", "attempt", attempt, "error", err)
			if attempt == 3 {
				// Final attempt failed
				return false, fmt.Sprintf("Cannot query server list after 3 attempts: %v", err)
			}
			// Wait 2 seconds before retry
			select {
			case <-ctx.Done():
				return false, "Context cancelled during cluster formation check"
			case <-time.After(2 * time.Second):
				continue
			}
		}

		// Count available servers
		availableServers := 0
		for _, server := range servers {
			if server.State == "Enabled" && server.Health == "Available" {
				availableServers++
			}
		}

		logger.Info("Cluster formation status",
			"attempt", attempt,
			"expectedServers", expectedServers,
			"availableServers", availableServers,
			"totalServers", len(servers))

		// Check if we have the expected number of servers
		if availableServers >= expectedServers {
			// Add a small stability delay to ensure service is consistently available
			if attempt == 1 {
				select {
				case <-ctx.Done():
					return false, "Context cancelled during stability check"
				case <-time.After(3 * time.Second):
					// Continue to verify stability
					continue
				}
			}
			// Cluster formation is complete and stable
			return true, fmt.Sprintf("Cluster formation complete and stable: %d/%d servers available", availableServers, expectedServers)
		}

		// Not enough servers, wait before retry
		if attempt < 3 {
			select {
			case <-ctx.Done():
				return false, "Context cancelled during cluster formation wait"
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}

	// Should not reach here, but handle gracefully
	return false, "Cluster formation verification incomplete"
}

// NewQueryMonitor creates a new query monitor
func NewQueryMonitor(client client.Client, scheme *runtime.Scheme) *QueryMonitor {
	return &QueryMonitor{
		Client: client,
		Scheme: scheme,
	}
}

// QueryMonitor handles query performance monitoring
type QueryMonitor struct {
	client.Client
	Scheme *runtime.Scheme
}

// ReconcileMonitoring sets up query monitoring for the cluster
func (qm *QueryMonitor) ReconcileMonitoring(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)
	logger.Info("Setting up query monitoring", "cluster", cluster.Name)

	// Set up metrics collection
	if err := qm.setupMetricsCollection(ctx, cluster); err != nil {
		return fmt.Errorf("failed to setup metrics: %w", err)
	}

	// Configure alerting rules if metrics export is enabled
	if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.MetricsExport != nil && cluster.Spec.Monitoring.MetricsExport.Prometheus {
		if err := qm.setupAlertingRules(ctx, cluster); err != nil {
			return fmt.Errorf("failed to setup alerting rules: %w", err)
		}
	}

	logger.Info("Query monitoring setup completed successfully")
	return nil
}

// setupMetricsCollection sets up metrics collection for the cluster
func (qm *QueryMonitor) setupMetricsCollection(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)
	logger.Info("Setting up metrics collection", "cluster", cluster.Name)

	// Create ServiceMonitor for Prometheus integration
	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "ServiceMonitor",
	})
	serviceMonitor.SetName(cluster.Name + "-monitoring")
	serviceMonitor.SetNamespace(cluster.Namespace)
	serviceMonitor.SetLabels(map[string]string{
		"app":               "neo4j",
		"neo4j.com/cluster": cluster.Name,
	})

	// Set ServiceMonitor spec. Unstructured content holds JSON types only:
	// lists are []any, never []map[string]any (operations.md id 148).
	serviceMonitor.Object["spec"] = map[string]any{
		"selector": map[string]any{
			"matchLabels": map[string]any{
				"app.kubernetes.io/name": "neo4j",
				"neo4j.com/cluster":      cluster.Name,
				"neo4j.com/role":         "metrics",
			},
		},
		"endpoints": []any{
			map[string]any{
				"port":     "metrics",
				"interval": "30s",
				"path":     "/metrics",
			},
		},
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(cluster, serviceMonitor, qm.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Create ServiceMonitor (ignore if CRD not available)
	if err := qm.Create(ctx, serviceMonitor); err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsNotFound(err) {
			logger.Info("ServiceMonitor creation failed (Prometheus Operator may not be installed)", "error", err.Error())
		}
	}

	logger.Info("Metrics collection setup completed")
	return nil
}

// setupAlertingRules sets up alerting rules for query monitoring
func (qm *QueryMonitor) setupAlertingRules(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)
	logger.Info("Setting up alerting rules", "cluster", cluster.Name)

	// Create PrometheusRule for alerting
	prometheusRule := &unstructured.Unstructured{}
	prometheusRule.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "PrometheusRule",
	})
	prometheusRule.SetName(cluster.Name + "-query-alerts")
	prometheusRule.SetNamespace(cluster.Namespace)
	prometheusRule.SetLabels(map[string]string{
		"app":               "neo4j",
		"neo4j.com/cluster": cluster.Name,
		"prometheus":        "kube-prometheus",
		"role":              "alert-rules",
	})

	// Generate alerting rules using actual Neo4j Prometheus metric names.
	// Neo4j exposes query latency as a histogram in milliseconds and heap as gauges in bytes.
	// []any, not []map[string]any: unstructured content holds JSON types only
	// (operations.md id 148).
	rules := []any{
		map[string]any{
			"alert": "Neo4jSlowQueries",
			"expr":  "histogram_quantile(0.99, rate(neo4j_db_query_execution_latency_millis_bucket[5m])) > 5000",
			"for":   "5m",
			"labels": map[string]any{
				"severity": "warning",
			},
			"annotations": map[string]any{
				"summary":     "Neo4j slow queries detected",
				"description": "Neo4j cluster {{ $labels.namespace }}/{{ $labels.job }} p99 query latency exceeds 5 seconds",
			},
		},
		map[string]any{
			"alert": "Neo4jHighHeapUsage",
			"expr":  "neo4j_vm_heap_used / neo4j_vm_heap_max > 0.8",
			"for":   "5m",
			"labels": map[string]any{
				"severity": "warning",
			},
			"annotations": map[string]any{
				"summary":     "Neo4j high heap usage",
				"description": "Neo4j cluster {{ $labels.namespace }}/{{ $labels.job }} JVM heap usage is above 80%",
			},
		},
	}

	prometheusRule.Object["spec"] = map[string]any{
		"groups": []any{
			map[string]any{
				"name":  "neo4j-monitoring",
				"rules": rules,
			},
		},
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(cluster, prometheusRule, qm.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Create PrometheusRule (ignore if CRD not available)
	if err := qm.Create(ctx, prometheusRule); err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsNotFound(err) {
			logger.Info("PrometheusRule creation failed (Prometheus Operator may not be installed)", "error", err.Error())
		}
	}

	logger.Info("Alerting rules setup completed")
	return nil
}

// CollectDiagnostics runs SHOW SERVERS and SHOW DATABASES against the cluster
// and writes the results into status.diagnostics and status.conditions.
// Non-blocking: all errors are surfaced in status but do not fail the reconciliation loop.
func (qm *QueryMonitor) CollectDiagnostics(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, neo4jClient *neo4jclient.Client) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Collecting cluster diagnostics", "cluster", cluster.Name)

	diagnostics := &neo4jv1beta1.ClusterDiagnosticsStatus{}

	// Collect server list
	servers, serverErr := neo4jClient.GetServerList(ctx)
	servers = withKnownAddresses(cluster, servers)
	if serverErr != nil {
		logDiagnosticsQueryFailure(logger, "SHOW SERVERS", serverErr)
		diagnostics.CollectionError = fmt.Sprintf("SHOW SERVERS failed: %v", serverErr)
	} else {
		for _, s := range servers {
			diagnostics.Servers = append(diagnostics.Servers, neo4jv1beta1.ServerDiagnosticInfo{
				Name:             s.Name,
				Address:          s.Address,
				State:            s.State,
				Health:           s.Health,
				HostingDatabases: len(s.Hosting),
			})
		}
		// Record per-server health metrics
		clusterM := metrics.NewClusterMetrics(cluster.Name, cluster.Namespace)
		serverHealthData := make([]metrics.ServerHealth, 0, len(servers))
		for _, s := range servers {
			serverHealthData = append(serverHealthData, metrics.ServerHealth{
				Name:      s.Name,
				Address:   s.Address,
				Enabled:   s.State == "Enabled",
				Available: s.Health == "Available",
			})
		}
		clusterM.RecordServerHealth(serverHealthData)
	}

	// Collect database list
	databases, dbErr := neo4jClient.GetDatabases(ctx)
	if dbErr != nil {
		logDiagnosticsQueryFailure(logger, "SHOW DATABASES", dbErr)
		if diagnostics.CollectionError == "" {
			diagnostics.CollectionError = fmt.Sprintf("SHOW DATABASES failed: %v", dbErr)
		} else {
			diagnostics.CollectionError += fmt.Sprintf("; SHOW DATABASES failed: %v", dbErr)
		}
	} else {
		diagnostics.Databases = append(diagnostics.Databases, toDatabaseDiagnostics(databases)...)
	}

	// Collect users + roles. Best-effort: if the connected user lacks
	// SHOW USER / SHOW ROLE privilege, we leave the lists empty rather than
	// failing the whole diagnostics pass.
	collectUsersAndRoles(ctx, neo4jClient,
		&diagnostics.Users, &diagnostics.UserCount,
		&diagnostics.Roles, &diagnostics.RoleCount,
		&diagnostics.CollectionError, logger)

	now := metav1.Now()
	diagnostics.LastCollected = &now
	sortServerDiagnostics(diagnostics.Servers)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := qm.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}

		before := latest.Status.DeepCopy()
		latest.Status.Diagnostics = diagnostics
		qm.updateServersCondition(latest, servers, serverErr)
		qm.updateDatabasesCondition(latest, databases, dbErr)

		// Only write what changed (#475): every status write is a watch event
		// that reconciles the cluster again at once, so writing a fresh
		// lastCollected on every pass kept a Ready cluster reconciling
		// continuously.
		if !clusterDiagnosticsWriteNeeded(before, &latest.Status, time.Now()) {
			return nil
		}
		return qm.Status().Update(ctx, latest)
	})
}

// diagnosticsRefreshInterval is how old status.diagnostics.lastCollected may
// get while nothing else in the diagnostics changed (#475).
const diagnosticsRefreshInterval = 5 * time.Minute

// diagnosticsStale reports whether a stored lastCollected is missing or older
// than diagnosticsRefreshInterval.
func diagnosticsStale(lastCollected *metav1.Time, now time.Time) bool {
	return lastCollected == nil || now.Sub(lastCollected.Time) >= diagnosticsRefreshInterval
}

// clusterDiagnosticsWriteNeeded reports whether a diagnostics pass changed the
// cluster's status in anything but lastCollected — the server and database
// lists, users, roles, the collection error, or the ServersHealthy /
// DatabasesHealthy conditions — or the stored lastCollected is stale.
func clusterDiagnosticsWriteNeeded(before, after *neo4jv1beta1.Neo4jEnterpriseClusterStatus, now time.Time) bool {
	var stored *metav1.Time
	if before.Diagnostics != nil {
		stored = before.Diagnostics.LastCollected
	}
	b, a := before.DeepCopy(), after.DeepCopy()
	for _, st := range []*neo4jv1beta1.Neo4jEnterpriseClusterStatus{b, a} {
		if st.Diagnostics != nil {
			st.Diagnostics.LastCollected = nil
			st.Diagnostics.Databases = withoutTransactionCounters(st.Diagnostics.Databases)
			sortServerDiagnostics(st.Diagnostics.Servers)
		}
	}
	return !equality.Semantic.DeepEqual(b, a) || diagnosticsStale(stored, now)
}

// sortServerDiagnostics orders the SHOW SERVERS rows by name, then address.
func sortServerDiagnostics(servers []neo4jv1beta1.ServerDiagnosticInfo) {
	sort.SliceStable(servers, func(i, j int) bool {
		if servers[i].Name != servers[j].Name {
			return servers[i].Name < servers[j].Name
		}
		return servers[i].Address < servers[j].Address
	})
}

// updateServersCondition sets the ServersHealthy condition from SHOW SERVERS results.
func (qm *QueryMonitor) updateServersCondition(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, servers []neo4jclient.ServerInfo, collectErr error) {
	if collectErr != nil {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeServersHealthy,
			cluster.Generation, metav1.ConditionUnknown,
			ConditionReasonDiagnosticsUnavailable,
			fmt.Sprintf("Could not collect server list: %v", collectErr))
		return
	}
	if len(servers) == 0 {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeServersHealthy,
			cluster.Generation, metav1.ConditionUnknown,
			ConditionReasonDiagnosticsUnavailable, "No servers returned by SHOW SERVERS")
		return
	}

	var degraded []string
	for _, s := range servers {
		if s.State != "Enabled" || s.Health != "Available" {
			degraded = append(degraded, fmt.Sprintf("%s (state=%s health=%s)", s.Name, s.State, s.Health))
		}
	}

	if len(degraded) > 0 {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeServersHealthy,
			cluster.Generation, metav1.ConditionFalse,
			ConditionReasonServerDegraded,
			fmt.Sprintf("%d server(s) unhealthy: %s", len(degraded), strings.Join(degraded, ", ")))
	} else {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeServersHealthy,
			cluster.Generation, metav1.ConditionTrue,
			ConditionReasonAllServersHealthy,
			fmt.Sprintf("All %d servers are Enabled and Available", len(servers)))
	}
}

// updateDatabasesCondition sets the DatabasesHealthy condition from SHOW DATABASES results.
func (qm *QueryMonitor) updateDatabasesCondition(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, databases []neo4jclient.DatabaseInfo, collectErr error) {
	if collectErr != nil {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeDatabasesHealthy,
			cluster.Generation, metav1.ConditionUnknown,
			ConditionReasonDiagnosticsUnavailable,
			fmt.Sprintf("Could not collect database list: %v", collectErr))
		return
	}
	if len(databases) == 0 {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeDatabasesHealthy,
			cluster.Generation, metav1.ConditionUnknown,
			ConditionReasonDiagnosticsUnavailable, "No databases returned by SHOW DATABASES")
		return
	}

	var offline []string
	userDBCount := 0
	for _, d := range databases {
		if d.Name == "system" {
			continue
		}
		userDBCount++
		if d.RequestedStatus == "online" && d.Status != "online" {
			offline = append(offline, fmt.Sprintf("%s (status=%s)", d.Name, d.Status))
		}
	}

	if len(offline) > 0 {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeDatabasesHealthy,
			cluster.Generation, metav1.ConditionFalse,
			ConditionReasonDatabaseOffline,
			fmt.Sprintf("%d database(s) not online: %s", len(offline), strings.Join(offline, ", ")))
	} else {
		SetNamedCondition(&cluster.Status.Conditions, ConditionTypeDatabasesHealthy,
			cluster.Generation, metav1.ConditionTrue,
			ConditionReasonAllDatabasesOnline,
			fmt.Sprintf("All %d user database(s) are online", userDBCount))
	}
}

// validatePropertyShardingConfiguration validates property sharding settings.
// Its errors say what is wrong and no more: Reconcile writes them after
// "Property sharding validation failed: ", and an error that said so too read
// "…validation failed: property sharding version validation failed: …"
// (#549; operations.md id 136).
func (r *Neo4jEnterpriseClusterReconciler) validatePropertyShardingConfiguration(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	// Validate Neo4j version supports property sharding (2025.12+)
	if err := validatePropertyShardingVersion(cluster.Spec.Image.Tag); err != nil {
		return err
	}

	// Validate minimum cluster size for property sharding
	if cluster.Spec.Topology.Servers < 1 {
		return fmt.Errorf("property sharding requires at least 1 server, got %d", cluster.Spec.Topology.Servers)
	}
	if cluster.Spec.Topology.Servers < 3 {
		logger.Info("Property sharding running without HA (recommended: 3+ graph shard primaries)",
			"servers", cluster.Spec.Topology.Servers)
	}

	// Validate required configuration settings
	// db.query.default_language is NOT required: shard sub-databases inherit
	// the parent database's language, and the sharded CREATE sets that
	// (design §5.9). It is the user's to set, via serverDefaultCypherLanguage.
	requiredSettings := map[string]string{
		"internal.dbms.sharded_property_database.enabled":                     "true",
		"internal.dbms.sharded_property_database.allow_external_shard_access": "false",
	}

	if cluster.Spec.PropertySharding.Config != nil {
		for key, expectedValue := range requiredSettings {
			if actualValue, exists := cluster.Spec.PropertySharding.Config[key]; exists && actualValue != expectedValue {
				return fmt.Errorf("property sharding requires %s=%s, got %s=%s", key, expectedValue, key, actualValue)
			}
		}
	} else {
		// If config is nil, create it with required settings
		logger.Info("Property sharding config is empty, will use default required settings")
	}

	// Validate resource requirements with lenient but realistic minimums.
	//
	// The 4GB hard floor is the operator's defensive minimum, not a Neo4j
	// JVM requirement. Below 4GB the JVM running multiple shards can OOM
	// or thrash under real workloads. For CI / smoke tests with empty
	// databases this is overly strict — the env var
	// `NEO4J_SHARDING_RELAX_MEMORY_MIN=true` downgrades the hard reject to
	// a warning so a minimal sharded smoke test can run in GitHub-hosted
	// runners (2-server × 1.5Gi clusters). Never set this in production.
	relaxMemoryMin := os.Getenv("NEO4J_SHARDING_RELAX_MEMORY_MIN") == "true"
	if cluster.Spec.Resources != nil && cluster.Spec.Resources.Requests != nil {
		if memory := cluster.Spec.Resources.Requests.Memory(); memory != nil {
			memoryMB := memory.Value() / (1024 * 1024)
			if memoryMB < 8192 { // 8GB recommended for production
				logger.Info("Property sharding memory below recommended 8GB for production, proceeding with caution",
					"requestedMB", memoryMB, "recommendedMB", 8192)
			}
			if memoryMB < 4096 { // 4GB absolute minimum for dev/test
				if relaxMemoryMin {
					logger.Info("Property sharding memory below 4GB minimum; relaxed via NEO4J_SHARDING_RELAX_MEMORY_MIN — DEV/TEST ONLY",
						"requestedMB", memoryMB)
				} else {
					return fmt.Errorf("property sharding requires minimum 4GB memory for basic operation, got %dMB (recommended: 8GB+ for production)", memoryMB)
				}
			}
		}

		// Check CPU requirements (lenient validation)
		if cpu := cluster.Spec.Resources.Requests.Cpu(); cpu != nil {
			cpuMillis := cpu.MilliValue()
			if cpuMillis < 2000 { // 2 cores recommended minimum
				logger.Info("Property sharding CPU below recommended 2+ cores, may impact cross-shard query performance",
					"requestedMillis", cpuMillis, "recommendedMillis", 2000)
			}
			if cpuMillis < 1000 { // 1 core absolute minimum
				if relaxMemoryMin {
					logger.Info("Property sharding CPU below 1-core minimum; relaxed via NEO4J_SHARDING_RELAX_MEMORY_MIN — DEV/TEST ONLY",
						"requestedMillis", cpuMillis)
				} else {
					return fmt.Errorf("property sharding requires minimum 1 CPU core, got %dm (recommended: 2+ cores)", cpuMillis)
				}
			}
		}
	}

	logger.Info("Property sharding configuration validation passed")
	return nil
}

// validatePropertyShardingVersion checks if Neo4j version supports property sharding
func validatePropertyShardingVersion(imageTag string) error {
	if imageTag == "" {
		return fmt.Errorf("image tag is required for property sharding")
	}

	if resources.IsNeo4jVersion202512OrHigher(imageTag) {
		return nil
	}

	return fmt.Errorf("property sharding requires Neo4j 2025.12+ Enterprise, got %s", imageTag)
}

// updatePropertyShardingStatus updates the property sharding ready status
func (r *Neo4jEnterpriseClusterReconciler) updatePropertyShardingStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, ready bool) error {
	logger := log.FromContext(ctx)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get latest version
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}

		// Update property sharding ready status
		if latest.Status.PropertyShardingReady == nil || *latest.Status.PropertyShardingReady != ready {
			latest.Status.PropertyShardingReady = &ready
			if err := r.Status().Update(ctx, latest); err != nil {
				return err
			}
			logger.Info("Updated property sharding readiness", "ready", ready)
		}

		return nil
	})
}

// reconcileRoute ensures an OpenShift Route exists when requested
func (r *Neo4jEnterpriseClusterReconciler) reconcileRoute(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	route := resources.BuildRouteForEnterprise(cluster)
	if route == nil {
		return nil
	}

	if err := controllerutil.SetControllerReference(cluster, route, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on route: %w", err)
	}

	desired := route.DeepCopy()

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, route, func() error {
		applyDesiredRoute(route, desired)
		return nil
	})
	// A cluster without the Route API stays without it: an advisory warning,
	// announced when it starts to apply, not on every pass (knowledge rules
	// 102 and 145).
	var noRouteAPI []string
	if err != nil && meta.IsNoMatchError(err) {
		noRouteAPI = append(noRouteAPI, "route.openshift.io/v1 not available; skipping Route reconciliation")
	}
	r.advisoryWarnings.recordScoped(r.Recorder, cluster, EventReasonRouteAPINotFound, "route", noRouteAPI)
	if err != nil {
		if meta.IsNoMatchError(err) {
			logger.Info("Route API not available; skipping Route reconciliation")
			return nil
		}
		return fmt.Errorf("failed to create or update Route: %w", err)
	}

	logger.Info("Successfully reconciled Route", "name", route.GetName())
	return nil
}

func (r *Neo4jEnterpriseClusterReconciler) reconcileMCP(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	if cluster.Spec.MCP == nil || !cluster.Spec.MCP.Enabled {
		return nil
	}

	if service := resources.BuildMCPServiceForCluster(cluster); service != nil {
		if err := r.createOrUpdateResource(ctx, service, cluster); err != nil {
			return fmt.Errorf("failed to reconcile MCP service: %w", err)
		}
	}

	if deployment := resources.BuildMCPDeploymentForCluster(cluster); deployment != nil {
		if err := r.createOrUpdateResource(ctx, deployment, cluster); err != nil {
			return fmt.Errorf("failed to reconcile MCP deployment: %w", err)
		}
	}

	if ingress := resources.BuildMCPIngressForCluster(cluster); ingress != nil {
		if err := r.createOrUpdateResource(ctx, ingress, cluster); err != nil {
			return fmt.Errorf("failed to reconcile MCP ingress: %w", err)
		}
	}

	if err := r.reconcileMCPRoute(ctx, cluster); err != nil {
		return err
	}

	r.warnIfMCPMissingAPOC(ctx, cluster)
	return nil
}

func (r *Neo4jEnterpriseClusterReconciler) reconcileMCPRoute(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	route := resources.BuildMCPRouteForCluster(cluster)
	if route == nil {
		return nil
	}

	if err := controllerutil.SetControllerReference(cluster, route, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on MCP route: %w", err)
	}

	desired := route.DeepCopy()

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, route, func() error {
			applyDesiredRoute(route, desired)
			return nil
		})
		return err
	})
	var noRouteAPI []string
	if err != nil && meta.IsNoMatchError(err) {
		noRouteAPI = append(noRouteAPI, "route.openshift.io/v1 not available; skipping MCP Route reconciliation")
	}
	r.advisoryWarnings.recordScoped(r.Recorder, cluster, EventReasonRouteAPINotFound, "mcp-route", noRouteAPI)
	if err != nil {
		if meta.IsNoMatchError(err) {
			logger.Info("Route API not available; skipping MCP Route reconciliation")
			return nil
		}
		return fmt.Errorf("failed to create or update MCP Route: %w", err)
	}

	logger.Info("Successfully reconciled MCP Route", "name", route.GetName())
	return nil
}

func (r *Neo4jEnterpriseClusterReconciler) warnIfMCPMissingAPOC(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	if cluster.Spec.MCP == nil || !cluster.Spec.MCP.Enabled || r.Recorder == nil {
		return
	}

	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, plugins, client.InNamespace(cluster.Namespace)); err != nil {
		log.FromContext(ctx).V(1).Info("Unable to list plugins for MCP APOC check", "error", err)
		return
	}

	// Advisory: announced when it starts to apply and refreshed every
	// advisoryWarningRefresh, not on every pass (knowledge rules 102, 145).
	r.advisoryWarnings.record(r.Recorder, cluster, EventReasonMCPApocMissing, mcpMissingAPOCWarnings(plugins.Items, cluster.Name))
}

// mcpMissingAPOCWarnings returns the MCPApocMissing warning when no enabled
// APOC Neo4jPlugin targets the deployment named target, and nothing otherwise.
func mcpMissingAPOCWarnings(plugins []neo4jv1beta1.Neo4jPlugin, target string) []string {
	for _, plugin := range plugins {
		if plugin.Spec.ClusterRef != target || !plugin.Spec.Enabled {
			continue
		}
		if isAPOCPluginName(plugin.Spec.Name) {
			return nil
		}
	}
	return []string{"MCP is enabled but APOC is not configured via Neo4jPlugin; MCP may fail in stdio mode and some tools may be unavailable."}
}

func isAPOCPluginName(name string) bool {
	return strings.EqualFold(name, "apoc") || strings.EqualFold(name, "apoc-extended")
}

// reconcileAuraFleetManagement handles Aura Fleet Management in two phases:
//
// Phase 1 — Plugin installation (runs unconditionally when enabled):
//
//	Merges "fleet-management" into the NEO4J_PLUGINS env var on the live StatefulSet via a
//	targeted patch. This is identical to how Neo4jPlugin controller installs plugins, so it
//	coexists safely: both controllers read the current list and append only if not present.
//	The updated StatefulSet triggers a rolling pod restart; the Docker entrypoint copies the
//	pre-bundled jar from /var/lib/neo4j/products/ to /plugins/ (no internet access needed).
//
// Phase 2 — Token registration (runs only when cluster is Ready):
//
//	Reads the Aura token from the referenced Kubernetes Secret and calls
//	CALL fleetManagement.registerToken($token). Registration is idempotent.
func (r *Neo4jEnterpriseClusterReconciler) reconcileAuraFleetManagement(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)
	spec := cluster.Spec.AuraFleetManagement

	// --- Phase 1: ensure the plugin jar is loaded ---
	// Merge "fleet-management" into NEO4J_PLUGINS on the live StatefulSet.
	// This is always done (regardless of Ready state) so pods restart promptly after
	// the feature is first enabled, before we attempt token registration.
	stsName := fmt.Sprintf("%s-server", cluster.Name)
	if err := r.mergeFleetManagementPlugin(ctx, stsName, cluster.Namespace); err != nil {
		if errors.IsNotFound(err) {
			// A new cluster's first pass gets here before it writes its server
			// StatefulSet. Nothing failed: the pass that finds it adds the
			// plugin. A Warning here greeted every new cluster (#531).
			logger.V(1).Info("Server StatefulSet not created yet; fleet-management is added on a later pass",
				"statefulSet", stsName)
			return nil
		}
		// Non-fatal for the reconcile: the cluster is functional, plugin patching
		// failed, and the next pass tries again. Announced — and logged at info —
		// when the failure starts or its message changes, at V(1) on the passes in
		// between: a StatefulSet without a neo4j container, a write the API server
		// refuses, lasts. It was an ERROR on every pass, about eight a minute
		// (operations.md ids 145, 164).
		// Worded for what failed: on an image with Fleet Manager built in nothing
		// is added, only the StatefulSet read (fleetPluginPatchFailure).
		event, logMsg := fleetPluginPatchFailure(err)
		announced := r.fleetPluginPatchFailures.changed(client.ObjectKeyFromObject(cluster).String(), err.Error())
		logRefusal(logger, announced, logMsg, err)
		if announced {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonAuraFleetPluginPatchFailed, event)
		}
		return nil
	}
	// The patch went through: a failure that comes back is news again.
	r.fleetPluginPatchFailures.forget(client.ObjectKeyFromObject(cluster).String())

	// --- Phase 2: token registration ---
	// The token can come from either place, and they are mutually exclusive:
	// spec.tokenSecretRef (user-supplied) or the Secret Phase 0 minted into when
	// spec.provision is set. Keying off tokenSecretRef alone meant a provisioned
	// cluster never registered its own minted token.
	secretName, secretKey, haveTokenSource := ResolveFleetTokenSource(spec, cluster.Name)
	if !haveTokenSource {
		logger.Info("Aura Fleet Management enabled but neither tokenSecretRef nor provision is configured; " +
			"plugin installed, registration deferred")
		return nil
	}

	if cluster.Status.Phase != "Ready" {
		logger.Info("Cluster not yet Ready; fleet-management plugin patch applied, registration deferred",
			"phase", cluster.Status.Phase)
		return nil
	}

	if cluster.Status.AuraFleetManagement != nil && cluster.Status.AuraFleetManagement.Registered {
		logger.V(1).Info("Aura Fleet Management already registered; nothing to do")
		return nil
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: secretName}, secret); err != nil {
		return r.failFleetRegistration(ctx, cluster, fmt.Sprintf("cannot read token secret %s: %v", secretName, err))
	}

	tokenBytes, ok := secret.Data[secretKey]
	if !ok || len(tokenBytes) == 0 {
		return r.failFleetRegistration(ctx, cluster, fmt.Sprintf("key %q not found in secret %s", secretKey, secretName))
	}
	token := strings.TrimSpace(string(tokenBytes))

	neo4jClient, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, getClusterAdminSecretName(cluster))
	if err != nil {
		return r.failFleetRegistration(ctx, cluster, fmt.Sprintf("cannot connect to Neo4j: %v", err))
	}
	defer neo4jClient.Close()

	installed, err := neo4jClient.IsFleetManagementInstalled(ctx)
	if err != nil {
		return r.failFleetRegistration(ctx, cluster, fmt.Sprintf("cannot check fleet management plugin: %v", err))
	}
	if !installed {
		// Pods may still be mid-restart after the NEO4J_PLUGINS patch above.
		logger.Info("Fleet management plugin not yet loaded; will retry on next reconcile")
		return nil
	}

	if err := neo4jClient.RegisterFleetManagementToken(ctx, token); err != nil {
		return r.failFleetRegistration(ctx, cluster, fmt.Sprintf("token registration failed: %v", err))
	}

	logger.Info("Aura Fleet Management token registered successfully")
	r.Recorder.Event(cluster, corev1.EventTypeNormal, EventReasonAuraFleetRegistered,
		"Successfully registered with Aura Fleet Management")
	r.fleetFailures.clear(client.ObjectKeyFromObject(cluster))
	return r.setFleetManagementStatus(ctx, cluster, true, "Registered with Aura Fleet Management")
}

// failFleetRegistration records a registration failure on
// status.auraFleetManagement and raises the AuraFleetManagementFailed Warning
// when the failure is new or its message changed. The failure stays non-fatal:
// the returned error is only ever the status write's, never the registration's,
// because the cluster is operational and only the Aura registration failed.
func (r *Neo4jEnterpriseClusterReconciler) failFleetRegistration(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, message string) error {
	announceFleetFailure(r.Recorder, &r.fleetFailures, cluster, message)
	return r.setFleetManagementStatus(ctx, cluster, false, message)
}

// mergeFleetManagementPlugin patches the named StatefulSet to ensure "fleet-management"
// appears in the NEO4J_PLUGINS env var of the neo4j container. The merge is idempotent:
// if "fleet-management" is already present, the StatefulSet is not touched.
// Uses retry.RetryOnConflict to handle concurrent update conflicts.
func (r *Neo4jEnterpriseClusterReconciler) mergeFleetManagementPlugin(ctx context.Context, stsName, namespace string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: namespace}, sts); err != nil {
			return fmt.Errorf("get StatefulSet %s: %w", stsName, err)
		}
		stsCopy := sts.DeepCopy()
		changed, err := addFleetManagementPlugin(&stsCopy.Spec.Template)
		if err != nil {
			return fmt.Errorf("%w in StatefulSet %s", err, stsName)
		}
		if !changed {
			return nil // already present, or built into the image — nothing to patch
		}
		if err := r.Update(ctx, stsCopy); err != nil {
			// Marked as the write, for the event's wording (fleetPluginPatchFailure);
			// a Conflict is still one to RetryOnConflict.
			return &fleetPluginWriteError{err: err}
		}
		return nil
	})
}

func (r *Neo4jEnterpriseClusterReconciler) setFleetManagementStatus(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, registered bool, message string) error {
	now := metav1.Now()
	// Refetch + RetryOnConflict: fleet registration runs after other status
	// writes in the same reconcile, so the reconcile-start object is stale and a
	// bare Status().Update conflicts (the standalone twin already refetches; the
	// cluster path did not). Mirrors updateClusterStatus.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		// Mutate in place rather than replacing the struct: Phase 0
		// (reconcileAuraFleetProvision) also writes here — deploymentId, token
		// metadata, telemetry — and a wholesale replace would silently wipe all of
		// it on every registration update.
		if latest.Status.AuraFleetManagement == nil {
			latest.Status.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementStatus{}
		}
		latest.Status.AuraFleetManagement.Registered = registered
		latest.Status.AuraFleetManagement.Message = message
		if registered {
			latest.Status.AuraFleetManagement.LastRegistrationTime = &now
		}
		cluster.Status.AuraFleetManagement = latest.Status.AuraFleetManagement // mirror for the in-memory caller
		return r.Status().Update(ctx, latest)
	})
}

// writeFleetStatus applies mutate to status.auraFleetManagement, creating it if
// absent. This is the fleetProvisioner's StatusWriter for clusters.
func (r *Neo4jEnterpriseClusterReconciler) writeFleetStatus(
	ctx context.Context, key types.NamespacedName, mutate func(*neo4jv1beta1.AuraFleetManagementStatus),
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		if latest.Status.AuraFleetManagement == nil {
			latest.Status.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementStatus{}
		}
		mutate(latest.Status.AuraFleetManagement)
		return r.Status().Update(ctx, latest)
	})
}

// newFleetProvisioner builds the shared Phase 0 helper for this reconciler.
func (r *Neo4jEnterpriseClusterReconciler) newFleetProvisioner() *fleetProvisioner {
	return &fleetProvisioner{
		Client:        r.Client,
		Recorder:      r.Recorder,
		ClientFactory: r.FleetClientFactory,
		StatusWriter:  r.writeFleetStatus,
	}
}

// SetupWithManager sets up the controller with the Manager.
// controllerOptions are the cluster controller's work-queue settings.
//
// Clusters are reconciled MaxConcurrentReconciles at a time, so one cluster
// whose servers are slow to answer does not delay the others; controller-runtime
// never reconciles the same cluster twice at once, and the state the reconciler
// keeps across clusters is held in sync.Maps or behind mutexes (operations.md
// id 129). ReconcileTimeout puts a deadline on each pass's context, which the
// Kubernetes client and the Neo4j driver both honour.
func (r *Neo4jEnterpriseClusterReconciler) controllerOptions() controller.Options {
	workers := r.MaxConcurrentReconciles
	if workers < 1 {
		workers = 1
	}
	return controller.Options{
		MaxConcurrentReconciles: workers,
		ReconciliationTimeout:   r.ReconcileTimeout,
		RateLimiter: workqueue.NewTypedMaxOfRateLimiter(
			// Exponential backoff starting at 5 seconds
			workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](5*time.Second, 30*time.Second),
			// A guard against a retry storm across every cluster (controller-
			// runtime's default: 10 a second, bursts of 100). It paces only
			// failed and Requeue passes, never RequeueAfter or watch events;
			// the per-cluster backoff above does the real pacing. It used to
			// be one every 6 seconds (bursts of 10), which, after a shared
			// failure such as an API-server blip, held the 20th cluster's first
			// retry for a minute and the 30th's for two (operations.md id 129).
			&workqueue.TypedBucketRateLimiter[reconcile.Request]{
				Limiter: rate.NewLimiter(rate.Limit(10), 100),
			},
		),
	}
}

func (r *Neo4jEnterpriseClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		// Note: Removed ConfigMap from Owns() to prevent reconciliation feedback loops
		// ConfigMaps are managed manually by ConfigMapManager with debounce
		Owns(&corev1.Secret{}).
		// cert-manager, not the cluster, owns {name}-tls-secret: watch it by
		// name so a renewal is reloaded on the running servers (#469).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(tlsSecretOwnerRequest)).
		WithOptions(r.controllerOptions())

	routeGVK := schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"}
	if _, err := mgr.GetRESTMapper().RESTMapping(routeGVK.GroupKind(), routeGVK.Version); err == nil {
		routeObj := &unstructured.Unstructured{}
		routeObj.SetGroupVersionKind(routeGVK)
		routeHandler := handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(), &neo4jv1beta1.Neo4jEnterpriseCluster{}, handler.OnlyControllerOwner())
		builder = builder.WatchesRawSource(source.Kind(mgr.GetCache(), routeObj, routeHandler))
	}

	return builder.Complete(r)
}

// holdImageDriftUntilReady neutralizes spec-vs-StatefulSet image drift on the
// IN-MEMORY cluster object while the cluster is not Ready, so the normal
// reconcile path keeps converging the cluster on its CURRENT image instead of
// silently applying a version change outside the upgrade state machine
// (#262). No-ops when there are no servers yet (initial install) or there is
// no drift.
//
// The image in place is the server StatefulSet's, or, while a storage
// expansion has orphan-deleted it, the image its orphaned pods run
// (serverImageInPlace): the StatefulSet created again must not carry a held
// image change in as an ungated roll of every server (#538).
func (r *Neo4jEnterpriseClusterReconciler) holdImageDriftUntilReady(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	place, found := r.serverImageInPlace(ctx, cluster)
	if !found {
		return
	}
	currentImage := place.image
	desiredImage := fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	if currentImage == desiredImage {
		return
	}
	// Escape hatch: with ZERO ready replicas there is no formed quorum for
	// the hold to protect — and the held image may be the very thing
	// blocking formation (an unpullable tag on a first deploy left the
	// cluster wedged Forming forever: never Ready, so the user's corrected
	// tag was deferred indefinitely). Let the spec image flow straight to
	// the StatefulSet; pods that never came up are safe to re-roll. This
	// also lets a new image through to a cluster that was Ready once and has
	// lost every server: the version rule has already checked the tag
	// against status.version, and the Ready pass records the new version once
	// every server runs it (clusterVersionToRecord, operations.md id 144).
	if place.ready == 0 {
		log.FromContext(ctx).Info("Applying image change without the Ready gate: no ready replicas exist to protect (formation blocked, possibly by the current image)",
			"current", currentImage, "desired", desiredImage)
		return
	}
	// A tag can carry a digest: split as the image was built, never on the
	// last colon (#531).
	repo, tag, ok := splitImage(currentImage)
	if !ok {
		return // unparseable image — leave the spec alone
	}
	log.FromContext(ctx).V(1).Info("Image change deferred until the cluster is Ready — the rolling-upgrade state machine will perform it",
		"current", currentImage, "desired", desiredImage, "phase", cluster.Status.Phase)
	r.announceHeldImageChange(cluster, desiredImage)
	cluster.Spec.Image.Repo = repo
	cluster.Spec.Image.Tag = tag
}

// recordClusterReady writes the Ready status of a cluster whose formation and
// rollout are both verified, and reports whether the phase, message or Ready
// condition moved (updateClusterStatusWithVersion).
//
// status.version is the version every server runs, read from the rolled server
// StatefulSet. The upgrade machine records it after a verified upgrade; this
// pass records it at the first Ready (#262: a cluster that never upgraded had
// it empty, which skipped the next upgrade's version check) and after an image
// reached the servers outside the machine, through the zero-ready escape hatch
// in holdImageDriftUntilReady (clusterVersionToRecord, operations.md id 144).
//
// servers is the SHOW SERVERS the formation check read on this pass. For a
// tag without a patch the versions they report are recorded with it, when
// they change (reportedVersionsToRecord, operations.md id 153): no query of
// their own.
func (r *Neo4jEnterpriseClusterReconciler) recordClusterReady(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, servers []neo4jclient.ServerInfo, conditions ...metav1.Condition) bool {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		sts = nil
	}
	rec := reportedVersionsToRecord(cluster, sts, servers)
	if rec.version != "" && cluster.Status.Version != "" {
		log.FromContext(ctx).Info("Recording the version the servers now run",
			"previous", cluster.Status.Version, "running", rec.version)
	}
	var outcome reportedVersionsOutcome
	rec.outcome = &outcome
	changed := r.updateClusterStatusRecording(ctx, cluster, "Ready", clusterReadyMessage(cluster), rec, conditions...)
	// Said once it is stored. With the CRDs of an earlier release installed,
	// the API server drops the fields, so every Ready pass sends them again
	// (the first pass after the CRDs are applied records them): that write
	// changes nothing, and is not worth an Info line each time (#550; the
	// standalone does the same, recordStandaloneRunningVersion).
	switch {
	case outcome.dropped:
		log.FromContext(ctx).V(1).Info("The API server dropped the versions the servers report (this release's CRDs are not applied); sending them again on the next pass",
			"version", firstNonEmpty(rec.version, cluster.Status.Version), "runningVersion", reportedValue(rec.runningVersion),
			"resolvedTargetVersion", reportedValue(rec.resolvedTargetVersion))
	case outcome.sent:
		log.FromContext(ctx).Info("Recorded the versions the servers report",
			"version", firstNonEmpty(rec.version, cluster.Status.Version), "runningVersion", reportedValue(rec.runningVersion),
			"resolvedTargetVersion", reportedValue(rec.resolvedTargetVersion))
	}
	return changed
}

// reportedVersionsToRecord is what a Ready pass records about the versions the
// servers run, from the rolled server StatefulSet (nil when it cannot be read)
// and the SHOW SERVERS rows the pass read:
//
//   - status.version, the tag every server runs (clusterVersionToRecord,
//     operations.md id 144);
//   - status.runningVersion, what the servers report for that tag when it
//     names no patch, cleared when it names one. While an upgrade is held,
//     only the servers below the partition it kept run status.version;
//   - for a held upgrade whose target names no patch, what the servers it
//     rolled report, in status.upgradeStatus.resolvedTargetVersion
//     (operations.md id 153).
func reportedVersionsToRecord(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, servers []neo4jclient.ServerInfo) statusRecord {
	var rec statusRecord
	if sts != nil {
		rec.version = clusterVersionToRecord(&cluster.Status, rolledImageTag(sts))
	}
	tag := firstNonEmpty(rec.version, cluster.Status.Version)
	// An upgrade a check refused before it staged anything holds nothing:
	// every server runs status.version, and what they report is recorded as
	// for any cluster. It used to be left alone for as long as the record
	// stayed Paused or Failed (#550).
	if !upgradeHeld(cluster) || (sts != nil && !upgradeStaged(cluster.Status.UpgradeStatus, sts)) {
		if running, ok := runningVersionFor(tag, highestReportedVersion(servers, nil)); ok {
			rec.runningVersion = &running
		}
		return rec
	}
	if sts == nil {
		return rec
	}
	if partition := keptUpgradePartition(cluster, sts); partition > 0 {
		below := highestReportedVersion(servers, func(s neo4jclient.ServerInfo) bool {
			for i := int32(0); i < partition; i++ {
				if addressIsPod(s.Address, fmt.Sprintf("%s-server-%d", cluster.Name, i)) {
					return true
				}
			}
			return false
		})
		if running, ok := runningVersionFor(tag, below); ok {
			rec.runningVersion = &running
		}
	}
	if resolved, ok := resolvedTargetVersionFor(cluster.Status.UpgradeStatus, heldTargetServerVersion(cluster, sts, servers)); ok {
		rec.resolvedTargetVersion = &resolved
	}
	return rec
}

// firstNonEmpty returns the first of values that is not "".
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// noteServerStatefulSetWrite remembers the generation of the server
// StatefulSet as this pass left it, so a read from a cache that has not seen
// the write yet is not taken for the rolled-out StatefulSet it replaced
// (serverTemplateWriteUnobserved).
func (r *Neo4jEnterpriseClusterReconciler) noteServerStatefulSetWrite(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) {
	if sts == nil || sts.UID == "" {
		return
	}
	r.serverStatefulSetWrites.Store(clusterKey(cluster), writtenGeneration{uid: sts.UID, generation: sts.Generation})
}

// writtenGeneration is the server StatefulSet a pass wrote: its UID (a
// StatefulSet created again starts at generation 1) and generation.
type writtenGeneration struct {
	uid        types.UID
	generation int64
}

// serverTemplateWriteUnobserved reports whether sts carries a spec the
// StatefulSet controller has not acted on yet: its observedGeneration is
// behind its generation, or the read is older than the generation this
// operator last wrote. Either way the pods may be about to roll, so a status
// read off it describes the template before the write, not the one the pods
// are moving to.
func (r *Neo4jEnterpriseClusterReconciler) serverTemplateWriteUnobserved(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) bool {
	if sts.Status.ObservedGeneration < sts.Generation {
		return true
	}
	v, ok := r.serverStatefulSetWrites.Load(clusterKey(cluster))
	if !ok {
		return false
	}
	w, ok := v.(writtenGeneration)
	return ok && w.uid == sts.UID && sts.Generation < w.generation
}

// serverStatefulSetFullyRolled reports whether the server StatefulSet has no
// rollout in flight: the StatefulSet controller has seen its current spec, and
// every replica is updated to the current template revision and ready. A false
// result carries a human-readable detail for the status message.
//
// A template write the StatefulSet controller has not seen yet is a rollout in
// flight. Right after the pass's own write its status still described the
// template before it, every replica updated and ready, so the cluster was
// reported Ready and users, roles, databases and an upgrade started against
// it, just before its servers restarted.
func (r *Neo4jEnterpriseClusterReconciler) serverStatefulSetFullyRolled(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (bool, string) {
	serverSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, serverSts); err != nil {
		return false, "server StatefulSet not found"
	}
	replicas := int32(1)
	if serverSts.Spec.Replicas != nil {
		replicas = *serverSts.Spec.Replicas
	}
	if r.serverTemplateWriteUnobserved(cluster, serverSts) {
		return false, "the StatefulSet controller has not seen the current template"
	}
	// A Paused or Failed upgrade keeps its partition: the servers below it run
	// the version they ran until the upgrade goes on, which is not a rollout
	// in flight. The servers it rolled must be on the template and every
	// server ready (operations.md id 149).
	if partition := keptUpgradePartition(cluster, serverSts); partition > 0 {
		if pending, detail := heldRolloutPending(serverSts, partition); pending {
			return false, detail
		}
		if serverSts.Status.ReadyReplicas < replicas {
			return false, fmt.Sprintf("%d/%d pods ready", serverSts.Status.ReadyReplicas, replicas)
		}
		return true, ""
	}
	if serverSts.Status.UpdateRevision != "" && serverSts.Status.CurrentRevision != serverSts.Status.UpdateRevision {
		return false, fmt.Sprintf("revision rollout in progress (%d/%d pods updated)", serverSts.Status.UpdatedReplicas, replicas)
	}
	if serverSts.Status.UpdatedReplicas < replicas || serverSts.Status.ReadyReplicas < replicas {
		return false, fmt.Sprintf("%d/%d pods updated, %d/%d ready", serverSts.Status.UpdatedReplicas, replicas, serverSts.Status.ReadyReplicas, replicas)
	}
	return true, ""
}

// connectivityFailureEventThreshold and connectivityFailureEventMinDuration
// decide when a failure streak earns a ConnectivityDegraded Warning (once per
// streak, see dueForEvent): ten consecutive failures spanning at least five
// minutes — past formation and any normal rolling-restart blip, early enough to
// be actionable. The count alone used to be assumed to take five minutes at the
// ~30s requeue cadence; during formation it took under ninety seconds.
const (
	connectivityFailureEventThreshold   = 10
	connectivityFailureEventMinDuration = 5 * time.Minute
)

// recordConnectivityFailure increments the cluster's consecutive
// connectivity-failure streak and returns it (#263 forensics).
func (r *Neo4jEnterpriseClusterReconciler) recordConnectivityFailure(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *connectivityFailureStreak {
	key := cluster.Namespace + "/" + cluster.Name
	v, _ := r.connectivityFailures.LoadOrStore(key, &connectivityFailureStreak{since: time.Now()})
	streak, ok := v.(*connectivityFailureStreak)
	if !ok { // unreachable — the map only ever holds streaks
		streak = &connectivityFailureStreak{since: time.Now()}
		r.connectivityFailures.Store(key, streak)
	}
	streak.count++
	return streak
}

// clearConnectivityFailures resets the streak after a successful connection,
// logging a closing summary if a long streak just ended so the recovery is
// visible next to the failures.
func (r *Neo4jEnterpriseClusterReconciler) clearConnectivityFailures(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	key := cluster.Namespace + "/" + cluster.Name
	if v, loaded := r.connectivityFailures.LoadAndDelete(key); loaded {
		streak, ok := v.(*connectivityFailureStreak)
		if ok && streak.count >= connectivityFailureEventThreshold {
			log.FromContext(ctx).Info("Neo4j connectivity recovered after persistent failure streak",
				"consecutiveFailures", streak.count,
				"failingSince", streak.since.Format(time.RFC3339),
				"outageDuration", time.Since(streak.since).Round(time.Second).String())
		}
	}
}

// firstNonNilErr returns the first non-nil error, for log/event messages
// that want the most specific failure available.
// benignWaitingReasons are container waiting reasons that occur on the normal
// startup path. They are reported only when the container has already terminated
// at least once, which turns them from "starting" into "restarting".
var benignWaitingReasons = map[string]bool{
	"ContainerCreating": true,
	"PodInitializing":   true,
}

// diagnoseUnhealthyMembers summarises member pods that are not running, so a
// container-level failure is reported on the CR instead of only in a container
// log nobody knows to look at.
//
// Returns "" when every pod looks fine (or the listing fails) — this is a
// reporting aid on an already-failing path, so it must never turn a transient
// List error into a reconcile failure. Output is bounded to a few pods to keep it
// inside the event/status message length a human will actually read.
func (r *Neo4jEnterpriseClusterReconciler) diagnoseUnhealthyMembers(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(resources.ServerPodSelector(cluster.Name)),
	); err != nil {
		return ""
	}

	const maxReported = 3
	var issues []memberIssue
	for i := range pods.Items {
		pod := &pods.Items[i]
		for j := range pod.Status.ContainerStatuses {
			cs := &pod.Status.ContainerStatuses[j]
			if cs.Name != "neo4j" || cs.Ready {
				continue
			}
			var issue memberIssue
			switch {
			case cs.State.Waiting != nil:
				reason := cs.State.Waiting.Reason
				// Every healthy cluster passes through these on the way up, so
				// reporting them would put "member pod not running" on the CR
				// during normal formation and train people to ignore it. They
				// only matter if the container has ALREADY died once, in which
				// case the last-terminated state below carries the real cause.
				if benignWaitingReasons[reason] &&
					cs.LastTerminationState.Terminated == nil {
					continue
				}
				issue.detail = reason
				// The waiting reason on a restart loop is just
				// "CrashLoopBackOff"; the useful part is why the previous
				// attempt died, which lives on the last terminated state.
				lt := cs.LastTerminationState.Terminated
				if lt != nil {
					issue.detail = fmt.Sprintf("%s (last exit %d %s: %s)",
						issue.detail, lt.ExitCode, lt.Reason, truncateForMessage(lt.Message, 160))
				}
				switch {
				case imagePullWaitingReasons[reason]:
					issue.hint = memberHintImagePull
					issue.image = memberPullImage(pod, cs)
				case lt != nil || reason == "CrashLoopBackOff":
					issue.hint = memberHintPreviousLog
				default:
					issue.hint = memberHintNeverStarted
				}
			case cs.State.Terminated != nil:
				issue.detail = fmt.Sprintf("Terminated (exit %d %s: %s)",
					cs.State.Terminated.ExitCode, cs.State.Terminated.Reason,
					truncateForMessage(cs.State.Terminated.Message, 160))
				issue.hint = memberHintLog
			default:
				continue // running but not yet Ready — normal during startup
			}
			issue.detail = fmt.Sprintf("%s: %s", pod.Name, issue.detail)
			issues = append(issues, issue)
		}
	}
	if len(issues) == 0 {
		return ""
	}

	// Stable text so the event does not churn.
	sort.Slice(issues, func(i, j int) bool { return issues[i].detail < issues[j].detail })
	suffix := ""
	if len(issues) > maxReported {
		suffix = fmt.Sprintf(" (+%d more)", len(issues)-maxReported)
		issues = issues[:maxReported]
	}
	details := make([]string, len(issues))
	for i := range issues {
		details[i] = issues[i].detail
	}
	return "member pod not running — " + strings.Join(details, "; ") + suffix +
		". " + memberIssueHints(issues)
}

// imagePullWaitingReasons are the waiting reasons of a container whose image
// was never pulled. Nothing ran, so there is no log to read.
var imagePullWaitingReasons = map[string]bool{
	"ErrImagePull":     true,
	"ImagePullBackOff": true,
	"InvalidImageName": true,
}

// memberHint is where to look next for one kind of member failure. The values
// order the hints in the message.
type memberHint int

const (
	memberHintImagePull    memberHint = iota // the image cannot be pulled
	memberHintNeverStarted                   // created, never started (config error, start error)
	memberHintPreviousLog                    // ran, died, and was restarted
	memberHintLog                            // ran and died, not restarted yet
)

type memberIssue struct {
	detail string // "<pod>: <reason and exit>"
	hint   memberHint
	image  string // for memberHintImagePull: the image it cannot pull
}

// memberIssueHints says where the cause of each reported kind of failure is.
// One "Inspect with: kubectl logs <pod> -c neo4j --previous" used to cover
// them all, which for an image that will not pull sends the user to a log that
// cannot exist: no container ever ran.
func memberIssueHints(issues []memberIssue) string {
	seen := map[memberHint]bool{}
	var images []string
	for _, issue := range issues {
		seen[issue.hint] = true
		if issue.hint == memberHintImagePull && issue.image != "" && !slices.Contains(images, issue.image) {
			images = append(images, issue.image)
		}
	}
	sort.Strings(images)

	var hints []string
	if seen[memberHintImagePull] {
		image := "The image"
		if len(images) > 0 {
			image = "Image " + strings.Join(images, ", ")
		}
		hints = append(hints, image+" cannot be pulled, so no container ran and there is no log "+
			"to read: check spec.image for a typo and that the tag exists (the official "+
			"neo4j:<version>-enterprise images on Docker Hub are public and need no pull secret), "+
			"that the registry is reachable and not rate-limiting, and that a private registry or "+
			"mirror has a Secret in spec.image.pullSecrets. The registry's answer: kubectl describe pod <pod>")
	}
	if seen[memberHintNeverStarted] {
		hints = append(hints, "A container that never started has no log; the pod's events say why: "+
			"kubectl describe pod <pod>")
	}
	if seen[memberHintPreviousLog] {
		hints = append(hints, "Inspect a crash loop with: kubectl logs <pod> -c neo4j --previous")
	}
	if seen[memberHintLog] {
		hints = append(hints, "Inspect the exited container with: kubectl logs <pod> -c neo4j")
	}
	return strings.Join(hints, ". ")
}

// memberPullImage is the image the pod's neo4j container is trying to run:
// the pod spec's, which is what the kubelet pulls, else the one in its status.
func memberPullImage(pod *corev1.Pod, cs *corev1.ContainerStatus) string {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == cs.Name && pod.Spec.Containers[i].Image != "" {
			return pod.Spec.Containers[i].Image
		}
	}
	return cs.Image
}

// truncateForMessage keeps a container message short enough for a Kubernetes
// event without dropping the leading text, which is where the cause usually is.
func truncateForMessage(s string, limit int) string {
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

func firstNonNilErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
