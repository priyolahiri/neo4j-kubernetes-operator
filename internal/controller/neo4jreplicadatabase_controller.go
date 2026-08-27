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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jReplicaDatabaseFinalizer ensures the controller can drop the replica
// (or deliberately decline to) before the CR disappears.
const Neo4jReplicaDatabaseFinalizer = "neo4j.com/replicadatabase-finalizer"

// Neo4jReplicaDatabaseReconciler reconciles a Neo4jReplicaDatabase resource.
type Neo4jReplicaDatabaseReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.ReplicaValidator
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicadatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicadatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicadatabases/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jReplicaDatabase toward its desired state.
//
// The load-bearing property of this loop is that it OBSERVES before it acts.
// A replica's promotion is irreversible, and after it the database is no
// longer a replica — so a naive level-triggered reconcile would see
// "desired: replica / actual: standard database", call it drift, and correct
// it by dropping and recreating. That would destroy the database the user
// promoted precisely because it had become their live system.
//
// Every mutating path therefore re-reads the live `type` column first and
// refuses to act when it is not "replica". Status is a fast path, never the
// authority: it can be lost to an etcd restore, the live type cannot.
func (r *Neo4jReplicaDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jreplicadatabase", req.NamespacedName)

	replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	if err := r.Get(ctx, req.NamespacedName, replica); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	dbName := effectiveReplicaName(replica)
	requeue := r.requeueAfter()

	if replica.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, replica, dbName)
	}

	if !controllerutil.ContainsFinalizer(replica, Neo4jReplicaDatabaseFinalizer) {
		controllerutil.AddFinalizer(replica, Neo4jReplicaDatabaseFinalizer)
		if err := r.Update(ctx, replica); err != nil {
			return ctrl.Result{}, err
		}
		// The Update above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Terminal fast path. Once promoted, this CR describes something that no
	// longer exists and the controller must never touch the database again.
	// The live re-check below is the actual guard; this is the cheap one.
	if replica.Status.Phase == neo4jv1beta1.ReplicaPhasePromoted {
		return ctrl.Result{}, nil
	}

	if r.Validator != nil {
		res := r.Validator.Validate(ctx, replica)
		for _, w := range res.Warnings {
			r.Recorder.Eventf(replica, corev1.EventTypeWarning, EventReasonValidationWarning, "%s", w)
		}
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseFailed, metav1.ConditionFalse,
				EventReasonValidationFailed, msg, nil)
			r.Recorder.Event(replica, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	target, err := ResolveClusterRef(ctx, r.Client, replica.Namespace, replica.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve cluster ref")
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(replica.Spec.ClusterRef))
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhasePending, metav1.ConditionFalse,
			EventReasonClusterNotFound, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		msg := fmt.Sprintf("%s is not Ready", targetRefDisplay(replica.Spec.ClusterRef))
		r.setNamedCondition(ctx, replica, ConditionTypeClusterNotReady, metav1.ConditionTrue,
			ConditionReasonClusterNotReady, msg)
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhasePending, metav1.ConditionFalse,
			ConditionReasonClusterNotReady, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, replica, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	// Version gate — hosting a replica requires Neo4j 2026.08+.
	if !targetSupportsCCDRReplica(target) {
		msg := fmt.Sprintf("Neo4jReplicaDatabase requires Neo4j %s or later on the downstream cluster; %s runs %s",
			neo4jclient.MinCCDRReplicaVersion, replica.Spec.ClusterRef, targetVersionString(target))
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseFailed, metav1.ConditionFalse,
			EventReasonReplicaVersionTooOld, msg, nil)
		r.Recorder.Event(replica, corev1.EventTypeWarning, EventReasonReplicaVersionTooOld, msg)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := target.NewClient(r.Client)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseFailed, metav1.ConditionFalse,
			EventReasonConnectionFailed, msg, nil)
		r.Recorder.Event(replica, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// OBSERVE. Everything below branches on the live state, never on spec.
	info, err := nc.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		return r.fail(ctx, replica, "database lookup failed", err, requeue)
	}

	switch {
	case info == nil:
		// Absent — create it.
		src := replica.Spec.Source
		mode := src.Mode
		if mode == "" {
			mode = neo4jv1beta1.ReplicaSourceModeBackup
		}

		switch mode {
		case neo4jv1beta1.ReplicaSourceModeNetwork:
			addresses := src.Addresses
			if src.UpstreamClusterRef != nil {
				resolved, ok, err := r.resolveUpstreamClusterAddresses(ctx, replica, src.UpstreamClusterRef)
				if err != nil {
					return r.fail(ctx, replica, "resolve upstreamClusterRef failed", err, requeue)
				}
				if !ok {
					// Not found or not ready yet — status already set inside
					// the helper; this is an ordinary transient state, not a
					// terminal failure.
					return ctrl.Result{RequeueAfter: requeue}, nil
				}
				addresses = resolved
			}
			networkSrc := neo4jclient.ReplicaNetworkSource{
				UpstreamDatabase: replica.Spec.UpstreamDatabase,
				Addresses:        addresses,
			}
			if t := replica.Spec.Topology; t != nil {
				networkSrc.Primaries = t.Primaries
				networkSrc.Secondaries = t.Secondaries
			}
			r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseSeeding, metav1.ConditionFalse,
				"Seeding", fmt.Sprintf("creating replica %q streaming from %v", dbName, addresses), nil)
			if err := nc.CreateReplicaDatabaseFromNetwork(ctx, dbName, networkSrc); err != nil {
				return r.fail(ctx, replica, "create replica database failed", err, requeue)
			}
		default:
			backupSrc := neo4jclient.ReplicaBackupSource{
				UpstreamDatabase: replica.Spec.UpstreamDatabase,
				PullURI:          src.PullURI,
				SeedURI:          src.SeedURI,
			}
			if t := replica.Spec.Topology; t != nil {
				backupSrc.Primaries = t.Primaries
				backupSrc.Secondaries = t.Secondaries
			}
			r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseSeeding, metav1.ConditionFalse,
				"Seeding", fmt.Sprintf("creating replica %q from %s", dbName, src.PullURI), nil)
			if err := nc.CreateReplicaDatabaseFromBackup(ctx, dbName, backupSrc); err != nil {
				return r.fail(ctx, replica, "create replica database failed", err, requeue)
			}
		}
		r.Recorder.Eventf(replica, corev1.EventTypeNormal, EventReasonReplicaCreated,
			"Replica database %q created from %q", dbName, replica.Spec.UpstreamDatabase)
		return ctrl.Result{RequeueAfter: requeue}, nil

	case info.Type != neo4jclient.DatabaseTypeReplica:
		// THE GUARD. The database exists but is no longer a replica, so it was
		// promoted — by a Neo4jReplicaPromotion whose status write we have not
		// seen yet, or by someone at a cypher-shell, or this CR's status was
		// restored from a backup taken before the promotion.
		//
		// Whatever the cause, correcting the "drift" would mean dropping a
		// live read-write database. Go terminal instead.
		return r.markPromoted(ctx, replica, info, "out-of-band")

	default:
		// Still a replica. Report observed state; do not attempt to re-point
		// replicaConfig (spec.source is immutable and Neo4j offers no way to
		// change it in place).
		msg := fmt.Sprintf("replica of %q, %d transactions behind", replica.Spec.UpstreamDatabase, info.ReplicationLag)
		if replica.Status.Phase != neo4jv1beta1.ReplicaPhaseReplicating {
			r.Recorder.Eventf(replica, corev1.EventTypeNormal, EventReasonReplicaReady,
				"Replica database %q is online (lag %d)", dbName, info.ReplicationLag)
		}
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseReplicating, metav1.ConditionTrue,
			"Replicating", msg, info)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
}

// markPromoted records the terminal Promoted phase. Called both when the
// operator observes an out-of-band promotion and (via the promotion
// controller) after one it performed itself.
func (r *Neo4jReplicaDatabaseReconciler) markPromoted(
	ctx context.Context,
	replica *neo4jv1beta1.Neo4jReplicaDatabase,
	info *neo4jclient.DatabaseInfo,
	promotedBy string,
) (ctrl.Result, error) {
	msg := fmt.Sprintf("database is no longer a replica (type=%q); this CR is now inert and will not "+
		"modify the database. Manage it with a Neo4jDatabase CR (ifNotExists: true) to adopt it.", info.Type)

	if replica.Status.Phase != neo4jv1beta1.ReplicaPhasePromoted {
		r.Recorder.Eventf(replica, corev1.EventTypeWarning, EventReasonReplicaPromotedDetected,
			"Database %q is type %q, not a replica — promotion detected (%s). This CR will no longer "+
				"touch the database, and deleting it will NOT drop the database.",
			info.Name, info.Type, promotedBy)
	}

	update := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(replica), latest); err != nil {
			return err
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, metav1.ConditionTrue,
			"Promoted", msg)
		latest.Status.Phase = neo4jv1beta1.ReplicaPhasePromoted
		latest.Status.Message = msg
		latest.Status.ObservedGeneration = latest.Generation
		latest.Status.DatabaseType = info.Type
		latest.Status.LastCommittedTxn = info.LastCommittedTxn
		latest.Status.ReplicationLag = info.ReplicationLag
		if latest.Status.PromotedAt == nil {
			now := metav1.Now()
			latest.Status.PromotedAt = &now
		}
		if latest.Status.PromotedBy == "" {
			latest.Status.PromotedBy = promotedBy
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to mark replica promoted")
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, err
	}
	return ctrl.Result{}, nil
}

// handleDeletion drops the replica — unless it has been promoted, in which
// case it must NOT.
func (r *Neo4jReplicaDatabaseReconciler) handleDeletion(ctx context.Context, replica *neo4jv1beta1.Neo4jReplicaDatabase, dbName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(replica, Neo4jReplicaDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}

	requeue := r.requeueAfter()
	releaseOnly := func() (ctrl.Result, error) {
		controllerutil.RemoveFinalizer(replica, Neo4jReplicaDatabaseFinalizer)
		return ctrl.Result{}, r.Update(ctx, replica)
	}

	// Promoted per status: never drop. Deliberate asymmetry with
	// Neo4jDatabase, whose finalizer always drops — a promoted database is
	// the live system, and removing a CR that no longer describes it must not
	// be a data-loss event.
	if replica.Status.Phase == neo4jv1beta1.ReplicaPhasePromoted {
		r.Recorder.Eventf(replica, corev1.EventTypeWarning, EventReasonReplicaRetainedPromoted,
			"Database %q was promoted and is RETAINED; deleting this CR does not drop it. Manage it with "+
				"a Neo4jDatabase CR.", dbName)
		return releaseOnly()
	}

	if strings.EqualFold(replica.Spec.DeletionPolicy, "Retain") {
		return releaseOnly()
	}

	target, err := ResolveClusterRef(ctx, r.Client, replica.Namespace, replica.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		return releaseOnly()
	}
	if !target.IsReady() {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := target.NewClient(r.Client)
	if err != nil {
		logger.Error(err, "cannot connect during replica deletion; releasing finalizer")
		return releaseOnly()
	}
	defer func() { _ = nc.Close() }()

	// Re-check live state before dropping. Status could be stale — this is the
	// same guard as the reconcile path, and it is the one that matters most:
	// dropping here is unrecoverable.
	info, err := nc.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		logger.Error(err, "cannot read database type during deletion; releasing finalizer without dropping")
		return releaseOnly()
	}
	if info == nil {
		return releaseOnly()
	}
	if info.Type != neo4jclient.DatabaseTypeReplica {
		r.Recorder.Eventf(replica, corev1.EventTypeWarning, EventReasonReplicaRetainedPromoted,
			"Database %q is type %q, not a replica — it was promoted. RETAINING it; deleting this CR does "+
				"not drop it.", dbName, info.Type)
		return releaseOnly()
	}

	if err := nc.DropDatabase(ctx, dbName); err != nil {
		r.Recorder.Eventf(replica, corev1.EventTypeWarning, EventReasonReplicaFailed,
			"DROP DATABASE %q failed; releasing finalizer to avoid wedging deletion: %v", dbName, err)
		return releaseOnly()
	}

	r.Recorder.Eventf(replica, corev1.EventTypeNormal, EventReasonReplicaDropped,
		"Replica database %q dropped", dbName)
	return releaseOnly()
}

func (r *Neo4jReplicaDatabaseReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// resolveUpstreamClusterAddresses reads the upstream Neo4jEnterpriseCluster
// named by ref and returns its status.internalAddresses. A cluster that
// doesn't exist yet, or hasn't published internalAddresses yet, is an
// ordinary Pending/requeue condition — not a terminal failure — since the
// upstream may simply not have reconciled yet. The bool return is false in
// both of those cases, with status/events already recorded by this
// function; the caller just requeues.
func (r *Neo4jReplicaDatabaseReconciler) resolveUpstreamClusterAddresses(
	ctx context.Context, replica *neo4jv1beta1.Neo4jReplicaDatabase, ref *neo4jv1beta1.UpstreamClusterRef,
) ([]string, bool, error) {
	ns := ref.Namespace
	if ns == "" {
		ns = replica.Namespace
	}
	upstream := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ns}, upstream)
	if apierrors.IsNotFound(err) {
		msg := fmt.Sprintf("upstream cluster %s/%s (source.upstreamClusterRef) not found", ns, ref.Name)
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhasePending, metav1.ConditionFalse,
			EventReasonUpstreamClusterNotFound, msg, nil)
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(upstream.Status.InternalAddresses) == 0 {
		msg := fmt.Sprintf("upstream cluster %s/%s has not published status.internalAddresses yet", ns, ref.Name)
		r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhasePending, metav1.ConditionFalse,
			EventReasonUpstreamClusterNotReady, msg, nil)
		return nil, false, nil
	}
	return upstream.Status.InternalAddresses, true, nil
}

func (r *Neo4jReplicaDatabaseReconciler) fail(ctx context.Context, replica *neo4jv1beta1.Neo4jReplicaDatabase, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	r.setStatus(ctx, replica, neo4jv1beta1.ReplicaPhaseFailed, metav1.ConditionFalse,
		EventReasonReplicaFailed, msg, nil)
	r.Recorder.Event(replica, corev1.EventTypeWarning, EventReasonReplicaFailed, msg)
	return ctrl.Result{RequeueAfter: requeue}, err
}

func (r *Neo4jReplicaDatabaseReconciler) setStatus(
	ctx context.Context,
	replica *neo4jv1beta1.Neo4jReplicaDatabase,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	info *neo4jclient.DatabaseInfo,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(replica), latest); err != nil {
			return err
		}
		// Never regress out of the terminal phase.
		if latest.Status.Phase == neo4jv1beta1.ReplicaPhasePromoted {
			return nil
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message)
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if info != nil {
			latest.Status.DatabaseType = info.Type
			latest.Status.LastCommittedTxn = info.LastCommittedTxn
			latest.Status.ReplicationLag = info.ReplicationLag
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jReplicaDatabase status")
	}
}

func (r *Neo4jReplicaDatabaseReconciler) setNamedCondition(ctx context.Context, replica *neo4jv1beta1.Neo4jReplicaDatabase, condType string, status metav1.ConditionStatus, reason, message string) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(replica), latest); err != nil {
			return err
		}
		SetNamedCondition(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jReplicaDatabase", "condition", condType)
	}
}

// effectiveReplicaName returns spec.name if set, else metadata.name.
func effectiveReplicaName(replica *neo4jv1beta1.Neo4jReplicaDatabase) string {
	if replica.Spec.Name != "" {
		return replica.Spec.Name
	}
	return replica.Name
}

// targetSupportsCCDRReplica parses the resolved target's Neo4j version and
// reports whether it can host a cross-cluster replica.
func targetSupportsCCDRReplica(target ResolvedTarget) bool {
	v := targetVersionString(target)
	if v == "" {
		return false
	}
	parsed, err := neo4jclient.ParseVersion(v)
	if err != nil || parsed == nil {
		return false
	}
	return parsed.SupportsCCDRReplica()
}

// SetupWithManager registers the controller and re-enqueues replicas when
// their cluster flips to Ready.
func (r *Neo4jReplicaDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueue := EnqueueDependentsForClusterChange(
		mgr.GetClient(),
		func() client.ObjectList { return &neo4jv1beta1.Neo4jReplicaDatabaseList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			replicas, ok := list.(*neo4jv1beta1.Neo4jReplicaDatabaseList)
			if !ok {
				return
			}
			for i := range replicas.Items {
				rep := &replicas.Items[i]
				emit(rep.Name, rep.Namespace, rep.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jReplicaDatabase{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueue).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueue).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}
