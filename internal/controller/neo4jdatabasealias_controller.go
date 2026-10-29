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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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

// Neo4jDatabaseAliasFinalizer ensures the controller can drop the alias before
// the CR disappears.
const Neo4jDatabaseAliasFinalizer = "neo4j.com/databasealias-finalizer"

// Neo4jDatabaseAliasReconciler reconciles a Neo4jDatabaseAlias resource.
type Neo4jDatabaseAliasReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.AliasValidator

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// newNeo4jClient connects Reconcile to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newNeo4jClient func(ResolvedTarget) (aliasNeo4jClient, error)

	// newDeletionClient connects the deletion to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newDeletionClient func(ResolvedTarget) (aliasDeletionClient, error)

	// targetGone defers the ClusterNotFound Warning of a target that went
	// away (target_gone.go).
	targetGone targetGoneNotices
}

// aliasDeletionClient is what the Neo4jDatabaseAlias deletion asks of Neo4j.
// *neo4jclient.Client is one.
type aliasDeletionClient interface {
	DropAliasIfExists(ctx context.Context, name string) error
	Close() error
}

func (r *Neo4jDatabaseAliasReconciler) connectForDeletion(target ResolvedTarget) (aliasDeletionClient, error) {
	if r.newDeletionClient != nil {
		return r.newDeletionClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jDatabaseAlias toward its desired state.
func (r *Neo4jDatabaseAliasReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jdatabasealias", req.NamespacedName)

	alias := &neo4jv1beta1.Neo4jDatabaseAlias{}
	if err := r.Get(ctx, req.NamespacedName, alias); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	aliasName := effectiveAliasName(alias)
	requeue := r.requeueAfter()

	if alias.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, alias, aliasName)
	}

	if !controllerutil.ContainsFinalizer(alias, Neo4jDatabaseAliasFinalizer) {
		if err := addFinalizer(ctx, r.Client, alias, Neo4jDatabaseAliasFinalizer); err != nil {
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	if r.Validator != nil {
		res := r.Validator.Validate(ctx, alias)
		r.advisoryWarnings.record(r.Recorder, alias, EventReasonValidationWarning, res.Warnings)
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass while the spec stays invalid (knowledge rule 136).
			if r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, "") {
				r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// An older Neo4jDatabaseAlias that gives this name on this target owns
	// it: Neo4j's names ignore case (operations.md id 217).
	duplicate, err := aliasNames.duplicate(ctx, r.Client, alias)
	if err != nil {
		return ctrl.Result{}, err
	}
	if duplicate != "" {
		// Announced when it starts, not on every pass (knowledge rule 136).
		if r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonDuplicateName, duplicate, "") {
			r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonDuplicateName, duplicate)
		}
		return ctrl.Result{}, nil
	}

	target, err := ResolveClusterRef(ctx, r.Client, alias.Namespace, alias.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve cluster ref")
		return ctrl.Result{}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(alias.Spec.ClusterRef))
		// A wait, announced once: when it starts, not on every pass until the
		// deployment exists (operations.md ids 145 and 182).
		hadTarget := hadFoundTarget(alias.Status.Conditions)
		entered := r.setStatus(ctx, alias, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, "")
		// A target that went away is announced only if the dependent outlasts
		// it by a minute: a teardown deletes the dependent first (#594).
		if r.targetGone.announce(alias, entered, hadTarget, time.Now()) {
			r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonClusterNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		// An alias Ready at its generation stays Ready while its target
		// restarts (knowledge rule 183).
		r.waitForTarget(ctx, alias, ConditionReasonClusterNotReady,
			fmt.Sprintf("%s is not Ready", targetRefDisplay(alias.Spec.ClusterRef)))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := r.connect(target)
	if err != nil {
		if targetAway(err) {
			return r.unreachable(ctx, alias, err, requeue)
		}
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		// Retried with backoff; the same failure is announced once (knowledge
		// rule 145).
		if r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, "") {
			r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// The target database does not have to exist yet. An alias and the replica
	// it fronts are typically applied together, and CREATE ALIAS against a
	// missing database fails — so wait rather than flapping into Failed.
	dbInfo, err := nc.GetDatabaseInfo(ctx, alias.Spec.TargetDatabase)
	if err != nil {
		return r.fail(ctx, alias, "target database lookup failed", err, requeue)
	}
	if dbInfo == nil {
		msg := fmt.Sprintf("target database %q does not exist yet", alias.Spec.TargetDatabase)
		r.setStatus(ctx, alias, "Pending", metav1.ConditionFalse, ConditionReasonClusterNotReady, msg, "")
		// A wait on the database, not the target, which answered.
		r.setNamedCondition(ctx, alias, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	existing, err := nc.ShowAlias(ctx, aliasName)
	if err != nil {
		return r.fail(ctx, alias, "alias lookup failed", err, requeue)
	}

	switch {
	case existing == nil:
		if err := nc.CreateAlias(ctx, aliasName, alias.Spec.TargetDatabase); err != nil {
			return r.fail(ctx, alias, "create alias failed", err, requeue)
		}
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasCreated,
			"Alias %q created for database %q", aliasName, alias.Spec.TargetDatabase)

	case aliasTargetDiffers(existing.Database, alias.Spec.TargetDatabase):
		// Drift: someone re-pointed the alias out of band, or spec.targetDatabase
		// changed. Spec wins.
		if err := nc.AlterAliasTarget(ctx, aliasName, alias.Spec.TargetDatabase); err != nil {
			return r.fail(ctx, alias, "retarget alias failed", err, requeue)
		}
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasRetargeted,
			"Alias %q re-pointed from %q to %q", aliasName, existing.Database, alias.Spec.TargetDatabase)
	}

	// Only a target that served the pass serves this alias: written once its
	// statements succeeded, not on the connection alone — a cluster that lost
	// its majority accepts the connection and refuses the statements, and
	// writing it after the connection restarted the outage's clock on every
	// pass (knowledge rules 172, 210).
	r.setNamedCondition(ctx, alias, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")
	// Announced from the write that moves the alias into Ready, never from
	// this pass's copy, which may predate the last write (knowledge rules 145
	// and 183).
	msg := fmt.Sprintf("alias %q resolves to %q", aliasName, alias.Spec.TargetDatabase)
	if _, entered := r.writeStatus(ctx, alias, "Ready", metav1.ConditionTrue, "AliasReady", msg, alias.Spec.TargetDatabase); entered {
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasReady,
			"Alias %q resolves to %q", aliasName, alias.Spec.TargetDatabase)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// aliasTargetDiffers reports whether the database SHOW ALIASES lists for an
// alias is not the one spec.targetDatabase names. Database names are compared
// ignoring case: Neo4j lists the target lower-cased, and an exact match
// against a target spelled `Orders` re-pointed the alias, and announced it, on
// every pass.
func aliasTargetDiffers(live, desired string) bool {
	return !strings.EqualFold(live, desired)
}

func (r *Neo4jDatabaseAliasReconciler) handleDeletion(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, aliasName string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(alias, Neo4jDatabaseAliasFinalizer) {
		return ctrl.Result{}, nil
	}
	release := func() error { return removeFinalizer(ctx, r.Client, alias, Neo4jDatabaseAliasFinalizer) }

	if strings.EqualFold(alias.Spec.DeletionPolicy, "Retain") {
		return ctrl.Result{}, release()
	}
	// An alias another Neo4jDatabaseAlias still gives is left to it
	// (operations.md id 217).
	kept, err := aliasNames.keptFor(ctx, r.Client, alias)
	if err != nil {
		return ctrl.Result{}, err
	}
	if kept != "" {
		r.Recorder.Event(alias, corev1.EventTypeNormal, EventReasonDuplicateNameKept, kept)
		return ctrl.Result{}, release()
	}

	target, err := ResolveClusterRef(ctx, r.Client, alias.Namespace, alias.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	// The alias still routes queries to its target — a remote alias with the
	// credentials stored on it — so a deletion that cannot drop it waits,
	// bounded, as every other cleanup. A target that was not Ready made it
	// wait for ever; a connection or a drop that failed released the
	// finalizer at once (knowledge rule 178).
	cleanup := deletionCleanup{
		obj: alias, recorder: r.Recorder, reason: EventReasonAliasFailed,
		leftBehind: fmt.Sprintf("alias %q on %s", aliasName, targetRefDisplay(alias.Spec.ClusterRef)),
		byHand:     fmt.Sprintf("DROP ALIAS `%s` FOR DATABASE", neo4jclient.EscapeBackticks(aliasName)),
		record: func(msg string) bool {
			return r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonAliasFailed, msg, "")
		},
		release: release, requeue: r.requeueAfter(),
	}
	if done, res, err := cleanup.settleByTarget(ctx, r.Client, alias.Namespace, alias.Spec.ClusterRef, target); done {
		return res, err
	}
	if !target.IsReady() {
		return cleanup.blocked(ctx, targetDownCause(alias.Spec.ClusterRef))
	}

	nc, err := r.connectForDeletion(target)
	if err != nil {
		return cleanup.failed(ctx, connectFailedCause(alias.Spec.ClusterRef, err), err)
	}
	defer func() { _ = nc.Close() }()

	if err := nc.DropAliasIfExists(ctx, aliasName); err != nil {
		return cleanup.failed(ctx, statementFailedCause(alias.Spec.ClusterRef, "DROP ALIAS", err), err)
	}

	r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasDropped, "Alias %q dropped", aliasName)
	return ctrl.Result{}, release()
}

func (r *Neo4jDatabaseAliasReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jDatabaseAliasReconciler) fail(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	if targetAway(err) {
		// The target went away mid-pass: an outage, not this alias's failure
		// (knowledge rule 183). The deletion does not come through here.
		return r.unreachable(ctx, alias, err, requeue)
	}
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	// Announced when the failure starts or its message changes, not on every
	// retry of the same failure (knowledge rule 145).
	if r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonAliasFailed, msg, "") {
		r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonAliasFailed, msg)
	}
	// A rejection the server will repeat (Neo.ClientError.*: a malformed
	// statement, a missing role or user, an argument it refuses) cannot
	// resolve without someone changing something, and error backoff would
	// retry it within milliseconds and bury the one event that explains it.
	// It is already in status and the event above; retry it on the fixed
	// interval. Anything else — a connectivity blip, a transient conflict —
	// goes back as an error, to controller-runtime's backoff. Same rule as
	// Neo4jReplicaDatabase's fail(). Never return a requeue WITH an error:
	// controller-runtime ignores it (TestNoRequeueWithError).
	if isPermanentServerRejection(err) {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	return ctrl.Result{}, err
}

// setStatus writes the phase, message and Ready condition. It reports whether
// the write changed the phase, the message or the Ready condition's status or
// reason — a state transition — from the attempt whose write succeeded, so a
// caller can emit the matching event once, on the transition.
func (r *Neo4jDatabaseAliasReconciler) setStatus(
	ctx context.Context,
	alias *neo4jv1beta1.Neo4jDatabaseAlias,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message, observedTarget string,
) bool {
	transitioned, _ := r.writeStatus(ctx, alias, phase, readyStatus, readyReason, message, observedTarget)
	return transitioned
}

// writeStatus is setStatus that also reports whether the write moved the
// persisted phase into the one given (entered), the transition AliasReady
// announces.
func (r *Neo4jDatabaseAliasReconciler) writeStatus(
	ctx context.Context,
	alias *neo4jv1beta1.Neo4jDatabaseAlias,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message, observedTarget string,
) (transitioned, entered bool) {
	update := func() error {
		transitioned, entered = false, false
		latest := &neo4jv1beta1.Neo4jDatabaseAlias{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(alias), latest); err != nil {
			return err
		}
		phaseMoved := latest.Status.Phase != phase
		changed := phaseMoved || latest.Status.Message != message
		if SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message) {
			changed = true
		}
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if observedTarget != "" {
			latest.Status.ObservedTarget = observedTarget
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned, entered = changed, phaseMoved
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jDatabaseAlias status")
		return false, false
	}
	return transitioned, entered
}

// waitForTarget records that the target cannot serve the alias — reason
// ClusterNotReady (not Ready) or ClusterUnreachable (reads Ready, does not
// answer): the ClusterNotReady condition, and phase Pending unless the alias is
// already Ready at its generation, which it stays. The alias lives on the
// target and does not change while the target restarts, and the target
// announces its own state; demoting it made the pass that found the target
// back an AliasReady (#565; knowledge rule 183). The status is written only
// when it changes. It reports lasting when an unreachable target has outlasted
// targetUnreachableGrace.
func (r *Neo4jDatabaseAliasReconciler) waitForTarget(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, reason, msg string) (lasting bool) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jDatabaseAlias{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(alias), latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		lasting = recordTargetWait(&latest.Status.Conditions, &latest.Status.Phase, &latest.Status.Message,
			latest.Generation, reason, msg, time.Now())
		latest.Status.ObservedGeneration = latest.Generation
		if apiequality.Semantic.DeepEqual(*before, latest.Status) {
			return nil
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record the target wait on Neo4jDatabaseAlias")
		return false
	}
	return lasting
}

// unreachable handles a target that reads Ready but cannot be reached, or
// cannot serve the statement for now (targetAway: a cluster that lost its
// majority answers a transient error; knowledge rule 210):
// waitForTarget with reason ClusterUnreachable, logged at info and retried on
// the fixed interval. Only an outage that outlasts targetUnreachableGrace is
// this alias's failure: Failed, ConnectionFailed, announced once (knowledge
// rules 172 and 183).
func (r *Neo4jDatabaseAliasReconciler) unreachable(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, err error, requeue time.Duration) (ctrl.Result, error) {
	if r.waitForTarget(ctx, alias, ConditionReasonClusterUnreachable, targetAwayMessage(alias.Spec.ClusterRef, err)) {
		msg := fmt.Sprintf("failed to connect to Neo4j for more than %s: %v", targetUnreachableGrace, err)
		if r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, "") {
			r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	log.FromContext(ctx).Info("Target reads Ready but cannot be reached; retrying",
		"clusterRef", alias.Spec.ClusterRef, "error", err.Error())
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// aliasNeo4jClient is what the Neo4jDatabaseAlias reconcile asks of Neo4j.
// *neo4jclient.Client is one.
type aliasNeo4jClient interface {
	GetDatabaseInfo(ctx context.Context, databaseName string) (*neo4jclient.DatabaseInfo, error)
	ShowAlias(ctx context.Context, aliasName string) (*neo4jclient.AliasInfo, error)
	CreateAlias(ctx context.Context, aliasName, targetDatabase string) error
	AlterAliasTarget(ctx context.Context, aliasName, targetDatabase string) error
	Close() error
}

// connect returns the Neo4j client Reconcile uses for the resolved target.
func (r *Neo4jDatabaseAliasReconciler) connect(target ResolvedTarget) (aliasNeo4jClient, error) {
	if r.newNeo4jClient != nil {
		return r.newNeo4jClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

func (r *Neo4jDatabaseAliasReconciler) setNamedCondition(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, condType string, status metav1.ConditionStatus, reason, message string) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jDatabaseAlias{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(alias), latest); err != nil {
			return err
		}
		SetNamedCondition(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jDatabaseAlias", "condition", condType)
	}
}

// effectiveAliasName returns spec.name if set, else metadata.name.
func effectiveAliasName(alias *neo4jv1beta1.Neo4jDatabaseAlias) string {
	if alias.Spec.Name != "" {
		return alias.Spec.Name
	}
	return alias.Name
}

// SetupWithManager registers the controller and re-enqueues aliases when their
// target cluster flips to Ready.
func (r *Neo4jDatabaseAliasReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueue := EnqueueDependentsForClusterChange(
		mgr.GetClient(),
		func() client.ObjectList { return &neo4jv1beta1.Neo4jDatabaseAliasList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			aliases, ok := list.(*neo4jv1beta1.Neo4jDatabaseAliasList)
			if !ok {
				return
			}
			for i := range aliases.Items {
				a := &aliases.Items[i]
				emit(a.Name, a.Namespace, a.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jDatabaseAlias{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueue).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueue).
		// The other CRs of a name, when one comes, goes or is renamed
		// (operations.md id 217).
		Watches(&neo4jv1beta1.Neo4jDatabaseAlias{}, aliasNames.othersOfTheName(mgr.GetClient()), aliasNames.onClaimChange()).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(endsWithItsNamespace(r))
}
