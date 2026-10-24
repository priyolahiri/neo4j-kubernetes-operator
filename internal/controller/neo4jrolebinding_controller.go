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
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jRoleBindingFinalizer is the finalizer that ensures the controller
// gets a chance to revoke its grants (or release the finalizer when policy
// is Retain) before the CR is removed.
const Neo4jRoleBindingFinalizer = "neo4j.com/rolebinding-finalizer"

// Neo4jRoleBindingReconciler reconciles a Neo4jRoleBinding resource.
//
// Unlike Neo4jUser, this controller never creates or drops the underlying
// Neo4j user — it only manages role grants. The user is expected to exist
// in Neo4j (typically provisioned via SSO/LDAP first-login) by the time
// the cluster is Ready. If absent, the binding waits in a UserNotFound
// state and reconciles when the user appears.
type Neo4jRoleBindingReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.RoleBindingValidator

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// newNeo4jClient connects to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newNeo4jClient func(ResolvedTarget) (roleBindingNeo4jClient, error)
}

// roleBindingNeo4jClient is what the Neo4jRoleBinding reconciler asks of
// Neo4j. *neo4jclient.Client is one.
type roleBindingNeo4jClient interface {
	roleLister
	ShowUser(ctx context.Context, username string) (*neo4jclient.UserInfo, error)
	ListUserRoles(ctx context.Context, username string) ([]string, error)
	GrantRoleToUser(ctx context.Context, roleName, username string) error
	RevokeRoleFromUser(ctx context.Context, roleName, username string) error
	Close() error
}

func (r *Neo4jRoleBindingReconciler) connect(target ResolvedTarget) (roleBindingNeo4jClient, error) {
	if r.newNeo4jClient != nil {
		return r.newNeo4jClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jrolebindings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jrolebindings/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jroles,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jusers,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jRoleBinding resource toward its desired state.
func (r *Neo4jRoleBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jrolebinding", req.NamespacedName)

	rb := &neo4jv1beta1.Neo4jRoleBinding{}
	if err := r.Get(ctx, req.NamespacedName, rb); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	requeue := r.requeueAfter()

	if rb.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, rb)
	}

	if !controllerutil.ContainsFinalizer(rb, Neo4jRoleBindingFinalizer) {
		if err := addFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer); err != nil {
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Validate
	if r.Validator != nil {
		res := r.Validator.Validate(ctx, rb)
		r.advisoryWarnings.record(r.Recorder, rb, EventReasonValidationWarning, res.Warnings)
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass while the spec stays invalid (knowledge rule 136).
			if r.setStatus(ctx, rb, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, nil) {
				r.Recorder.Event(rb, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// Resolve the target (cluster/standalone via clusterRef, or Aura instance).
	target, err := ResolveClusterRef(ctx, r.Client, rb.Namespace, rb.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve target ref")
		return ctrl.Result{}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(rb.Spec.ClusterRef))
		r.setStatus(ctx, rb, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		r.waitForTarget(ctx, rb, fmt.Sprintf("%s is not Ready", targetRefDisplay(rb.Spec.ClusterRef)))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, rb, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	// Connect
	nc, err := r.connect(target)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		// Retried with backoff; the same failure is announced once (knowledge
		// rule 145).
		if r.setStatus(ctx, rb, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil) {
			r.Recorder.Event(rb, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{}, err
	}
	defer func() { _ = nc.Close() }()

	// Verify the user exists. We never create users from a binding.
	info, err := nc.ShowUser(ctx, rb.Spec.Username)
	if err != nil {
		return r.fail(ctx, rb, "lookup failed", err, requeue)
	}
	if info == nil {
		r.reportUserNotFound(ctx, rb)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, rb, ConditionTypeUserNotFound, metav1.ConditionFalse, "UserPresent", "")

	// Resolve same-namespace Neo4jRole CR names in spec.roles to their
	// effective Neo4j role name (#260); literal Neo4j names pass through.
	desiredRoles := normaliseRoles(rb.Spec.Roles)
	desiredRoles, resolvedRoles := resolveRoleNames(ctx, r.Client, rb.Namespace, targetRefKey(rb.Spec.ClusterRef), desiredRoles)
	desiredRoles = normaliseRoles(desiredRoles) // dedupe any CR-name/spec.name collisions
	r.reportResolvedRoles(ctx, rb, resolvedRoles)
	currentRoles := normaliseRoles(info.Roles)

	addRoles, dropRoles, missing, err := r.diffRoles(ctx, nc, rb, desiredRoles, currentRoles)
	if err != nil {
		return r.fail(ctx, rb, "role lookup failed", err, requeue)
	}
	for _, role := range addRoles {
		if err := nc.GrantRoleToUser(ctx, role, rb.Spec.Username); err != nil {
			return r.fail(ctx, rb, fmt.Sprintf("grant %q failed", role), err, requeue)
		}
	}
	for _, role := range dropRoles {
		if err := nc.RevokeRoleFromUser(ctx, role, rb.Spec.Username); err != nil {
			return r.fail(ctx, rb, fmt.Sprintf("revoke %q failed", role), err, requeue)
		}
	}
	if len(addRoles) > 0 {
		r.Recorder.Eventf(rb, corev1.EventTypeNormal, EventReasonRolesGranted, "granted %v", addRoles)
	}
	if len(dropRoles) > 0 {
		r.Recorder.Eventf(rb, corev1.EventTypeNormal, EventReasonRolesRevoked, "revoked %v", dropRoles)
	}

	// Re-read for the GrantedRoles status field. This is the intersection of
	// desiredRoles and the user's actual current roles.
	finalRoles, err := nc.ListUserRoles(ctx, rb.Spec.Username)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to read final role list (non-fatal)")
		finalRoles = currentRoles
	}
	finalRoles = normaliseRoles(finalRoles)

	desiredSet := stringSet(desiredRoles)
	granted := make([]string, 0, len(desiredRoles))
	for _, role := range finalRoles {
		if _, ok := desiredSet[role]; ok {
			granted = append(granted, role)
		}
	}
	sort.Strings(granted)

	r.reportPendingRoles(ctx, rb, missing)

	allDesiredGranted := stringSlicesEqualSorted(desiredRoles, granted)
	if allDesiredGranted {
		r.setNamedCondition(ctx, rb, ConditionTypeRolesSynced, metav1.ConditionTrue, ConditionReasonRolesSynced, "all desired roles granted")
	} else {
		r.setNamedCondition(ctx, rb, ConditionTypeRolesSynced, metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("granted %v of desired %v", granted, desiredRoles))
	}

	if allDesiredGranted && len(missing) == 0 {
		// Distinguish first-time success ("Created") from a subsequent
		// successful reconcile after a spec change ("Updated"), each from what
		// the write changed — never from this pass's copy, which the informer
		// cache may hold from before the last write. A restart of the target
		// is neither (waitForTarget; knowledge rule 158).
		_, entered, grantedChanged := r.writeStatus(ctx, rb, "Ready", metav1.ConditionTrue, "BindingReady",
			fmt.Sprintf("user %q has all %d desired roles", rb.Spec.Username, len(granted)),
			granted)
		switch {
		case entered:
			// This binding has reached Ready (a fresh CR, or recovering from a
			// failure of its own). EventReasonBindingCreated names the
			// initial-grant transition.
			r.Recorder.Eventf(rb, corev1.EventTypeNormal, EventReasonBindingCreated,
				"RoleBinding for user %q ready with %d roles", rb.Spec.Username, len(granted))
		case grantedChanged:
			// Already Ready before, but the granted-role set has changed —
			// either spec.roles edited, or an externally granted/revoked role
			// has been reconciled.
			r.Recorder.Eventf(rb, corev1.EventTypeNormal, EventReasonBindingUpdated,
				"RoleBinding for user %q updated; granted roles now %v", rb.Spec.Username, granted)
		}
	} else {
		r.setStatus(ctx, rb, "Pending", metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("user %q has %d of %d desired roles", rb.Spec.Username, len(granted), len(desiredRoles)),
			granted)
	}

	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *Neo4jRoleBindingReconciler) handleDeletion(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(rb, Neo4jRoleBindingFinalizer) {
		return ctrl.Result{}, nil
	}
	requeue := r.requeueAfter()

	if strings.EqualFold(rb.Spec.DeletionPolicy, "Retain") {
		return ctrl.Result{}, removeFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer)
	}

	target, err := ResolveClusterRef(ctx, r.Client, rb.Namespace, rb.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !target.Found {
		return ctrl.Result{}, removeFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer)
	}
	if !target.IsReady() {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := r.connect(target)
	if err != nil {
		logger.Error(err, "cannot connect during binding deletion; releasing finalizer")
		return ctrl.Result{}, removeFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer)
	}
	defer func() { _ = nc.Close() }()

	// Revoke only the roles previously recorded as granted by this binding.
	// We never touch roles we did not add — that's the whole point of
	// non-exclusive bindings.
	for _, role := range rb.Status.GrantedRoles {
		err := nc.RevokeRoleFromUser(ctx, role, rb.Spec.Username)
		if err == nil {
			continue
		}
		// A "not found" for this grant (the user lacks the role, or the
		// role/user is already gone) means THIS revoke is satisfied. Skip it
		// and keep revoking the rest — don't abandon the remaining grants the
		// way a host-level failure would.
		if isAlreadyGoneCleanup(err) {
			continue
		}
		if classifyFinalizerCleanup(rb, err) == retryCleanup {
			// Retried every RequeueAfter and on every status change of the
			// target until the revoke succeeds or the grace period ends. The
			// failure is written to status and announced when it starts or
			// its message changes, not on every retry (knowledge rule 145).
			msg := fmt.Sprintf("revoke %q from %q failed, will retry: %v", role, rb.Spec.Username, err)
			if r.setStatus(ctx, rb, "Failed", metav1.ConditionFalse, EventReasonBindingFailed, msg, nil) {
				r.Recorder.Event(rb, corev1.EventTypeWarning, EventReasonBindingFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		// Host gone or grace period exceeded: the remaining revokes would fail
		// the same way, so release the finalizer rather than wedge deletion.
		r.Recorder.Eventf(rb, corev1.EventTypeWarning, EventReasonBindingFailed,
			"revoke %q from %q failed; releasing finalizer to avoid wedging deletion: %v", role, rb.Spec.Username, err)
		return ctrl.Result{}, removeFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer)
	}
	r.Recorder.Eventf(rb, corev1.EventTypeNormal, EventReasonBindingDeleted,
		"revoked %d role(s) from user %q", len(rb.Status.GrantedRoles), rb.Spec.Username)

	return ctrl.Result{}, removeFinalizer(ctx, r.Client, rb, Neo4jRoleBindingFinalizer)
}

// diffRoles returns the role grants to add and revoke, and the desired roles
// that do not exist yet (missingRoles). When EnforceExclusive is false
// (default), only the desired roles are considered for revoke; extra roles
// granted by other means are left alone. PUBLIC is always excluded from both
// directions.
func (r *Neo4jRoleBindingReconciler) diffRoles(
	ctx context.Context, roles roleLister, rb *neo4jv1beta1.Neo4jRoleBinding, desired, current []string,
) (add, drop, missing []string, err error) {
	currentSet := stringSet(current)
	desiredSet := stringSet(desired)

	missing, err = missingRoles(ctx, r.Client, roles, rb.Namespace, targetRefKey(rb.Spec.ClusterRef), desired, currentSet)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, d := range desired {
		if _, ok := currentSet[d]; ok {
			continue
		}
		if containsString(missing, d) {
			continue
		}
		add = append(add, d)
	}

	if rb.Spec.EnforceExclusive {
		for _, c := range current {
			if strings.EqualFold(c, "PUBLIC") {
				continue
			}
			if _, ok := desiredSet[c]; !ok {
				drop = append(drop, c)
			}
		}
	}

	// Revoke roles previously granted by this binding that have since been
	// removed from .spec.roles, even when not exclusive.
	previouslyGranted := stringSet(rb.Status.GrantedRoles)
	for prev := range previouslyGranted {
		if _, ok := desiredSet[prev]; ok {
			continue
		}
		if strings.EqualFold(prev, "PUBLIC") {
			continue
		}
		if containsString(drop, prev) {
			continue
		}
		drop = append(drop, prev)
	}

	sort.Strings(add)
	sort.Strings(drop)
	return add, drop, missing, nil
}

func (r *Neo4jRoleBindingReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jRoleBindingReconciler) fail(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	// Announced when the failure starts or its message changes, not on every
	// retry of the same failure (knowledge rule 145).
	if r.setStatus(ctx, rb, "Failed", metav1.ConditionFalse, EventReasonBindingFailed, msg, nil) {
		r.Recorder.Event(rb, corev1.EventTypeWarning, EventReasonBindingFailed, msg)
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
func (r *Neo4jRoleBindingReconciler) setStatus(
	ctx context.Context,
	rb *neo4jv1beta1.Neo4jRoleBinding,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	grantedRoles []string,
) bool {
	changed, _, _ := r.writeStatus(ctx, rb, phase, readyStatus, readyReason, message, grantedRoles)
	return changed
}

// writeStatus is setStatus that also reports whether the write moved the
// persisted phase into the one given (entered) and whether it changed
// status.grantedRoles — the transitions BindingCreated and BindingUpdated
// announce.
func (r *Neo4jRoleBindingReconciler) writeStatus(
	ctx context.Context,
	rb *neo4jv1beta1.Neo4jRoleBinding,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	grantedRoles []string,
) (transitioned, entered, grantedChanged bool) {
	update := func() error {
		transitioned, entered, grantedChanged = false, false, false
		latest := &neo4jv1beta1.Neo4jRoleBinding{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rb), latest); err != nil {
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
		granted := false
		if grantedRoles != nil {
			granted = !stringSlicesEqualSorted(latest.Status.GrantedRoles, grantedRoles)
			latest.Status.GrantedRoles = grantedRoles
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned, entered, grantedChanged = changed, phaseMoved, granted
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jRoleBinding status")
		return false, false, false
	}
	return transitioned, entered, grantedChanged
}

// waitForTarget records that the target is not Ready: the ClusterNotReady
// condition, and phase Pending unless the binding is already Ready at its
// generation, which it stays while the target restarts — so the pass that
// finds the target back announces nothing (recordTargetWait; knowledge rule
// 158).
func (r *Neo4jRoleBindingReconciler) waitForTarget(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding, msg string) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jRoleBinding{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rb), latest); err != nil {
			return err
		}
		recordTargetWait(&latest.Status.Conditions, &latest.Status.Phase, &latest.Status.Message, latest.Generation, msg)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record the target wait on Neo4jRoleBinding")
	}
}

// reportUserNotFound records that the bound user does not exist (yet) and
// announces UserNotFound when the wait starts, not on every pass until the
// user is provisioned (knowledge rule 145).
func (r *Neo4jRoleBindingReconciler) reportUserNotFound(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding) {
	msg := fmt.Sprintf("user %q does not exist in Neo4j; bindings only manage grants for externally provisioned users", rb.Spec.Username)
	r.setNamedCondition(ctx, rb, ConditionTypeUserNotFound, metav1.ConditionTrue, EventReasonUserNotFound, msg)
	if r.setStatus(ctx, rb, "Pending", metav1.ConditionFalse, EventReasonUserNotFound, msg, nil) {
		r.Recorder.Eventf(rb, corev1.EventTypeWarning, EventReasonUserNotFound, "user %q does not exist", rb.Spec.Username)
	}
}

// reportPendingRoles writes the PendingDependencies condition and announces
// RolePending when the wait starts or its set of missing roles changes — not
// on every pass while the same roles stay missing (knowledge rule 145).
func (r *Neo4jRoleBindingReconciler) reportPendingRoles(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding, missing []string) {
	if len(missing) == 0 {
		r.setNamedCondition(ctx, rb, ConditionTypePendingDependencies, metav1.ConditionFalse, "AllDependenciesPresent", "")
		return
	}
	msg := pendingRolesMessage(missing)
	if r.setNamedCondition(ctx, rb, ConditionTypePendingDependencies, metav1.ConditionTrue, ConditionReasonRolesPending, msg) {
		r.Recorder.Eventf(rb, corev1.EventTypeWarning, EventReasonRolePending, "%s", msg)
	}
}

// reportResolvedRoles records the CR names spec.roles resolved (the
// RoleNamesResolved condition) and announces RolesResolved when the mapping
// changes, from the attempt whose write succeeded — not on every pass.
func (r *Neo4jRoleBindingReconciler) reportResolvedRoles(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding, resolved map[string]string) {
	announce := false
	update := func() error {
		announce = false
		latest := &neo4jv1beta1.Neo4jRoleBinding{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rb), latest); err != nil {
			return err
		}
		write, a := setResolvedRolesCondition(&latest.Status.Conditions, latest.Generation, resolved)
		if !write {
			return nil
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		announce = a
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record resolved role names on Neo4jRoleBinding")
		return
	}
	if announce {
		r.Recorder.Event(rb, corev1.EventTypeNormal, EventReasonRolesResolved, resolvedRolesMessage(resolved))
	}
}

// setNamedCondition writes one named condition. It reports whether the write
// changed the condition's status, reason or message, from the attempt whose
// write succeeded, so a caller can announce the change once.
func (r *Neo4jRoleBindingReconciler) setNamedCondition(ctx context.Context, rb *neo4jv1beta1.Neo4jRoleBinding, condType string, status metav1.ConditionStatus, reason, message string) bool {
	changed := false
	update := func() error {
		changed = false
		latest := &neo4jv1beta1.Neo4jRoleBinding{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rb), latest); err != nil {
			return err
		}
		c := setNamedConditionChanged(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		changed = c
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jRoleBinding", "condition", condType)
		return false
	}
	return changed
}

// SetupWithManager registers the controller and wires up watches:
//   - Neo4jRole: bindings pending on a missing role re-reconcile when the
//     role lands.
//   - Neo4jEnterpriseCluster / Neo4jEnterpriseStandalone: re-reconcile every
//     binding pointing at a changed cluster, so bindings react immediately
//     to the cluster's Ready condition flipping (otherwise we wait up to
//     30s for the next requeue).
func (r *Neo4jRoleBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	enqueueBindingsForCluster := EnqueueDependentsForClusterChange(
		c,
		func() client.ObjectList { return &neo4jv1beta1.Neo4jRoleBindingList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			bindings, ok := list.(*neo4jv1beta1.Neo4jRoleBindingList)
			if !ok {
				return
			}
			for i := range bindings.Items {
				b := &bindings.Items[i]
				emit(b.Name, b.Namespace, b.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jRoleBinding{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueueBindingsForCluster).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueueBindingsForCluster).
		Watches(&neo4jv1beta1.Neo4jRole{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			role, ok := obj.(*neo4jv1beta1.Neo4jRole)
			if !ok {
				return nil
			}
			// Match a binding's spec.roles against BOTH the role's effective
			// Neo4j name AND its CR metadata.name, so a binding that referenced
			// the role by either form re-reconciles when the role lands (#260).
			roleName := role.Spec.Name
			if roleName == "" {
				roleName = role.Name
			}
			roleKey := targetRefKey(role.Spec.ClusterRef)
			bindings := &neo4jv1beta1.Neo4jRoleBindingList{}
			if err := c.List(ctx, bindings, client.InNamespace(role.Namespace)); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for i := range bindings.Items {
				b := &bindings.Items[i]
				if targetRefKey(b.Spec.ClusterRef) != roleKey {
					continue
				}
				for _, rname := range b.Spec.Roles {
					if rname == roleName || rname == role.Name {
						reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: b.Namespace, Name: b.Name}})
						break
					}
				}
			}
			return reqs
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}

// stringSet builds a presence map from a string slice.
func stringSet(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, s := range in {
		out[s] = struct{}{}
	}
	return out
}
