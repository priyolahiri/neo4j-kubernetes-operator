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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jAuthRuleFinalizer guards the controller's chance to drop the rule (or
// release the finalizer when DeletionPolicy is Retain) before the CR is
// removed.
const Neo4jAuthRuleFinalizer = "neo4j.com/authrule-finalizer"

// abacAuthorizationProvidersKey is the Neo4j configuration key that names
// the OIDC provider(s) wired up for ABAC. Auth rules don't function without
// at least one provider listed here.
const abacAuthorizationProvidersKey = "dbms.security.abac.authorization_providers"

// ConditionTypeAuthRule* extends the shared condition vocabulary with
// rule-specific conditions. Defined locally rather than in events.go because
// they are only meaningful on Neo4jAuthRule status.
const (
	ConditionTypeOIDCProviderConfigured = "OIDCProviderConfigured"
	ConditionTypeAuthRuleVersionTooOld  = "AuthRuleVersionTooOld"
	ConditionReasonAuthRuleReady        = "AuthRuleReady"
	ConditionReasonAuthRuleVersionGate  = "ClusterTooOld"
	ConditionReasonOIDCNotConfigured    = "OIDCProviderNotConfigured"
)

// Neo4jAuthRuleReconciler reconciles a Neo4jAuthRule resource.
type Neo4jAuthRuleReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.AuthRuleValidator

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// newDeletionClient connects the deletion to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newDeletionClient func(ResolvedTarget) (authRuleDeletionClient, error)

	// connectTarget connects the reconcile to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests make the connection fail here.
	connectTarget func(ResolvedTarget) (*neo4j.Client, error)
}

// authRuleDeletionClient is what the Neo4jAuthRule deletion asks of Neo4j.
// *neo4j.Client is one.
type authRuleDeletionClient interface {
	DropAuthRuleIfExists(ctx context.Context, ruleName string) error
	Close() error
}

func (r *Neo4jAuthRuleReconciler) connectForDeletion(target ResolvedTarget) (authRuleDeletionClient, error) {
	if r.newDeletionClient != nil {
		return r.newDeletionClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jauthrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jauthrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jauthrules/finalizers,verbs=update

// Reconcile drives a Neo4jAuthRule toward its desired state.
func (r *Neo4jAuthRuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jauthrule", req.NamespacedName)

	rule := &neo4jv1beta1.Neo4jAuthRule{}
	if err := r.Get(ctx, req.NamespacedName, rule); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	requeue := r.requeueAfter()

	if rule.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, rule)
	}

	if !controllerutil.ContainsFinalizer(rule, Neo4jAuthRuleFinalizer) {
		if err := addFinalizer(ctx, r.Client, rule, Neo4jAuthRuleFinalizer); err != nil {
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Validate
	if r.Validator != nil {
		res := r.Validator.Validate(ctx, rule)
		r.advisoryWarnings.record(r.Recorder, rule, EventReasonValidationWarning, res.Warnings)
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass while the spec stays invalid (knowledge rule 136).
			if r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, nil, nil) {
				r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// Resolve cluster
	target, err := ResolveClusterRef(ctx, r.Client, rule.Namespace, rule.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve clusterRef")
		return ctrl.Result{}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("clusterRef %q not found", rule.Spec.ClusterRef)
		// A wait, announced once: when it starts, not on every pass until the
		// deployment exists (operations.md ids 145 and 182).
		if r.setStatus(ctx, rule, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, nil, nil) {
			r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonClusterNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		r.waitForTarget(ctx, rule, ConditionReasonClusterNotReady,
			fmt.Sprintf("clusterRef %q is not Ready", rule.Spec.ClusterRef))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// Version gate — auth rules require Neo4j 2026.03+.
	if !targetSupportsAuthRules(target) {
		r.targetNotAwaited(ctx, rule)
		msg := fmt.Sprintf("Neo4jAuthRule requires Neo4j 2026.03 or later; cluster %q runs %s. See https://neo4j.com/docs/operations-manual/current/authentication-authorization/attribute-based-access-control/.",
			rule.Spec.ClusterRef, targetVersionString(target))
		r.setNamedCondition(ctx, rule, ConditionTypeAuthRuleVersionTooOld, metav1.ConditionTrue, ConditionReasonAuthRuleVersionGate, msg)
		// Announced when the refusal starts or its message changes, not on
		// every pass while the cluster stays on the older version (knowledge
		// rule 145).
		if r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonAuthRuleVersionTooOld, msg, nil, nil) {
			r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonAuthRuleVersionTooOld, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, rule, ConditionTypeAuthRuleVersionTooOld, metav1.ConditionFalse, "VersionSupported", "")

	// OIDC provider configuration check (Option A from the design proposal:
	// document as prerequisite, surface a clear status condition rather
	// than mutating the cluster's spec.config).
	if !targetHasABACProvider(target) {
		r.targetNotAwaited(ctx, rule)
		msg := fmt.Sprintf(
			"clusterRef %q does not configure %s in spec.config. ABAC requires at least one OIDC provider name listed there. See docs/api_reference/neo4jauthrule.md for setup.",
			rule.Spec.ClusterRef, abacAuthorizationProvidersKey,
		)
		r.setNamedCondition(ctx, rule, ConditionTypeOIDCProviderConfigured, metav1.ConditionFalse, ConditionReasonOIDCNotConfigured, msg)
		// Announced when the wait starts, not on every pass until the cluster
		// lists a provider (knowledge rule 145).
		if r.setStatus(ctx, rule, "Pending", metav1.ConditionFalse, EventReasonOIDCProviderNotConfigured, msg, nil, nil) {
			r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonOIDCProviderNotConfigured, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, rule, ConditionTypeOIDCProviderConfigured, metav1.ConditionTrue, "OIDCConfigured", "")

	// Connect.
	connect := target.NewClient
	if r.connectTarget != nil {
		connect = func(client.Client) (*neo4j.Client, error) { return r.connectTarget(target) }
	}
	nc, err := connect(r.Client)
	if err != nil {
		if targetUnreachable(err) {
			return r.unreachable(ctx, rule, err, requeue)
		}
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		// Retried with backoff; the same failure is announced once (knowledge
		// rule 145).
		if r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil, nil) {
			r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{}, err
	}
	defer func() { _ = nc.Close() }()
	// Only a target that answered is ready for this rule: written after the
	// connection, so an unreachable one keeps the time its outage began.
	r.setNamedCondition(ctx, rule, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	ruleName := rule.Spec.Name
	if ruleName == "" {
		ruleName = rule.Name
	}
	desiredCondition := strings.TrimSpace(rule.Spec.Condition)
	desiredEnabled := true
	if rule.Spec.Enabled != nil {
		desiredEnabled = *rule.Spec.Enabled
	}
	desiredRoles := normaliseRoles(rule.Spec.GrantedRoles)

	// Verify granted roles exist (either as Neo4jRole CRs in this namespace
	// or as live roles in Neo4j). Roles missing from both are reported via
	// PendingDependencies; we still proceed with whatever roles are present.
	missingRoles, err := r.findMissingRoles(ctx, nc, rule, desiredRoles)
	if err != nil {
		return r.fail(ctx, rule, "role lookup failed", err, requeue)
	}
	r.reportPendingRoles(ctx, rule, missingRoles)

	// Read live state.
	live, err := nc.ShowAuthRule(ctx, ruleName)
	if err != nil {
		return r.fail(ctx, rule, "SHOW AUTH RULES failed", err, requeue)
	}

	// Create-or-replace (which subsumes "alter" semantics) is the simplest
	// path when the live condition or enabled flag drift from spec. The
	// condition string is interpolated as-is; the validator has already
	// rejected DDL keywords so the surface is bounded to expression syntax.
	if live == nil || live.Condition != desiredCondition || live.Enabled != desiredEnabled {
		if err := nc.CreateOrReplaceAuthRule(ctx, ruleName, desiredCondition, desiredEnabled); err != nil {
			return r.fail(ctx, rule, "CREATE OR REPLACE AUTH RULE failed", err, requeue)
		}
		if live == nil {
			r.Recorder.Eventf(rule, corev1.EventTypeNormal, EventReasonAuthRuleCreated, "auth rule %q created", ruleName)
		} else {
			r.Recorder.Eventf(rule, corev1.EventTypeNormal, EventReasonAuthRuleUpdated, "auth rule %q condition or enabled flag updated", ruleName)
		}
	}

	// Diff role grants. CREATE OR REPLACE clears existing role grants on
	// the rule, so when we just re-created the rule we always re-grant the
	// full desired set.
	currentRoles := []string{}
	if live != nil {
		currentRoles = live.Roles
	}
	if live == nil || live.Condition != desiredCondition || live.Enabled != desiredEnabled {
		// CREATE OR REPLACE wiped any prior grants — reset baseline.
		currentRoles = []string{}
	}

	add, drop := diffRolesSimple(desiredRoles, currentRoles, missingRoles, rule.Spec.EnforceRoles)
	if len(add) > 0 {
		if err := nc.GrantRolesToAuthRule(ctx, ruleName, add); err != nil {
			return r.fail(ctx, rule, fmt.Sprintf("grant %v failed", add), err, requeue)
		}
		r.Recorder.Eventf(rule, corev1.EventTypeNormal, EventReasonRolesGranted, "granted %v to auth rule %q", add, ruleName)
	}
	if len(drop) > 0 {
		if err := nc.RevokeRolesFromAuthRule(ctx, ruleName, drop); err != nil {
			return r.fail(ctx, rule, fmt.Sprintf("revoke %v failed", drop), err, requeue)
		}
		r.Recorder.Eventf(rule, corev1.EventTypeNormal, EventReasonRolesRevoked, "revoked %v from auth rule %q", drop, ruleName)
	}

	// Re-read for status truth.
	final, err := nc.ShowAuthRule(ctx, ruleName)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to re-read auth rule after reconcile (non-fatal)")
		final = &neo4j.AuthRuleInfo{Name: ruleName, Condition: desiredCondition, Enabled: desiredEnabled, Roles: desiredRoles}
	}

	appliedRoles := append([]string(nil), final.Roles...)
	sort.Strings(appliedRoles)
	allDesiredGranted := stringSlicesEqualSorted(desiredRoles, appliedRoles)

	if allDesiredGranted && len(missingRoles) == 0 {
		r.setNamedCondition(ctx, rule, ConditionTypeRolesSynced, metav1.ConditionTrue, ConditionReasonRolesSynced, "all granted roles in sync")
		r.setStatus(ctx, rule, "Ready", metav1.ConditionTrue, ConditionReasonAuthRuleReady,
			fmt.Sprintf("auth rule %q is in sync (enabled=%t, roles=%v)", ruleName, final.Enabled, appliedRoles),
			appliedRoles, &final.Enabled)
	} else if len(missingRoles) > 0 {
		r.setStatus(ctx, rule, neo4jv1beta1.PhasePendingDependencies, metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("auth rule %q applied; %d/%d granted, waiting on missing roles %v", ruleName, len(appliedRoles), len(desiredRoles), missingRoles),
			appliedRoles, &final.Enabled)
	} else {
		r.setNamedCondition(ctx, rule, ConditionTypeRolesSynced, metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("granted %v of desired %v", appliedRoles, desiredRoles))
		r.setStatus(ctx, rule, "Pending", metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("auth rule %q has %d of %d desired role grants", ruleName, len(appliedRoles), len(desiredRoles)),
			appliedRoles, &final.Enabled)
	}

	return ctrl.Result{RequeueAfter: requeue}, nil
}

// targetNotAwaited records ClusterNotReady=False for a rule its target's spec
// refuses — a version older than 2026.03, no OIDC provider listed — once the
// target reads Ready. The refusal is the rule's state, read from the spec
// without a connection, so there is no outage whose start the condition must
// keep (id 172). Returning before the write after the connection left
// ClusterNotReady=True from the last restart for as long as the refusal held
// (knowledge rule 158).
func (r *Neo4jAuthRuleReconciler) targetNotAwaited(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule) {
	r.setNamedCondition(ctx, rule, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")
}

func (r *Neo4jAuthRuleReconciler) handleDeletion(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(rule, Neo4jAuthRuleFinalizer) {
		return ctrl.Result{}, nil
	}
	release := func() error { return removeFinalizer(ctx, r.Client, rule, Neo4jAuthRuleFinalizer) }

	if strings.EqualFold(rule.Spec.DeletionPolicy, "Retain") {
		return ctrl.Result{}, release()
	}

	target, err := ResolveClusterRef(ctx, r.Client, rule.Namespace, rule.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	ruleName := rule.Spec.Name
	if ruleName == "" {
		ruleName = rule.Name
	}
	// A target that is not Ready used to release the finalizer at once, so a
	// rule deleted during a restart stayed in Neo4j and kept granting its
	// roles. It waits, bounded, as every other cleanup (knowledge rule 178).
	cleanup := deletionCleanup{
		obj: rule, recorder: r.Recorder, reason: EventReasonAuthRuleFailed,
		leftBehind: fmt.Sprintf("auth rule %q on %s", ruleName, targetRefDisplay(rule.Spec.ClusterRef)),
		byHand:     fmt.Sprintf("DROP AUTH RULE `%s`", neo4j.EscapeBackticks(ruleName)),
		record: func(msg string) bool {
			return r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonAuthRuleFailed, msg, nil, nil)
		},
		release: release, requeue: r.requeueAfter(),
	}
	if done, res, err := cleanup.settleByTarget(ctx, r.Client, rule.Namespace, rule.Spec.ClusterRef, target); done {
		return res, err
	}
	if !targetSupportsAuthRules(target) {
		// The deployment runs a version that cannot hold auth rules: there
		// is no rule to drop.
		return ctrl.Result{}, release()
	}
	if !target.IsReady() {
		return cleanup.blocked(ctx, targetDownCause(rule.Spec.ClusterRef))
	}

	nc, err := r.connectForDeletion(target)
	if err != nil {
		return cleanup.blocked(ctx, connectFailedCause(rule.Spec.ClusterRef, err))
	}
	defer func() { _ = nc.Close() }()

	if err := nc.DropAuthRuleIfExists(ctx, ruleName); err != nil {
		return cleanup.blocked(ctx, statementFailedCause(rule.Spec.ClusterRef, "DROP AUTH RULE", err))
	}
	r.Recorder.Eventf(rule, corev1.EventTypeNormal, EventReasonAuthRuleDeleted, "auth rule %q dropped", ruleName)

	return ctrl.Result{}, release()
}

// findMissingRoles returns the subset of desired role names that exist
// neither as a Neo4jRole CR in the same namespace nor as a live role in
// Neo4j. Built-in roles are always considered to exist.
func (r *Neo4jAuthRuleReconciler) findMissingRoles(ctx context.Context, nc *neo4j.Client, rule *neo4jv1beta1.Neo4jAuthRule, desired []string) ([]string, error) {
	if len(desired) == 0 {
		return nil, nil
	}
	liveRoles, err := nc.ListRoles(ctx)
	if err != nil {
		return nil, err
	}
	livePresent := make(map[string]struct{}, len(liveRoles))
	for _, lr := range liveRoles {
		livePresent[lr.Role] = struct{}{}
	}
	var missing []string
	for _, name := range desired {
		if validation.IsBuiltInRole(name) {
			continue
		}
		if _, ok := livePresent[name]; ok {
			continue
		}
		if r.roleResourceExists(ctx, rule.Namespace, rule.Spec.ClusterRef, name) {
			continue
		}
		missing = append(missing, name)
	}
	return missing, nil
}

// roleResourceExists reports whether a Neo4jRole CR with the given Neo4j
// role name exists in the same namespace and points at the same cluster.
func (r *Neo4jAuthRuleReconciler) roleResourceExists(ctx context.Context, namespace, clusterRef, roleName string) bool {
	list := &neo4jv1beta1.Neo4jRoleList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return false
	}
	for i := range list.Items {
		role := &list.Items[i]
		if role.Spec.ClusterRef != clusterRef {
			continue
		}
		name := role.Spec.Name
		if name == "" {
			name = role.Name
		}
		if name == roleName {
			return true
		}
	}
	return false
}

// diffRolesSimple computes the add/drop sets given desired and current role
// slices. Roles in `missing` are excluded from the add set so we don't try
// (and fail) to grant a non-existent role. When enforceExclusive is false,
// the drop set is empty (we never revoke anything not in `desired`).
func diffRolesSimple(desired, current, missing []string, enforceExclusive bool) (add, drop []string) {
	missingSet := stringSet(missing)
	currentSet := stringSet(current)
	desiredSet := stringSet(desired)

	for _, d := range desired {
		if _, ok := missingSet[d]; ok {
			continue
		}
		if _, ok := currentSet[d]; ok {
			continue
		}
		add = append(add, d)
	}
	if enforceExclusive {
		for _, c := range current {
			if _, ok := desiredSet[c]; !ok {
				drop = append(drop, c)
			}
		}
	}
	sort.Strings(add)
	sort.Strings(drop)
	return add, drop
}

func (r *Neo4jAuthRuleReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jAuthRuleReconciler) fail(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	if targetUnreachable(err) {
		// The target went away mid-pass: an outage, not this rule's failure
		// (knowledge rule 172).
		return r.unreachable(ctx, rule, err, requeue)
	}
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	// Announced when the failure starts or its message changes, not on every
	// retry of the same failure (knowledge rule 145).
	if r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonAuthRuleFailed, msg, nil, nil) {
		r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonAuthRuleFailed, msg)
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
func (r *Neo4jAuthRuleReconciler) setStatus(
	ctx context.Context,
	rule *neo4jv1beta1.Neo4jAuthRule,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	appliedRoles []string,
	appliedEnabled *bool,
) bool {
	transitioned := false
	update := func() error {
		transitioned = false
		latest := &neo4jv1beta1.Neo4jAuthRule{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rule), latest); err != nil {
			return err
		}
		changed := latest.Status.Phase != phase || latest.Status.Message != message
		if SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message) {
			changed = true
		}
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if appliedRoles != nil {
			latest.Status.AppliedRoles = appliedRoles
		}
		if appliedEnabled != nil {
			b := *appliedEnabled
			latest.Status.AppliedEnabled = &b
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned = changed
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jAuthRule status")
		return false
	}
	return transitioned
}

// waitForTarget records that the target cannot serve the rule (reason
// ClusterNotReady or ClusterUnreachable): the ClusterNotReady condition, and
// phase Pending unless the rule already has an outcome at its generation —
// Ready, a failure or a wait of its own — which it keeps while the target
// restarts, as a user, role and binding do (recordTargetWait; knowledge rules
// 158, 189). The status is written only when it
// changes. It reports lasting when an unreachable target has outlasted
// targetUnreachableGrace (id 172).
func (r *Neo4jAuthRuleReconciler) waitForTarget(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule, reason, msg string) (lasting bool) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jAuthRule{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rule), latest); err != nil {
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
		log.FromContext(ctx).Error(err, "failed to record the target wait on Neo4jAuthRule")
		return false
	}
	return lasting
}

// unreachable handles a target that reads Ready but cannot be reached, as the
// user's does (Neo4jUserReconciler.unreachable): a rule with an outcome at
// its generation keeps it, any other waits in Pending, logged at info and
// retried on the fixed interval; only an outage that outlasts
// targetUnreachableGrace is announced, once (knowledge rule 172).
func (r *Neo4jAuthRuleReconciler) unreachable(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule, err error, requeue time.Duration) (ctrl.Result, error) {
	if r.waitForTarget(ctx, rule, ConditionReasonClusterUnreachable, targetUnreachableMessage(rule.Spec.ClusterRef, err)) {
		msg := fmt.Sprintf("failed to connect to Neo4j for more than %s: %v", targetUnreachableGrace, err)
		if r.setStatus(ctx, rule, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil, nil) {
			r.Recorder.Event(rule, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	log.FromContext(ctx).Info("Target reads Ready but cannot be reached; retrying",
		"clusterRef", rule.Spec.ClusterRef, "error", err.Error())
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reportPendingRoles writes the PendingDependencies condition and announces
// RolePending when the wait starts or its set of missing roles changes — not
// on every pass while the same roles stay missing (knowledge rule 145).
func (r *Neo4jAuthRuleReconciler) reportPendingRoles(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule, missing []string) {
	if len(missing) == 0 {
		r.setNamedCondition(ctx, rule, ConditionTypePendingDependencies, metav1.ConditionFalse, "AllDependenciesPresent", "")
		return
	}
	msg := pendingRolesMessage(missing)
	if r.setNamedCondition(ctx, rule, ConditionTypePendingDependencies, metav1.ConditionTrue, ConditionReasonRolesPending, msg) {
		r.Recorder.Eventf(rule, corev1.EventTypeWarning, EventReasonRolePending, "%s", msg)
	}
}

// setNamedCondition writes one named condition. It reports whether the write
// changed the condition's status, reason or message, from the attempt whose
// write succeeded, so a caller can announce the change once.
func (r *Neo4jAuthRuleReconciler) setNamedCondition(ctx context.Context, rule *neo4jv1beta1.Neo4jAuthRule, condType string, status metav1.ConditionStatus, reason, message string) bool {
	changed := false
	update := func() error {
		changed = false
		latest := &neo4jv1beta1.Neo4jAuthRule{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(rule), latest); err != nil {
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
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jAuthRule", "condition", condType)
		return false
	}
	return changed
}

// targetSupportsAuthRules parses the cluster's reported Neo4j version and
// returns true only when it satisfies the AUTH RULE feature gate.
func targetSupportsAuthRules(target ResolvedTarget) bool {
	v := targetVersionString(target)
	if v == "" {
		return false
	}
	parsed, err := neo4j.ParseVersion(v)
	if err != nil || parsed == nil {
		return false
	}
	return parsed.SupportsAuthRules()
}

// targetVersionString returns the Neo4j image tag (e.g. "2026.04.0-enterprise")
// from the resolved target's spec, or "" if not set.
func targetVersionString(target ResolvedTarget) string {
	if target.Cluster != nil {
		return target.Cluster.Spec.Image.Tag
	}
	if target.Standalone != nil {
		return target.Standalone.Spec.Image.Tag
	}
	return ""
}

// targetHasABACProvider reports whether the cluster's spec.config sets the
// abac.authorization_providers key to a non-empty value.
func targetHasABACProvider(target ResolvedTarget) bool {
	var cfg map[string]string
	if target.Cluster != nil {
		cfg = target.Cluster.Spec.Config
	} else if target.Standalone != nil {
		cfg = target.Standalone.Spec.Config
	}
	if cfg == nil {
		return false
	}
	v, ok := cfg[abacAuthorizationProvidersKey]
	if !ok {
		return false
	}
	return strings.TrimSpace(v) != ""
}

// SetupWithManager registers the controller. We watch:
//   - Neo4jRole, so rules pending on a missing role re-reconcile when it lands.
//   - Neo4jEnterpriseCluster / Neo4jEnterpriseStandalone, so we react
//     immediately to the cluster's Ready transition or version bump.
func (r *Neo4jAuthRuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	enqueueRulesForCluster := EnqueueDependentsForClusterChange(
		c,
		func() client.ObjectList { return &neo4jv1beta1.Neo4jAuthRuleList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			rules, ok := list.(*neo4jv1beta1.Neo4jAuthRuleList)
			if !ok {
				return
			}
			for i := range rules.Items {
				ar := &rules.Items[i]
				emit(ar.Name, ar.Namespace, ar.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jAuthRule{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueueRulesForCluster).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueueRulesForCluster).
		Watches(&neo4jv1beta1.Neo4jRole{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			role, ok := obj.(*neo4jv1beta1.Neo4jRole)
			if !ok {
				return nil
			}
			roleName := role.Spec.Name
			if roleName == "" {
				roleName = role.Name
			}
			rules := &neo4jv1beta1.Neo4jAuthRuleList{}
			if err := c.List(ctx, rules, client.InNamespace(role.Namespace)); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for i := range rules.Items {
				ar := &rules.Items[i]
				if ar.Spec.ClusterRef != role.Spec.ClusterRef {
					continue
				}
				for _, n := range ar.Spec.GrantedRoles {
					if n == roleName {
						reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ar.Namespace, Name: ar.Name}})
						break
					}
				}
			}
			return reqs
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}
