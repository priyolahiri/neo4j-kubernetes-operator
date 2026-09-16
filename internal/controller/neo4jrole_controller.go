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

// Neo4jRoleFinalizer is the finalizer that ensures the controller gets a
// chance to drop the underlying role (or release the finalizer when policy
// is Retain) before the CR is removed.
const Neo4jRoleFinalizer = "neo4j.com/role-finalizer"

// Neo4jRoleReconciler reconciles a Neo4jRole resource.
type Neo4jRoleReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.RoleValidator
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jroles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jroles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jroles/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jRole resource toward its desired state.
func (r *Neo4jRoleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jrole", req.NamespacedName)

	role := &neo4jv1beta1.Neo4jRole{}
	if err := r.Get(ctx, req.NamespacedName, role); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	roleName := effectiveRoleName(role)
	requeue := r.requeueAfter()

	// Deletion path
	if role.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, role, roleName)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(role, Neo4jRoleFinalizer) {
		controllerutil.AddFinalizer(role, Neo4jRoleFinalizer)
		if err := r.Update(ctx, role); err != nil {
			return ctrl.Result{}, err
		}
		// The Update above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Validate
	if r.Validator != nil {
		res := r.Validator.Validate(ctx, role)
		for _, w := range res.Warnings {
			r.Recorder.Eventf(role, corev1.EventTypeWarning, EventReasonValidationWarning, "%s", w)
		}
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			r.setStatus(ctx, role, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, nil, false)
			r.Recorder.Event(role, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// Resolve the target (cluster/standalone via clusterRef, or Aura instance).
	target, err := ResolveClusterRef(ctx, r.Client, role.Namespace, role.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve target ref")
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(role.Spec.ClusterRef))
		r.setStatus(ctx, role, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, nil, false)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		msg := fmt.Sprintf("%s is not Ready", targetRefDisplay(role.Spec.ClusterRef))
		r.setNamedCondition(ctx, role, ConditionTypeClusterNotReady, metav1.ConditionTrue, ConditionReasonClusterNotReady, msg)
		r.setStatus(ctx, role, "Pending", metav1.ConditionFalse, ConditionReasonClusterNotReady, msg, nil, false)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, role, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	// Connect
	nc, err := target.NewClient(r.Client)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		r.setStatus(ctx, role, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil, false)
		r.Recorder.Event(role, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// Ensure role exists
	info, err := nc.ShowRole(ctx, roleName)
	if err != nil {
		return r.fail(ctx, role, "lookup failed", err, requeue)
	}
	if info == nil {
		if validation.IsBuiltInRole(roleName) {
			return r.fail(ctx, role, fmt.Sprintf("built-in role %q not found in cluster", roleName), nil, requeue)
		}
		if err := nc.CreateRoleAdvanced(ctx, roleName, role.Spec.CopyOf, true); err != nil {
			return r.fail(ctx, role, "create role failed", err, requeue)
		}
		r.Recorder.Eventf(role, corev1.EventTypeNormal, EventReasonRoleCreated, "Role %q created", roleName)
	}

	// Privilege diff
	desired, desiredByCanonical, errs := r.canonicaliseDesired(role.Spec.Privileges)
	if errs != nil {
		return r.fail(ctx, role, "privilege canonicalisation failed", errs, requeue)
	}
	current, immutableSet, currentByCanonical, err := r.fetchCurrentPrivileges(ctx, nc, roleName)
	if err != nil {
		return r.fail(ctx, role, "fetch current privileges failed", err, requeue)
	}

	toAdd := setDifference(desired, current)
	toRemove := []string{}
	if role.Spec.EnforcePrivileges {
		toRemove = setDifference(current, desired)
	}

	for _, canon := range toAdd {
		// Execute the original spec text, never the canonical form: canonical
		// upper-cases bare tokens including unquoted identifiers, so a role/
		// graph/database named e.g. `users` written unquoted would canonicalise
		// to `... TO USERS` and target the wrong (case-sensitive) role. The diff
		// is keyed on canonical; the statement run is the user's original.
		stmt := desiredByCanonical[canon]
		if stmt == "" {
			stmt = canon // defensive fallback (should not happen)
		}
		if err := nc.ExecutePrivilegeStatement(ctx, stmt); err != nil {
			return r.fail(ctx, role, fmt.Sprintf("apply privilege %q failed", stmt), err, requeue)
		}
	}

	drift := false
	for _, canon := range toRemove {
		if _, immutable := immutableSet[canon]; immutable {
			drift = true
			r.Recorder.Eventf(role, corev1.EventTypeWarning, EventReasonPrivilegesDriftKept,
				"cannot revoke immutable privilege %q", canon)
			continue
		}
		original := currentByCanonical[canon]
		if original == "" {
			original = canon
		}
		revoke, err := neo4jclient.DerivePrivilegeRevoke(original)
		if err != nil {
			drift = true
			r.Recorder.Eventf(role, corev1.EventTypeWarning, EventReasonPrivilegesDriftKept,
				"cannot derive REVOKE for %q: %v", original, err)
			continue
		}
		if err := nc.ExecutePrivilegeStatement(ctx, revoke); err != nil {
			return r.fail(ctx, role, fmt.Sprintf("revoke privilege %q failed", revoke), err, requeue)
		}
	}

	// A privilege can be perfectly in sync with spec and still grant access to
	// nothing, because Neo4j accepts a grant against a database that does not
	// exist without a word. Reported, never enforced — see
	// reportUnresolvedPrivilegeDatabases.
	r.reportUnresolvedPrivilegeDatabases(ctx, nc, role, roleName)

	// Re-read for the AppliedPrivileges status field.
	final, _, _, err := r.fetchCurrentPrivileges(ctx, nc, roleName)
	if err != nil {
		return r.fail(ctx, role, "post-apply read failed", err, requeue)
	}

	// PrivilegesSynced condition
	if drift {
		r.setNamedCondition(ctx, role, ConditionTypePrivilegesSynced, metav1.ConditionFalse, ConditionReasonPrivilegesDrifted,
			"some privileges could not be reconciled (e.g. immutable). See events.")
	} else {
		r.setNamedCondition(ctx, role, ConditionTypePrivilegesSynced, metav1.ConditionTrue, ConditionReasonPrivilegesSynced,
			"privileges match spec")
	}

	if len(toAdd)+len(toRemove) > 0 {
		r.Recorder.Eventf(role, corev1.EventTypeNormal, EventReasonPrivilegesApplied,
			"applied %d added / %d revoked privileges", len(toAdd), len(toRemove))
	}

	// Emit RoleReady on the first transition to Ready (avoids spamming the
	// event stream on every reconcile after the role is already Ready).
	if role.Status.Phase != "Ready" {
		r.Recorder.Eventf(role, corev1.EventTypeNormal, EventReasonRoleReady,
			"Role %q is ready (%d privileges in sync)", roleName, len(final))
	}

	r.setStatus(ctx, role, "Ready", metav1.ConditionTrue, ConditionReasonRoleReady,
		fmt.Sprintf("role %q is in sync (%d privileges)", roleName, len(final)), final, drift)

	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *Neo4jRoleReconciler) handleDeletion(ctx context.Context, role *neo4jv1beta1.Neo4jRole, roleName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(role, Neo4jRoleFinalizer) {
		return ctrl.Result{}, nil
	}

	requeue := r.requeueAfter()

	// Built-in roles are never dropped; just release the finalizer.
	if validation.IsBuiltInRole(roleName) {
		controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
		return ctrl.Result{}, r.Update(ctx, role)
	}

	if strings.EqualFold(role.Spec.DeletionPolicy, "Retain") {
		controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
		return ctrl.Result{}, r.Update(ctx, role)
	}

	target, err := ResolveClusterRef(ctx, r.Client, role.Namespace, role.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		// Cluster gone — release the finalizer.
		controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
		return ctrl.Result{}, r.Update(ctx, role)
	}

	if !target.IsReady() {
		// Wait for the cluster to be ready so we can DROP cleanly.
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := target.NewClient(r.Client)
	if err != nil {
		// If we cannot connect, surface the error but do not block deletion
		// indefinitely.
		logger.Error(err, "cannot connect during role deletion; releasing finalizer")
		controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
		return ctrl.Result{}, r.Update(ctx, role)
	}
	defer func() { _ = nc.Close() }()

	if err := nc.DropRoleIfExists(ctx, roleName); err != nil {
		if classifyFinalizerCleanup(role, err) == retryCleanup {
			r.Recorder.Eventf(role, corev1.EventTypeWarning, EventReasonRoleDeletionFailed,
				"DROP ROLE %q failed, will retry: %v", roleName, err)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		r.Recorder.Eventf(role, corev1.EventTypeWarning, EventReasonRoleDeletionFailed,
			"DROP ROLE %q failed; releasing finalizer to avoid wedging deletion: %v", roleName, err)
		controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
		return ctrl.Result{}, r.Update(ctx, role)
	}

	r.Recorder.Eventf(role, corev1.EventTypeNormal, EventReasonRoleDeleted, "Role %q dropped", roleName)
	controllerutil.RemoveFinalizer(role, Neo4jRoleFinalizer)
	return ctrl.Result{}, r.Update(ctx, role)
}

// reportUnresolvedPrivilegeDatabases warns when spec.privileges name a
// database this cluster does not have.
//
// Neo4j accepts `GRANT ACCESS ON DATABASE does_not_exist TO r` in silence — no
// error, no warning — so the role reaches Ready, `enforcePrivileges: true`
// holds it there, and every dashboard stays green while the role grants access
// to nothing.
//
// The case that makes this matter is disaster recovery. A replica of `foo` is
// named `foo-replica` (Cypher has no RENAME DATABASE), and privileges attach
// to the database, not to an alias — so a Neo4jRole copied verbatim from the
// upstream, which is the obvious thing to do, is inert on the DR cluster and
// stays inert until someone fails over and finds out. The guide tells users to
// rewrite the names; nothing checked that they had.
//
// A warning and a condition, never a rejection. The operator cannot know the
// database is not about to be created — a Neo4jDatabase CR landing seconds
// later is an ordinary GitOps ordering — and refusing a privilege because a
// database has not appeared yet would break apply-everything-at-once. The
// condition clears on its own when the database shows up.
func (r *Neo4jRoleReconciler) reportUnresolvedPrivilegeDatabases(
	ctx context.Context, nc *neo4jclient.Client,
	role *neo4jv1beta1.Neo4jRole, roleName string,
) {
	logger := log.FromContext(ctx)

	named := privilegeDatabaseNames(role.Spec.Privileges)
	if len(named) == 0 {
		r.setNamedCondition(ctx, role, ConditionTypePrivilegesResolve, metav1.ConditionTrue,
			"NoDatabaseScopedPrivileges", "no privilege names a specific database")
		return
	}

	known, err := r.resolvableDatabaseNames(ctx, nc)
	if err != nil {
		// Unknown is not the same as missing. Say nothing rather than warn
		// about databases we simply could not list.
		logger.V(1).Info("could not list databases to check privilege targets", "error", err)
		return
	}

	// A graph privilege on a COMPOSITE database is accepted, persisted, shown
	// back by SHOW ROLE PRIVILEGES — and does nothing, because graph
	// privileges attach to the constituents' target databases. Verified on
	// 5.26.30 and 2026.08.1. The database exists, so the unresolved check
	// below will never see it; this is a separate, equally silent failure.
	if composites := r.compositeGraphTargets(ctx, nc, role); len(composites) > 0 {
		msg := fmt.Sprintf(
			"role %q grants GRAPH privileges on %s, which %s composite database(s). "+
				"Neo4j accepts such a grant and shows it back, but it does nothing: graph "+
				"privileges attach to a composite's CONSTITUENT target databases, not to the "+
				"composite. Grant ACCESS on the composite (that part is needed), and grant the "+
				"graph privileges on each constituent's target database instead.",
			roleName, quoteAndJoin(composites), pluralIs(len(composites)))
		if r.setNamedCondition(ctx, role, ConditionTypePrivilegesResolve, metav1.ConditionFalse,
			ReasonGraphPrivilegeOnComposite, msg) {
			r.Recorder.Event(role, corev1.EventTypeWarning, EventReasonPrivilegeUnknownDatabase, msg)
		}
		return
	}

	unresolved := unresolvedDatabaseNames(named, known)
	if len(unresolved) == 0 {
		r.setNamedCondition(ctx, role, ConditionTypePrivilegesResolve, metav1.ConditionTrue,
			"AllDatabasesResolve",
			fmt.Sprintf("every database named by a privilege exists (%s)", strings.Join(named, ", ")))
		return
	}

	msg := fmt.Sprintf(
		"role %q grants privileges on %s, which %s not exist on this cluster and %s not an alias "+
			"for a database that does. Neo4j accepts such a grant silently, so the role is Ready "+
			"and those privileges do nothing. On a DR cluster this is usually the replica's name: "+
			"a replica of \"foo\" is called \"foo-replica\", and privileges attach to the database, "+
			"not to an alias — rewrite the database name in spec.privileges. If the database is "+
			"simply not created yet, this clears by itself once it is.",
		roleName, quoteAndJoin(unresolved),
		pluralDo(len(unresolved)), pluralIs(len(unresolved)))

	if r.setNamedCondition(ctx, role, ConditionTypePrivilegesResolve, metav1.ConditionFalse,
		"DatabaseNotFound", msg) {
		r.Recorder.Event(role, corev1.EventTypeWarning, EventReasonPrivilegeUnknownDatabase, msg)
	}
}

// privilegeDatabaseNames is the deduplicated, order-preserving set of database
// names a role's privileges target.
func privilegeDatabaseNames(privileges []string) []string {
	var named []string
	seen := map[string]bool{}
	for _, stmt := range privileges {
		for _, name := range neo4jclient.PrivilegeDatabaseTargets(stmt) {
			if !seen[name] {
				seen[name] = true
				named = append(named, name)
			}
		}
	}
	return named
}

// unresolvedDatabaseNames returns the named databases that are neither a
// database nor an alias on this cluster.
func unresolvedDatabaseNames(named []string, known map[string]bool) []string {
	var unresolved []string
	for _, name := range named {
		if !known[name] {
			unresolved = append(unresolved, name)
		}
	}
	return unresolved
}

// compositeGraphTargets returns the composite databases this role grants GRAPH
// privileges on — a grant that is accepted and inert.
//
// Only GRAPH scope counts. `GRANT ACCESS ON DATABASE <composite>` is correct
// and required; flagging it would be telling users to remove the one privilege
// that makes a composite usable.
func (r *Neo4jRoleReconciler) compositeGraphTargets(
	ctx context.Context, nc *neo4jclient.Client, role *neo4jv1beta1.Neo4jRole,
) []string {
	var named []string
	seen := map[string]bool{}
	for _, stmt := range role.Spec.Privileges {
		for _, name := range neo4jclient.PrivilegeGraphTargets(stmt) {
			if !seen[name] {
				seen[name] = true
				named = append(named, name)
			}
		}
	}
	if len(named) == 0 {
		return nil
	}

	databases, err := nc.GetDatabases(ctx)
	if err != nil {
		// Cannot look, so cannot claim. Silence beats a wrong warning.
		return nil
	}
	composite := map[string]bool{}
	for _, db := range databases {
		if db.Type == neo4jclient.CompositeDatabaseType {
			composite[db.Name] = true
		}
	}

	var out []string
	for _, name := range named {
		if composite[name] {
			out = append(out, name)
		}
	}
	return out
}

// resolvableDatabaseNames is every name a privilege could legitimately target:
// the databases themselves plus the aliases that point at one. An alias is
// included because naming one is not by itself a mistake — the privilege
// simply resolves to the alias's target.
func (r *Neo4jRoleReconciler) resolvableDatabaseNames(
	ctx context.Context, nc *neo4jclient.Client,
) (map[string]bool, error) {
	databases, err := nc.GetDatabases(ctx)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, db := range databases {
		known[db.Name] = true
	}
	// Aliases are best-effort: a server that cannot list them still gives a
	// useful answer for databases, and treating that failure as fatal would
	// suppress the warning entirely.
	if aliases, err := nc.ShowAliases(ctx); err == nil {
		for _, a := range aliases {
			known[a.Name] = true
		}
	}
	return known, nil
}

func quoteAndJoin(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, fmt.Sprintf("%q", n))
	}
	return strings.Join(quoted, ", ")
}

func pluralDo(n int) string {
	if n == 1 {
		return "does"
	}
	return "do"
}

func pluralIs(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

// canonicaliseDesired turns the spec privileges into a deduplicated, sorted
// slice of canonical statements plus a map from each canonical form back to the
// original spec text. The diff is keyed on the canonical form, but callers
// execute the original text — the canonical form upper-cases bare tokens
// (including unquoted identifiers) and is explicitly not safe to feed to Neo4j.
func (r *Neo4jRoleReconciler) canonicaliseDesired(stmts []string) ([]string, map[string]string, error) {
	set := map[string]struct{}{}
	byCanonical := map[string]string{}
	for _, s := range stmts {
		canon := neo4jclient.CanonicalisePrivilegeStatement(s)
		if canon == "" {
			continue
		}
		set[canon] = struct{}{}
		if _, exists := byCanonical[canon]; !exists {
			byCanonical[canon] = s // first original wins for a given canonical form
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, byCanonical, nil
}

// fetchCurrentPrivileges reads SHOW ROLE PRIVILEGES AS COMMANDS and returns:
//   - canonical: sorted, deduplicated set of canonical statements
//   - immutable: set (by canonical form) of statements flagged immutable
//   - byCanonical: map from canonical form back to the original command,
//     used to derive REVOKE statements verbatim.
func (r *Neo4jRoleReconciler) fetchCurrentPrivileges(ctx context.Context, nc *neo4jclient.Client, roleName string) ([]string, map[string]struct{}, map[string]string, error) {
	rows, err := nc.ShowRolePrivileges(ctx, roleName)
	if err != nil {
		return nil, nil, nil, err
	}
	set := map[string]struct{}{}
	immutable := map[string]struct{}{}
	byCanonical := map[string]string{}
	for _, row := range rows {
		canon := neo4jclient.CanonicalisePrivilegeStatement(row.Command)
		if canon == "" {
			continue
		}
		set[canon] = struct{}{}
		byCanonical[canon] = row.Command
		if row.Immutable {
			immutable[canon] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, immutable, byCanonical, nil
}

// setDifference returns the elements in a not present in b. Both inputs must
// be sorted; the result is also sorted.
func setDifference(a, b []string) []string {
	bset := make(map[string]struct{}, len(b))
	for _, x := range b {
		bset[x] = struct{}{}
	}
	out := make([]string, 0, len(a))
	for _, x := range a {
		if _, ok := bset[x]; !ok {
			out = append(out, x)
		}
	}
	return out
}

func (r *Neo4jRoleReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jRoleReconciler) fail(ctx context.Context, role *neo4jv1beta1.Neo4jRole, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	r.setStatus(ctx, role, "Failed", metav1.ConditionFalse, EventReasonRoleSyncFailed, msg, nil, false)
	r.Recorder.Event(role, corev1.EventTypeWarning, EventReasonRoleSyncFailed, msg)
	return ctrl.Result{RequeueAfter: requeue}, err
}

func (r *Neo4jRoleReconciler) setStatus(
	ctx context.Context,
	role *neo4jv1beta1.Neo4jRole,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	appliedPrivileges []string,
	drift bool,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jRole{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(role), latest); err != nil {
			return err
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message)
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if appliedPrivileges != nil {
			latest.Status.AppliedPrivileges = appliedPrivileges
		}
		latest.Status.PrivilegeDrift = drift
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jRole status")
	}
}

// setNamedCondition writes one named condition, reporting whether it actually
// changed. Callers use that to emit an event on the transition only — a
// warning re-announced on every reconcile is a warning nobody reads.
func (r *Neo4jRoleReconciler) setNamedCondition(ctx context.Context, role *neo4jv1beta1.Neo4jRole, condType string, status metav1.ConditionStatus, reason, message string) bool {
	var changed bool
	update := func() error {
		latest := &neo4jv1beta1.Neo4jRole{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(role), latest); err != nil {
			return err
		}
		changed = SetNamedCondition(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jRole", "condition", condType)
		return false
	}
	return changed
}

// effectiveRoleName returns spec.name if non-empty, otherwise metadata.name.
func effectiveRoleName(role *neo4jv1beta1.Neo4jRole) string {
	if role.Spec.Name != "" {
		return role.Spec.Name
	}
	return role.Name
}

// SetupWithManager registers the controller and watches the cluster /
// standalone CRs whose state transitions should re-trigger role
// reconciliation (Pending → Ready). Without this watch, roles only
// notice cluster status changes on their next 30-second requeue, which
// can starve reconciles in CI when clusters bootstrap quickly.
func (r *Neo4jRoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueRolesForCluster := EnqueueDependentsForClusterChange(
		mgr.GetClient(),
		func() client.ObjectList { return &neo4jv1beta1.Neo4jRoleList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			roles, ok := list.(*neo4jv1beta1.Neo4jRoleList)
			if !ok {
				return
			}
			for i := range roles.Items {
				role := &roles.Items[i]
				emit(role.Name, role.Namespace, role.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jRole{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueueRolesForCluster).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueueRolesForCluster).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}
