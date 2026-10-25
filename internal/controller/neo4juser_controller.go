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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
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
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jUserFinalizer is the finalizer that ensures the controller gets a
// chance to drop the underlying user (or release the finalizer when policy
// is Retain) before the CR is removed.
const Neo4jUserFinalizer = "neo4j.com/user-finalizer"

// Neo4jUserReconciler reconciles a Neo4jUser resource.
type Neo4jUserReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.UserValidator

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// newNeo4jClient connects to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newNeo4jClient func(ResolvedTarget) (userNeo4jClient, error)
}

// userNeo4jClient is what the Neo4jUser reconciler asks of Neo4j.
// *neo4jclient.Client is one.
type userNeo4jClient interface {
	roleLister
	ShowUser(ctx context.Context, username string) (*neo4jclient.UserInfo, error)
	CreateUserAdvanced(ctx context.Context, username string, opts *neo4jclient.AlterUserOptions, ifNotExists bool) error
	AlterUser(ctx context.Context, username string, opts *neo4jclient.AlterUserOptions) error
	ListUserRoles(ctx context.Context, username string) ([]string, error)
	GrantRoleToUser(ctx context.Context, roleName, username string) error
	RevokeRoleFromUser(ctx context.Context, roleName, username string) error
	DropUserIfExists(ctx context.Context, username string) error
	Close() error
}

func (r *Neo4jUserReconciler) connect(target ResolvedTarget) (userNeo4jClient, error) {
	if r.newNeo4jClient != nil {
		return r.newNeo4jClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jroles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jUser resource toward its desired state.
func (r *Neo4jUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4juser", req.NamespacedName)

	user := &neo4jv1beta1.Neo4jUser{}
	if err := r.Get(ctx, req.NamespacedName, user); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	username := effectiveUsername(user)
	requeue := r.requeueAfter()

	if user.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, user, username)
	}

	if !controllerutil.ContainsFinalizer(user, Neo4jUserFinalizer) {
		if err := addFinalizer(ctx, r.Client, user, Neo4jUserFinalizer); err != nil {
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Validate
	if r.Validator != nil {
		res := r.Validator.Validate(ctx, user)
		r.advisoryWarnings.record(r.Recorder, user, EventReasonValidationWarning, res.Warnings)
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass while the spec stays invalid (knowledge rule 136).
			if r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, nil, "") {
				r.Recorder.Event(user, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		if len(res.Pending) > 0 {
			// Transient dependency gaps (e.g. password Secret not applied
			// yet): Pending + requeue, like missing roles — apply order is
			// documented as irrelevant (#259).
			msg := strings.Join(res.Pending, "; ")
			r.setStatus(ctx, user, "Pending", metav1.ConditionFalse, ConditionReasonSecretPending, msg, nil, "")
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// Resolve the target (cluster/standalone via clusterRef, or Aura instance).
	target, err := ResolveClusterRef(ctx, r.Client, user.Namespace, user.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve target ref")
		return ctrl.Result{}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(user.Spec.ClusterRef))
		// A wait, announced once: when it starts, not on every pass until the
		// deployment exists (operations.md ids 145 and 182).
		if r.setStatus(ctx, user, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, nil, "") {
			r.Recorder.Event(user, corev1.EventTypeWarning, EventReasonClusterNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		r.waitForTarget(ctx, user, ConditionReasonClusterNotReady,
			fmt.Sprintf("%s is not Ready", targetRefDisplay(user.Spec.ClusterRef)))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// Read password Secret if configured
	var passwordValue string
	var passwordHash string
	if user.Spec.PasswordSecretRef != nil {
		pw, token, err := r.readPasswordSecret(ctx, user)
		if err != nil {
			msg := fmt.Sprintf("password secret read failed: %v", err)
			r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonUserSyncFailed, msg, nil, "")
			return ctrl.Result{}, err
		}
		passwordValue = pw
		// NOT a hash of the password — see readPasswordSecret.
		passwordHash = token
	}

	// Connect
	nc, err := r.connect(target)
	if err != nil {
		if targetUnreachable(err) {
			return r.unreachable(ctx, user, err, requeue)
		}
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		// Retried with backoff; the same failure is announced once (knowledge
		// rule 145).
		if r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil, "") {
			r.Recorder.Event(user, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()
	// Only a target that answered is ready for this user: written after the
	// connection, so an unreachable one keeps the time its outage began.
	r.setNamedCondition(ctx, user, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	// Existence
	info, err := nc.ShowUser(ctx, username)
	if err != nil {
		return r.fail(ctx, user, "lookup failed", err, requeue)
	}

	desiredSuspended := user.Spec.AccountStatus == "suspended"

	if info == nil {
		// Create user
		opts := neo4jclient.AlterUserOptions{}
		if passwordValue != "" {
			opts.WithPassword(passwordValue)
			opts.WithPasswordChangeRequired(user.Spec.RequirePasswordChange)
		}
		opts.WithSuspended(desiredSuspended)
		if user.Spec.HomeDatabase != "" {
			opts.WithHomeDatabase(user.Spec.HomeDatabase)
		}
		for _, ap := range user.Spec.ExternalAuth {
			opts.WithSetAuthProviders(neo4jclient.AuthProviderInfo{Provider: ap.Provider, ID: ap.ID})
		}
		if err := nc.CreateUserAdvanced(ctx, username, &opts, true); err != nil {
			return r.fail(ctx, user, "create user failed", err, requeue)
		}
		r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserCreated, "User %q created", username)
		if r.passwordApplied(ctx, user, passwordHash) {
			r.Recorder.Event(user, corev1.EventTypeNormal, EventReasonPasswordRotated, "password updated from Secret")
		}
		// Re-read so we have fresh state for the rest of the loop.
		info, err = nc.ShowUser(ctx, username)
		if err != nil {
			return r.fail(ctx, user, "post-create read failed", err, requeue)
		}
		if info == nil {
			// The user demonstrably exists — CREATE USER just succeeded — but
			// the system database has not made it visible to this session yet.
			// That is benign eventual consistency on a multi-server cluster,
			// NOT a failure: requeue and re-read rather than recording Failed.
			//
			// Returning an error here used to be the entry point to a
			// permanent livelock. The applied password hash is written to
			// status only at the end of a successful pass, so failing here
			// left it empty; the next reconcile then saw "password needs
			// rotating", issued an ALTER to the SAME password, and Neo4j
			// rejects that outright — forever. IsPasswordUnchangedError below
			// closes that trap too, but not entering it is cheaper.
			r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserCreated,
				"User %q created; awaiting visibility in the system database", username)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	} else {
		// Diff and ALTER existing user.
		opts := r.computeAlter(user, info, passwordHash, passwordValue, desiredSuspended)
		setsPassword := passwordRotationDue(user, passwordHash)
		if !opts.IsEmpty() {
			if err := nc.AlterUser(ctx, username, &opts); err != nil {
				// "Old password and new password cannot be the same" means the
				// stored password ALREADY equals what the spec asks for — the
				// desired state holds. Treat it as success so the reconcile
				// proceeds and records the hash; failing here instead makes the
				// condition permanent, because the hash is what would stop us
				// re-issuing the same ALTER next time. Announced only by the
				// write that records it: a pass on a copy from before that
				// write sends the same ALTER again and says nothing. Nothing
				// was rotated, so it is this event alone, not PasswordRotated
				// too: a Neo4jUser for neo4j, whose Secret holds the admin
				// password, announced both at its creation.
				if !neo4jclient.IsPasswordUnchangedError(err) {
					return r.fail(ctx, user, "alter user failed", err, requeue)
				}
				if r.passwordApplied(ctx, user, passwordHash) {
					r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserUpdated,
						"User %q password already matches spec; recording as applied", username)
				}
			} else {
				r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserUpdated, "User %q updated", username)
				if setsPassword && r.passwordApplied(ctx, user, passwordHash) {
					r.Recorder.Event(user, corev1.EventTypeNormal, EventReasonPasswordRotated, "password updated from Secret")
				}
			}
		}
	}

	// Role binding diff. Resolve same-namespace Neo4jRole CR names in
	// spec.roles to their effective Neo4j role name first (#260) — literal
	// Neo4j names (built-ins, externally-created roles) pass through unchanged.
	desiredRoles := normaliseRoles(user.Spec.Roles)
	desiredRoles, resolvedRoles := resolveRoleNames(ctx, r.Client, user.Namespace, targetRefKey(user.Spec.ClusterRef), desiredRoles)
	desiredRoles = normaliseRoles(desiredRoles) // dedupe any CR-name/spec.name collisions
	r.reportResolvedRoles(ctx, user, resolvedRoles)
	currentRoles := normaliseRoles(info.Roles) // Note: may be slightly stale after CREATE; re-read just below.
	// info is non-nil here: the create branch above re-reads it (and bails if
	// still nil) and the else branch only runs when it was already non-nil.
	if len(info.Roles) == 0 {
		// fresh-create path: fetch live roles since create-time info has none granted.
		if live, err := nc.ListUserRoles(ctx, username); err == nil {
			currentRoles = normaliseRoles(live)
		}
	}

	addRoles, dropRoles, missing, err := r.diffRoles(ctx, nc, user, desiredRoles, currentRoles)
	if err != nil {
		return r.fail(ctx, user, "role lookup failed", err, requeue)
	}
	for _, role := range addRoles {
		if err := nc.GrantRoleToUser(ctx, role, username); err != nil {
			return r.fail(ctx, user, fmt.Sprintf("grant %q failed", role), err, requeue)
		}
	}
	for _, role := range dropRoles {
		if err := nc.RevokeRoleFromUser(ctx, role, username); err != nil {
			return r.fail(ctx, user, fmt.Sprintf("revoke %q failed", role), err, requeue)
		}
	}
	if len(addRoles) > 0 {
		r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonRolesGranted, "granted %v", addRoles)
	}
	if len(dropRoles) > 0 {
		r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonRolesRevoked, "revoked %v", dropRoles)
	}

	// Re-read final role list for status.
	finalRoles, err := nc.ListUserRoles(ctx, username)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to read final role list (non-fatal)")
		finalRoles = currentRoles
	}
	finalRoles = normaliseRoles(finalRoles)

	// Conditions
	r.reportPendingRoles(ctx, user, missing)

	rolesEqual := stringSlicesEqualSorted(desiredRoles, finalRoles)
	if rolesEqual {
		r.setNamedCondition(ctx, user, ConditionTypeRolesSynced, metav1.ConditionTrue, ConditionReasonRolesSynced, "granted roles match spec")
	} else {
		r.setNamedCondition(ctx, user, ConditionTypeRolesSynced, metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("granted roles %v differ from spec %v", finalRoles, desiredRoles))
	}

	if user.Spec.PasswordSecretRef != nil {
		r.setNamedCondition(ctx, user, ConditionTypePasswordSynced, metav1.ConditionTrue, ConditionReasonPasswordSynced,
			"password secret hash applied")
	}

	// Phase / Ready. PasswordRotated was announced by the write that recorded
	// the password as applied (passwordApplied), not here from this pass's copy.
	overallReady := rolesEqual && len(missing) == 0
	if overallReady {
		// UserReady is announced when the write moves the persisted phase into
		// Ready — never from this pass's copy, which the informer cache may
		// hold from before the last write — so it is once per transition, and
		// a restart of the target is not one (waitForTarget; knowledge rule
		// 158).
		if _, entered := r.writeStatus(ctx, user, "Ready", metav1.ConditionTrue, ConditionReasonUserReady,
			fmt.Sprintf("user %q is in sync (%d roles)", username, len(finalRoles)),
			finalRoles, passwordHash); entered {
			r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserReady,
				"User %q is ready (%d roles in sync)", username, len(finalRoles))
		}
	} else {
		r.setStatus(ctx, user, "Pending", metav1.ConditionFalse, ConditionReasonRolesPending,
			fmt.Sprintf("user %q reconciled with pending dependencies", username),
			finalRoles, passwordHash)
	}

	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *Neo4jUserReconciler) handleDeletion(ctx context.Context, user *neo4jv1beta1.Neo4jUser, username string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(user, Neo4jUserFinalizer) {
		return ctrl.Result{}, nil
	}
	release := func() error { return removeFinalizer(ctx, r.Client, user, Neo4jUserFinalizer) }

	if strings.EqualFold(user.Spec.DeletionPolicy, "Retain") {
		return ctrl.Result{}, release()
	}

	target, err := ResolveClusterRef(ctx, r.Client, user.Namespace, user.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !target.Found {
		// The deployment is gone, and the user with its system database.
		return ctrl.Result{}, release()
	}
	// Anything else that keeps DROP USER from running waits, bounded: the
	// user may still be in Neo4j, and releasing now would leave the access
	// the deletion was meant to revoke (knowledge rule 178).
	cleanup := deletionCleanup{
		obj: user, recorder: r.Recorder, reason: EventReasonUserDeletionFailed,
		leftBehind: fmt.Sprintf("user %q on %s", username, targetRefDisplay(user.Spec.ClusterRef)),
		byHand:     fmt.Sprintf("DROP USER `%s`", neo4jclient.EscapeBackticks(username)),
		record: func(msg string) bool {
			return r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonUserDeletionFailed, msg, nil, "")
		},
		release: release, requeue: r.requeueAfter(),
	}
	if !target.IsReady() {
		return cleanup.blocked(ctx, targetDownCause(user.Spec.ClusterRef))
	}

	nc, err := r.connect(target)
	if err != nil {
		return cleanup.blocked(ctx, connectFailedCause(user.Spec.ClusterRef, err))
	}
	defer func() { _ = nc.Close() }()

	if err := nc.DropUserIfExists(ctx, username); err != nil {
		return cleanup.blocked(ctx, statementFailedCause(user.Spec.ClusterRef, "DROP USER", err))
	}
	r.Recorder.Eventf(user, corev1.EventTypeNormal, EventReasonUserDeleted, "User %q dropped", username)
	return ctrl.Result{}, release()
}

// computeAlter builds the set of ALTER USER clauses needed to bring `info`
// (current state) into agreement with `user.Spec` (desired state).
func (r *Neo4jUserReconciler) computeAlter(
	user *neo4jv1beta1.Neo4jUser,
	info *neo4jclient.UserInfo,
	passwordHash, passwordValue string,
	desiredSuspended bool,
) neo4jclient.AlterUserOptions {
	opts := neo4jclient.AlterUserOptions{}

	// Password rotation: triggered when the Secret value SHA-256 differs
	// from the last-applied hash recorded in status.
	if passwordRotationDue(user, passwordHash) {
		opts.WithPassword(passwordValue)
	}

	// Password change-required toggle: only emit a clause when the bit needs
	// to change.
	if info.PasswordChangeRequired != user.Spec.RequirePasswordChange {
		opts.WithPasswordChangeRequired(user.Spec.RequirePasswordChange)
	}

	// Status
	if info.Suspended != desiredSuspended {
		opts.WithSuspended(desiredSuspended)
	}

	// Home database. Compared ignoring case: Neo4j stores the name lower-cased
	// (SET HOME DATABASE `Orders` is shown as `orders`), and an exact match
	// re-sent ALTER USER … SET HOME DATABASE, with a UserUpdated event, on
	// every pass.
	switch {
	case user.Spec.HomeDatabase == "" && info.HomeDatabase != "":
		opts.WithoutHomeDatabase()
	case user.Spec.HomeDatabase != "" && !strings.EqualFold(user.Spec.HomeDatabase, info.HomeDatabase):
		opts.WithHomeDatabase(user.Spec.HomeDatabase)
	}

	// External auth providers
	desired := map[string]string{}
	for _, ap := range user.Spec.ExternalAuth {
		desired[ap.Provider] = ap.ID
	}
	current := map[string]string{}
	for _, ap := range info.AuthProviders {
		// Skip the implicit `native` provider — it's managed via SET PASSWORD.
		if strings.EqualFold(ap.Provider, "native") {
			continue
		}
		current[ap.Provider] = ap.ID
	}

	// Providers to remove or update
	for prov, curID := range current {
		desiredID, ok := desired[prov]
		if !ok {
			opts.WithRemovedAuthProviders(prov)
			continue
		}
		if desiredID != curID {
			// Re-set in place: a SET AUTH overwrites the existing entry's ID.
			opts.WithSetAuthProviders(neo4jclient.AuthProviderInfo{Provider: prov, ID: desiredID})
		}
	}
	// Providers to add
	for prov, id := range desired {
		if _, ok := current[prov]; ok {
			continue
		}
		opts.WithSetAuthProviders(neo4jclient.AuthProviderInfo{Provider: prov, ID: id})
	}

	return opts
}

// diffRoles computes role grants/revokes and reports any desired roles that
// do not exist yet — neither built in, nor a Neo4jRole CR, nor a role in Neo4j
// (missingRoles). PUBLIC is implicit and is never granted/revoked here.
func (r *Neo4jUserReconciler) diffRoles(
	ctx context.Context, roles roleLister, user *neo4jv1beta1.Neo4jUser, desired, current []string,
) (add []string, drop []string, missing []string, err error) {
	currentSet := map[string]struct{}{}
	for _, c := range current {
		currentSet[c] = struct{}{}
	}
	desiredSet := map[string]struct{}{}
	for _, d := range desired {
		desiredSet[d] = struct{}{}
	}

	missing, err = missingRoles(ctx, r.Client, roles, user.Namespace, targetRefKey(user.Spec.ClusterRef), desired, currentSet)
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
	for _, c := range current {
		// Never revoke PUBLIC — it is implicit and Neo4j refuses removal.
		if strings.EqualFold(c, "PUBLIC") {
			continue
		}
		if _, ok := desiredSet[c]; ok {
			continue
		}
		drop = append(drop, c)
	}
	sort.Strings(add)
	sort.Strings(drop)
	return add, drop, missing, nil
}

// readPasswordSecret returns the password and a CHANGE TOKEN for it.
//
// The token deliberately does NOT derive from the password value. status.
// passwordSecretHash used to be sha256(password) — an unsalted hash of a
// human-chosen secret, published in CR status and readable by anyone holding
// the deliberately-low-privilege neo4juser-viewer-role. That is offline-
// crackable with commodity hardware, so a read-only viewer could recover a live
// database credential.
//
// The field only ever needed to answer "did the password change since I last
// applied it?", which the Secret's namespace/name/key/resourceVersion answers
// without involving the secret material at all. resourceVersion changes on
// every write to the Secret, so a real rotation is always detected.
//
// Trade-off, accepted deliberately: a cosmetic write to the Secret (a label
// edit, say) also changes resourceVersion and triggers one no-op rotation. That
// is harmless because a same-password ALTER is now tolerated as
// already-satisfied (IsPasswordUnchangedError, #340) — before that fix this
// change would have livelocked every affected user, which is why the ordering
// mattered.
func (r *Neo4jUserReconciler) readPasswordSecret(ctx context.Context, user *neo4jv1beta1.Neo4jUser) (password, changeToken string, err error) {
	ref := user.Spec.PasswordSecretRef
	key := ref.Key
	if key == "" {
		key = "password"
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: user.Namespace, Name: ref.Name}, secret); err != nil {
		return "", "", err
	}
	val, ok := secret.Data[key]
	if !ok {
		return "", "", fmt.Errorf("secret %q has no key %q", ref.Name, key)
	}
	if len(val) == 0 {
		return "", "", fmt.Errorf("secret %q key %q is empty", ref.Name, key)
	}
	// Namespace/name/key are included so re-pointing PasswordSecretRef at a
	// different Secret is detected even if resourceVersions happen to collide.
	token := sha256Hex([]byte(strings.Join([]string{
		secret.Namespace, secret.Name, key, secret.ResourceVersion,
	}, "\x00")))
	return string(val), token, nil
}

func (r *Neo4jUserReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jUserReconciler) fail(ctx context.Context, user *neo4jv1beta1.Neo4jUser, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	if targetUnreachable(err) {
		// The target went away mid-pass: an outage, not this user's failure
		// (knowledge rule 172).
		return r.unreachable(ctx, user, err, requeue)
	}
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	// Announced when the failure starts or its message changes, not on every
	// retry of the same failure (knowledge rule 145).
	if r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonUserSyncFailed, msg, nil, "") {
		r.Recorder.Event(user, corev1.EventTypeWarning, EventReasonUserSyncFailed, msg)
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
func (r *Neo4jUserReconciler) setStatus(
	ctx context.Context,
	user *neo4jv1beta1.Neo4jUser,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	currentRoles []string,
	passwordHash string,
) bool {
	changed, _ := r.writeStatus(ctx, user, phase, readyStatus, readyReason, message, currentRoles, passwordHash)
	return changed
}

// writeStatus is setStatus that also reports whether the write moved the
// persisted phase into the one given (entered), the transition a phase event
// such as UserReady announces.
func (r *Neo4jUserReconciler) writeStatus(
	ctx context.Context,
	user *neo4jv1beta1.Neo4jUser,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	currentRoles []string,
	passwordHash string,
) (transitioned, entered bool) {
	update := func() error {
		transitioned, entered = false, false
		latest := &neo4jv1beta1.Neo4jUser{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(user), latest); err != nil {
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
		if currentRoles != nil {
			latest.Status.CurrentRoles = currentRoles
		}
		if passwordHash != "" && latest.Status.PasswordSecretHash != passwordHash {
			// Normally recorded already, by passwordApplied; this covers a
			// pass whose earlier write did not land.
			latest.Status.PasswordSecretHash = passwordHash
			now := metav1.Now()
			latest.Status.PasswordLastRotated = &now
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned, entered = changed, phaseMoved
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jUser status")
		return false, false
	}
	return transitioned, entered
}

// passwordRotationDue reports whether the password Secret, by its change
// token, is not the one status records as applied: the pass sends SET PASSWORD.
func passwordRotationDue(user *neo4jv1beta1.Neo4jUser, passwordHash string) bool {
	return user.Spec.PasswordSecretRef != nil && passwordHash != "" && passwordHash != user.Status.PasswordSecretHash
}

// passwordApplied records that the Secret's password — its change token — is
// the user's, in a status write right after the statement that applied it:
// CREATE USER, ALTER USER … SET PASSWORD, or the refusal that says the
// password was already set. It reports whether the write changed the recorded
// token; the caller announces what happened — PasswordRotated for a password
// the statement set, the "already matches" UserUpdated for one Neo4j already
// held, never both (knowledge rule 15).
//
// The token used to be written only at the end of a successful pass. A pass
// that stopped at a failing grant never wrote it, so every retry sent the same
// ALTER again and announced "password already matches spec" (18 times in two
// minutes on the v1.22.0 walk), and a pass on a copy from before the end write
// announced PasswordRotated a second time for a new user. Announced from this
// write, re-read inside the conflict retry, each is said once.
func (r *Neo4jUserReconciler) passwordApplied(ctx context.Context, user *neo4jv1beta1.Neo4jUser, passwordHash string) bool {
	if passwordHash == "" {
		return false
	}
	changed := false
	update := func() error {
		changed = false
		latest := &neo4jv1beta1.Neo4jUser{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(user), latest); err != nil {
			return err
		}
		if latest.Status.PasswordSecretHash == passwordHash {
			return nil
		}
		latest.Status.PasswordSecretHash = passwordHash
		now := metav1.Now()
		latest.Status.PasswordLastRotated = &now
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		changed = true
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record the applied password on Neo4jUser")
		return false
	}
	return changed
}

// waitForTarget records that the target cannot serve the user — reason
// ClusterNotReady (not Ready) or ClusterUnreachable (reads Ready, cannot be
// reached): the ClusterNotReady condition, and phase Pending unless the user
// already has an outcome at its generation. A Ready user stays Ready, and a
// Failed one Failed, while its target restarts — nothing about the user
// changed, and the target announces its own state — so the pass that finds the
// target back is not a transition and announces nothing. Demoting it to
// Pending made every restart of a standalone a UserReady event (28 in 40
// minutes of restarts, v1.22.0 re-walk; knowledge rule 158), or a
// UserSyncFailed for the same failure (#573; id 189). The status is written
// only when it changes. It reports lasting
// when an unreachable target has outlasted targetUnreachableGrace (id 172).
func (r *Neo4jUserReconciler) waitForTarget(ctx context.Context, user *neo4jv1beta1.Neo4jUser, reason, msg string) (lasting bool) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jUser{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(user), latest); err != nil {
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
		log.FromContext(ctx).Error(err, "failed to record the target wait on Neo4jUser")
		return false
	}
	return lasting
}

// unreachable handles a target that reads Ready but cannot be reached: a
// standalone whose Bolt port refuses connections for a few seconds after it
// reads Ready, or for longer during a stopCluster restore. That is the rest of
// the same outage waitForTarget waits through, not a failure of this user: a
// user with an outcome at its generation keeps it (ClusterNotReady=True,
// reason ClusterUnreachable), any other waits in Pending, it is logged at info and
// retried on the fixed interval — an error would be logged at ERROR by
// controller-runtime and retried within milliseconds. It used to write Failed
// with a ConnectionFailed Warning and announce UserReady again on the next
// pass (v1.22.0 combined-build walk, #559). Only an outage that outlasts
// targetUnreachableGrace is this user's failure, announced once (knowledge
// rule 172).
func (r *Neo4jUserReconciler) unreachable(ctx context.Context, user *neo4jv1beta1.Neo4jUser, err error, requeue time.Duration) (ctrl.Result, error) {
	if r.waitForTarget(ctx, user, ConditionReasonClusterUnreachable, targetUnreachableMessage(user.Spec.ClusterRef, err)) {
		msg := fmt.Sprintf("failed to connect to Neo4j for more than %s: %v", targetUnreachableGrace, err)
		if r.setStatus(ctx, user, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil, "") {
			r.Recorder.Event(user, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	log.FromContext(ctx).Info("Target reads Ready but cannot be reached; retrying",
		"clusterRef", user.Spec.ClusterRef, "error", err.Error())
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reportPendingRoles writes the PendingDependencies condition and announces
// RolePending when the wait starts or its set of missing roles changes — not
// on every pass while the same roles stay missing (knowledge rule 145).
func (r *Neo4jUserReconciler) reportPendingRoles(ctx context.Context, user *neo4jv1beta1.Neo4jUser, missing []string) {
	if len(missing) == 0 {
		r.setNamedCondition(ctx, user, ConditionTypePendingDependencies, metav1.ConditionFalse, "AllDependenciesPresent", "")
		return
	}
	msg := pendingRolesMessage(missing)
	if r.setNamedCondition(ctx, user, ConditionTypePendingDependencies, metav1.ConditionTrue, ConditionReasonRolesPending, msg) {
		r.Recorder.Eventf(user, corev1.EventTypeWarning, EventReasonRolePending, "%s", msg)
	}
}

// reportResolvedRoles records the CR names spec.roles resolved (the
// RoleNamesResolved condition) and announces RolesResolved when the mapping
// changes, from the attempt whose write succeeded — not on every pass.
func (r *Neo4jUserReconciler) reportResolvedRoles(ctx context.Context, user *neo4jv1beta1.Neo4jUser, resolved map[string]string) {
	announce := false
	update := func() error {
		announce = false
		latest := &neo4jv1beta1.Neo4jUser{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(user), latest); err != nil {
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
		log.FromContext(ctx).Error(err, "failed to record resolved role names on Neo4jUser")
		return
	}
	if announce {
		r.Recorder.Event(user, corev1.EventTypeNormal, EventReasonRolesResolved, resolvedRolesMessage(resolved))
	}
}

// setNamedCondition writes one named condition. It reports whether the write
// changed the condition's status, reason or message, from the attempt whose
// write succeeded, so a caller can announce the change once.
func (r *Neo4jUserReconciler) setNamedCondition(ctx context.Context, user *neo4jv1beta1.Neo4jUser, condType string, status metav1.ConditionStatus, reason, message string) bool {
	changed := false
	update := func() error {
		changed = false
		latest := &neo4jv1beta1.Neo4jUser{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(user), latest); err != nil {
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
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jUser", "condition", condType)
		return false
	}
	return changed
}

// pendingRolesMessage is the PendingDependencies message, and the RolePending
// event text, for a set of missing roles. Sorted, so the same set always reads
// the same and only a different set is a new message.
func pendingRolesMessage(missing []string) string {
	return fmt.Sprintf("waiting for roles to exist: %v", slices.Sorted(slices.Values(missing)))
}

// setNamedConditionChanged is SetNamedCondition that also counts a changed
// message as a change. SetNamedCondition reports only a new status or reason,
// which keeps LastTransitionTime honest; a caller announcing what the message
// names (the missing roles) needs to know when that changes too.
func setNamedConditionChanged(conditions *[]metav1.Condition, condType string, generation int64, status metav1.ConditionStatus, reason, message string) bool {
	prevMessage, had := "", false
	if existing := findCondition(*conditions, condType); existing != nil {
		prevMessage, had = existing.Message, true
	}
	changed := SetNamedCondition(conditions, condType, generation, status, reason, message)
	return changed || !had || prevMessage != message
}

// SetupWithManager registers the controller and wires up watches:
//   - Neo4jRole: re-reconciles users with PendingDependencies when a referenced
//     role is created.
//   - Neo4jEnterpriseCluster / Neo4jEnterpriseStandalone: re-reconciles every
//     user pointing at a changed cluster, so users react immediately to the
//     cluster's Ready condition flipping (otherwise we wait up to 30s for the
//     next requeue, which compounds across multiple cluster status updates
//     during formation and can starve the user reconcile in CI).
func (r *Neo4jUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	enqueueUsersForCluster := EnqueueDependentsForClusterChange(
		c,
		func() client.ObjectList { return &neo4jv1beta1.Neo4jUserList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			users, ok := list.(*neo4jv1beta1.Neo4jUserList)
			if !ok {
				return
			}
			for i := range users.Items {
				u := &users.Items[i]
				emit(u.Name, u.Namespace, u.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jUser{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueueUsersForCluster).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueueUsersForCluster).
		Watches(&neo4jv1beta1.Neo4jRole{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			role, ok := obj.(*neo4jv1beta1.Neo4jRole)
			if !ok {
				return nil
			}
			// Match a user's spec.roles against BOTH the role's effective Neo4j
			// name AND its CR metadata.name, so a user that referenced the role
			// by either form re-reconciles when the role lands (#260).
			roleName := role.Spec.Name
			if roleName == "" {
				roleName = role.Name
			}
			roleKey := targetRefKey(role.Spec.ClusterRef)
			users := &neo4jv1beta1.Neo4jUserList{}
			if err := c.List(ctx, users, client.InNamespace(role.Namespace)); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for i := range users.Items {
				u := &users.Items[i]
				if targetRefKey(u.Spec.ClusterRef) != roleKey {
					continue
				}
				for _, rname := range u.Spec.Roles {
					if rname == roleName || rname == role.Name {
						reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: u.Namespace, Name: u.Name}})
						break
					}
				}
			}
			return reqs
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}

// effectiveUsername returns spec.username if non-empty, otherwise metadata.name.
func effectiveUsername(user *neo4jv1beta1.Neo4jUser) string {
	if user.Spec.Username != "" {
		return user.Spec.Username
	}
	return user.Name
}

// normaliseRoles trims, deduplicates, sorts, and filters out the implicit
// PUBLIC role from a role-name slice.
//
// PUBLIC is auto-assigned to every Neo4j user — it always shows up in
// `SHOW USERS YIELD roles`, but listing it in spec.roles has no effect
// (the validator warns). Filtering here means downstream `rolesEqual`
// comparisons match cleanly: a fresh user with `roles: [reader]` has live
// roles `[PUBLIC, reader]`, which after this filter becomes `[reader]` on
// both sides of the diff. Without this filter the user would sit in
// `Pending` forever — observed empirically as a 9+ min reconcile loop in
// CI before the spec timeout fires.
func normaliseRoles(in []string) []string {
	set := map[string]struct{}{}
	for _, r := range in {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if strings.EqualFold(r, "PUBLIC") {
			continue
		}
		set[r] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func stringSlicesEqualSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsString(slice []string, s string) bool {
	for _, x := range slice {
		if x == s {
			return true
		}
	}
	return false
}

// sha256Hex returns the hex-encoded SHA-256 of b.
//
// IMPORTANT: this is NOT used as a password-storage hash and is NOT used in
// any authentication path. The operator computes a digest of the password
// Secret's value purely as a change-detection fingerprint — equality
// comparison only — so the controller can answer "did the Secret rotate
// since I last applied it?" on each reconcile loop. The actual password
// authentication is performed by Neo4j against its own (scrypt-based)
// hash in the `system` database; the password text itself lives in the
// Kubernetes Secret. For change detection what matters is collision
// resistance (SHA-256 is correct), not computational cost (slow hashes
// like bcrypt/Argon2/scrypt would actively hurt the loop for zero
// security benefit). The fingerprint sits in `status.passwordSecretHash`,
// which requires `get neo4jusers` RBAC; an attacker with that permission
// can already read the operator's Secret directly via `get secrets`, so
// the hash leaks no information not otherwise available.
//
// codeql[go/weak-sensitive-data-hashing]: Intentional — fingerprint for
// Secret-rotation detection, not a password-storage hash. See doc above.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
