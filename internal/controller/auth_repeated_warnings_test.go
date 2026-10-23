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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The auth, composite, alias and promotion controllers requeue every 30s (and
// on every status write of their cluster) while a failure or a missing
// dependency persists. Their Warnings used to be emitted on each of those
// passes, spending the object's event budget on a state already in status.
// They are announced when the state starts or its message changes, read from
// the persisted status, so an operator restart announces nothing either
// (knowledge rule 145).

const repeatedWarningsNS = "default"

func repeatedWarningsMeta(name, finalizer string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: repeatedWarningsNS, Generation: 1, Finalizers: []string{finalizer}}
}

// readyClusterWithoutAdminSecret is Ready, so dependents get as far as
// connecting, and has no admin Secret, so connecting fails at once — the
// ConnectionFailed path, with no Neo4j involved.
func readyClusterWithoutAdminSecret(tag string, config map[string]string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: repeatedWarningsNS},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
			Config: config,
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase: "Ready",
			Conditions: []metav1.Condition{{
				Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "ClusterReady",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

func abacConfig() map[string]string {
	return map[string]string{abacAuthorizationProvidersKey: "oidc-okta"}
}

type repeatedWarningCase struct {
	name          string
	reason        string
	objs          func() []client.Object
	newReconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
}

func repeatedWarningCases() []repeatedWarningCase {
	authRule := func() client.Object {
		return &neo4jv1beta1.Neo4jAuthRule{
			ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer),
			Spec:       neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "ec", Name: "rule", Condition: "true"},
		}
	}
	newAuthRuleReconciler := func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
		return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
	}
	return []repeatedWarningCase{
		{
			name:   "Neo4jAuthRule/AuthRuleVersionTooOld",
			reason: EventReasonAuthRuleVersionTooOld,
			objs: func() []client.Object {
				return []client.Object{authRule(), readyClusterWithoutAdminSecret("5.26.0-enterprise", abacConfig())}
			},
			newReconciler: newAuthRuleReconciler,
		},
		{
			name:   "Neo4jAuthRule/OIDCProviderNotConfigured",
			reason: EventReasonOIDCProviderNotConfigured,
			objs: func() []client.Object {
				return []client.Object{authRule(), readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil)}
			},
			newReconciler: newAuthRuleReconciler,
		},
		{
			name:   "Neo4jAuthRule/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{authRule(), readyClusterWithoutAdminSecret("2026.04.0-enterprise", abacConfig())}
			},
			newReconciler: newAuthRuleReconciler,
		},
		{
			name:   "Neo4jRole/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{
					&neo4jv1beta1.Neo4jRole{
						ObjectMeta: repeatedWarningsMeta("role", Neo4jRoleFinalizer),
						Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "ec", Name: "analyst"},
					},
					readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil),
				}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
		{
			name:   "Neo4jRoleBinding/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{
					&neo4jv1beta1.Neo4jRoleBinding{
						ObjectMeta: repeatedWarningsMeta("rb", Neo4jRoleBindingFinalizer),
						Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "ec", Username: "alice", Roles: []string{"reader"}},
					},
					readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil),
				}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
		{
			name:   "Neo4jUser/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{
					&neo4jv1beta1.Neo4jUser{
						ObjectMeta: repeatedWarningsMeta("user", Neo4jUserFinalizer),
						Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "ec", Username: "alice"},
					},
					readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil),
				}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
		{
			name:   "Neo4jCompositeDatabase/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{
					&neo4jv1beta1.Neo4jCompositeDatabase{
						ObjectMeta: repeatedWarningsMeta("cd", Neo4jCompositeDatabaseFinalizer),
						Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "ec", Name: "sales"},
					},
					readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil),
				}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
		{
			name:   "Neo4jDatabaseAlias/ConnectionFailed",
			reason: EventReasonConnectionFailed,
			objs: func() []client.Object {
				return []client.Object{
					&neo4jv1beta1.Neo4jDatabaseAlias{
						ObjectMeta: repeatedWarningsMeta("alias", Neo4jDatabaseAliasFinalizer),
						Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "ec", Name: "latest", TargetDatabase: "orders"},
					},
					readyClusterWithoutAdminSecret("2026.04.0-enterprise", nil),
				}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
	}
}

// TestReconcile_PersistentWarningIsAnnouncedOnce drives the real Reconcile:
// three passes over an unchanged condition announce it once, and a restarted
// reconciler announces nothing.
func TestReconcile_PersistentWarningIsAnnouncedOnce(t *testing.T) {
	for _, tc := range repeatedWarningCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			objs := tc.objs()
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(objs...).
				WithStatusSubresource(objs...).Build()
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(objs[0])
			reconcileWith := func(r reconcile.Reconciler) {
				t.Helper()
				// ConnectionFailed returns its error for backoff; the event
				// is what is under test.
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			}

			r := tc.newReconciler(c, rec)
			for range 3 {
				reconcileWith(r)
			}
			got := eventsWithReason(drainEvents(rec), tc.reason)
			require.Len(t, got, 1, "announced when it starts, not on every pass: %q", got)

			reconcileWith(tc.newReconciler(c, rec))
			assert.Empty(t, eventsWithReason(drainEvents(rec), tc.reason),
				"a restarted operator reads the state from status and stays quiet")
		})
	}
}

// failCase calls one controller's fail() helper, which every Neo4j refusal and
// lookup failure of that controller goes through.
type failCase struct {
	name   string
	reason string
	obj    client.Object
	// fail calls the helper on obj through a fresh reconciler.
	fail func(c client.Client, rec record.EventRecorder, label string, err error)
}

func failCases() []failCase {
	const requeue = 30 * time.Second
	ctx := context.Background()
	rule := &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer)}
	role := &neo4jv1beta1.Neo4jRole{ObjectMeta: repeatedWarningsMeta("role", Neo4jRoleFinalizer)}
	rb := &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: repeatedWarningsMeta("rb", Neo4jRoleBindingFinalizer)}
	user := &neo4jv1beta1.Neo4jUser{ObjectMeta: repeatedWarningsMeta("user", Neo4jUserFinalizer)}
	cd := &neo4jv1beta1.Neo4jCompositeDatabase{ObjectMeta: repeatedWarningsMeta("cd", Neo4jCompositeDatabaseFinalizer)}
	alias := &neo4jv1beta1.Neo4jDatabaseAlias{ObjectMeta: repeatedWarningsMeta("alias", Neo4jDatabaseAliasFinalizer)}
	return []failCase{
		{
			name: "Neo4jAuthRule", reason: EventReasonAuthRuleFailed, obj: rule,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jAuthRuleReconciler{Client: c, Recorder: rec}).fail(ctx, rule, label, err, requeue)
			},
		},
		{
			name: "Neo4jRole", reason: EventReasonRoleSyncFailed, obj: role,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jRoleReconciler{Client: c, Recorder: rec}).fail(ctx, role, label, err, requeue)
			},
		},
		{
			name: "Neo4jRoleBinding", reason: EventReasonBindingFailed, obj: rb,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jRoleBindingReconciler{Client: c, Recorder: rec}).fail(ctx, rb, label, err, requeue)
			},
		},
		{
			name: "Neo4jUser", reason: EventReasonUserSyncFailed, obj: user,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jUserReconciler{Client: c, Recorder: rec}).fail(ctx, user, label, err, requeue)
			},
		},
		{
			name: "Neo4jCompositeDatabase", reason: EventReasonCompositeFailed, obj: cd,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jCompositeDatabaseReconciler{Client: c, Recorder: rec}).fail(ctx, cd, label, err, requeue)
			},
		},
		{
			name: "Neo4jDatabaseAlias", reason: EventReasonAliasFailed, obj: alias,
			fail: func(c client.Client, rec record.EventRecorder, label string, err error) {
				_, _ = (&Neo4jDatabaseAliasReconciler{Client: c, Recorder: rec}).fail(ctx, alias, label, err, requeue)
			},
		},
	}
}

// TestFail_AnnouncesTheSameFailureOnce: a refusal Neo4j repeats (requeued on
// the fixed interval) and a failure retried with backoff both reach fail() on
// every pass. The same failure is announced once, a different one again, and a
// restarted reconciler stays quiet.
func TestFail_AnnouncesTheSameFailureOnce(t *testing.T) {
	refused := errors.New("Neo.ClientError.Statement.SyntaxError: Invalid input")
	for _, tc := range failCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			rec := record.NewFakeRecorder(100)

			for range 3 {
				tc.fail(c, rec, "create failed", refused)
			}
			got := eventsWithReason(drainEvents(rec), tc.reason)
			require.Len(t, got, 1, "the same failure is announced once: %q", got)

			tc.fail(c, rec, "grant failed", refused)
			tc.fail(c, rec, "grant failed", refused)
			again := eventsWithReason(drainEvents(rec), tc.reason)
			require.Len(t, again, 1, "a different failure is news: %q", again)
			assert.NotEqual(t, got[0], again[0])

			// Each call builds its reconciler afresh, so every call above is
			// already a "restarted" one; one more, for the record.
			tc.fail(c, rec, "grant failed", refused)
			assert.Empty(t, eventsWithReason(drainEvents(rec), tc.reason))
		})
	}
}

// pendingRolesCase calls one controller's reportPendingRoles.
type pendingRolesCase struct {
	name string
	obj  client.Object
	// report calls reportPendingRoles on obj through a fresh reconciler.
	report func(c client.Client, rec record.EventRecorder, missing []string)
}

func pendingRolesCases() []pendingRolesCase {
	ctx := context.Background()
	rule := &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer)}
	rb := &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: repeatedWarningsMeta("rb", Neo4jRoleBindingFinalizer)}
	user := &neo4jv1beta1.Neo4jUser{ObjectMeta: repeatedWarningsMeta("user", Neo4jUserFinalizer)}
	return []pendingRolesCase{
		{
			name: "Neo4jAuthRule", obj: rule,
			report: func(c client.Client, rec record.EventRecorder, missing []string) {
				(&Neo4jAuthRuleReconciler{Client: c, Recorder: rec}).reportPendingRoles(ctx, rule, missing)
			},
		},
		{
			name: "Neo4jRoleBinding", obj: rb,
			report: func(c client.Client, rec record.EventRecorder, missing []string) {
				(&Neo4jRoleBindingReconciler{Client: c, Recorder: rec}).reportPendingRoles(ctx, rb, missing)
			},
		},
		{
			name: "Neo4jUser", obj: user,
			report: func(c client.Client, rec record.EventRecorder, missing []string) {
				(&Neo4jUserReconciler{Client: c, Recorder: rec}).reportPendingRoles(ctx, user, missing)
			},
		},
	}
}

// TestReportPendingRoles_AnnouncesTheWaitOnce: RolePending was emitted before
// the status write, on every pass while a role stayed missing. It is announced
// when the wait starts and when the set of missing roles changes.
func TestReportPendingRoles_AnnouncesTheWaitOnce(t *testing.T) {
	for _, tc := range pendingRolesCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			rec := record.NewFakeRecorder(100)

			for range 3 {
				tc.report(c, rec, []string{"auditor", "analyst"})
			}
			got := eventsWithReason(drainEvents(rec), EventReasonRolePending)
			require.Len(t, got, 1, "announced when the wait starts: %q", got)
			assert.Contains(t, got[0], "waiting for roles to exist: [analyst auditor]")

			// The same set in another order is the same wait.
			tc.report(c, rec, []string{"analyst", "auditor"})
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRolePending))

			// One of them landed: a different set is news.
			tc.report(c, rec, []string{"auditor"})
			tc.report(c, rec, []string{"auditor"})
			require.Len(t, eventsWithReason(drainEvents(rec), EventReasonRolePending), 1)

			// All present, then missing again: announced again.
			tc.report(c, rec, nil)
			tc.report(c, rec, []string{"auditor"})
			require.Len(t, eventsWithReason(drainEvents(rec), EventReasonRolePending), 1)
		})
	}
}

// resolvedRolesCase calls one controller's reportResolvedRoles.
type resolvedRolesCase struct {
	name string
	obj  client.Object
	// report calls reportResolvedRoles on obj through a fresh reconciler.
	report func(c client.Client, rec record.EventRecorder, resolved map[string]string)
	// conditions reads obj's persisted conditions.
	conditions func(t *testing.T, c client.Client) []metav1.Condition
}

func resolvedRolesCases() []resolvedRolesCase {
	ctx := context.Background()
	rb := &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: repeatedWarningsMeta("rb", Neo4jRoleBindingFinalizer)}
	user := &neo4jv1beta1.Neo4jUser{ObjectMeta: repeatedWarningsMeta("user", Neo4jUserFinalizer)}
	return []resolvedRolesCase{
		{
			name: "Neo4jRoleBinding", obj: rb,
			report: func(c client.Client, rec record.EventRecorder, resolved map[string]string) {
				(&Neo4jRoleBindingReconciler{Client: c, Recorder: rec}).reportResolvedRoles(ctx, rb, resolved)
			},
			conditions: func(t *testing.T, c client.Client) []metav1.Condition {
				live := &neo4jv1beta1.Neo4jRoleBinding{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(rb), live))
				return live.Status.Conditions
			},
		},
		{
			name: "Neo4jUser", obj: user,
			report: func(c client.Client, rec record.EventRecorder, resolved map[string]string) {
				(&Neo4jUserReconciler{Client: c, Recorder: rec}).reportResolvedRoles(ctx, user, resolved)
			},
			conditions: func(t *testing.T, c client.Client) []metav1.Condition {
				live := &neo4jv1beta1.Neo4jUser{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(user), live))
				return live.Status.Conditions
			},
		},
	}
}

// TestReportResolvedRoles_AnnouncesTheMappingOnce: RolesResolved was emitted on
// every pass that resolved a Neo4jRole CR name in spec.roles — 40 in five
// minutes on one Neo4jUser, 7 on one Neo4jRoleBinding (v1.22.0 journey). The
// mapping is recorded in the RoleNamesResolved condition and announced when it
// changes. Each call builds its reconciler afresh, so every repeat below is
// also what a restarted operator does.
func TestReportResolvedRoles_AnnouncesTheMappingOnce(t *testing.T) {
	for _, tc := range resolvedRolesCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			rec := record.NewFakeRecorder(100)
			mapping := map[string]string{"analyst-role": "analyst", "auditor-role": "auditor"}

			for range 3 {
				tc.report(c, rec, mapping)
			}
			got := eventsWithReason(drainEvents(rec), EventReasonRolesResolved)
			require.Len(t, got, 1, "announced when the mapping appears, not on every pass: %q", got)
			assert.Contains(t, got[0], "resolved Neo4jRole CR names to Neo4j role names: map[analyst-role:analyst auditor-role:auditor]")
			cond := meta.FindStatusCondition(tc.conditions(t, c), ConditionTypeRoleNamesResolved)
			require.NotNil(t, cond, "the mapping is kept in status, which outlives the operator")
			assert.Equal(t, metav1.ConditionTrue, cond.Status)
			assert.Equal(t, resolvedRolesMessage(mapping), cond.Message)

			// The same mapping, built again: not news.
			tc.report(c, rec, map[string]string{"auditor-role": "auditor", "analyst-role": "analyst"})
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRolesResolved))

			// A role CR now names a different Neo4j role: news, once.
			mapping["analyst-role"] = "analysts"
			tc.report(c, rec, mapping)
			tc.report(c, rec, mapping)
			require.Len(t, eventsWithReason(drainEvents(rec), EventReasonRolesResolved), 1)

			// spec.roles stops naming CRs: the condition goes, silently...
			tc.report(c, rec, nil)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRolesResolved))
			assert.Nil(t, meta.FindStatusCondition(tc.conditions(t, c), ConditionTypeRoleNamesResolved))

			// ...and a CR name coming back is announced again.
			tc.report(c, rec, map[string]string{"analyst-role": "analysts"})
			require.Len(t, eventsWithReason(drainEvents(rec), EventReasonRolesResolved), 1)
		})
	}
}

func TestReportUserNotFound_AnnouncesTheWaitOnce(t *testing.T) {
	ctx := context.Background()
	rb := &neo4jv1beta1.Neo4jRoleBinding{
		ObjectMeta: repeatedWarningsMeta("rb", Neo4jRoleBindingFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "ec", Username: "alice"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(rb).WithStatusSubresource(rb).Build()
	rec := record.NewFakeRecorder(100)

	for range 3 {
		(&Neo4jRoleBindingReconciler{Client: c, Recorder: rec}).reportUserNotFound(ctx, rb)
	}
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserNotFound), 1)

	live := &neo4jv1beta1.Neo4jRoleBinding{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(rb), live))
	assert.Equal(t, "Pending", live.Status.Phase)
}

func TestReportNameBlocked_AnnouncesOnce(t *testing.T) {
	ctx := context.Background()
	cd := &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: repeatedWarningsMeta("cd", Neo4jCompositeDatabaseFinalizer),
		Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "ec", Name: "cineasts"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cd).WithStatusSubresource(cd).Build()
	rec := record.NewFakeRecorder(100)

	for range 3 {
		(&Neo4jCompositeDatabaseReconciler{Client: c, Recorder: rec}).reportNameBlocked(ctx, cd, "cineasts", "cineasts.latest")
	}
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonCompositeNameBlocked), 1)

	// A different alias blocks it now: news.
	(&Neo4jCompositeDatabaseReconciler{Client: c, Recorder: rec}).reportNameBlocked(ctx, cd, "cineasts", "cineasts.old")
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonCompositeNameBlocked), 1)
}

// TestRevokeUnwanted_KeptDriftIsAnnouncedOnce: a row the role cannot revoke is
// kept on every pass, on a role that is otherwise Ready. Its warning stays
// true, so it goes through advisoryWarnings: once when it starts, not on every
// pass, and again at once if it goes away and comes back.
func TestRevokeUnwanted_KeptDriftIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	role := &neo4jv1beta1.Neo4jRole{ObjectMeta: repeatedWarningsMeta("role", Neo4jRoleFinalizer)}
	rec := record.NewFakeRecorder(100)
	r := &Neo4jRoleReconciler{Recorder: rec}
	const immutable = "DENY ACCESS ON DATABASE * TO `analyst`"
	immutableSet := map[string]struct{}{immutable: {}}

	revoke := func(toRemove ...string) {
		t.Helper()
		// Only immutable rows: nothing is sent to Neo4j, so no client.
		drift, revoked, res, err := r.revokeUnwanted(ctx, nil, role, toRemove, immutableSet, map[string]string{}, time.Minute)
		require.NoError(t, err)
		require.Nil(t, res)
		assert.Empty(t, revoked)
		assert.Equal(t, len(toRemove) > 0, drift)
	}

	for range 3 {
		revoke(immutable)
	}
	got := eventsWithReason(drainEvents(rec), EventReasonPrivilegesDriftKept)
	require.Len(t, got, 1, "announced when it starts, not on every pass: %q", got)
	assert.Contains(t, got[0], "cannot revoke immutable privilege")

	// Gone (someone changed the role), then back: announced again.
	revoke()
	revoke(immutable)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPrivilegesDriftKept), 1)
}

// TestPromotion_StartAndFailureAreAnnouncedOnce: a promotion retries its
// PROMOTE step until the database is no longer a replica. PromotionStarted is
// announced when it enters Promoting, not on each retry; a failed promote call
// is announced once while it keeps failing the same way, and its failure is
// not overwritten by the next attempt's optimistic message.
func TestPromotion_StartAndFailureAreAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	promo := &neo4jv1beta1.Neo4jReplicaPromotion{
		ObjectMeta: metav1.ObjectMeta{Name: "promo", Namespace: repeatedWarningsNS, Generation: 1},
		Spec:       neo4jv1beta1.Neo4jReplicaPromotionSpec{ReplicaRef: "replica"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(promo).WithStatusSubresource(promo).Build()
	rec := record.NewFakeRecorder(100)
	live := func() *neo4jv1beta1.Neo4jReplicaPromotion {
		t.Helper()
		p := &neo4jv1beta1.Neo4jReplicaPromotion{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(promo), p))
		return p
	}
	// Each pass starts from what the reconcile read, a fresh reconciler as
	// after a restart included.
	start := func() {
		(&Neo4jReplicaPromotionReconciler{Client: c, Recorder: rec}).startPromoting(ctx, live(), "orders", 7)
	}
	callFailed := func() {
		(&Neo4jReplicaPromotionReconciler{Client: c, Recorder: rec}).promoteCallFailed(ctx, live(),
			`promote call failed for "orders": boom`)
	}

	start()
	start() // a retry after a lost confirmation: still Promoting
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPromotionStarted), 1)

	for range 3 {
		start()
		callFailed()
	}
	events := drainEvents(rec)
	assert.Empty(t, eventsWithReason(events, EventReasonPromotionStarted), "a retry is not a new promotion")
	assert.Len(t, eventsWithReason(events, EventReasonPromotionFailed), 1, "the same failure, once: %q", events)
	assert.Equal(t, `promote call failed for "orders": boom`, live().Status.Message,
		"the failure stays in status across retries")

	// The terminal failure is announced once, too.
	for range 2 {
		(&Neo4jReplicaPromotionReconciler{Client: c, Recorder: rec}).fail(ctx, live(), "ec", `database "orders" does not exist`)
	}
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPromotionFailed), 1)
	assert.Equal(t, neo4jv1beta1.PromotionPhaseFailed, live().Status.Phase)
}
