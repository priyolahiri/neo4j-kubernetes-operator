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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
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
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// A standalone reads Ready a few seconds before its Bolt port accepts
// connections, and longer during a stopCluster restore. The v1.22.0
// combined-build walk saw bob, analyst and extuser-binding go Failed with a
// ConnectionFailed Warning for about five seconds after a restart, then
// announce UserReady, RoleReady and BindingCreated again (counts 2, and 4 over
// the restores), with some 180 ERROR lines in two minutes: rule 158 kept them
// Ready while the standalone said it was not, and the connection failure that
// followed undid it. A connection the target refuses while it reads Ready is
// the rest of the same outage (knowledge rule 172).

// readyAuthStatus is what a Ready object's status holds at generation 1.
func readyAuthStatus() (string, []metav1.Condition) {
	return "Ready", []metav1.Condition{{Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "Ready",
		ObservedGeneration: 1, Message: "in sync", LastTransitionTime: metav1.Now()}}
}

// unreachableCase is one auth kind whose connection to its target fails.
type unreachableCase struct {
	name string
	// obj builds the object, Ready at generation 1 when ready is set.
	obj func(ready bool) client.Object
	// reconciler builds the controller with every connection refused.
	reconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
	// fail calls the controller's fail() with err, as a call that reached the
	// server and lost it mid-pass does.
	fail func(c client.Client, rec record.EventRecorder, obj client.Object, err error) (ctrl.Result, error)
	// status reads the persisted phase and conditions.
	status func(t *testing.T, c client.Client, key client.ObjectKey) (string, []metav1.Condition)
}

func unreachableCases() []unreachableCase {
	ctx := context.Background()
	const requeue = 30 * time.Second
	refused := func(ResolvedTarget) (*neo4jclient.Client, error) { return nil, errBoltRefused }
	return []unreachableCase{
		{
			name: "Neo4jUser",
			obj: func(ready bool) client.Object {
				u := &neo4jv1beta1.Neo4jUser{
					ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
					Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice"},
				}
				if ready {
					u.Status.Phase, u.Status.Conditions = readyAuthStatus()
				}
				return u
			},
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return nil, errBoltRefused }}
			},
			fail: func(c client.Client, rec record.EventRecorder, obj client.Object, err error) (ctrl.Result, error) {
				u, _ := obj.(*neo4jv1beta1.Neo4jUser)
				return (&Neo4jUserReconciler{Client: c, Recorder: rec}).fail(ctx, u, "lookup failed", err, requeue)
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, []metav1.Condition) {
				o := &neo4jv1beta1.Neo4jUser{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Conditions
			},
		},
		{
			name: "Neo4jRole",
			obj: func(ready bool) client.Object {
				r := &neo4jv1beta1.Neo4jRole{
					ObjectMeta: repeatedWarningsMeta("analyst", Neo4jRoleFinalizer),
					Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"},
				}
				if ready {
					r.Status.Phase, r.Status.Conditions = readyAuthStatus()
				}
				return r
			},
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, connectTarget: refused}
			},
			fail: func(c client.Client, rec record.EventRecorder, obj client.Object, err error) (ctrl.Result, error) {
				r, _ := obj.(*neo4jv1beta1.Neo4jRole)
				return (&Neo4jRoleReconciler{Client: c, Recorder: rec}).fail(ctx, r, "lookup failed", err, requeue)
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, []metav1.Condition) {
				o := &neo4jv1beta1.Neo4jRole{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Conditions
			},
		},
		{
			name: "Neo4jRoleBinding",
			obj: func(ready bool) client.Object {
				b := &neo4jv1beta1.Neo4jRoleBinding{
					ObjectMeta: repeatedWarningsMeta("extuser-binding", Neo4jRoleBindingFinalizer),
					Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "extuser", Roles: []string{"reader"}},
				}
				if ready {
					b.Status.Phase, b.Status.Conditions = readyAuthStatus()
				}
				return b
			},
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return nil, errBoltRefused }}
			},
			fail: func(c client.Client, rec record.EventRecorder, obj client.Object, err error) (ctrl.Result, error) {
				b, _ := obj.(*neo4jv1beta1.Neo4jRoleBinding)
				return (&Neo4jRoleBindingReconciler{Client: c, Recorder: rec}).fail(ctx, b, "lookup failed", err, requeue)
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, []metav1.Condition) {
				o := &neo4jv1beta1.Neo4jRoleBinding{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Conditions
			},
		},
		{
			name: "Neo4jAuthRule",
			obj: func(ready bool) client.Object {
				r := &neo4jv1beta1.Neo4jAuthRule{
					ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer),
					Spec:       neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule", Condition: "true"},
				}
				if ready {
					r.Status.Phase, r.Status.Conditions = readyAuthStatus()
				}
				return r
			},
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, connectTarget: refused}
			},
			fail: func(c client.Client, rec record.EventRecorder, obj client.Object, err error) (ctrl.Result, error) {
				r, _ := obj.(*neo4jv1beta1.Neo4jAuthRule)
				return (&Neo4jAuthRuleReconciler{Client: c, Recorder: rec}).fail(ctx, r, "lookup failed", err, requeue)
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, []metav1.Condition) {
				o := &neo4jv1beta1.Neo4jAuthRule{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Conditions
			},
		},
	}
}

// unreachableTarget is the standalone "sa", Ready, on a version and with the
// configuration every auth kind gets past its own checks with.
func unreachableTarget() *neo4jv1beta1.Neo4jEnterpriseStandalone {
	sa := authTargetStandalone()
	sa.Spec.Config = abacConfig()
	return sa
}

func warnings(events []string) []string {
	var out []string
	for _, e := range events {
		if strings.HasPrefix(e, "Warning ") {
			out = append(out, e)
		}
	}
	return out
}

// TestReconcile_ATargetThatReadsReadyButRefusesIsAnOutage: a Ready object
// stays Ready while the connection is refused, says why in ClusterNotReady
// (reason ClusterUnreachable), emits nothing and returns no error, which
// controller-runtime would log at ERROR and retry within milliseconds; a new
// object waits in Pending, quietly too.
func TestReconcile_ATargetThatReadsReadyButRefusesIsAnOutage(t *testing.T) {
	for _, tc := range unreachableCases() {
		t.Run(tc.name, func(t *testing.T) {
			ready, fresh := tc.obj(true), tc.obj(false)
			fresh.SetName(fresh.GetName() + "-new")
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(ready, fresh, unreachableTarget()).
				WithStatusSubresource(ready, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			r := tc.reconciler(c, rec)

			for _, obj := range []client.Object{ready, fresh} {
				for range repeatPasses {
					res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
					require.NoError(t, err, "an outage is not returned as an error (it would be logged at ERROR)")
					assert.Positive(t, res.RequeueAfter, "retried on the fixed interval")
				}
			}
			assert.Empty(t, warnings(drainEvents(rec)), "an outage of a target that reads Ready announces nothing")

			for obj, want := range map[client.Object]string{ready: "Ready", fresh: "Pending"} {
				phase, conds := tc.status(t, c, client.ObjectKeyFromObject(obj))
				assert.Equal(t, want, phase, "%s", obj.GetName())
				cnr := meta.FindStatusCondition(conds, ConditionTypeClusterNotReady)
				require.NotNil(t, cnr)
				assert.Equal(t, metav1.ConditionTrue, cnr.Status)
				assert.Equal(t, ConditionReasonClusterUnreachable, cnr.Reason)
				assert.Contains(t, cnr.Message, "connection refused")
			}
		})
	}
}

// The same outage met mid-pass, by a call after the connection: fail() keeps
// the object Ready and quiet too.
func TestFail_ALostConnectionIsAnOutage(t *testing.T) {
	for _, tc := range unreachableCases() {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.obj(true)
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).WithStatusSubresource(obj).Build()
			rec := record.NewFakeRecorder(50)

			for _, err := range []error{errBoltRefused,
				fmt.Errorf("failed to run SHOW USERS: %w", &driver.ConnectivityError{Inner: io.EOF}),
				errors.New("Neo.TransientError.General.DatabaseUnavailable: Database 'system' is unavailable.")} {
				res, ferr := tc.fail(c, rec, obj, err)
				require.NoError(t, ferr)
				assert.Equal(t, 30*time.Second, res.RequeueAfter)
			}
			assert.Empty(t, drainEvents(rec))
			phase, conds := tc.status(t, c, client.ObjectKeyFromObject(obj))
			assert.Equal(t, "Ready", phase)
			assert.True(t, meta.IsStatusConditionTrue(conds, ConditionTypeClusterNotReady))
		})
	}
}

// An outage that outlasts targetUnreachableGrace is the object's own failure:
// Failed, with one ConnectionFailed Warning however many passes meet it.
func TestReconcile_AnUnreachableTargetIsReportedAfterTheGrace(t *testing.T) {
	for _, tc := range unreachableCases() {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.obj(true)
			setConditions(t, obj, append(conditionsOf(t, obj), metav1.Condition{
				Type: ConditionTypeClusterNotReady, Status: metav1.ConditionTrue, Reason: ConditionReasonClusterUnreachable,
				ObservedGeneration: 1, LastTransitionTime: metav1.NewTime(time.Now().Add(-targetUnreachableGrace - time.Minute)),
			}))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)

			for _, err := range reconcileTimes(t, tc.reconciler(c, rec), client.ObjectKeyFromObject(obj), repeatPasses) {
				require.NoError(t, err)
			}
			failed := eventsWithReason(drainEvents(rec), EventReasonConnectionFailed)
			require.Len(t, failed, 1, "announced once: %q", failed)
			assert.Contains(t, failed[0], "for more than 5m0s")
			phase, _ := tc.status(t, c, client.ObjectKeyFromObject(obj))
			assert.Equal(t, "Failed", phase)
		})
	}
}

// A connection failure that is not an outage — no admin Secret, a refused
// login — is still the object's failure at once (knowledge rule 145):
// TestReconcile_PersistentWarningIsAnnouncedOnce covers it for every kind.

func conditionsOf(t *testing.T, obj client.Object) []metav1.Condition {
	t.Helper()
	switch o := obj.(type) {
	case *neo4jv1beta1.Neo4jUser:
		return o.Status.Conditions
	case *neo4jv1beta1.Neo4jRole:
		return o.Status.Conditions
	case *neo4jv1beta1.Neo4jRoleBinding:
		return o.Status.Conditions
	case *neo4jv1beta1.Neo4jAuthRule:
		return o.Status.Conditions
	}
	t.Fatalf("unexpected %T", obj)
	return nil
}

func setConditions(t *testing.T, obj client.Object, conds []metav1.Condition) {
	t.Helper()
	switch o := obj.(type) {
	case *neo4jv1beta1.Neo4jUser:
		o.Status.Conditions = conds
	case *neo4jv1beta1.Neo4jRole:
		o.Status.Conditions = conds
	case *neo4jv1beta1.Neo4jRoleBinding:
		o.Status.Conditions = conds
	case *neo4jv1beta1.Neo4jAuthRule:
		o.Status.Conditions = conds
	default:
		t.Fatalf("unexpected %T", obj)
	}
}

// TestUserReconcile_ARestartThatRefusesBoltIsNotATransition replays the walk:
// a Ready user, its standalone restarting — not Ready, then Ready with Bolt
// still refusing, then serving — three times. No Warning, no second
// UserReady.
func TestUserReconcile_ARestartThatRefusesBoltIsNotATransition(t *testing.T) {
	server := newFakeAuthServer()
	var refusing bool
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("bob", Neo4jUserFinalizer),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"reader"}},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user, authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) {
			if refusing {
				return nil, errBoltRefused
			}
			return server, nil
		}}
	key := client.ObjectKeyFromObject(user)
	pass := func() {
		t.Helper()
		for _, err := range reconcileTimes(t, r, key, 1) {
			require.NoError(t, err)
		}
	}

	pass()
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserReady), 1)

	for range targetRestarts {
		setStandaloneReady(t, c, false)
		pass()
		setStandaloneReady(t, c, true)
		refusing = true
		pass()
		pass()
		refusing = false
		pass()
	}
	events := drainEvents(rec)
	assert.Empty(t, warnings(events), "no ConnectionFailed, no UserSyncFailed")
	assert.Empty(t, eventsWithReason(events, EventReasonUserReady), "the restart was not a transition of the user")
	live := &neo4jv1beta1.Neo4jUser{}
	require.NoError(t, c.Get(context.Background(), key, live))
	assert.Equal(t, "Ready", live.Status.Phase)
	assert.False(t, meta.IsStatusConditionTrue(live.Status.Conditions, ConditionTypeClusterNotReady),
		"the pass that reached the target clears the wait")
}
