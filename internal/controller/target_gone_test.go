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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// #594: in a teardown that deleted the cluster first, every dependent warned
// ClusterNotFound for a target deleted a moment before its own deletion. A
// dependent whose target went away still waits for it (Pending, reason
// ClusterNotFound, at once), but its Warning comes only if it outlasts the
// target by targetGoneNoticeDelay: a dependent deleted in the meantime takes
// the deletion path and announces nothing (knowledge rules 178, 182).

// readyBefore gives obj, one of deletionKinds' objects, the status of a pass
// that found its target, and no deletion timestamp.
func readyBefore(t *testing.T, obj client.Object) client.Object {
	t.Helper()
	obj.SetDeletionTimestamp(nil)
	ready := []metav1.Condition{{Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "InSync",
		Message: "in sync", LastTransitionTime: metav1.Now()}}
	switch v := obj.(type) {
	case *neo4jv1beta1.Neo4jUser:
		v.Status.Phase, v.Status.Conditions = "Ready", ready
	case *neo4jv1beta1.Neo4jRole:
		v.Status.Phase, v.Status.Conditions = "Ready", ready
	case *neo4jv1beta1.Neo4jRoleBinding:
		v.Status.Phase, v.Status.Conditions = "Ready", ready
	case *neo4jv1beta1.Neo4jAuthRule:
		v.Status.Phase, v.Status.Conditions = "Ready", ready
	case *neo4jv1beta1.Neo4jCompositeDatabase:
		v.Status.Phase, v.Status.Conditions = neo4jv1beta1.PhaseReady, ready
	case *neo4jv1beta1.Neo4jDatabaseAlias:
		v.Status.Phase, v.Status.Conditions = "Ready", ready
	case *neo4jv1beta1.Neo4jReplicaDatabase:
		v.Status.Phase, v.Status.Conditions = neo4jv1beta1.ReplicaPhaseReplicating, ready
	case *neo4jv1beta1.Neo4jDatabase:
		v.Status.Phase, v.Status.Conditions = neo4jv1beta1.PhaseReady, ready
	default:
		t.Fatalf("no status for %T", obj)
	}
	return obj
}

func targetGoneOf(t *testing.T, r reconcile.Reconciler) *targetGoneNotices {
	t.Helper()
	switch v := r.(type) {
	case *Neo4jUserReconciler:
		return &v.targetGone
	case *Neo4jRoleReconciler:
		return &v.targetGone
	case *Neo4jRoleBindingReconciler:
		return &v.targetGone
	case *Neo4jAuthRuleReconciler:
		return &v.targetGone
	case *Neo4jCompositeDatabaseReconciler:
		return &v.targetGone
	case *Neo4jDatabaseAliasReconciler:
		return &v.targetGone
	case *Neo4jReplicaDatabaseReconciler:
		return &v.targetGone
	case *Neo4jDatabaseReconciler:
		return &v.targetGone
	}
	t.Fatalf("no targetGone on %T", r)
	return nil
}

func TestDependents_ATargetThatWentAwayIsAnnouncedOnlyIfTheDependentOutlastsIt(t *testing.T) {
	for _, k := range deletionKinds() {
		t.Run(k.name+"/outlasts it", func(t *testing.T) {
			obj := readyBefore(t, k.obj(time.Now()))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).
				WithStatusSubresource(obj).Build() // the target is gone
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, _ := k.setup(c, rec, &connErr)
			key := client.ObjectKeyFromObject(obj)

			for _, err := range reconcileTimes(t, r, key, repeatPasses) {
				require.NoError(t, err)
			}
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound),
				"not while a teardown may still delete the dependent")
			live, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(context.Background(), key, live))
			assert.Contains(t, statusMessage(t, c, obj), "not found", "the wait is on status at once")

			// A minute later the dependent is still there: announced, once.
			gone := targetGoneOf(t, r)
			gone.m.Store(key.String(), time.Now().Add(-targetGoneNoticeDelay-time.Second))
			for _, err := range reconcileTimes(t, r, key, repeatPasses) {
				require.NoError(t, err)
			}
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), 1)
		})

		t.Run(k.name+"/deleted with it", func(t *testing.T) {
			obj := readyBefore(t, k.obj(time.Now()))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).
				WithStatusSubresource(obj).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, _ := k.setup(c, rec, &connErr)
			key := client.ObjectKeyFromObject(obj)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			// The teardown reaches the dependent.
			require.NoError(t, c.Delete(context.Background(), obj))
			targetGoneOf(t, r).m.Store(key.String(), time.Now().Add(-targetGoneNoticeDelay-time.Second))
			_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			assert.True(t, finalizerReleased(t, c, obj, k.finalizer), "the target took it with its volumes")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), "a teardown says nothing")
		})
	}
}
