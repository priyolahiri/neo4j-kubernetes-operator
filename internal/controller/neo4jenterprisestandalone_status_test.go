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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// On a standalone's first reconcile, reconcileStatefulSet creates the
// StatefulSet and updateStatus then reads it back through the cached client,
// which has not seen it yet. Returning that NotFound as an error put a
// ReconcileFailed warning ("failed to update status: failed to get
// StatefulSet: ... not found") on every new standalone, and diagnose reported
// it (found on the v1.18.0 journey). A StatefulSet that is not visible yet is
// a standalone that is not ready yet.
func TestStandaloneUpdateStatus_StatefulSetNotInCacheYetIsPendingNotAnError(t *testing.T) {
	scheme := newTestScheme()
	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sa).WithStatusSubresource(sa).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	require.NoError(t, r.updateStatus(context.Background(), sa),
		"a StatefulSet the cache has not seen yet must not fail the reconcile")

	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(sa), got))
	require.Equal(t, "Pending", got.Status.Phase)
	require.False(t, got.Status.Ready)
}

// The defect (#548): a standalone was Ready whenever its one pod was, also
// between a template write and the old pod's stop, when the pod still ready
// was about to restart. Everything that goes by the standalone's Ready — a
// database, a user, a plugin — acted on a server on its way down (the plugin
// gate checks the roll itself, operations.md id 141). Ready is the pod ready on
// the StatefulSet's latest pod template.
func TestStandaloneUpdateStatus_ARollInFlightIsNotReady(t *testing.T) {
	one := int32(1)
	for _, tc := range []struct {
		name   string
		status appsv1.StatefulSetStatus
		gen    int64
		ready  bool
	}{
		{"rolled out", appsv1.StatefulSetStatus{ObservedGeneration: 3, ReadyReplicas: 1, UpdatedReplicas: 1,
			CurrentRevision: "r3", UpdateRevision: "r3"}, 3, true},
		{"a template write its controller has not seen", appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: 1,
			UpdatedReplicas: 1, CurrentRevision: "r2", UpdateRevision: "r2"}, 3, false},
		{"the old pod still ready, about to be replaced", appsv1.StatefulSetStatus{ObservedGeneration: 3, ReadyReplicas: 1,
			UpdatedReplicas: 0, CurrentRevision: "r2", UpdateRevision: "r3"}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme()
			sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
				Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"}},
			}
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default", Generation: tc.gen},
				Spec: appsv1.StatefulSetSpec{Replicas: &one, Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:5.26-enterprise"}}},
				}},
				Status: tc.status,
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sa, sts).WithStatusSubresource(sa).Build()
			r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

			require.NoError(t, r.updateStatus(context.Background(), sa))

			got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(sa), got))
			assert.Equal(t, tc.ready, got.Status.Ready)
			if tc.ready {
				assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
				return
			}
			assert.Equal(t, neo4jv1beta1.PhasePending, got.Status.Phase)
			assert.Contains(t, got.Status.Message, "Waiting for the standalone to restart on its latest pod template")
		})
	}
}
