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

	"github.com/stretchr/testify/require"
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
