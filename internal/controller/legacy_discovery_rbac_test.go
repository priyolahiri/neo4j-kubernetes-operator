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
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The {cluster}-discovery Role and RoleBinding releases up to v1.20 created
// are deleted when the cluster owns them, and left alone when it does not.
func TestDeleteLegacyDiscoveryRBAC(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "uid-c"}}
	key := types.NamespacedName{Name: "c-discovery", Namespace: "ns"}
	legacy := func(owned bool) []client.Object {
		role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"services", "endpoints"}, Verbs: []string{"get", "list", "watch"}}}}
		binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: key.Name}}
		if owned {
			require.NoError(t, controllerutil.SetControllerReference(cluster, role, scheme))
			require.NoError(t, controllerutil.SetControllerReference(cluster, binding, scheme))
		}
		return []client.Object{role, binding}
	}
	exists := func(c client.Client, obj client.Object) bool {
		err := c.Get(context.Background(), key, obj)
		require.True(t, err == nil || apierrors.IsNotFound(err), "%v", err)
		return err == nil
	}

	t.Run("owned by the cluster: deleted", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(legacy(true), cluster)...).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
		require.NoError(t, r.deleteLegacyDiscoveryRBAC(context.Background(), cluster))
		assert.False(t, exists(c, &rbacv1.Role{}), "the Role is gone")
		assert.False(t, exists(c, &rbacv1.RoleBinding{}), "the RoleBinding is gone")
	})

	t.Run("not owned by the cluster: kept", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(legacy(false), cluster)...).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
		require.NoError(t, r.deleteLegacyDiscoveryRBAC(context.Background(), cluster))
		assert.True(t, exists(c, &rbacv1.Role{}), "someone else's Role is kept")
		assert.True(t, exists(c, &rbacv1.RoleBinding{}), "someone else's RoleBinding is kept")
	})

	t.Run("nothing there: no error", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
		assert.NoError(t, r.deleteLegacyDiscoveryRBAC(context.Background(), cluster))
	})
}
