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

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// deleteLegacyDiscoveryObjects removes the {cluster}-discovery objects that
// releases up to v1.20 created and nothing uses (operations.md id 130):
//
//   - a Role and RoleBinding granting the server pods get/list/watch on
//     services and endpoints, for Neo4j's K8S discovery resolver;
//   - a ClusterIP Service on the V1 discovery port 5000.
//
// The operator uses LIST discovery, which resolves pod FQDNs through DNS. Only
// objects this cluster controls are deleted: one of the same name that someone
// else created is left alone. Deleting any of them restarts nothing. The
// {cluster}-discovery ServiceAccount stays; the server pods run under it.
func (r *Neo4jEnterpriseClusterReconciler) deleteLegacyDiscoveryObjects(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	key := types.NamespacedName{Name: resources.LegacyDiscoveryName(cluster), Namespace: cluster.Namespace}
	for _, obj := range []client.Object{&rbacv1.RoleBinding{}, &rbacv1.Role{}, &corev1.Service{}} {
		if err := r.Get(ctx, key, obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !metav1.IsControlledBy(obj, cluster) {
			continue
		}
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
