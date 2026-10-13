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

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// deleteLegacyDiscoveryRBAC removes the {cluster}-discovery Role and
// RoleBinding that releases up to v1.20 created for the server pods. They
// granted get/list/watch on services and endpoints, for Neo4j's K8S discovery
// resolver; the operator uses LIST discovery, which never calls the
// Kubernetes API, so the grant was unused. Only objects this cluster controls
// are deleted: a Role or RoleBinding of the same name that someone else
// created is left alone. Deleting either restarts nothing (operations.md id 130).
func (r *Neo4jEnterpriseClusterReconciler) deleteLegacyDiscoveryRBAC(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	key := types.NamespacedName{Name: resources.LegacyDiscoveryRBACName(cluster), Namespace: cluster.Namespace}
	for _, obj := range []client.Object{&rbacv1.RoleBinding{}, &rbacv1.Role{}} {
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
