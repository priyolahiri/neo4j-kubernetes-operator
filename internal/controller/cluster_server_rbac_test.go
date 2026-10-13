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

package controller_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// LIST discovery needs no Kubernetes API access, so the server pods'
// ServiceAccount is bound to no Role (operations.md id 130).
var _ = Describe("Neo4jEnterpriseCluster server ServiceAccount", func() {
	var cluster *neo4jv1beta1.Neo4jEnterpriseCluster

	AfterEach(func() {
		if cluster == nil {
			return
		}
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, latest); err == nil {
			if len(latest.GetFinalizers()) > 0 {
				latest.SetFinalizers([]string{})
				_ = k8sClient.Update(context.Background(), latest)
			}
			_ = k8sClient.Delete(context.Background(), latest)
		}
		cluster = nil
	})

	It("creates the ServiceAccount and no discovery Role or RoleBinding", func() {
		ctx := context.Background()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("testpassword")},
		}
		if err := k8sClient.Create(ctx, secret); err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
		cluster = &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("sa-%d", time.Now().UnixNano()), Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key := types.NamespacedName{Name: cluster.Name + "-discovery", Namespace: "default"}

		Eventually(func() error {
			return k8sClient.Get(ctx, key, &corev1.ServiceAccount{})
		}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
		// The StatefulSet comes after the RBAC step, so once it exists a pass
		// has gone through the point where the Role used to be created.
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: "default"}, &appsv1.StatefulSet{})
		}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
		Expect(errors.IsNotFound(k8sClient.Get(ctx, key, &rbacv1.Role{}))).To(BeTrue(), "no discovery Role")
		Expect(errors.IsNotFound(k8sClient.Get(ctx, key, &rbacv1.RoleBinding{}))).To(BeTrue(), "no discovery RoleBinding")
	})
})
