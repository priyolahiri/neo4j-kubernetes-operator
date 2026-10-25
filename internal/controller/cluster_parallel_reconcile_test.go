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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The suite's cluster controller runs with the production default of workers,
// so these clusters are reconciled in parallel, and CI runs this under -race:
// state the reconciler shares between clusters that is not safe for that
// fails here (operations.md id 129).
var _ = Describe("Neo4jEnterpriseCluster parallel reconciles", func() {
	const (
		timeout  = 90 * time.Second
		interval = 250 * time.Millisecond
		count    = 6
	)
	var clusters []*neo4jv1beta1.Neo4jEnterpriseCluster

	AfterEach(func() {
		for _, c := range clusters {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: c.Name, Namespace: c.Namespace}, latest); err == nil {
				if len(latest.GetFinalizers()) > 0 {
					_ = releaseFinalizers(context.Background(), latest)
				}
				_ = k8sClient.Delete(context.Background(), latest)
			}
		}
		clusters = nil
	})

	It("reconciles several clusters at once, each into its own resources", func() {
		ctx := context.Background()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: "default"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("testpassword")},
		}
		if err := k8sClient.Create(ctx, secret); err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		stamp := time.Now().UnixNano()
		for i := 0; i < count; i++ {
			c := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("par-%d-%d", stamp, i), Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
					Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
					Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
					Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
				},
			}
			Expect(k8sClient.Create(ctx, c)).To(Succeed())
			clusters = append(clusters, c)
		}

		for _, c := range clusters {
			Eventually(func() error {
				sts := &appsv1.StatefulSet{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: c.Name + "-server", Namespace: c.Namespace}, sts); err != nil {
					return err
				}
				if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 3 {
					return fmt.Errorf("%s: replicas %v", sts.Name, sts.Spec.Replicas)
				}
				return k8sClient.Get(ctx, types.NamespacedName{Name: c.Name + "-config", Namespace: c.Namespace}, &corev1.ConfigMap{})
			}, timeout, interval).Should(Succeed(), c.Name)
		}
	})
})
