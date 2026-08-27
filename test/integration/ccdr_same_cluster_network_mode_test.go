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

package integration_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// isCCDRReplicaCompatible returns true when the current Neo4j image tag can
// HOST a cross-cluster replica (Version.SupportsCCDRReplica — 2026.08+).
// Reuses the same production version-gate the operator itself enforces
// (internal/neo4j/version.go), rather than duplicating the comparison the
// way isPropertyShardingCompatible does — the gate here is a single method
// call, not a multi-field comparison.
func isCCDRReplicaCompatible() bool {
	v, err := neo4j.ParseVersion(getNeo4jImageTag())
	if err != nil {
		return false
	}
	return v.SupportsCCDRReplica()
}

// Same-Kubernetes-cluster network-mode CCDR, end-to-end.
//
// Automates docs/developer_guide/release_verification.md Part C: the first
// time CREATE REPLICA DATABASE ... OPTIONS {replicaConfig: {remote,
// addresses}} runs against a real server in any automated suite, and the
// first live (non-fake-client) exercise of resolveUpstreamClusterAddresses.
//
// Dormant on the default CI anchor (below 2026.08); runs when
// integration-tests.yml is dispatched with neo4j-version: 2026.08-enterprise+.
//
// Deliberately runs two clusters CONCURRENTLY — a documented, bounded
// exception to the project's one-Enterprise-deployment-at-a-time rule.
// Network replication cannot be verified sequentially the way backup-mode
// CCDR is (see ccdr_replica_test.go): the two clusters need a live
// connection to each other, not just a shared object store. Both are kept
// to CI-appropriate sizing (2 servers, getCIAppropriateResourceRequirements)
// to bound the resource-contention risk that rule exists to avoid.
var _ = Describe("Same-Cluster Network Mode CCDR", Label("extended"), Serial, func() {
	var (
		upstreamNamespace   string
		downstreamNamespace string
		upstream            *neo4jv1beta1.Neo4jEnterpriseCluster
		downstream          *neo4jv1beta1.Neo4jEnterpriseCluster
		replica             *neo4jv1beta1.Neo4jReplicaDatabase
		promotion           *neo4jv1beta1.Neo4jReplicaPromotion
	)

	BeforeEach(func() {
		if !isOperatorRunning() {
			Skip("Same-cluster network mode test requires the operator to be running")
		}
		if !isCCDRReplicaCompatible() {
			Skip(fmt.Sprintf("Skipping same-cluster network mode CCDR: requires Neo4j %s+, got %s",
				neo4j.MinCCDRReplicaVersion, getNeo4jImageTag()))
		}
		upstreamNamespace = createTestNamespace("ccdr-net-up")
		downstreamNamespace = createTestNamespace("ccdr-net-down")

		for _, ns := range []string{upstreamNamespace, downstreamNamespace} {
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: ns},
				Data: map[string][]byte{
					"username": []byte("neo4j"),
					"password": []byte("password123"),
				},
				Type: corev1.SecretTypeOpaque,
			})).To(Succeed())
		}
	})

	AfterEach(func() {
		for _, cr := range []client.Object{promotion, replica, downstream, upstream} {
			if cr == nil {
				continue
			}
			if len(cr.GetFinalizers()) > 0 {
				cr.SetFinalizers(nil)
				_ = k8sClient.Update(ctx, cr)
			}
			_ = k8sClient.Delete(ctx, cr)
		}
		promotion, replica, downstream, upstream = nil, nil, nil, nil
		if upstreamNamespace != "" {
			cleanupCustomResourcesInNamespace(upstreamNamespace)
		}
		if downstreamNamespace != "" {
			cleanupCustomResourcesInNamespace(downstreamNamespace)
		}
	})

	It("streams a replica via upstreamClusterRef and promotes it, with no proxy involved", func() {
		upstreamName := fmt.Sprintf("ccdr-up-%d", GinkgoRandomSeed())
		downstreamName := fmt.Sprintf("ccdr-down-%d", GinkgoRandomSeed())

		newSmallCluster := func(name, ns string) *neo4jv1beta1.Neo4jEnterpriseCluster {
			c := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: getNeo4jImageTag()},
					Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
					Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 2},
					Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
					Resources:              getCIAppropriateResourceRequirements(),
					TLS:                    &neo4jv1beta1.TLSSpec{Mode: "disabled"},
				},
			}
			applyCIOptimizations(c)
			return c
		}

		By("Creating the upstream and downstream clusters concurrently")
		upstream = newSmallCluster(upstreamName, upstreamNamespace)
		downstream = newSmallCluster(downstreamName, downstreamNamespace)
		Expect(k8sClient.Create(ctx, upstream)).To(Succeed())
		Expect(k8sClient.Create(ctx, downstream)).To(Succeed())

		waitForReady := func(c *neo4jv1beta1.Neo4jEnterpriseCluster, ns string) {
			Eventually(func() error {
				if err := crashLoopError(ctx, ns); err != nil {
					return StopTrying("Neo4j server pod is crash-looping").Wrap(err)
				}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: c.Name, Namespace: ns}, c); err != nil {
					return err
				}
				if c.Status.Phase != "Ready" {
					return fmt.Errorf("waiting for cluster %s/%s, phase=%s message=%q", ns, c.Name, c.Status.Phase, c.Status.Message)
				}
				return nil
			}, clusterTimeout, interval).Should(Succeed())
		}
		By("Waiting for both clusters to reach Ready")
		waitForReady(upstream, upstreamNamespace)
		waitForReady(downstream, downstreamNamespace)

		By("Creating the replica on the downstream, referencing the upstream by name")
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-replica", Namespace: downstreamNamespace},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       downstreamName,
				UpstreamDatabase: "neo4j", // the default database, always present once Ready
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode: neo4jv1beta1.ReplicaSourceModeNetwork,
					UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{
						Name:      upstreamName,
						Namespace: upstreamNamespace,
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		By("Waiting for the replica to reach Replicating")
		Eventually(func() error {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: replica.Name, Namespace: downstreamNamespace}, replica); err != nil {
				return err
			}
			if replica.Status.Phase != neo4jv1beta1.ReplicaPhaseReplicating {
				return fmt.Errorf("waiting for replica, phase=%s message=%q", replica.Status.Phase, replica.Status.Message)
			}
			return nil
		}, clusterTimeout, interval).Should(Succeed())
		Expect(replica.Status.DatabaseType).To(Equal(neo4j.DatabaseTypeReplica))

		By("Promoting the replica")
		promotion = &neo4jv1beta1.Neo4jReplicaPromotion{
			ObjectMeta: metav1.ObjectMeta{Name: "promote-neo4j-replica", Namespace: downstreamNamespace},
			Spec:       neo4jv1beta1.Neo4jReplicaPromotionSpec{ReplicaRef: replica.Name},
		}
		Expect(k8sClient.Create(ctx, promotion)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: promotion.Name, Namespace: downstreamNamespace}, promotion); err != nil {
				return ""
			}
			return promotion.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Completed"))

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: replica.Name, Namespace: downstreamNamespace}, replica); err != nil {
				return ""
			}
			return replica.Status.Phase
		}, clusterTimeout, interval).Should(Equal(neo4jv1beta1.ReplicaPhasePromoted))
	})
})
