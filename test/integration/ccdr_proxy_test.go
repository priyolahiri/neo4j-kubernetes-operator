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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// CCDR proxy + same-Kubernetes-cluster ergonomics — the part of network-mode
// CCDR that needs no Neo4j 2026.08+ image at all, because none of it hosts a
// replica: the proxy toggle, status.internalAddresses, and the
// networkPolicy.allowReplicasFrom peer are pure functions of the upstream
// cluster's own spec/status. Labelled "core" (unlike ccdr_replica_test.go's
// version-gated specs) and runs on every PR against both CI anchors.
//
// What is NOT covered here: actually hosting and streaming a replica. That
// needs Neo4j 2026.08+ and is covered by the dispatch-gated spec in
// ccdr_same_cluster_network_mode_test.go, and by the manual pre-release
// journey (docs/developer_guide/release_verification.md, Parts C/D).
var _ = Describe("CCDR Proxy (same-cluster ergonomics)", Label("core"), func() {
	var (
		testNamespace string
		cluster       *neo4jv1beta1.Neo4jEnterpriseCluster
	)

	BeforeEach(func() {
		if !isOperatorRunning() {
			Skip("CCDR proxy test requires the operator to be running")
		}
		testNamespace = createTestNamespace("ccdr-proxy")

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: testNamespace},
			Data: map[string][]byte{
				"username": []byte("neo4j"),
				"password": []byte("password123"),
			},
			Type: corev1.SecretTypeOpaque,
		})).To(Succeed())
	})

	AfterEach(func() {
		if cluster != nil {
			if len(cluster.GetFinalizers()) > 0 {
				cluster.SetFinalizers([]string{})
				_ = k8sClient.Update(ctx, cluster)
			}
			_ = k8sClient.Delete(ctx, cluster)
			cluster = nil
		}
		if testNamespace != "" {
			cleanupCustomResourcesInNamespace(testNamespace)
		}
	})

	It("provisions the proxy, publishes internalAddresses, wires the NetworkPolicy peer, and tears down cleanly", func() {
		clusterName := fmt.Sprintf("ccdr-cluster-%d", GinkgoRandomSeed())

		By("Creating a cluster with crossClusterReplication and an allowReplicasFrom peer")
		cluster = &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: getNeo4jImageTag()},
				Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 2},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				Resources:              getCIAppropriateResourceRequirements(),
				TLS:                    &neo4jv1beta1.TLSSpec{Mode: "disabled"},
				CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationSpec{
					Enabled: true,
				},
				NetworkPolicy: &neo4jv1beta1.NetworkPolicySpec{
					Enabled: true,
					AllowReplicasFrom: []neo4jv1beta1.NetworkPolicyPeerCluster{
						{Name: "dr-cluster", Namespace: "dr"},
					},
				},
			},
		}
		applyCIOptimizations(cluster)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		By("Waiting for the cluster to reach Ready with the proxy enabled")
		Eventually(func() error {
			if err := crashLoopError(ctx, testNamespace); err != nil {
				return StopTrying("Neo4j server pod is crash-looping").Wrap(err)
			}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, cluster); err != nil {
				return err
			}
			if cluster.Status.Phase != "Ready" {
				return fmt.Errorf("waiting for cluster formation, phase=%s message=%q", cluster.Status.Phase, cluster.Status.Message)
			}
			return nil
		}, clusterTimeout, interval).Should(Succeed())

		By("Checking status.internalAddresses is populated unconditionally")
		Expect(cluster.Status.InternalAddresses).To(HaveLen(int(cluster.Spec.Topology.Servers)))
		for i, addr := range cluster.Status.InternalAddresses {
			Expect(addr).To(Equal(fmt.Sprintf("%s-server-%d.%s-headless.%s.svc.cluster.local:%d",
				clusterName, i, clusterName, testNamespace, resources.DiscoveryPort)))
		}

		By("Checking the CCDR proxy ConfigMap/Deployment/Service exist")
		proxyName := resources.CCDRProxyName(clusterName)
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Name: proxyName, Namespace: testNamespace}, &corev1.ConfigMap{})
		}, clusterTimeout, interval).Should(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: proxyName, Namespace: testNamespace}, &appsv1.Deployment{})).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: proxyName, Namespace: testNamespace}, &corev1.Service{})).To(Succeed())

		By("Checking the NetworkPolicy admits the allowReplicasFrom peer on port 6000 only")
		netpol := &networkingv1.NetworkPolicy{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: fmt.Sprintf("%s-server-netpol", clusterName), Namespace: testNamespace,
		}, netpol)).To(Succeed())
		foundPeerRule := false
		for _, rule := range netpol.Spec.Ingress {
			for _, peer := range rule.From {
				if peer.PodSelector != nil && peer.PodSelector.MatchLabels["neo4j.com/cluster"] == "dr-cluster" {
					foundPeerRule = true
					Expect(rule.Ports).To(HaveLen(1))
					Expect(rule.Ports[0].Port.IntValue()).To(Equal(resources.DiscoveryPort))
					Expect(peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]).To(Equal("dr"))
				}
			}
		}
		Expect(foundPeerRule).To(BeTrue(), "expected an ingress rule admitting neo4j.com/cluster=dr-cluster")

		By("Disabling crossClusterReplication and confirming the proxy resources are torn down")
		Eventually(func() error {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, cluster); err != nil {
				return err
			}
			cluster.Spec.CrossClusterReplication.Enabled = false
			return k8sClient.Update(ctx, cluster)
		}, clusterTimeout, interval).Should(Succeed())

		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Name: proxyName, Namespace: testNamespace}, &appsv1.Deployment{})
			return apierrors.IsNotFound(err)
		}, clusterTimeout, interval).Should(BeTrue(), "proxy Deployment should be deleted once crossClusterReplication is disabled")
	})

	It("rejects an allowReplicasFrom entry with an empty name", func() {
		clusterName := fmt.Sprintf("ccdr-cluster-bad-%d", GinkgoRandomSeed())
		cluster = &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: getNeo4jImageTag()},
				Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 2},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				TLS:                    &neo4jv1beta1.TLSSpec{Mode: "disabled"},
				NetworkPolicy: &neo4jv1beta1.NetworkPolicySpec{
					Enabled:           true,
					AllowReplicasFrom: []neo4jv1beta1.NetworkPolicyPeerCluster{{Namespace: "dr"}}, // Name empty
				},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, cluster); err != nil {
				return ""
			}
			return cluster.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Failed"))
	})
})
