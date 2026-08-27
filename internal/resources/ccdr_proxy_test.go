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

package resources_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func ccdrCluster(servers int32, enabled bool) *neo4jv1beta1.Neo4jEnterpriseCluster {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: servers},
		},
	}
	if enabled {
		cluster.Spec.CrossClusterReplication = &neo4jv1beta1.CrossClusterReplicationSpec{Enabled: true}
	}
	return cluster
}

func TestBuildCCDRProxyConfigMap_DisabledByDefault(t *testing.T) {
	assert.Nil(t, resources.BuildCCDRProxyConfigMap(ccdrCluster(3, false)))

	unset := ccdrCluster(3, false)
	unset.Spec.CrossClusterReplication = nil
	assert.Nil(t, resources.BuildCCDRProxyConfigMap(unset))
}

// TestBuildCCDRProxyConfigMap_OneFrontendBackendPerOrdinal pins the
// haproxy.cfg rendering: one frontend/backend pair per server ordinal,
// listening on CCDRProxyBasePort+i and forwarding to that ordinal's pod FQDN
// on the cluster port (6000), pure TCP passthrough throughout.
func TestBuildCCDRProxyConfigMap_OneFrontendBackendPerOrdinal(t *testing.T) {
	cluster := ccdrCluster(3, true)
	cm := resources.BuildCCDRProxyConfigMap(cluster)
	require.NotNil(t, cm)

	cfg := cm.Data["haproxy.cfg"]
	assert.Contains(t, cfg, "mode tcp")
	// Runtime DNS resolution (not HAProxy's default synchronous, resolve-
	// once-at-startup behavior) — without this the proxy crash-loops
	// whenever it starts before the backend pods'/headless Service's DNS
	// records exist, a real startup race caught by the integration suite.
	assert.Contains(t, cfg, "resolvers k8s")
	assert.Contains(t, cfg, "parse-resolv-conf")
	for i := int32(0); i < 3; i++ {
		port := resources.CCDRProxyBasePort + i
		assert.Contains(t, cfg, fmt.Sprintf("bind *:%d", port))
		backendAddr := fmt.Sprintf("prod-server-%d.prod-headless.ns.svc.cluster.local:%d", i, resources.DiscoveryPort)
		assert.Contains(t, cfg, backendAddr)
		assert.Contains(t, cfg, fmt.Sprintf("server s%d %s check resolvers k8s init-addr none", i, backendAddr))
	}
	// Never terminate or inspect TLS — the proxy is pure L4 passthrough so
	// end-to-end mutual TLS between the two clusters is unaffected.
	assert.NotContains(t, cfg, "ssl")
	assert.NotContains(t, cfg, "crt")
}

func TestBuildCCDRProxyDeployment_DisabledByDefault(t *testing.T) {
	assert.Nil(t, resources.BuildCCDRProxyDeployment(ccdrCluster(3, false)))
}

func TestBuildCCDRProxyDeployment_OnePortPerOrdinal(t *testing.T) {
	cluster := ccdrCluster(3, true)
	dep := resources.BuildCCDRProxyDeployment(cluster)
	require.NotNil(t, dep)
	require.Len(t, dep.Spec.Template.Spec.Containers, 1)

	container := dep.Spec.Template.Spec.Containers[0]
	require.Len(t, container.Ports, 3)
	for i, p := range container.Ports {
		assert.Equal(t, resources.CCDRProxyBasePort+int32(i), p.ContainerPort)
		assert.Equal(t, corev1.ProtocolTCP, p.Protocol)
	}

	// Non-root, no privilege escalation, all capabilities dropped — same
	// hardening posture as the pinned seed-proxy image.
	sc := container.SecurityContext
	require.NotNil(t, sc)
	assert.True(t, *sc.RunAsNonRoot)
	assert.False(t, *sc.AllowPrivilegeEscalation)
	assert.True(t, *sc.ReadOnlyRootFilesystem)
	require.NotNil(t, sc.Capabilities)
	assert.Equal(t, []corev1.Capability{"ALL"}, sc.Capabilities.Drop)
}

func TestBuildCCDRProxyService_DisabledByDefault(t *testing.T) {
	assert.Nil(t, resources.BuildCCDRProxyService(ccdrCluster(3, false)))
}

func TestBuildCCDRProxyService_LoadBalancerOnePortPerOrdinal(t *testing.T) {
	cluster := ccdrCluster(2, true)
	svc := resources.BuildCCDRProxyService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, corev1.ServiceTypeLoadBalancer, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 2)
	for i, p := range svc.Spec.Ports {
		assert.Equal(t, resources.CCDRProxyBasePort+int32(i), p.Port)
	}
}

func TestCCDRProxyLoadBalancerInternalEffective_DefaultsTrue(t *testing.T) {
	cluster := ccdrCluster(2, true)
	assert.True(t, resources.CCDRProxyLoadBalancerInternalEffective(cluster))

	f := false
	cluster.Spec.CrossClusterReplication.LoadBalancerInternal = &f
	assert.False(t, resources.CCDRProxyLoadBalancerInternalEffective(cluster))
}

// TestBuildNetworkPolicyForEnterprise_CCDRProxyRule pins the 4th, additive
// ingress rule: when crossClusterReplication is enabled, the proxy pod (and
// only the proxy pod) may reach the cluster port (6000) — on top of, not
// instead of, the existing intra-cluster peer rule for the same port.
func TestBuildNetworkPolicyForEnterprise_CCDRProxyRule(t *testing.T) {
	cluster := ccdrCluster(3, true)
	cluster.Spec.NetworkPolicy = &neo4jv1beta1.NetworkPolicySpec{Enabled: true}
	np := resources.BuildNetworkPolicyForEnterprise(cluster)
	require.NotNil(t, np)

	var rulesFor6000 []int
	var foundProxyRule bool
	for i, rule := range np.Spec.Ingress {
		for _, p := range rule.Ports {
			if p.Port != nil && p.Port.IntValue() == resources.DiscoveryPort {
				rulesFor6000 = append(rulesFor6000, i)
				for _, peer := range rule.From {
					if peer.PodSelector != nil && peer.PodSelector.MatchLabels["app.kubernetes.io/component"] == "ccdr-proxy" {
						foundProxyRule = true
					}
				}
			}
		}
	}
	// The existing cluster-peer rule for 6000 plus the new proxy rule — the
	// proxy admission must be additive, never replacing the peer rule.
	assert.Len(t, rulesFor6000, 2, "expected both the peer rule and the CCDR proxy rule to cover port 6000")
	assert.True(t, foundProxyRule, "expected a rule admitting the ccdr-proxy component on port 6000")
}

// TestBuildNetworkPolicyForEnterprise_NoCCDRRuleWhenDisabled confirms the
// default/disabled path renders exactly the pre-existing 3 ingress rules —
// no proxy-admission rule appended.
func TestBuildNetworkPolicyForEnterprise_NoCCDRRuleWhenDisabled(t *testing.T) {
	cluster := ccdrCluster(3, false)
	cluster.Spec.NetworkPolicy = &neo4jv1beta1.NetworkPolicySpec{Enabled: true}
	np := resources.BuildNetworkPolicyForEnterprise(cluster)
	require.NotNil(t, np)
	assert.Len(t, np.Spec.Ingress, 3)
}
