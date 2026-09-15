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

package main

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// cloudNode is a node identified only by its providerID — the signal
// detectCloudProvider reads. preflight_test.go's `node` helper builds nodes for
// the capacity check and carries no providerID.
func cloudNode(providerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
	}
}

func ccdrCluster(internal *bool) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationSpec{
				Enabled:              true,
				LoadBalancerInternal: internal,
			},
		},
	}
}

func proxySvc(ingress corev1.LoadBalancerIngress) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: resources.CCDRProxyName("prod"), Namespace: "neo4j",
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{ingress},
			},
		},
	}
}

func marksOf(s []symptom) string {
	var b strings.Builder
	for _, x := range s {
		b.WriteString(x.mark + " " + x.what + "\n")
	}
	return b.String()
}

func TestDetectCloudProvider(t *testing.T) {
	tests := []struct{ providerID, want string }{
		{"aws:///us-east-1a/i-0abc", "aws"},
		{"gce://my-proj/us-central1-a/gke-node", "gce"},
		{"azure:///subscriptions/x/vm", "azure"},
		{"kind://docker/neo4j-operator-dev/control-plane", "kind"},
		{"digitalocean://123", "digitalocean"},
		// Unrecognised still answers the question that matters — it is not one
		// of the three the operator's annotations cover.
		{"acme://whatever", "acme"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.providerID, func(t *testing.T) {
			c := testClient(t, cloudNode(tc.providerID))
			assert.Equal(t, tc.want, detectCloudProvider(context.Background(), c).id)
		})
	}
}

// Every annotation the OPERATOR emits must have an owner here, or the coverage
// check silently believes a provider is handled when it is not. This is the
// guard that keeps the two lists in step.
func TestInternalLBAnnotationOwners_CoversEveryOperatorAnnotation(t *testing.T) {
	covered, unknown := providersCoveredByInternalLBAnnotations()
	assert.Empty(t, unknown,
		"the operator emits annotation(s) with no owner in internalLBAnnotationOwners; add them")
	for _, want := range []string{"aws", "azure", "gce"} {
		assert.True(t, covered[want], "the operator should cover %s", want)
	}
	require.NotEmpty(t, resources.CCDRInternalLoadBalancerAnnotations())
}

func TestCCDRProxyExposure(t *testing.T) {
	yes, no := true, false

	t.Run("no CCDR means nothing to say", func(t *testing.T) {
		cluster := ccdrCluster(&yes)
		cluster.Spec.CrossClusterReplication = nil
		c := testClient(t, cloudNode("gce://p/z/n"))
		assert.Empty(t, checkCCDRProxyExposure(context.Background(), c, "neo4j", cluster))
	})

	t.Run("a covered cloud is silent", func(t *testing.T) {
		for _, pid := range []string{"aws:///z/i-1", "gce://p/z/n", "azure:///s/vm"} {
			c := testClient(t, cloudNode(pid))
			got := checkCCDRProxyExposure(context.Background(), c, "neo4j", ccdrCluster(&yes))
			assert.Empty(t, got, "%s is covered; a passing check says nothing: %s", pid, marksOf(got))
		}
	})

	// The defect class this exists for: the request is accepted, reported, and
	// silently does nothing because this cloud reads none of those keys.
	t.Run("an uncovered cloud is a problem, not a warning", func(t *testing.T) {
		c := testClient(t, cloudNode("openstack://abc"))
		got := checkCCDRProxyExposure(context.Background(), c, "neo4j", ccdrCluster(&yes))
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "OpenStack")
		assert.Contains(t, got[0].action, "PUBLIC")
	})

	t.Run("an unknown provider warns rather than asserting either way", func(t *testing.T) {
		c := testClient(t, cloudNode(""))
		got := checkCCDRProxyExposure(context.Background(), c, "neo4j", ccdrCluster(&yes))
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
	})

	t.Run("kind is a dev limitation, not an exposure", func(t *testing.T) {
		c := testClient(t, cloudNode("kind://docker/dev/cp"))
		got := checkCCDRProxyExposure(context.Background(), c, "neo4j", ccdrCluster(&yes))
		require.Len(t, got, 1)
		assert.Equal(t, markWaiting, got[0].mark)
		assert.NotEqual(t, markProblem, got[0].mark)
	})

	t.Run("opting out is said once, as a warning", func(t *testing.T) {
		c := testClient(t, cloudNode("gce://p/z/n"))
		got := checkCCDRProxyExposure(context.Background(), c, "neo4j", ccdrCluster(&no))
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
		assert.Contains(t, got[0].what, "public")
	})
}

// The address the provider actually assigned settles what the annotation table
// only predicts — a public IP on a Service that asked to be internal is proof,
// not inference.
func TestCCDRProxyAssignedAddress(t *testing.T) {
	yes := true
	ctx := context.Background()

	t.Run("a public IP on a covered cloud is still a problem", func(t *testing.T) {
		c := testClient(t, cloudNode("gce://p/z/n"), proxySvc(corev1.LoadBalancerIngress{IP: "34.120.5.9"}))
		got := checkCCDRProxyExposure(ctx, c, "neo4j", ccdrCluster(&yes))
		require.Len(t, got, 1, "coverage passes, the observed address does not: %s", marksOf(got))
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "34.120.5.9")
	})

	t.Run("a private IP is what was asked for", func(t *testing.T) {
		for _, ip := range []string{"10.1.2.3", "172.16.4.5", "192.168.9.9", "100.64.1.1"} {
			c := testClient(t, cloudNode("gce://p/z/n"), proxySvc(corev1.LoadBalancerIngress{IP: ip}))
			got := checkCCDRProxyExposure(ctx, c, "neo4j", ccdrCluster(&yes))
			assert.Empty(t, got, "%s is private: %s", ip, marksOf(got))
		}
	})

	t.Run("a hostname is reported as unresolved, not assumed safe", func(t *testing.T) {
		c := testClient(t, cloudNode("aws:///z/i-1"),
			proxySvc(corev1.LoadBalancerIngress{Hostname: "abc.elb.amazonaws.com"}))
		got := checkCCDRProxyExposure(ctx, c, "neo4j", ccdrCluster(&yes))
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
		assert.Contains(t, got[0].action, "dig +short")
	})

	t.Run("no Service yet says nothing", func(t *testing.T) {
		c := testClient(t, cloudNode("gce://p/z/n"))
		assert.Empty(t, checkCCDRProxyExposure(ctx, c, "neo4j", ccdrCluster(&yes)))
	})
}

func TestIsPrivateIP(t *testing.T) {
	private := []string{"10.0.0.1", "172.31.255.254", "192.168.0.1", "127.0.0.1",
		"169.254.1.1", "100.64.0.1", "100.127.255.255", "fd00::1", "::1"}
	public := []string{"34.120.5.9", "8.8.8.8", "100.128.0.1", "100.63.255.255", "2606:4700::1111"}

	for _, s := range private {
		assert.True(t, isPrivateIP(net.ParseIP(s)), "%s should be private", s)
	}
	for _, s := range public {
		assert.False(t, isPrivateIP(net.ParseIP(s)), "%s should be public", s)
	}
}
