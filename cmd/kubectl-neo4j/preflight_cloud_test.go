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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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

func storageClass(name string, isDefault, expand bool, created time.Time) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
		AllowVolumeExpansion: &expand,
	}
	if isDefault {
		sc.Annotations = map[string]string{"storageclass.kubernetes.io/is-default-class": "true"}
	}
	return sc
}

// An empty spec.storage.className used to report "not checked". The class is
// in fact resolvable: the DefaultStorageClass admission plugin stamps it onto
// the PVC before any scheduling happens.
func TestCheckDefaultStorageClass(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().Add(-time.Hour)

	t.Run("an expanding default is silent", func(t *testing.T) {
		c := testClient(t, storageClass("gp3", true, true, t0))
		assert.Empty(t, checkDefaultStorageClass(ctx, c))
	})

	// EKS's in-tree gp2 default has shipped with allowVolumeExpansion unset,
	// which makes spec.storage.size immutable on a cluster nobody configured
	// storage for.
	t.Run("a non-expanding default is a warning naming it", func(t *testing.T) {
		c := testClient(t, storageClass("gp2", true, false, t0))
		got := checkDefaultStorageClass(ctx, c)
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
		assert.Contains(t, got[0].subject, "gp2")
		assert.Contains(t, got[0].subject, "cluster default")
	})

	t.Run("no default at all is a problem", func(t *testing.T) {
		c := testClient(t, storageClass("gp3", false, true, t0))
		got := checkDefaultStorageClass(ctx, c)
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "no default class")
	})

	// Kubernetes uses the most recently created default. Say which one wins
	// rather than picking silently — it changes what the expansion check is
	// even about.
	t.Run("several defaults name the winner", func(t *testing.T) {
		older := storageClass("gp2", true, true, t0)
		newer := storageClass("gp3", true, false, t0.Add(time.Minute))
		c := testClient(t, older, newer)
		got := checkDefaultStorageClass(ctx, c)
		require.Len(t, got, 2, "ambiguity, then the winner's expansion: %s", marksOf(got))
		assert.Contains(t, got[0].what, "several classes are marked default")
		assert.Contains(t, got[0].action, "gp3")
		assert.Contains(t, got[1].subject, "gp3", "the newest default is the one checked")
	})
}

func zonedNode(name, zone string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if zone != "" {
		n.Labels = map[string]string{"topology.kubernetes.io/zone": zone}
	}
	return n
}

func withSpread(servers int32, whenUnsatisfiable string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{
				Servers: servers,
				Placement: &neo4jv1beta1.PlacementConfig{
					TopologySpread: &neo4jv1beta1.TopologySpreadConfig{
						Enabled:           true,
						WhenUnsatisfiable: whenUnsatisfiable,
					},
				},
			},
		},
	}
}

// A hard zone constraint the cluster's zones cannot satisfy leaves servers
// Pending forever, and nothing in the manifest hints at it.
func TestCheckZoneCapacity(t *testing.T) {
	ctx := context.Background()

	t.Run("no placement means nothing to check", func(t *testing.T) {
		cl := withSpread(3, "")
		cl.Spec.Topology.Placement = nil
		c := testClient(t, zonedNode("n1", "a"))
		assert.Empty(t, checkZoneCapacity(ctx, c, cl))
	})

	t.Run("enough zones is silent", func(t *testing.T) {
		c := testClient(t, zonedNode("n1", "a"), zonedNode("n2", "b"), zonedNode("n3", "c"))
		assert.Empty(t, checkZoneCapacity(ctx, c, withSpread(3, "")))
	})

	t.Run("too few zones for a hard constraint is a problem", func(t *testing.T) {
		c := testClient(t, zonedNode("n1", "a"), zonedNode("n2", "b"))
		got := checkZoneCapacity(ctx, c, withSpread(3, ""))
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "2 zone(s) for 3 server(s)")
		assert.Contains(t, got[0].detail, "a, b")
	})

	// A soft constraint degrades instead of blocking, which is what it is for.
	t.Run("ScheduleAnyway is left alone", func(t *testing.T) {
		c := testClient(t, zonedNode("n1", "a"))
		assert.Empty(t, checkZoneCapacity(ctx, c, withSpread(3, "ScheduleAnyway")))
	})

	// The hard case: a required constraint on a key no node carries can never
	// be satisfied, however many nodes are added.
	t.Run("unlabelled nodes cannot satisfy a zone constraint", func(t *testing.T) {
		c := testClient(t, zonedNode("n1", ""), zonedNode("n2", ""))
		got := checkZoneCapacity(ctx, c, withSpread(2, ""))
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "no Ready node carries")
	})

	t.Run("required anti-affinity counts too", func(t *testing.T) {
		cl := withSpread(3, "ScheduleAnyway")
		cl.Spec.Topology.Placement.AntiAffinity = &neo4jv1beta1.PodAntiAffinityConfig{
			Enabled: true, Type: "required",
		}
		c := testClient(t, zonedNode("n1", "a"))
		got := checkZoneCapacity(ctx, c, cl)
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "antiAffinity")
	})
}

func nsWithLevel(name, level string) *corev1.Namespace {
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if level != "" {
		n.Labels = map[string]string{podSecurityEnforceLabel: level}
	}
	return n
}

// spec.securityContext REPLACES the operator's default rather than merging, so
// setting one field to fix a permissions problem drops runAsNonRoot, the
// seccomp profile and the dropped capabilities with it.
func TestCheckPodSecurityAdmission(t *testing.T) {
	ctx := context.Background()
	fsGroupOnly := &neo4jv1beta1.SecurityContextSpec{
		PodSecurityContext: &corev1.PodSecurityContext{FSGroup: ptrInt64(1000)},
	}

	t.Run("no override means the hardened default applies", func(t *testing.T) {
		c := testClient(t, nsWithLevel("neo4j", "restricted"))
		assert.Empty(t, checkPodSecurityAdmission(ctx, c, "neo4j", nil))
	})

	t.Run("an override in an unenforced namespace is a warning", func(t *testing.T) {
		c := testClient(t, nsWithLevel("neo4j", ""))
		got := checkPodSecurityAdmission(ctx, c, "neo4j", fsGroupOnly)
		require.Len(t, got, 1)
		assert.Equal(t, markWarning, got[0].mark)
		assert.Contains(t, got[0].what, "replaces")
	})

	t.Run("an override under restricted names every missing field", func(t *testing.T) {
		c := testClient(t, nsWithLevel("neo4j", "restricted"))
		got := checkPodSecurityAdmission(ctx, c, "neo4j", fsGroupOnly)
		require.Len(t, got, 1)
		assert.Equal(t, markProblem, got[0].mark)
		assert.Contains(t, got[0].what, "runAsNonRoot: true")
		assert.Contains(t, got[0].what, "seccompProfile.type: RuntimeDefault")
		assert.Contains(t, got[0].action, "StatefulSet")
	})

	// baseline does not require seccomp or dropped capabilities, so reporting
	// them would send the user to restate fields nothing will reject.
	t.Run("baseline asks for less than restricted", func(t *testing.T) {
		c := testClient(t, nsWithLevel("neo4j", "baseline"))
		got := checkPodSecurityAdmission(ctx, c, "neo4j", fsGroupOnly)
		require.Len(t, got, 1)
		assert.NotContains(t, got[0].what, "seccompProfile")
		assert.Contains(t, got[0].what, "runAsNonRoot: true")
	})

	t.Run("an override that restates everything passes", func(t *testing.T) {
		yes, no := true, false
		c := testClient(t, nsWithLevel("neo4j", "restricted"))
		complete := &neo4jv1beta1.SecurityContextSpec{
			PodSecurityContext: &corev1.PodSecurityContext{
				FSGroup:      ptrInt64(1000),
				RunAsNonRoot: &yes,
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			ContainerSecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &no,
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		}
		assert.Empty(t, checkPodSecurityAdmission(ctx, c, "neo4j", complete))
	})
}

func ptrInt64(v int64) *int64 { return &v }
