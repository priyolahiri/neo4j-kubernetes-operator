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
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func TestReadyPollInterval(t *testing.T) {
	if got := readyPollInterval(0, 30*time.Second); got != 30*time.Second {
		t.Errorf("unset: got %s, want RequeueAfter (30s)", got)
	}
	if got := readyPollInterval(2*time.Minute, 30*time.Second); got != 2*time.Minute {
		t.Errorf("configured: got %s, want 2m", got)
	}
}

func serverPod(name, uid string, ready bool, restarts int32) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid),
			Labels: map[string]string{"neo4j.com/cluster": "c", "neo4j.com/clustering": "true"}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "neo4j", RestartCount: restarts}},
		},
	}
}

// TestServerPodsFingerprint: the fingerprint moves on exactly the pod changes
// that can bring a server back on its own — recreation, a container restart,
// a readiness change — and not on list order.
func TestServerPodsFingerprint(t *testing.T) {
	base := []corev1.Pod{serverPod("c-server-0", "a", true, 0), serverPod("c-server-1", "b", true, 0)}
	fp := serverPodsFingerprint(base)
	if fp == "" {
		t.Fatal("fingerprint of running pods must not be empty")
	}
	if got := serverPodsFingerprint([]corev1.Pod{base[1], base[0]}); got != fp {
		t.Error("list order must not change the fingerprint")
	}
	terminating := serverPod("c-server-0", "a", true, 0)
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	for name, pods := range map[string][]corev1.Pod{
		"recreated":         {serverPod("c-server-0", "a2", true, 0), base[1]},
		"container restart": {serverPod("c-server-0", "a", true, 1), base[1]},
		"turned un-ready":   {serverPod("c-server-0", "a", false, 0), base[1]},
		"pod gone":          {base[1]},
		"being deleted":     {terminating, base[1]},
	} {
		if serverPodsFingerprint(pods) == fp {
			t.Errorf("%s: fingerprint must change", name)
		}
	}
}

func availableServers(n int) []neo4jclient.ServerInfo {
	servers := make([]neo4jclient.ServerInfo, n)
	for i := range servers {
		servers[i] = neo4jclient.ServerInfo{State: "Enabled", Health: "Available"}
	}
	return servers
}

func TestSplitBrainCheckSkippable(t *testing.T) {
	now := time.Now()
	ready := minimalCluster("c", "default")
	ready.Spec.Topology.Servers = 3
	ready.Status.Phase = neo4jv1beta1.PhaseReady
	recent := &splitBrainCheck{at: now.Add(-time.Minute), pods: "p"}

	if !splitBrainCheckSkippable(ready, availableServers(3), recent, "p", 5*time.Minute, now) {
		t.Fatal("a Ready, whole cluster with unchanged pods and a recent clean check must skip")
	}
	// A cordoned server is a member that keeps its databases, and the full
	// check counts it so (operations.md id 224): it is no shortfall, so a
	// cluster with one cordoned by hand is not checked in full on every pass.
	cordoned := availableServers(3)
	cordoned[1].State = "Cordoned"
	if !splitBrainCheckSkippable(ready, cordoned, recent, "p", 5*time.Minute, now) {
		t.Fatal("a cordoned server is a member: the cluster is whole")
	}

	degraded := ready.DeepCopy()
	degraded.Status.Phase = neo4jv1beta1.PhaseDegraded
	forming := ready.DeepCopy()
	forming.Status.Phase = neo4jv1beta1.PhaseForming
	short := availableServers(3)
	short[2].Health = "Unavailable"
	deallocating := availableServers(3)
	deallocating[1].State = "Deallocating"

	for _, tc := range []struct {
		name     string
		cluster  *neo4jv1beta1.Neo4jEnterpriseCluster
		servers  []neo4jclient.ServerInfo
		last     *splitBrainCheck
		pods     string
		interval time.Duration
	}{
		{"interval 0 checks every pass", ready, availableServers(3), recent, "p", 0},
		{"never checked", ready, availableServers(3), nil, "p", 5 * time.Minute},
		{"pods unknown", ready, availableServers(3), recent, "", 5 * time.Minute},
		{"Degraded", degraded, availableServers(3), recent, "p", 5 * time.Minute},
		{"Forming", forming, availableServers(3), recent, "p", 5 * time.Minute},
		{"a server unavailable", ready, short, recent, "p", 5 * time.Minute},
		{"a server deallocating", ready, deallocating, recent, "p", 5 * time.Minute},
		{"scaled up past the servers listed", ready, availableServers(2), recent, "p", 5 * time.Minute},
		{"pods changed", ready, availableServers(3), recent, "q", 5 * time.Minute},
		{"interval elapsed", ready, availableServers(3), &splitBrainCheck{at: now.Add(-5 * time.Minute), pods: "p"}, "p", 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if splitBrainCheckSkippable(tc.cluster, tc.servers, tc.last, tc.pods, tc.interval, now) {
				t.Error("must run the full check")
			}
		})
	}
}

// TestSkipSplitBrainCheck_TracksTheLastCleanCheck drives the reconciler's
// record: nothing recorded checks; a recorded check against the same pods
// skips; a pod restart since then, or a forgotten record, checks again.
func TestSkipSplitBrainCheck_TracksTheLastCleanCheck(t *testing.T) {
	ctx := context.Background()
	cluster := minimalCluster("c", "default")
	cluster.Spec.Topology.Servers = 2
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	p0, p1 := serverPod("c-server-0", "a", true, 0), serverPod("c-server-1", "b", true, 0)
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(&p0, &p1).Build()
	detector := NewSplitBrainDetector(fc)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, SplitBrainCheckInterval: 5 * time.Minute}

	skip, fp := r.skipSplitBrainCheck(ctx, cluster, detector, availableServers(2))
	if skip {
		t.Fatal("no clean check recorded yet: must check")
	}
	if fp == "" {
		t.Fatal("the fingerprint to record must not be empty")
	}
	forming := cluster.DeepCopy()
	forming.Status.Phase = neo4jv1beta1.PhaseForming
	r.recordCleanSplitBrainCheck(forming, fp, time.Now())
	r.recordCleanSplitBrainCheck(cluster, "", time.Now())
	if _, ok := r.splitBrainChecks.Load(clusterKey(cluster)); ok {
		t.Fatal("a check on a cluster that is not Ready, or without pods, must not be recorded")
	}
	r.recordCleanSplitBrainCheck(cluster, fp, time.Now())
	if skip, _ := r.skipSplitBrainCheck(ctx, cluster, detector, availableServers(2)); !skip {
		t.Error("clean check against the same pods within the interval: must skip")
	}

	p1.Status.ContainerStatuses[0].RestartCount = 1
	if err := fc.Status().Update(ctx, &p1); err != nil {
		t.Fatal(err)
	}
	if skip, _ := r.skipSplitBrainCheck(ctx, cluster, detector, availableServers(2)); skip {
		t.Error("a server restarted since the last check: must check")
	}

	r.SplitBrainCheckInterval = 0
	if skip, fp := r.skipSplitBrainCheck(ctx, cluster, detector, availableServers(2)); skip || fp != "" {
		t.Error("interval 0: must check on every pass and record nothing")
	}
}

// Clusters are reconciled MaxConcurrentReconciles at a time (at least one),
// each pass bounded by ReconcileTimeout (zero: none).
func TestClusterControllerOptions(t *testing.T) {
	opts := (&Neo4jEnterpriseClusterReconciler{}).controllerOptions()
	if opts.MaxConcurrentReconciles != 1 || opts.ReconciliationTimeout != 0 {
		t.Errorf("zero values: got %d workers, timeout %s; want 1 and none", opts.MaxConcurrentReconciles, opts.ReconciliationTimeout)
	}
	if opts.RateLimiter == nil {
		t.Error("the rate limiter must be kept")
	}
	opts = (&Neo4jEnterpriseClusterReconciler{
		MaxConcurrentReconciles: DefaultClusterMaxConcurrentReconciles,
		ReconcileTimeout:        DefaultClusterReconcileTimeout,
	}).controllerOptions()
	if opts.MaxConcurrentReconciles != 4 || opts.ReconciliationTimeout != 10*time.Minute {
		t.Errorf("defaults: got %d workers, timeout %s; want 4 and 10m", opts.MaxConcurrentReconciles, opts.ReconciliationTimeout)
	}
}

// A cluster the Ignore seam selects is left alone: no finalizer, no
// StatefulSet. The envtest suite uses it for stand-in clusters.
func TestReconcileLeavesIgnoredClustersAlone(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = neo4jv1beta1.AddToScheme(scheme)
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme,
		Ignore: func(*neo4jv1beta1.Neo4jEnterpriseCluster) bool { return true }}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c", Namespace: "ns"}})
	if err != nil || res != (ctrl.Result{}) {
		t.Fatalf("got %v, %v; want an empty result and no error", res, err)
	}
	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c", Namespace: "ns"}, latest)
	if len(latest.Finalizers) != 0 || latest.Status.Phase != "" {
		t.Errorf("an ignored cluster was touched: finalizers %v, phase %q", latest.Finalizers, latest.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-server", Namespace: "ns"}, &appsv1.StatefulSet{}); err == nil {
		t.Error("an ignored cluster got a StatefulSet")
	}
}

// When many clusters fail at once (an API-server blip), each one's first
// retry comes from its own backoff (5s), not from a bucket shared by every
// cluster: the old one-every-6-seconds bucket (bursts of 10) held the 20th
// for a minute and the 30th for two.
func TestClusterRetriesAreNotThrottledAcrossClusters(t *testing.T) {
	limiter := (&Neo4jEnterpriseClusterReconciler{}).controllerOptions().RateLimiter
	for i := 0; i < 50; i++ {
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: fmt.Sprintf("c%d", i)}}
		if d := limiter.When(req); d > 5*time.Second {
			t.Fatalf("cluster %d's first retry waits %s; want at most its own 5s backoff", i, d)
		}
	}
}
