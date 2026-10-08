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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

	degraded := ready.DeepCopy()
	degraded.Status.Phase = neo4jv1beta1.PhaseDegraded
	forming := ready.DeepCopy()
	forming.Status.Phase = neo4jv1beta1.PhaseForming
	short := availableServers(3)
	short[2].Health = "Unavailable"
	cordoned := availableServers(3)
	cordoned[1].State = "Cordoned"

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
		{"a server cordoned", ready, cordoned, recent, "p", 5 * time.Minute},
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
