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

// A raise waits for what is left of the servers a scale-down removed
// (operations.md id 229). The v1.24.0 journey (#619 S1, also v1.23.0, 3 ×
// 2026.08.1): 5 → 3, "Scale-down drain complete", and 2 s later back to 5.
// The StatefulSet created server-3 and server-4 again on their old volumes,
// which the garbage collector had not deleted yet; the servers started with
// the identities Neo4j had dropped, and the cluster stayed Forming ("Cluster
// formation verification incomplete") for 4 minutes and more, with
// Degraded=False and nothing saying why.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// raised is cluster "c", Ready on 3 servers as the drain left it, its spec
// raised to 5, and its reconciler with objs.
func raised(t *testing.T, objs ...client.Object) (*Neo4jEnterpriseClusterReconciler, *neo4jv1beta1.Neo4jEnterpriseCluster, *record.FakeRecorder) {
	t.Helper()
	c := availCluster(neo4jv1beta1.PhaseReady)
	c.Spec.Topology.Servers = 5
	r := gateTestReconciler(t, gateTestSTS("neo4j:2026.08.1-enterprise", 3, "rev1", "rev1", 3, 3), c)
	for _, o := range objs {
		require.NoError(t, r.Create(context.Background(), o))
	}
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec
	return r, c, rec
}

// removedPod is the pod of a removed server, terminating.
func removedPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Finalizers: []string{"test/terminating"}}}
}

// removedVolume is a removed server's data volume as whenScaled: Delete
// leaves it: owned by its pod, for the collector.
func removedVolume(ordinal string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-c-server-" + ordinal, Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "c-server-" + ordinal, UID: types.UID("pod-uid-" + ordinal)}},
	}}
}

// passOn runs the pass's topology hold on the cluster as stored, and returns
// the server count the rest of the pass works on.
func passOn(t *testing.T, r *Neo4jEnterpriseClusterReconciler) int32 {
	t.Helper()
	c := refetch(t, r)
	r.holdTopologyChange(context.Background(), c)
	return c.Spec.Topology.Servers
}

func deferral(t *testing.T, r *Neo4jEnterpriseClusterReconciler) *metav1.Condition {
	t.Helper()
	return findCondition(refetch(t, r).Status.Conditions, ConditionTypeTopologyChangeDeferred)
}

// gone deletes an object the test holds with a finalizer: the finalizer
// first, as Kubernetes would once it lets go.
func gone(t *testing.T, r *Neo4jEnterpriseClusterReconciler, obj client.Object) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(obj), obj))
	if len(obj.GetFinalizers()) > 0 {
		require.NoError(t, patchObject(ctx, r.Client, obj, func() { obj.SetFinalizers(nil) }))
	}
	_ = r.Delete(ctx, obj)
}

func TestHoldRaise_WaitsForTheRemovedServersPodsAndVolumes(t *testing.T) {
	ctx := context.Background()
	pod3 := removedPod("c-server-3")
	r, c, rec := raised(t, pod3, removedVolume("3"), removedVolume("4"))
	require.NoError(t, r.Delete(ctx, pod3)) // terminating: its finalizer keeps it

	assert.Equal(t, int32(3), passOn(t, r), "no server is added while server-3's pod and both volumes are there")
	cond := deferral(t, r)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, ConditionReasonWaitsForRemovedServers, cond.Reason)
	assert.Equal(t, "spec.topology.servers is 5; the server StatefulSet keeps 3 servers until the pods and volumes of the servers "+
		"a scale-down removed are gone (c-server-3, c-server-4): a server added on a removed server's volume would start as the "+
		"server Neo4j dropped, and could not rejoin", cond.Message)
	got := drainEvents(rec)
	require.Len(t, got, 1)
	assert.Equal(t, "Normal ScaleUpDeferred Adding servers 3-4 waits until the pods and volumes of the servers a scale-down "+
		"removed are gone: c-server-3, c-server-4", got[0])
	v, ok := r.wakes.m.LoadAndDelete(clusterKey(c))
	require.True(t, ok, "it looks again soon: nothing wakes it when a volume goes")
	assert.Equal(t, raiseHoldPoll, v)

	assert.Equal(t, int32(3), passOn(t, r))
	assert.Empty(t, drainEvents(rec), "announced once")

	// Server-3's pod and volume go: server-3 is added, server-4 waits.
	gone(t, r, pod3)
	gone(t, r, removedVolume("3"))
	assert.Equal(t, int32(4), passOn(t, r))
	assert.Contains(t, deferral(t, r).Message, "keeps 4 servers until the pods and volumes of the servers a scale-down removed are gone (c-server-4)")
	assert.Empty(t, drainEvents(rec))

	// Server-4's volume goes: the raise goes through.
	gone(t, r, removedVolume("4"))
	assert.Equal(t, int32(5), passOn(t, r))
	cond = deferral(t, r)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, ConditionReasonNoTopologyChangeWaits, cond.Reason)
}

func TestHoldRaise_ANormalScaleUpIsNotHeld(t *testing.T) {
	// Nothing at ordinals 3 and 4; and a volume nothing reclaims (made
	// ready for a new server) is not a removed server's.
	for name, objs := range map[string][]client.Object{
		"nothing left":                     nil,
		"a volume made ready for server-3": {&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-c-server-3", Namespace: "default"}}},
	} {
		t.Run(name, func(t *testing.T) {
			r, c, rec := raised(t, objs...)
			assert.Equal(t, int32(5), passOn(t, r))
			assert.Nil(t, deferral(t, r))
			assert.Empty(t, drainEvents(rec))
			_, noted := r.wakes.m.Load(clusterKey(c))
			assert.False(t, noted)
		})
	}
}

// The replicas the hold keeps below are the API server's: a cache that has
// not seen the raise yet holds nothing, or the pass would lower the replicas
// with no drain.
func TestHoldRaise_ReadsTheReplicasFromTheAPIServer(t *testing.T) {
	r, _, _ := raised(t, removedPod("c-server-3"), removedPod("c-server-4"))
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	r.APIReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		gateTestSTS("neo4j:2026.08.1-enterprise", 5, "rev1", "rev1", 5, 5),
		removedPod("c-server-3"), removedPod("c-server-4")).Build()

	assert.Equal(t, int32(5), passOn(t, r), "server-3 and server-4 run already: their pods are the raise's")
	assert.Nil(t, deferral(t, r))

	// And a cache that has not seen the drain lower the replicas yet (5)
	// reads no raise; the API server's 3 does.
	stale, _, _ := raised(t, removedVolume("3"))
	sts := gateTestSTS("neo4j:2026.08.1-enterprise", 3, "rev1", "rev1", 3, 3)
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, removedVolume("3")).Build()
	stale.APIReader = live
	cached := gateTestSTS("neo4j:2026.08.1-enterprise", 3, "rev1", "rev1", 3, 3)
	require.NoError(t, stale.Get(context.Background(), client.ObjectKeyFromObject(cached), cached))
	five := int32(5)
	cached.Spec.Replicas = &five
	require.NoError(t, stale.Update(context.Background(), cached))
	assert.Equal(t, int32(3), passOn(t, stale), "held: server-3's volume is still there")
}

func TestHoldRaise_WhatOutlastsTheGraceIsNamed(t *testing.T) {
	ctx := context.Background()
	pod4 := removedPod("c-server-4")
	vol3 := removedVolume("3")
	vol3.Finalizers = []string{"backup.example.com/protect", "kubernetes.io/pvc-protection"}
	r, _, rec := raised(t, pod4, vol3)
	require.NoError(t, r.Delete(ctx, pod4))
	require.NoError(t, r.Delete(ctx, vol3))
	now := time.Now()
	r.clock = func() time.Time { return now }

	assert.Equal(t, int32(3), passOn(t, r))
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonScaleUpDeferred), 1)

	now = now.Add(removedServersGrace + time.Second)
	assert.Equal(t, int32(3), passOn(t, r), "still held: adding the server on that volume would start the dropped identity")
	cond := deferral(t, r)
	require.NotNil(t, cond)
	assert.Equal(t, ConditionReasonWaitsForRemovedServers, cond.Reason)
	// The pod's deletionTimestamp is the end of its grace period, not when
	// it began to terminate: the message gives it as the time it is due to
	// stop by (#619 walk).
	deleting := &corev1.Pod{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(pod4), deleting))
	require.NotNil(t, deleting.DeletionTimestamp)
	assert.NotContains(t, cond.Message, "terminating since")
	for _, want := range []string{
		"what is left of the servers a scale-down removed has not gone in 2m0s",
		"volume data-c-server-3 is being deleted, held by backup.example.com/protect",
		"pod c-server-4 is being deleted, due to stop by " + deleting.DeletionTimestamp.UTC().Format(time.RFC3339),
		"release data-c-server-3 from backup.example.com/protect",
		"kubectl delete pod -n default c-server-4 --grace-period=0 --force, if its node is gone",
		"Or set spec.topology.servers back to 3",
	} {
		assert.Contains(t, cond.Message, want)
	}
	blocked := eventsWithReason(drainEvents(rec), EventReasonScaleUpBlocked)
	require.Len(t, blocked, 1)
	assert.True(t, strings.HasPrefix(blocked[0], "Warning ScaleUpBlocked spec.topology.servers is 5"), blocked[0])

	now = now.Add(time.Minute)
	passOn(t, r)
	assert.Empty(t, drainEvents(rec), "announced once")
}

// A cluster already stuck — raised before the hold, or on a volume no pod
// owned — says so, and how out, once.
func TestReconcileServerShortfall_DroppedServersInKeptPodsAreNamed(t *testing.T) {
	ctx := context.Background()
	start := availCluster(neo4jv1beta1.PhaseForming, cond(ConditionTypeClusterFormed, metav1.ConditionTrue, ConditionReasonFormationComplete))
	start.Spec.Topology.Servers = 5
	start.Status.Message = "Cluster formation verification incomplete"
	r, rec := shortfallReconciler(t, start)
	for i := range 5 {
		pod := serverPod("c-server-"+string(rune('0'+i)), "u"+string(rune('0'+i)), true, 0)
		require.NoError(t, r.Create(ctx, &pod))
	}
	servers := append(availServers(3, 3),
		availServer("c-server-3", "Dropped", "Available"), availServer("c-server-4", "Dropped", "Available"))
	const want = "c-server-3, c-server-4 run servers Neo4j has dropped (id-c-server-3, id-c-server-4): they started on the " +
		"volumes of servers a scale-down removed, before those were deleted, and a dropped server cannot rejoin. Set " +
		"spec.topology.servers to 3; once ServersPendingDrain reads \"Scale-down drain complete\", raise it again: the " +
		"operator adds the servers once the removed ones' pods and volumes are gone, with new identities"

	for range 3 {
		r.reconcileServerShortfall(ctx, refetch(t, r), formationCheck{message: "Cluster formation verification incomplete", servers: servers},
			"Cluster formation verification incomplete")
	}
	c := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseForming, c.Status.Phase)
	assert.Equal(t, want, c.Status.Message)
	ready := findCondition(c.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, want, ready.Message, "the Ready condition says it too")
	got := eventsWithReason(drainEvents(rec), EventReasonDroppedServersReturned)
	require.Len(t, got, 1, "announced once")
	assert.Equal(t, "Warning DroppedServersReturned "+want, got[0])
}

func TestDroppedServersInKeptPods(t *testing.T) {
	noAddress := func(id string) neo4jclient.ServerInfo {
		return neo4jclient.ServerInfo{Name: id, State: "Dropped", Health: "Available"}
	}
	cases := []struct {
		name     string
		servers  []neo4jclient.ServerInfo
		notReady []string
		expected int
		want     []droppedServerInPod
	}{
		{"named by their addresses", append(availServers(3, 3), availServer("c-server-3", "Dropped", "Available")), nil, 4,
			[]droppedServerInPod{{pod: "c-server-3", id: "id-c-server-3"}}},
		{"a new server still starting while the old row lingers", append(availServers(3, 3), availServer("c-server-3", "Dropped", "Available")),
			[]string{"c-server-3 (not ready)"}, 4, nil},
		{"a scale-down's, terminating above the spec", append(availServers(3, 3),
			availServer("c-server-3", "Dropped", "Available"), availServer("c-server-4", "Dropped", "Available")), nil, 3, nil},
		{"an old identity of a pod whose new server is a member", append(availServers(4, 4),
			neo4jclient.ServerInfo{Name: "old-3", Address: "c-server-3.c-headless.default.svc.cluster.local:7687", State: "Dropped"}), nil, 4, nil},
		{"no address known: the ready pods whose server is no member", append(availServers(3, 3), noAddress("u-a"), noAddress("u-b")), nil, 5,
			[]droppedServerInPod{{pod: "c-server-3", id: "u-a, u-b"}, {pod: "c-server-4"}}},
		{"no address known, the pods not ready: a scale-up starting", append(availServers(3, 3), noAddress("u-a")),
			[]string{"c-server-3 (not ready)", "c-server-4 (missing)"}, 5, nil},
		{"nothing dropped", availServers(3, 3), nil, 5, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, droppedServersInKeptPods(tc.servers, tc.notReady, "c", tc.expected))
		})
	}
}
