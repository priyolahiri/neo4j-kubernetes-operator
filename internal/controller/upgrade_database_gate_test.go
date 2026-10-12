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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func TestAddressIsPod(t *testing.T) {
	for addr, want := range map[string]bool{
		"c-server-1.c-headless.default.svc.cluster.local:7687": true,
		"c-server-1:7687": true,
		"c-server-1":      true,
		"c-server-10.c-headless.default.svc.cluster.local:7687": false,
		"c-server-0.c-headless.default.svc.cluster.local:7687":  false,
		"": false,
	} {
		assert.Equal(t, want, addressIsPod(addr, "c-server-1"), addr)
	}
}

func own(db, status string, ok bool) neo4jclient.ClusterStatusCheckRow {
	return neo4jclient.ClusterStatusCheckRow{Database: db, Requester: true, ReplicationSuccessful: ok, MemberStatus: status}
}

func alloc(db, pod, current string) neo4jclient.DatabaseAllocation {
	return neo4jclient.DatabaseAllocation{Name: db, Address: pod + ".c-headless.default.svc.cluster.local:7687", CurrentStatus: current, RequestedStatus: "online"}
}

// evaluateDatabaseGate is the #488 decision: every copy on the restarted
// server replicating and applying (statusCheck, its own rows), and every
// database on it at its requested status (SHOW DATABASES, which covers
// single-primary databases and secondaries statusCheck does not).
func TestEvaluateDatabaseGate(t *testing.T) {
	const pod = "c-server-1"
	caughtUp := []neo4jclient.ClusterStatusCheckRow{own("system", "APPLYING", true), own("d01", "APPLYING", true)}
	online := []neo4jclient.DatabaseAllocation{alloc("system", pod, "online"), alloc("d01", pod, "online"), alloc("single", pod, "online")}

	ok, notReady := evaluateDatabaseGate(pod, caughtUp, online)
	assert.True(t, ok)
	assert.Empty(t, notReady)

	for name, tc := range map[string]struct {
		checks []neo4jclient.ClusterStatusCheckRow
		allocs []neo4jclient.DatabaseAllocation
		want   []string
	}{
		"replicating but not applying": {
			[]neo4jclient.ClusterStatusCheckRow{own("system", "APPLYING", true), own("d01", "REPLICATING", true)}, online, []string{"d01"}},
		"unavailable": {
			[]neo4jclient.ClusterStatusCheckRow{own("system", "APPLYING", true), own("d01", "UNAVAILABLE", false)}, online, []string{"d01"}},
		"replication failed": {
			[]neo4jclient.ClusterStatusCheckRow{own("system", "APPLYING", true), own("d01", "APPLYING", false)}, online, []string{"d01"}},
		"row carries an error": {
			[]neo4jclient.ClusterStatusCheckRow{own("system", "APPLYING", true), {Database: "d01", Requester: true, ReplicationSuccessful: true, MemberStatus: "APPLYING", Error: "timeout"}}, online, []string{"d01"}},
		"a single-primary database still starting": {
			caughtUp, []neo4jclient.DatabaseAllocation{alloc("system", pod, "online"), alloc("d01", pod, "online"), alloc("single", pod, "starting")}, []string{"single"}},
		"several, sorted and once each": {
			[]neo4jclient.ClusterStatusCheckRow{own("d02", "UNAVAILABLE", false), own("d01", "REPLICATING", true)},
			[]neo4jclient.DatabaseAllocation{alloc("d02", pod, "starting"), alloc("d01", pod, "online")}, []string{"d01", "d02"}},
	} {
		t.Run(name, func(t *testing.T) {
			ok, notReady := evaluateDatabaseGate(pod, tc.checks, tc.allocs)
			assert.False(t, ok)
			assert.Equal(t, tc.want, notReady)
		})
	}

	t.Run("other servers' rows and copies do not hold the gate", func(t *testing.T) {
		checks := append([]neo4jclient.ClusterStatusCheckRow{{Database: "d01", Requester: false, MemberStatus: "UNAVAILABLE"}}, caughtUp...)
		allocs := append([]neo4jclient.DatabaseAllocation{alloc("d01", "c-server-0", "starting"), alloc("d01", "c-server-10", "starting")}, online...)
		ok, _ := evaluateDatabaseGate(pod, checks, allocs)
		assert.True(t, ok, "only c-server-1's own copies decide; c-server-10 is a different server")
	})
}

func TestDatabaseGateMessage(t *testing.T) {
	assert.Equal(t, "Rolling server ordinal 1: waiting for its databases to catch up (d01, d02)", databaseGateMessage(1, []string{"d01", "d02"}))
	assert.Contains(t, databaseGateMessage(0, []string{"a", "b", "c", "d", "e", "f", "g"}), "(a, b, c, d, e and 2 more)")
}

// The real Rolling step, with the network replaced: server-2 has rolled and
// is Available, but a database on it is still catching up — the partition
// must stay, and the status must say why; once caught up, it lowers.
func TestStepUpgradeRolling_WaitsForTheRolledServersDatabases(t *testing.T) {
	scheme := makeUpgradeScheme()
	const replicas int32 = 3
	cluster := clusterForUpgrade("gate", "default", "5.26.30-enterprise", "5.26.31-enterprise", replicas)
	now := metav1.Now()
	cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: upgradePhaseRolling, TargetVersion: "5.26.31-enterprise",
		StartTime: &now, StepStartTime: &now, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: replicas, InProgress: 1, Pending: 2},
	}
	sts := serverSTSForUpgrade("gate", "default", "neo4j:5.26.31-enterprise", replicas)
	sts.Generation = 2
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(2))
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: replicas, UpdatedReplicas: 1}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts).WithStatusSubresource(cluster).Build()

	var asked []string
	notReady := []string{"d02"}
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10), UpgradeGates: &upgradeGates{
		serverAvailable: func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (bool, error) {
			return true, nil
		},
		databasesCaughtUp: func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (bool, []string, error) {
			asked = append(asked, pod)
			return len(notReady) == 0, notReady, nil
		},
	}}
	partition := func() int32 {
		got := &appsv1.StatefulSet{}
		require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Name: "gate-server", Namespace: "default"}, got))
		return *got.Spec.UpdateStrategy.RollingUpdate.Partition
	}
	latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Name: "gate", Namespace: "default"}, c))
		return c
	}

	_, err := r.stepUpgradeRolling(context.Background(), latest())
	require.NoError(t, err)
	assert.Equal(t, []string{"gate-server-2"}, asked, "the gate is read on the server that just rolled")
	assert.Equal(t, int32(2), partition(), "a database still catching up holds the walk")
	assert.True(t, strings.Contains(latest().Status.UpgradeStatus.Message, "waiting for its databases to catch up (d02)"), latest().Status.UpgradeStatus.Message)

	notReady = nil
	_, err = r.stepUpgradeRolling(context.Background(), latest())
	require.NoError(t, err)
	assert.Equal(t, int32(1), partition(), "caught up: the next server rolls")
}

// A database that never catches up fails the upgrade at upgradeTimeout, and
// the failure names it rather than blaming the server's rejoin.
func TestStepUpgradeRolling_TimeoutNamesTheLaggingDatabases(t *testing.T) {
	scheme := makeUpgradeScheme()
	const replicas int32 = 3
	cluster := clusterForUpgrade("gate", "default", "5.26.30-enterprise", "5.26.31-enterprise", replicas)
	longAgo := metav1.NewTime(time.Now().Add(-24 * time.Hour))
	cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: upgradePhaseRolling, TargetVersion: "5.26.31-enterprise",
		StartTime: &longAgo, StepStartTime: &longAgo, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: replicas, InProgress: 1, Pending: 2},
	}
	sts := serverSTSForUpgrade("gate", "default", "neo4j:5.26.31-enterprise", replicas)
	sts.Generation = 2
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(2))
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: replicas, UpdatedReplicas: 1}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts).WithStatusSubresource(cluster).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10), UpgradeGates: &upgradeGates{
		serverAvailable: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, error) { return true, nil },
		databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
			return false, []string{"d07"}, nil
		},
	}}

	_, _ = r.stepUpgradeRolling(context.Background(), cluster)
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Name: "gate", Namespace: "default"}, got))
	assert.Equal(t, upgradePhaseFailed, got.Status.UpgradeStatus.Phase)
	assert.Contains(t, got.Status.UpgradeStatus.LastError, "databases on server ordinal 2 to catch up (d07)")
}
