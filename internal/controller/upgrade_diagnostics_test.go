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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func formedCluster(name string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := clusterForUpgrade(name, "default", "5.26.30-enterprise", "5.26.31-enterprise", 3)
	c.Status.Conditions = []metav1.Condition{{Type: ConditionTypeClusterFormed, Status: metav1.ConditionTrue,
		Reason: "Formed", LastTransitionTime: metav1.Now()}}
	return c
}

// Diagnostics during an upgrade: at most once per ready-poll interval per
// cluster, only once the cluster has formed, never with monitoring disabled.
func TestCollectDiagnosticsDuringUpgrade_Gating(t *testing.T) {
	calls := map[string]int{}
	r := &Neo4jEnterpriseClusterReconciler{ReadyPollInterval: time.Hour, UpgradeGates: &upgradeGates{
		diagnostics: func(_ context.Context, c *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			calls[c.Name]++
			return nil
		},
	}}
	ctx := context.Background()

	a := formedCluster("a")
	r.collectDiagnosticsDuringUpgrade(ctx, a)
	r.collectDiagnosticsDuringUpgrade(ctx, a)
	assert.Equal(t, 1, calls["a"], "once per interval")

	r.collectDiagnosticsDuringUpgrade(ctx, formedCluster("b"))
	assert.Equal(t, 1, calls["b"], "each cluster has its own interval")

	off := formedCluster("off")
	off.Spec.Monitoring = &neo4jv1beta1.MonitoringSpec{Enabled: false}
	r.collectDiagnosticsDuringUpgrade(ctx, off)
	assert.Zero(t, calls["off"], "monitoring disabled")

	unformed := clusterForUpgrade("unformed", "default", "5.26.30-enterprise", "5.26.31-enterprise", 3)
	r.collectDiagnosticsDuringUpgrade(ctx, unformed)
	assert.Zero(t, calls["unformed"], "nothing to report before formation")

	r.ReadyPollInterval = time.Nanosecond
	time.Sleep(time.Millisecond)
	r.collectDiagnosticsDuringUpgrade(ctx, a)
	assert.Equal(t, 2, calls["a"], "again once the interval has passed")
}

// A pass during an upgrade runs the upgrade step and then collects
// diagnostics; it used to return straight after the step.
func TestReconcile_CollectsDiagnosticsDuringAnUpgrade(t *testing.T) {
	scheme := makeUpgradeScheme()
	const replicas int32 = 3
	cluster := formedCluster("diag")
	cluster.Spec.Storage.ClassName = ""
	cluster.Finalizers = []string{ClusterFinalizer}
	now := metav1.Now()
	cluster.Status.Phase = neo4jv1beta1.PhaseUpgrading
	cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: upgradePhaseRolling, TargetVersion: "5.26.31-enterprise",
		StartTime: &now, StepStartTime: &now, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: replicas, InProgress: 1, Pending: 2},
	}
	sts := serverSTSForUpgrade("diag", "default", "neo4j:5.26.31-enterprise", replicas)
	sts.Generation = 2
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(2))
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: replicas, UpdatedReplicas: 1}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts).WithStatusSubresource(cluster).Build()

	diagnosed := 0
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: scheme, Recorder: record.NewFakeRecorder(20), UpgradeGates: &upgradeGates{
		serverAvailable: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, string, error) {
			return true, "", nil
		},
		databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
			return false, []string{"d01"}, nil
		},
		diagnostics: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) error { diagnosed++; return nil },
	}}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "diag", Namespace: "default"}})
	require.NoError(t, err)
	assert.Equal(t, 1, diagnosed, "diagnostics ran during the upgrade")
	got := &appsv1.StatefulSet{}
	require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Name: "diag-server", Namespace: "default"}, got))
	assert.Equal(t, int32(2), *got.Spec.UpdateStrategy.RollingUpdate.Partition, "the upgrade step ran and held the walk")
}
