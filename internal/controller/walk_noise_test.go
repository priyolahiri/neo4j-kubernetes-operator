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

// The #562 re-walk: the pass after an expansion's orphan delete still read the
// deleted StatefulSet and created it again only about 10s later; a stopped
// upgrade was logged as an ERROR; every cluster deletion logged an ERROR and a
// "Reconciler error" for a finalizer released on an object already gone.

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func expandingCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			Storage:  neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "20Gi"},
		},
	}
}

// The pass that deleted the StatefulSet creates it again: the expansion
// reports no requeue once the delete has gone through, and the pass goes on to
// the StatefulSet. While the cache still holds it, a short requeue.
func TestClusterStorageExpansion_TheSamePassCreatesTheStatefulSetAgain(t *testing.T) {
	ctx := context.Background()
	scheme := newStorageTestScheme()
	objs := func() []client.Object {
		return []client.Object{expandingCluster(), makeStorageClass("standard", true), makeStatefulSet("ec-server", "default"),
			makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true)}
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs()...).WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	requeue, err := r.reconcileStorageExpansion(ctx, expandingCluster())
	require.NoError(t, err)
	assert.False(t, requeue, "the delete has gone through: this pass creates the StatefulSet again")
	assert.NotNil(t, r.deletedServerStatefulSets.remembered("default", "ec-server", ""))

	// The cache has not seen the delete yet: requeue shortly.
	stale := makeStatefulSet("ec-server", "default")
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs()...).WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if sts, ok := obj.(*appsv1.StatefulSet); ok && key.Name == "ec-server" {
				stale.DeepCopyInto(sts)
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		}}).Build()
	uncached := fake.NewClientBuilder().WithScheme(scheme).Build()
	r = &Neo4jEnterpriseClusterReconciler{Client: cached, APIReader: uncached, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	requeue, err = r.reconcileStorageExpansion(short, expandingCluster())
	require.NoError(t, err)
	assert.True(t, requeue)
}

// A stopped upgrade is a state in status and a Warning event, not an ERROR.
func TestFailUpgrade_AStoppedUpgradeIsNotAnError(t *testing.T) {
	for _, autoPause := range []bool{true, false} {
		sink := newLevelCaptureSink()
		ctx := log.IntoContext(context.Background(), logr.New(sink))
		cluster := clusterForUpgrade("s", "default", holdRunning, holdTarget, 3)
		cluster.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: autoPause}
		now := metav1.Now()
		cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseRolling, StartTime: &now}
		fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
		rec := record.NewFakeRecorder(10)
		r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}

		_, err := r.failUpgrade(ctx, cluster, assert.AnError)
		require.NoError(t, err, "a reconcile error is an ERROR line too")
		assert.Empty(t, sink.errors(), "autoPauseOnFailure %v", autoPause)
		assert.Len(t, sink.mentioning(0, "Rolling upgrade"), 1, "said once, at info")
		assert.Len(t, rec.Events, 1, "and announced")
	}
}

// A deletion that releases the finalizer of an object already gone — a pass
// that read it before an earlier one released it — is not an error.
func TestHandleDeletion_AnObjectAlreadyGoneIsNotAnError(t *testing.T) {
	now := metav1.Now()
	sink := newLevelCaptureSink()
	ctx := log.IntoContext(context.Background(), logr.New(sink))

	cluster := minimalCluster("gone", "default")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.DeletionTimestamp = &now
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).Build() // the API server has it no more
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: newTestScheme()}
	_, err := r.handleDeletion(ctx, cluster)
	require.NoError(t, err)

	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{
		Name: "gone", Namespace: "default", Finalizers: []string{StandaloneFinalizer}, DeletionTimestamp: &now}}
	rs := &Neo4jEnterpriseStandaloneReconciler{Client: fake.NewClientBuilder().WithScheme(newTestScheme()).Build(), Scheme: newTestScheme()}
	_, err = rs.handleDeletion(ctx, sa)
	require.NoError(t, err)
	assert.Empty(t, sink.errors())
}
