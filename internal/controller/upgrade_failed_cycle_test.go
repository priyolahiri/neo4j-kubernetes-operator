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

// #565: a Failed upgrade that failed mid-walk cycled Failed → Ready → retry,
// with an UpgradeFailed and a ClusterReady on each cycle; a stopped upgrade set
// the cluster's phase to Paused or Failed for one pass; and the standalone's
// UpgradeStarted read the StatefulSet from the cache.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A Failed upgrade the operator retries that stops again with the same cause
// is announced once per target and cause, and the cluster, Ready throughout,
// is not announced Ready again.
func TestFailUpgrade_ARetriedFailureIsAnnouncedOncePerTargetAndCause(t *testing.T) {
	ctx := context.Background()
	c := clusterForUpgrade("cyc", "default", holdRunning, holdTarget, 3)
	c.Status.Phase = neo4jv1beta1.PhaseReady
	now := metav1.Now()
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseRolling, StartTime: &now,
		PreviousVersion: holdRunning, TargetVersion: holdTarget}
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c).WithStatusSubresource(c).Build()
	rec := record.NewFakeRecorder(50)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}
	latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster { return latestCluster(t, fc, c) }
	timedOut := errors.New("timed out waiting for server ordinal 2 to roll and rejoin the cluster")
	retry := func(target string) {
		t.Helper()
		held := latest().Status.UpgradeStatus.DeepCopy()
		require.NoError(t, r.patchUpgradeStatus(ctx, latest(), func(us *neo4jv1beta1.UpgradeStatus) {
			*us = newUpgradeRecord(held, holdRunning, target, 3, metav1.Now())
			us.Phase = upgradePhaseRolling
		}))
	}
	failures := func() int { return len(eventsWithReason(drainEvents(rec), EventReasonUpgradeFailed)) }

	_, err := r.failUpgrade(ctx, latest(), timedOut)
	require.NoError(t, err)
	assert.Equal(t, 1, failures())
	assert.Equal(t, neo4jv1beta1.PhaseReady, latest().Status.Phase)
	assert.False(t, r.recordClusterReady(ctx, latest(), nil), "nothing to announce: the cluster stayed Ready")

	for cycle := 0; cycle < 3; cycle++ {
		retry(holdTarget)
		_, err = r.failUpgrade(ctx, latest(), timedOut)
		require.NoError(t, err)
		assert.False(t, r.recordClusterReady(ctx, latest(), nil))
	}
	assert.Zero(t, failures(), "the same target and cause, retried: no event")
	assert.Equal(t, timedOut.Error(), latest().Status.UpgradeStatus.LastError)

	retry(holdTarget)
	_, err = r.failUpgrade(ctx, latest(), errors.New("timed out waiting for the databases on server ordinal 2 to catch up (orders)"))
	require.NoError(t, err)
	assert.Equal(t, 1, failures(), "another cause")

	retry(repairTag) // the user moved the tag on
	_, err = r.failUpgrade(ctx, latest(), timedOut)
	require.NoError(t, err)
	assert.Equal(t, 1, failures(), "another target")
}

// The pass after the one that wrote a standalone's new image reads the
// template from the API server: a cache that still holds the template from
// before announced UpgradeStarted a second time.
func TestIsStandaloneUpgradeRequired_ReadsTheStatefulSetAsStored(t *testing.T) {
	ctx := context.Background()
	sa := standaloneForSTS("2026.09.0-enterprise")
	sa.Status.Version = "2026.08.1-enterprise"
	written := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"}}
	written.Spec.Template.Spec.Containers = []corev1.Container{{Name: "neo4j", Image: "neo4j:2026.09.0-enterprise"}}
	stale := written.DeepCopy()
	stale.Spec.Template.Spec.Containers[0].Image = "neo4j:2026.08.1-enterprise"

	stored := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(written).Build()
	cached := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(written).
		WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				stale.DeepCopyInto(sts)
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		}}).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{Client: cached, APIReader: stored, Scheme: makeUpgradeScheme()}
	assert.False(t, r.isStandaloneUpgradeRequired(ctx, sa), "the new image is written: this pass does not apply it")

	before := &appsv1.StatefulSet{}
	require.NoError(t, stored.Get(ctx, client.ObjectKeyFromObject(written), before))
	before.Spec.Template.Spec.Containers[0].Image = "neo4j:2026.08.1-enterprise"
	require.NoError(t, stored.Update(ctx, before))
	assert.True(t, r.isStandaloneUpgradeRequired(ctx, sa), "the pass that applies it")
}
