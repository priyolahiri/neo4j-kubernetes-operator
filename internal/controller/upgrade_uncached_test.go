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

// The upgrade steps decide from what is stored, not from the cache
// (upgrade_uncached.go, operations.md id 203).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A retarget staged the new target at partition 1 (servers 1 and 2 on the
// first one). A cache still holding the StatefulSet from before that write
// counts both as updated and every server ready: the Rolling step took the
// step as settled, found server-1 available on the first target and lowered
// the partition onto the new template — server-0 restarted with no check.
func TestRolling_DecidesFromTheStatefulSetAsStored(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := walkingTo(t, upgradePhaseRolling, 1)
	stale := emu.sts()

	setTag(t, fc, c, repairTag)
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c)) // the retarget
	require.NoError(t, err)
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c)) // Staging
	require.NoError(t, err)
	require.Equal(t, upgradePhaseRolling, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)
	require.Equal(t, "neo4j:"+repairTag, serverImage(emu.sts()))
	require.Equal(t, int32(1), *statefulSetPartition(emu.sts()))
	emu.observe() // seen; servers 1 and 2 not yet moved on

	underlying, ok := fc.(client.WithWatch)
	require.True(t, ok)
	var written []int32
	cache := interceptor.NewClient(underlying, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if sts, isSTS := obj.(*appsv1.StatefulSet); isSTS && key.Name == "c-server" {
				stale.DeepCopyInto(sts)
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if sts, isSTS := obj.(*appsv1.StatefulSet); isSTS {
				if p := statefulSetPartition(sts); p != nil {
					written = append(written, *p)
				}
			}
			return cl.Update(ctx, obj, opts...)
		},
	})
	cached := &Neo4jEnterpriseClusterReconciler{Client: cache, APIReader: fc, Scheme: r.Scheme, Recorder: record.NewFakeRecorder(10),
		ConfigMapManager: r.ConfigMapManager, UpgradeGates: r.UpgradeGates}

	_, err = cached.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Empty(t, written, "no partition is written: the servers have not moved on to the new target yet")
	assert.Equal(t, int32(1), *statefulSetPartition(emu.sts()))
	assert.Equal(t, int32(1), *latestCluster(t, fc, c).Status.UpgradeStatus.CurrentPartition)
}

// The repair of an upgrade held on a tag never published (id 162): Staging
// judges which servers cannot start from the pods as stored. A cache from
// before server-2 failed shows it Ready; judged from it, Staging froze every
// server, and the walk then waited for server-2, which stays down until it is
// replaced, until it paused again.
func TestStaging_JudgesTheServersDownFromThePodsAsStored(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	before := &corev1.PodList{}
	require.NoError(t, fc.List(ctx, before, client.InNamespace("default")))
	for i := range before.Items {
		pod := &before.Items[i]
		pod.Status = heldServerPod(0, "x", true).Status // as the cache saw them, every server up
	}

	fixed := latestCluster(t, fc, c)
	require.NoError(t, patchObject(ctx, fc, fixed, func() { fixed.Spec.Image.Tag = holdTarget }))
	require.True(t, r.isUpgradeRequired(ctx, latestCluster(t, fc, c)))
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c)) // the repair starts: Staging
	require.NoError(t, err)
	require.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)

	underlying, ok := fc.(client.WithWatch)
	require.True(t, ok)
	cache := interceptor.NewClient(underlying, interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if pods, isPods := list.(*corev1.PodList); isPods {
				before.DeepCopyInto(pods)
				return nil
			}
			return cl.List(ctx, list, opts...)
		},
	})
	cached := &Neo4jEnterpriseClusterReconciler{Client: cache, APIReader: fc, Scheme: r.Scheme, Recorder: record.NewFakeRecorder(10),
		ConfigMapManager: NewConfigMapManager(cache), UpgradeGates: r.UpgradeGates}
	_, err = cached.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, "neo4j:"+holdTarget, serverImage(emu.sts()))
	assert.Equal(t, int32(2), *statefulSetPartition(emu.sts()), "staged above server-2, which is replaced first")
}

// A pass whose cached copy of the cluster is from before the retarget was
// recorded: Rolling towards the first target at partition 1. It waits for the
// cache rather than lower the partition and write its step over the record
// the retarget wrote.
func TestUpgradeSteps_DecideFromTheRecordAsStored(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := walkingTo(t, upgradePhaseRolling, 1)
	stale := latestCluster(t, fc, c)

	setTag(t, fc, c, repairTag)
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c)) // the retarget, recorded
	require.NoError(t, err)
	require.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)

	_, err = r.handleRollingUpgrade(ctx, stale)
	require.NoError(t, err)
	assert.Equal(t, int32(1), *statefulSetPartition(emu.sts()), "the partition stays")
	got := latestCluster(t, fc, c).Status.UpgradeStatus
	assert.Equal(t, upgradePhaseStaging, got.Phase)
	assert.Equal(t, repairTag, got.TargetVersion)
	assert.Equal(t, int32(1), *got.CurrentPartition, "the record the retarget wrote is left as it is")
}
