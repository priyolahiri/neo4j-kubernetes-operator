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

// status.version follows what the servers run (operations.md id 144).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

func TestRolledImageTag(t *testing.T) {
	rolled := func(image string) *appsv1.StatefulSet {
		sts := gateTestSTS(image, 3, "rev2", "rev2", 3, 3)
		sts.Generation, sts.Status.ObservedGeneration = 4, 4
		return sts
	}
	cases := []struct {
		name   string
		sts    *appsv1.StatefulSet
		mutate func(*appsv1.StatefulSet)
		want   string
	}{
		{"fully rolled", rolled("neo4j:2026.08.1-enterprise"), nil, "2026.08.1-enterprise"},
		{"a registry with a port", rolled("registry.example:5000/neo4j:5.26.12-enterprise"), nil, "5.26.12-enterprise"},
		{"the controller has not seen the template yet", rolled("neo4j:2026.09.0-enterprise"),
			func(s *appsv1.StatefulSet) { s.Status.ObservedGeneration = 3 }, ""},
		{"a revision rollout in flight", rolled("neo4j:2026.09.0-enterprise"),
			func(s *appsv1.StatefulSet) { s.Status.CurrentRevision = "rev1" }, ""},
		{"a server not updated", rolled("neo4j:2026.09.0-enterprise"),
			func(s *appsv1.StatefulSet) { s.Status.UpdatedReplicas = 2 }, ""},
		{"a server not ready", rolled("neo4j:2026.09.0-enterprise"),
			func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 2 }, ""},
		{"scaled to zero", rolled("neo4j:2026.09.0-enterprise"),
			func(s *appsv1.StatefulSet) { z := int32(0); s.Spec.Replicas = &z }, ""},
		{"no tag", rolled("registry.example:5000/neo4j"), nil, ""},
		{"pinned by digest alone", rolled("neo4j@sha256:0123"), nil, ""},
		{"a tag with a digest, spelled as spec.image.tag", rolled("neo4j:2026.08.1-enterprise@sha256:0123"), nil,
			"2026.08.1-enterprise@sha256:0123"},
		{"no containers", rolled("neo4j:x"),
			func(s *appsv1.StatefulSet) { s.Spec.Template.Spec.Containers = nil }, ""},
		{"no StatefulSet", nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.mutate != nil {
				tc.mutate(tc.sts)
			}
			assert.Equal(t, tc.want, rolledImageTag(tc.sts))
		})
	}
}

func TestClusterVersionToRecord(t *testing.T) {
	const a, b = "2026.08.1-enterprise", "2026.09.0-enterprise"
	upgrade := func(phase, target string) *neo4jv1beta1.UpgradeStatus {
		return &neo4jv1beta1.UpgradeStatus{Phase: phase, PreviousVersion: a, TargetVersion: target}
	}
	cases := []struct {
		name    string
		status  neo4jv1beta1.Neo4jEnterpriseClusterStatus
		running string
		want    string
	}{
		{"first Ready", neo4jv1beta1.Neo4jEnterpriseClusterStatus{}, a, a},
		{"no change", neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a}, a, ""},
		{"nothing known about the servers", neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a}, "", ""},
		// The defect: an image the zero-ready escape hatch applied.
		{"an image applied outside the upgrade machine", neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a}, b, b},
		{"after an upgrade that completed",
			neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a, UpgradeStatus: upgrade(upgradePhaseCompleted, b)}, b, b},
		// The machine re-enters a Failed upgrade and records the version once
		// it has verified the servers; Paused waits for the user (id 139).
		{"a Failed upgrade to the same tag",
			neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a, UpgradeStatus: upgrade(upgradePhaseFailed, b)}, b, ""},
		{"a Paused upgrade to the same tag",
			neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a, UpgradeStatus: upgrade(upgradePhasePaused, b)}, b, ""},
		{"a Failed upgrade to another tag",
			neo4jv1beta1.Neo4jEnterpriseClusterStatus{Version: a, UpgradeStatus: upgrade(upgradePhaseFailed, "2026.10.0-enterprise")}, b, b},
		{"a Failed upgrade before anything was recorded",
			neo4jv1beta1.Neo4jEnterpriseClusterStatus{UpgradeStatus: upgrade(upgradePhaseFailed, b)}, b, b},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, clusterVersionToRecord(&tc.status, tc.running))
		})
	}
}

// The whole defect, in order. A cluster that was Ready on 2026.08.1 loses
// every server; the user moves the tag on to 2026.09.0, which the zero-ready
// escape hatch takes straight into the StatefulSet; the servers come back on
// it. status.version used to stay at 2026.08.1, because the Ready pass wrote it
// only when it was empty — so moving the tag back to 2026.08.1, a downgrade of
// what the servers now run, passed the version rule.
func TestRecordClusterReady_RecordsTheImageTheEscapeHatchApplied(t *testing.T) {
	const before, after = "2026.08.1-enterprise", "2026.09.0-enterprise"
	ctx := context.Background()

	sts := gateTestSTS("neo4j:"+before, 3, "rev1", "rev1", 3, 0) // every server down
	cluster := gateTestCluster(after, "Forming")
	cluster.Status.Version = before
	r := gateTestReconciler(t, sts, cluster)

	// 1. No ready server: the new image is not held.
	r.holdImageDriftUntilReady(ctx, cluster)
	require.Equal(t, after, cluster.Spec.Image.Tag, "the escape hatch lets the new image through")

	// 2. The normal pass applies it and the servers come back on it.
	live := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(sts), live))
	live.Spec.Template.Spec.Containers[0].Image = "neo4j:" + after
	require.NoError(t, r.Update(ctx, live))
	live.Status.CurrentRevision, live.Status.UpdateRevision = "rev2", "rev2"
	live.Status.UpdatedReplicas, live.Status.ReadyReplicas = 3, 3
	require.NoError(t, r.Status().Update(ctx, live))

	// 3. The Ready pass records what the servers run.
	r.recordClusterReady(ctx, cluster)
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "c", Namespace: "default"}, got))
	assert.Equal(t, "Ready", got.Status.Phase)
	assert.Equal(t, after, got.Status.Version, "status.version follows the servers")

	// 4. So moving the tag back is the downgrade it is.
	got.Spec.Image.Tag = before
	res := validation.NewClusterValidator(nil).ValidateWithWarnings(ctx, got)
	require.NotEmpty(t, res.Errors)
	assert.Contains(t, res.Errors.ToAggregate().Error(), "downgrades are not supported: the servers run "+after)
}

// The Ready pass never records an image the servers are not all on.
func TestRecordClusterReady_LeavesTheVersionWhileARolloutIsInFlight(t *testing.T) {
	const before, after = "2026.08.1-enterprise", "2026.09.0-enterprise"
	sts := gateTestSTS("neo4j:"+after, 3, "rev1", "rev2", 1, 3)
	cluster := gateTestCluster(after, "Forming")
	cluster.Status.Version = before
	r := gateTestReconciler(t, sts, cluster)

	r.recordClusterReady(context.Background(), cluster)
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "c", Namespace: "default"}, got))
	assert.Equal(t, before, got.Status.Version)
}

func standaloneVersionTestReconciler(t *testing.T, objs ...client.Object) *Neo4jEnterpriseStandaloneReconciler {
	t.Helper()
	scheme := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	return &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
}

func standaloneVersionTestSTS(image string, currentRev, updateRev string, updated, ready int32) *appsv1.StatefulSet {
	one := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: image}}},
			},
		},
		Status: appsv1.StatefulSetStatus{
			CurrentRevision: currentRev, UpdateRevision: updateRev,
			UpdatedReplicas: updated, ReadyReplicas: ready,
		},
	}
}

// A standalone's status.version was spec.image.tag, written on every pass: it
// named the new tag while the old pod still ran, and kept naming an image that
// never started, so setting the tag back to the version the server did run
// would have been refused as a downgrade. It now follows the pod.
func TestStandaloneUpdateStatus_VersionFollowsThePod(t *testing.T) {
	const running, next, broken = "2026.08.1-enterprise", "2026.09.0-enterprise", "2026.10.0-enterprise"
	ctx := context.Background()
	key := types.NamespacedName{Name: "sa", Namespace: "default"}

	sa := standaloneForSTS(next)
	sa.Status.Version = running
	// The new template is applied; the old pod is still the ready one.
	sts := standaloneVersionTestSTS("neo4j:"+next, "rev1", "rev2", 0, 1)
	r := standaloneVersionTestReconciler(t, sa, sts)

	require.NoError(t, r.updateStatus(ctx, sa))
	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, running, got.Status.Version, "the old pod still runs")

	// The new pod is ready on the new template.
	live := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx, key, live))
	live.Status.CurrentRevision, live.Status.UpdatedReplicas = "rev2", 1
	require.NoError(t, r.Status().Update(ctx, live))
	require.NoError(t, r.updateStatus(ctx, sa))
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, next, got.Status.Version)

	// A tag whose image never starts is never recorded, so moving back to the
	// version the server runs stays allowed.
	sa.Spec.Image.Tag = broken
	require.NoError(t, r.Get(ctx, key, live))
	live.Spec.Template.Spec.Containers[0].Image = "neo4j:" + broken
	require.NoError(t, r.Update(ctx, live))
	live.Status.UpdateRevision, live.Status.UpdatedReplicas, live.Status.ReadyReplicas = "rev3", 0, 0
	require.NoError(t, r.Status().Update(ctx, live))
	require.NoError(t, r.updateStatus(ctx, sa))
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, next, got.Status.Version)
	assert.Equal(t, "Pending", got.Status.Phase)

	got.Spec.Image.Tag = next
	assert.Empty(t, validation.NewStandaloneValidator().Validate(ctx, nil, got),
		"the tag back to what the server runs is not a downgrade")
}

// The pre-upgrade check and its UpgradeStarted event belong to the pass that
// applies the new image. With status.version following the pod, the old test
// (status.version differs from the spec) stayed true until the new pod was
// ready, and would have announced the upgrade on every pass in between.
func TestIsStandaloneUpgradeRequired_OnlyUntilTheTemplateCarriesTheImage(t *testing.T) {
	ctx := context.Background()
	sa := standaloneForSTS("2026.09.0-enterprise")
	sa.Status.Version = "2026.08.1-enterprise"

	r := standaloneVersionTestReconciler(t, sa, standaloneVersionTestSTS("neo4j:2026.08.1-enterprise", "rev1", "rev1", 1, 1))
	assert.True(t, r.isStandaloneUpgradeRequired(ctx, sa), "the template still carries the old image")

	r = standaloneVersionTestReconciler(t, sa, standaloneVersionTestSTS("neo4j:2026.09.0-enterprise", "rev1", "rev2", 0, 1))
	assert.False(t, r.isStandaloneUpgradeRequired(ctx, sa), "the new image is applied; the pod is being replaced")

	r = standaloneVersionTestReconciler(t, sa)
	assert.False(t, r.isStandaloneUpgradeRequired(ctx, sa), "no StatefulSet yet")

	sa.Status.Version = ""
	r = standaloneVersionTestReconciler(t, sa, standaloneVersionTestSTS("neo4j:2026.08.1-enterprise", "rev1", "rev1", 1, 1))
	assert.False(t, r.isStandaloneUpgradeRequired(ctx, sa), "nothing recorded yet")
}
