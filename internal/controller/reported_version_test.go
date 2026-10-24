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

// A tag without a patch says no version, so the operator records what the
// servers report and the version rule holds spec.image.tag to it
// (operations.md id 153). These pin where it is recorded; the rule itself is
// pinned in internal/validation, and the envtest spec "Neo4jEnterpriseCluster
// on a tag without a patch" drives a real Reconcile with what is recorded here.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

const (
	pinned   = "5.26.31-enterprise" // what the servers ran: a tag with a patch
	floating = "5.26-enterprise"    // floats to the newest 5.26 patch
	resolved = "5.26.32"            // what floating was on the day
)

// showServers is the SHOW SERVERS of cluster "c": one row per server, each
// reporting the version given for its ordinal.
func showServers(versions ...string) []neo4jclient.ServerInfo {
	rows := make([]neo4jclient.ServerInfo, 0, len(versions))
	for i, v := range versions {
		rows = append(rows, neo4jclient.ServerInfo{
			Name:    fmt.Sprintf("server-%d", i),
			Address: fmt.Sprintf("c-server-%d.c-headless.default.svc.cluster.local:7687", i),
			State:   "Enabled", Health: "Available", Version: v,
		})
	}
	return rows
}

func TestTagFloatsAndLaterReportedVersion(t *testing.T) {
	assert.True(t, tagFloats("5.26-enterprise"))
	assert.True(t, tagFloats("2026.08-enterprise"))
	assert.False(t, tagFloats("5.26.31-enterprise"))
	assert.False(t, tagFloats("2026.08.1-enterprise"))
	assert.False(t, tagFloats("latest"), "not a version: the image rule refuses it")
	assert.False(t, tagFloats(""))

	assert.Equal(t, "5.26.32", laterReportedVersion("5.26.31", "5.26.32"))
	assert.Equal(t, "5.26.32", laterReportedVersion("5.26.32", "5.26.31"))
	assert.Equal(t, "5.26.10", laterReportedVersion("5.26.9", "5.26.10"), "numbers, not strings")
	assert.Equal(t, "2025.01.0", laterReportedVersion("5.26.32", "2025.01.0"))
	assert.Equal(t, "5.26.32", laterReportedVersion("", "5.26.32"))
	assert.Equal(t, "5.26.32", laterReportedVersion("5.26.32", "garbage"))

	rows := showServers("5.26.31", "5.26.33", "5.26.32")
	rows = append(rows, neo4jclient.ServerInfo{State: "Cordoned", Version: "5.26.40"})
	assert.Equal(t, "5.26.33", highestReportedVersion(rows, nil), "the highest of the Enabled servers")
}

// What a Ready pass records, from the rolled StatefulSet and the SHOW SERVERS
// the formation check read.
func TestReportedVersionsToRecord(t *testing.T) {
	rolled := func(tag string) *appsv1.StatefulSet {
		sts := gateTestSTS("neo4j:"+tag, 3, "rev1", "rev1", 3, 3)
		sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType, RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{}}
		return sts
	}
	heldAt := func(partition int32) *appsv1.StatefulSet {
		sts := gateTestSTS("neo4j:"+floating, 3, "rev-before", "rev-target", 3-partition, 3)
		sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(partition)}}
		return sts
	}
	paused := func(partition int32) *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := gateTestCluster(floating, "Ready")
		c.Status.Version = pinned
		c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhasePaused, PreviousVersion: pinned,
			TargetVersion: floating, CurrentPartition: ptr.To(partition),
			Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: partition}}
		return c
	}

	t.Run("a floating status.version: what the servers report", func(t *testing.T) {
		c := gateTestCluster(floating, "Ready")
		c.Status.Version = floating
		rec := reportedVersionsToRecord(c, rolled(floating), showServers(resolved, resolved, resolved))
		require.NotNil(t, rec.runningVersion)
		assert.Equal(t, resolved, *rec.runningVersion)
		assert.Empty(t, rec.version, "status.version is already the tag")
		assert.Nil(t, rec.resolvedTargetVersion)
	})
	t.Run("the first Ready on a floating tag records both", func(t *testing.T) {
		c := gateTestCluster(floating, "Forming")
		rec := reportedVersionsToRecord(c, rolled(floating), showServers(resolved, resolved, resolved))
		assert.Equal(t, floating, rec.version)
		require.NotNil(t, rec.runningVersion)
		assert.Equal(t, resolved, *rec.runningVersion)
	})
	t.Run("a tag with a patch says the version: cleared", func(t *testing.T) {
		c := gateTestCluster("5.26.32-enterprise", "Ready")
		c.Status.Version, c.Status.RunningVersion = "5.26.32-enterprise", resolved
		rec := reportedVersionsToRecord(c, rolled("5.26.32-enterprise"), showServers(resolved, resolved, resolved))
		require.NotNil(t, rec.runningVersion)
		assert.Empty(t, *rec.runningVersion)
	})
	t.Run("nothing reported: left as it is", func(t *testing.T) {
		c := gateTestCluster(floating, "Ready")
		c.Status.Version = floating
		assert.Nil(t, reportedVersionsToRecord(c, rolled(floating), nil).runningVersion)
	})
	t.Run("the incident: a Paused upgrade whose floating target resolved on server-2", func(t *testing.T) {
		rec := reportedVersionsToRecord(paused(2), heldAt(2), showServers("5.26.31", "5.26.31", resolved))
		require.NotNil(t, rec.resolvedTargetVersion)
		assert.Equal(t, resolved, *rec.resolvedTargetVersion, "server-2 runs the target")
		require.NotNil(t, rec.runningVersion)
		assert.Empty(t, *rec.runningVersion, "status.version names its patch")
		assert.Empty(t, rec.version, "nothing verified a new version")
	})
	t.Run("a held upgrade from a floating status.version: the servers below the partition run it", func(t *testing.T) {
		c := paused(2)
		c.Status.Version = "5.26-enterprise"
		c.Status.UpgradeStatus.TargetVersion = "2026.08-enterprise"
		sts := heldAt(2)
		sts.Spec.Template.Spec.Containers[0].Image = "neo4j:2026.08-enterprise"
		rec := reportedVersionsToRecord(c, sts, showServers("5.26.32", "5.26.32", "2026.08.1"))
		require.NotNil(t, rec.runningVersion)
		assert.Equal(t, "5.26.32", *rec.runningVersion, "server-0 and server-1, not server-2")
		require.NotNil(t, rec.resolvedTargetVersion)
		assert.Equal(t, "2026.08.1", *rec.resolvedTargetVersion)
	})
	t.Run("a held upgrade that had not staged: nothing runs the target", func(t *testing.T) {
		c := paused(3)
		rec := reportedVersionsToRecord(c, rolled(pinned), showServers("5.26.31", "5.26.31", "5.26.31"))
		assert.Nil(t, rec.resolvedTargetVersion)
	})
	// A check refused it before Staging: every server runs status.version, and
	// a floating one is recorded as for any cluster. It was left alone for as
	// long as the record stayed Paused or Failed (#550).
	t.Run("a held upgrade that had not staged, from a floating status.version: recorded", func(t *testing.T) {
		c := paused(0)
		c.Status.Version = floating
		c.Status.UpgradeStatus.TargetVersion = "2026.08.1-enterprise"
		c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}
		rec := reportedVersionsToRecord(c, rolled(floating), showServers(resolved, resolved, resolved))
		require.NotNil(t, rec.runningVersion)
		assert.Equal(t, resolved, *rec.runningVersion)
	})
	t.Run("a held upgrade to a tag with a patch: the tag says it", func(t *testing.T) {
		c := paused(2)
		c.Status.UpgradeStatus.TargetVersion = "5.26.32-enterprise"
		sts := heldAt(2)
		sts.Spec.Template.Spec.Containers[0].Image = "neo4j:5.26.32-enterprise"
		assert.Nil(t, reportedVersionsToRecord(c, sts, showServers("5.26.31", "5.26.31", resolved)).resolvedTargetVersion)
	})
	t.Run("already recorded: nothing to write", func(t *testing.T) {
		c := paused(2)
		c.Status.UpgradeStatus.ResolvedTargetVersion = resolved
		assert.Nil(t, reportedVersionsToRecord(c, heldAt(2), showServers("5.26.31", "5.26.31", resolved)).resolvedTargetVersion)
	})
}

// The Ready pass writes what the servers report with the Ready status, and
// only when it changes; the version rule then refuses a tag below it.
func TestRecordClusterReady_RecordsWhatTheServersReport(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "c", Namespace: "default"}

	sts := gateTestSTS("neo4j:"+floating, 3, "rev1", "rev1", 3, 3)
	cluster := gateTestCluster(floating, "Ready")
	cluster.Status.Version = floating
	r := gateTestReconciler(t, sts, cluster)

	r.recordClusterReady(ctx, cluster, showServers(resolved, resolved, resolved))
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, floating, got.Status.Version, "status.version stays the tag")
	assert.Equal(t, resolved, got.Status.RunningVersion)

	// The next pass reads the same: no write.
	written := got.ResourceVersion
	r.recordClusterReady(ctx, got, showServers(resolved, resolved, resolved))
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, written, got.ResourceVersion, "a status write is a reconcile: only on a change")

	// So a patch of the line below what the servers run is the downgrade it is.
	got.Spec.Image.Tag = pinned
	got.Spec.Storage.Size = "1Gi"
	errs := validation.NewClusterValidator(nil).ValidateWithWarnings(ctx, got).Errors
	require.Len(t, errs, 1, "%v", errs)
	assert.Contains(t, errs[0].Detail, "the servers run "+resolved+" (status.runningVersion")
}

// The incident, as far as Neo4j allows without one: the upgrade to
// 5.26-enterprise paused while server-2 was still restarting, so the Rolling
// step never saw it available. The held cluster's next Ready pass records what
// server-2 reports, and setting the tag back to 5.26.31-enterprise is refused.
func TestRecordClusterReady_RecordsWhatAHeldUpgradesTargetResolvedTo(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "c", Namespace: "default"}

	sts := gateTestSTS("neo4j:"+floating, 3, "rev-before", "rev-target", 1, 3)
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}}
	cluster := gateTestCluster(floating, "Ready")
	cluster.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true}
	cluster.Status.Version = pinned
	cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhasePaused, PreviousVersion: pinned,
		TargetVersion: floating, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2}}
	r := gateTestReconciler(t, sts, cluster)

	rolled, detail := r.serverStatefulSetFullyRolled(ctx, cluster)
	require.True(t, rolled, detail)
	r.recordClusterReady(ctx, cluster, showServers("5.26.31", "5.26.31", resolved))

	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, resolved, got.Status.UpgradeStatus.ResolvedTargetVersion)
	assert.Equal(t, upgradePhasePaused, got.Status.UpgradeStatus.Phase)
	assert.Equal(t, pinned, got.Status.Version)

	got.Spec.Storage.Size = "1Gi"
	got.Spec.Image.Tag = pinned
	errs := validation.NewClusterValidator(nil).ValidateWithWarnings(ctx, got).Errors
	require.Len(t, errs, 1, "%v", errs)
	assert.Contains(t, errs[0].Detail, "they run "+resolved+" (status.upgradeStatus.resolvedTargetVersion)")
}

// The Rolling step's availability check reads the restarted server's SHOW
// SERVERS row; for a floating target, the version in it is recorded as soon as
// it passes, so a tag set back mid-flight is refused before the walk re-stages.
func TestStepUpgradeRolling_RecordsWhatTheRestartedServerReports(t *testing.T) {
	for _, tc := range []struct {
		name, target, want string
	}{
		{"a floating target", floating, resolved},
		{"a target with a patch says it", "5.26.32-enterprise", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := clusterForUpgrade("c", "default", pinned, tc.target, 3)
			c.Status.Phase = "Ready"
			c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseRolling, PreviousVersion: pinned,
				TargetVersion: tc.target, CurrentPartition: ptr.To(int32(2)), StepStartTime: ptr.To(metav1.Now()),
				Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2}}
			sts := gateTestSTS("neo4j:"+tc.target, 3, "rev-before", "rev-target", 1, 3)
			sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}}
			fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c, sts).WithStatusSubresource(c).Build()
			r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10), UpgradeGates: &upgradeGates{
				serverAvailable: func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (bool, string, error) {
					require.Equal(t, "c-server-2", pod)
					return true, resolved, nil
				},
				databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
					return false, []string{"d01"}, nil // the walk waits; the version is recorded anyway
				},
			}}
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), latest))

			_, err := r.stepUpgradeRolling(context.Background(), latest)
			require.NoError(t, err)
			require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), latest))
			assert.Equal(t, tc.want, latest.Status.UpgradeStatus.ResolvedTargetVersion)
			assert.Equal(t, upgradePhaseRolling, latest.Status.UpgradeStatus.Phase)
		})
	}
}

// Completed records what the servers reported to Verifying, in the write that
// moves status.version: for a floating tag, the version; for one with a
// patch, nothing (and a version recorded for the previous tag is cleared).
func TestUpdateUpgradeStatus_CompletedRecordsWhatTheServersReport(t *testing.T) {
	for _, tc := range []struct {
		name, target, before, wantRunning, wantResolved string
	}{
		{"a floating target", floating, "", resolved, resolved},
		{"a target with a patch", "5.26.32-enterprise", "5.26.31", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := clusterForUpgrade("c", "default", pinned, tc.target, 3)
			c.Status.RunningVersion = tc.before
			c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseVerifying, PreviousVersion: pinned,
				TargetVersion: tc.target, Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3}}
			fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c).WithStatusSubresource(c).Build()
			orch := NewRollingUpgradeOrchestrator(fc, "c", "default")

			require.NoError(t, orch.updateUpgradeStatus(context.Background(), c, upgradePhaseCompleted, "done", "", resolved))
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), got))
			assert.Equal(t, tc.target, got.Status.Version)
			assert.Equal(t, tc.wantRunning, got.Status.RunningVersion)
			assert.Equal(t, tc.wantResolved, got.Status.UpgradeStatus.ResolvedTargetVersion)
		})
	}
}

// An attempt that takes up a held upgrade keeps what its floating target
// resolved to: the servers it restarted still run it.
func TestNewUpgradeRecord_KeepsWhatTheTargetResolvedTo(t *testing.T) {
	held := &neo4jv1beta1.UpgradeStatus{Phase: upgradePhasePaused, PreviousVersion: pinned, TargetVersion: floating,
		ResolvedTargetVersion: resolved, Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2}}
	assert.Equal(t, resolved, newUpgradeRecord(held, pinned, "5.26.33-enterprise", 3, metav1.Now()).ResolvedTargetVersion)

	completed := held.DeepCopy()
	completed.Phase = upgradePhaseCompleted
	assert.Empty(t, newUpgradeRecord(completed, pinned, "2026.08-enterprise", 3, metav1.Now()).ResolvedTargetVersion,
		"a new upgrade starts afresh")
}

// The standalone reads the version its server reports once Ready, for a
// floating status.version, and again only when its container runs another
// image.
func TestRecordStandaloneRunningVersion_ReadsOncePerImage(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "sa", Namespace: "default"}
	sa := standaloneForSTS(floating)
	sa.Status.Phase, sa.Status.Version = "Ready", floating
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sa-0", Namespace: "default", UID: "pod-1"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "neo4j", ImageID: "docker.io/library/neo4j@sha256:aaa"}}},
	}
	r := standaloneVersionTestReconciler(t, sa, pod)
	reads, reports := 0, resolved
	r.ServerVersionReader = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseStandalone) (string, error) {
		reads++
		return reports, nil
	}
	got := func() *neo4jv1beta1.Neo4jEnterpriseStandalone {
		s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		require.NoError(t, r.Get(ctx, key, s))
		return s
	}

	for range 3 {
		r.recordStandaloneRunningVersion(ctx, sa)
	}
	assert.Equal(t, 1, reads, "one read, not one per pass")
	assert.Equal(t, resolved, got().Status.RunningVersion)

	// The pod was replaced and its container started on another image (a
	// pull of the floating tag).
	require.NoError(t, r.Delete(ctx, pod))
	replaced := pod.DeepCopy()
	replaced.ResourceVersion, replaced.UID = "", "pod-2"
	replaced.Status.ContainerStatuses[0].ImageID = "docker.io/library/neo4j@sha256:bbb"
	require.NoError(t, r.Create(ctx, replaced))
	reports = "5.26.33"
	r.recordStandaloneRunningVersion(ctx, sa)
	assert.Equal(t, 2, reads)
	assert.Equal(t, "5.26.33", got().Status.RunningVersion)

	// A tag with a patch says the version: cleared, nothing read.
	s := got()
	s.Status.Version = "5.26.33-enterprise"
	require.NoError(t, r.Status().Update(ctx, s))
	r.recordStandaloneRunningVersion(ctx, sa)
	assert.Equal(t, 2, reads)
	assert.Empty(t, got().Status.RunningVersion)
}

// A standalone whose CRD lacks status.runningVersion (the CRDs applied only
// after the operator was upgraded) has the field dropped on every write. The
// read is not repeated, but the write is, so once the CRDs are in the next pass
// records it — no operator restart (v1.22.0 re-walk: the field stayed empty
// until the operator restarted).
func TestRecordStandaloneRunningVersion_RecordsOnceTheCRDKeepsTheField(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "sa", Namespace: "default"}
	sa := standaloneForSTS(floating)
	sa.Status.Phase, sa.Status.Version = "Ready", floating
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sa-0", Namespace: "default", UID: "pod-1"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "neo4j", ImageID: "docker.io/library/neo4j@sha256:aaa"}}},
	}
	oldCRD := true
	scheme := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sa, pod).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if s, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseStandalone); ok && oldCRD {
					s.Status.RunningVersion = "" // pruned: the installed CRD has no such field
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
	reads := 0
	r.ServerVersionReader = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseStandalone) (string, error) {
		reads++
		return resolved, nil
	}
	got := func() string {
		s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		require.NoError(t, r.Get(ctx, key, s))
		return s.Status.RunningVersion
	}

	r.recordStandaloneRunningVersion(ctx, sa)
	r.recordStandaloneRunningVersion(ctx, sa)
	assert.Equal(t, 1, reads, "the server is asked once, not on every pass")
	assert.Empty(t, got(), "the old CRD drops the field")

	oldCRD = false // the CRDs are applied
	r.recordStandaloneRunningVersion(ctx, sa)
	assert.Equal(t, 1, reads, "recorded from the read already made")
	assert.Equal(t, resolved, got(), "the next pass records it, with no operator restart")
}

// updateStatus clears status.runningVersion when status.version moves: it
// described the previous tag.
func TestStandaloneUpdateStatus_ClearsTheRunningVersionWhenTheTagMoves(t *testing.T) {
	ctx := context.Background()
	sa := standaloneForSTS("2026.08-enterprise")
	sa.Status.Version, sa.Status.RunningVersion = floating, resolved
	sts := standaloneVersionTestSTS("neo4j:2026.08-enterprise", "rev2", "rev2", 1, 1)
	r := standaloneVersionTestReconciler(t, sa, sts)

	require.NoError(t, r.updateStatus(ctx, sa))
	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "sa", Namespace: "default"}, got))
	assert.Equal(t, "2026.08-enterprise", got.Status.Version)
	assert.Empty(t, got.Status.RunningVersion)
}
