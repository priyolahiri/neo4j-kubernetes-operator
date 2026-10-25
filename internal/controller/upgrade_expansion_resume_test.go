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

// The #562 re-walk's blocker, end to end against a fake API server with the
// StatefulSet controller's revision rules played by statefulSetEmulator: a
// 3-server cluster paused on a tag that was never published (server-2 cannot
// pull it, partition 2), a storage expansion, then the tag set to a fixed one.
// The StatefulSet was created again on the held template, so Kubernetes made
// that its current revision; the resume staged twice in one second (partition
// 2, then 3), server-2 came back on the unpullable current revision, the walk
// paused on "server ordinal 3", and the cluster was stuck for good.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// neverPublished is a tag whose image cannot be pulled.
const neverPublished = "2026.09.9-enterprise"

// statefulSetEmulator plays the StatefulSet controller's revision rules on the
// fake API server: a template is a revision named by its content; a new
// StatefulSet's current revision is its update revision; a pod at or above the
// partition is created from the update revision, one below it from the
// current revision; a rolling update replaces the highest pod at or above the
// partition that is not on the update revision, healthy or not.
type statefulSetEmulator struct {
	t          *testing.T
	c          client.Client
	revisions  map[string]corev1.PodTemplateSpec
	unpullable map[string]bool
	// created lists, per ordinal, the images its pods were created on.
	created map[int][]string
	// maxUnavailable135 plays the rolling update of Kubernetes 1.35.0–1.35.3,
	// where MaxUnavailableStatefulSet is on: a pod that is not available, at
	// any ordinal, counts against maxUnavailable (1), and nothing is deleted
	// while it does (operations.md id 204).
	maxUnavailable135 bool
}

func newStatefulSetEmulator(t *testing.T, c client.Client) *statefulSetEmulator {
	return &statefulSetEmulator{t: t, c: c, revisions: map[string]corev1.PodTemplateSpec{},
		unpullable: map[string]bool{"neo4j:" + neverPublished: true}, created: map[int][]string{}}
}

// revisionOf names a template as the StatefulSet controller does, by content
// — as the API server stores it, so a template read back from a revision is
// the same revision.
func revisionOf(template corev1.PodTemplateSpec) string {
	raw, _ := json.Marshal(template)
	var stored corev1.PodTemplateSpec
	_ = json.Unmarshal(raw, &stored)
	return "c-server-" + podTemplateSpecHash(stored)[:10]
}

// register records a template as a ControllerRevision.
func (e *statefulSetEmulator) register(template corev1.PodTemplateSpec) string {
	e.t.Helper()
	name := revisionOf(template)
	if _, ok := e.revisions[name]; ok {
		return name
	}
	e.revisions[name] = template
	raw, err := jsonRevisionData(template)
	require.NoError(e.t, err)
	require.NoError(e.t, e.c.Create(context.Background(), &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Data: raw}))
	return name
}

func (e *statefulSetEmulator) sts() *appsv1.StatefulSet {
	e.t.Helper()
	return storedServerSTS(e.t, e.c)
}

func (e *statefulSetEmulator) pods() map[int]*corev1.Pod {
	e.t.Helper()
	list := &corev1.PodList{}
	require.NoError(e.t, e.c.List(context.Background(), list, client.InNamespace("default")))
	out := map[int]*corev1.Pod{}
	for i := range list.Items {
		if ordinal, ok := statefulSetOrdinal("c-server", list.Items[i].Name); ok {
			out[ordinal] = &list.Items[i]
		}
	}
	return out
}

// observe is a sync of the StatefulSet controller: the update revision is the
// template's, a new StatefulSet's current revision is its update revision,
// and the status counts the pods.
func (e *statefulSetEmulator) observe() {
	e.t.Helper()
	sts := e.sts()
	update := e.register(sts.Spec.Template)
	sts.Status.UpdateRevision = update
	if sts.Status.CurrentRevision == "" {
		sts.Status.CurrentRevision = update
	}
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = 0, 0
	for _, pod := range e.pods() {
		if pod.Labels[appsv1.ControllerRevisionHashLabelKey] == update {
			sts.Status.UpdatedReplicas++
		}
		if isPodReady(pod) {
			sts.Status.ReadyReplicas++
		}
	}
	sts.Status.Replicas = *sts.Spec.Replicas
	// A rolling update every pod has completed makes the update revision the
	// current one, whatever the partition (completeRollingUpdate,
	// pkg/controller/statefulset/stateful_set_utils.go: every replica updated
	// and Ready).
	if sts.Status.UpdatedReplicas == *sts.Spec.Replicas && sts.Status.ReadyReplicas == *sts.Spec.Replicas {
		sts.Status.CurrentRevision = update
	}
	require.NoError(e.t, e.c.Status().Update(context.Background(), sts))
}

// createPod creates server pod ordinal from revision.
func (e *statefulSetEmulator) createPod(ordinal int, revision string) {
	e.t.Helper()
	template, ok := e.revisions[revision]
	require.True(e.t, ok, "revision %s", revision)
	image := template.Spec.Containers[0].Image
	var pod *corev1.Pod
	if e.unpullable[image] {
		pod = downServerPod(ordinal, image, pullBackOff(), corev1.ContainerState{})
	} else {
		pod = heldServerPod(ordinal, image, true)
	}
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = revision
	require.NoError(e.t, e.c.Create(context.Background(), pod))
	e.created[ordinal] = append(e.created[ordinal], image)
}

// revisionFor is the revision the StatefulSet controller creates pod ordinal
// from.
func (e *statefulSetEmulator) revisionFor(ordinal int) string {
	sts := e.sts()
	partition := int32(0)
	if p := statefulSetPartition(sts); p != nil {
		partition = *p
	}
	if int32(ordinal) >= partition {
		return sts.Status.UpdateRevision
	}
	return sts.Status.CurrentRevision
}

// deleteForUpdate is the rolling update's delete: the highest pod at or above
// the partition not on the update revision.
func (e *statefulSetEmulator) deleteForUpdate() {
	e.t.Helper()
	sts := e.sts()
	partition := int32(0)
	if p := statefulSetPartition(sts); p != nil {
		partition = *p
	}
	pods := e.pods()
	if e.maxUnavailable135 {
		for _, pod := range pods {
			if !isPodReady(pod) || pod.DeletionTimestamp != nil {
				return
			}
		}
	}
	for ordinal := int(*sts.Spec.Replicas) - 1; ordinal >= int(partition); ordinal-- {
		if pod := pods[ordinal]; pod != nil && pod.Labels[appsv1.ControllerRevisionHashLabelKey] != sts.Status.UpdateRevision {
			require.NoError(e.t, e.c.Delete(context.Background(), pod))
			return
		}
	}
}

// recreateMissing creates the missing pods from the revisions they get.
func (e *statefulSetEmulator) recreateMissing() {
	e.t.Helper()
	pods := e.pods()
	for ordinal := 0; ordinal < int(*e.sts().Spec.Replicas); ordinal++ {
		if pods[ordinal] == nil {
			e.createPod(ordinal, e.revisionFor(ordinal))
		}
	}
}

// heldOnNeverPublished is the cluster and StatefulSet the walk left: an
// upgrade to a tag never published paused with server-2 released (partition
// 2) and down, servers 0 and 1 on the version they ran.
func heldOnNeverPublished(t *testing.T) (*Neo4jEnterpriseClusterReconciler, client.Client, *statefulSetEmulator, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	c := clusterForUpgrade("c", "default", holdRunning, neverPublished, 3)
	c.UID = "c-uid"
	c.Finalizers = []string{ClusterFinalizer}
	c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true}
	c.Status.Phase = neo4jv1beta1.PhaseReady
	c.Status.Conditions = []metav1.Condition{clusterFormedCondition(),
		serversUnavailableCondition("2 of 3 servers available; unavailable: c-server-2 (Unavailable)")}
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: upgradePhasePaused, PreviousVersion: holdRunning, TargetVersion: neverPublished,
		StartedTargetVersion: holdRunning, StartTime: &start, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
	}

	scheme := makeUpgradeScheme()
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()
	emu := newStatefulSetEmulator(t, fc)

	running := clusterForUpgrade("c", "default", holdRunning, holdRunning, 3)
	before := emu.register(resources.BuildServerStatefulSetForEnterprise(running).Spec.Template)
	sts := resources.BuildServerStatefulSetForEnterprise(c)
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "neo4j.neo4j.com/v1beta1", Kind: "Neo4jEnterpriseCluster",
		Name: "c", UID: c.UID, Controller: ptr.To(true)}}
	sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}
	require.NoError(t, fc.Create(context.Background(), sts))
	held := emu.register(sts.Spec.Template)
	stored := emu.sts()
	stored.Status.CurrentRevision = before
	require.NoError(t, fc.Status().Update(context.Background(), stored))
	emu.observe()
	emu.createPod(0, before)
	emu.createPod(1, before)
	emu.createPod(2, held)
	emu.observe()
	emu.created = map[int][]string{}

	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
		ConfigMapManager: NewConfigMapManager(fc), UpgradeGates: &upgradeGates{
			serverAvailable: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, string, error) {
				return true, "", nil
			},
			databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
				return true, nil, nil
			},
		}}
	return r, fc, emu, c
}

func TestHeldUpgrade_AnExpansionThenTheFixedTagWalksToTheEnd(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	before, held := emu.sts().Status.CurrentRevision, emu.sts().Status.UpdateRevision

	// The storage expansion: deleted, then created again by the next pass.
	require.NoError(t, r.orphanDeleteStatefulSet(ctx, "default", "c-server"))
	grown := latestCluster(t, fc, c)
	grown.Spec.Storage.Size = "2Gi"
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
	emu.observe()
	emu.deleteForUpdate()
	emu.recreateMissing()
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
	emu.observe()
	emu.deleteForUpdate()
	emu.recreateMissing()

	sts := emu.sts()
	assert.Equal(t, held, sts.Status.UpdateRevision, "the held template is back")
	assert.Equal(t, int32(2), *statefulSetPartition(sts), "and its partition")
	assert.Equal(t, before, sts.Status.CurrentRevision,
		"servers below the partition still come back on the version they run if they restart")
	assert.Equal(t, before, emu.revisionFor(0))
	assert.Equal(t, held, emu.revisionFor(2))
	assert.Empty(t, emu.created, "no server restarted")

	// The fixed tag: the upgrade goes on, replacing server-2 first.
	fixed := latestCluster(t, fc, c)
	fixed.Spec.Image.Tag = holdTarget
	require.NoError(t, fc.Update(ctx, fixed))
	require.True(t, r.isUpgradeRequired(ctx, latestCluster(t, fc, c)), "the repair takes it up")
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	stale := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStaging, stale.Status.UpgradeStatus.Phase)

	_, err = r.handleRollingUpgrade(ctx, stale.DeepCopy())
	require.NoError(t, err)
	emu.observe()
	emu.deleteForUpdate() // the StatefulSet controller deletes server-2 for the new template
	// A pass whose cached copy still says Staging (#562 re-walk: twice in one
	// second, partition 2 then 3).
	_, err = r.stepUpgradeStaging(ctx, stale.DeepCopy())
	require.NoError(t, err)
	emu.observe()
	emu.recreateMissing()
	emu.observe()
	assert.Equal(t, []string{"neo4j:" + holdTarget}, emu.created[2], "server-2 is created once, on the fixed tag")

	// The walk, through its gates, to the end of Rolling.
	for step := 0; step < 12; step++ {
		got := latestCluster(t, fc, c)
		if got.Status.UpgradeStatus.Phase != upgradePhaseRolling {
			break
		}
		_, err := r.handleRollingUpgrade(ctx, got)
		require.NoError(t, err)
		emu.observe()
		emu.deleteForUpdate()
		emu.recreateMissing()
		emu.observe()
	}
	got := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
	for ordinal := 0; ordinal < 3; ordinal++ {
		assert.Equal(t, []string{"neo4j:" + holdTarget}, emu.created[ordinal], "server-%d restarts once, onto the fixed tag", ordinal)
	}
}

// After an operator restart between the delete and the create, the same from
// the revisions the orphaned pods run, the template hash stamped once the held
// template is back.
func TestHeldUpgrade_AnExpansionAcrossAnOperatorRestartKeepsTheRevisions(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	before, held := emu.sts().Status.CurrentRevision, emu.sts().Status.UpdateRevision

	require.NoError(t, r.orphanDeleteStatefulSet(ctx, "default", "c-server"))
	r.deletedServerStatefulSets.forget("default", "c-server") // the operator restarted
	grown := latestCluster(t, fc, c)
	grown.Spec.Storage.Size = "2Gi"
	desired := resources.BuildServerStatefulSetForEnterprise(grown)
	require.NoError(t, r.createOrUpdateResource(ctx, desired, grown))
	first := emu.sts()
	assert.Equal(t, "neo4j:"+holdRunning, first.Spec.Template.Spec.Containers[0].Image, "first on the template the servers below run")
	assert.Equal(t, int32(3), *statefulSetPartition(first), "frozen")
	assert.Equal(t, "2", first.Annotations[recreateRestorePartitionAnnotation])
	assert.Equal(t, held, first.Annotations[recreateRestoreRevisionAnnotation])

	// The image in place is the servers', as while it was deleted (the hold).
	place, ok := r.serverImageInPlace(ctx, grown)
	require.True(t, ok)
	assert.Equal(t, "neo4j:"+neverPublished, place.image)

	// Before the StatefulSet controller has recorded the current revision, the
	// next pass changes nothing.
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
	assert.Equal(t, first.Spec.Template, emu.sts().Spec.Template)
	emu.observe()
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
	emu.observe()

	sts := emu.sts()
	assert.Equal(t, held, sts.Status.UpdateRevision)
	assert.Equal(t, before, sts.Status.CurrentRevision)
	assert.Equal(t, int32(2), *statefulSetPartition(sts))
	assert.NotContains(t, sts.Annotations, recreateRestorePartitionAnnotation)
	assert.NotContains(t, sts.Annotations, recreateRestoreRevisionAnnotation)
	assert.Equal(t, podTemplateSpecHash(resources.BuildServerStatefulSetForEnterprise(grown).Spec.Template),
		sts.Annotations[clusterTemplateHashAnnotation],
		"the stamp, so a change of the spec reaches the StatefulSet while server-2 is down")
	assert.Empty(t, emu.created)
}

// Staging runs once per target, whatever the cache says, and a second run on
// the same template keeps the partition.
func TestStepUpgradeStaging_RunsOncePerTarget(t *testing.T) {
	ctx := context.Background()
	c, sts := repairFixture()
	now := metav1.Now()
	r, fc, _ := repairReconciler(t, c, sts,
		heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
		downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{}))
	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	stale := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStaging, stale.Status.UpgradeStatus.Phase)
	_, err = r.stepUpgradeStaging(ctx, stale.DeepCopy())
	require.NoError(t, err)
	live := func() *appsv1.StatefulSet { return storedServerSTS(t, fc) }
	require.Equal(t, int32(2), *statefulSetPartition(live()))

	// server-2 is being replaced: its pod is gone for a moment.
	require.NoError(t, fc.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "c-server-2", Namespace: "default"}}))

	t.Run("a pass whose cached copy still says Staging", func(t *testing.T) {
		_, err := r.stepUpgradeStaging(ctx, stale.DeepCopy())
		require.NoError(t, err)
		assert.Equal(t, int32(2), *statefulSetPartition(live()), "server-2 is created on the target, not on the current revision")
		assert.Equal(t, upgradePhaseRolling, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)
	})

	t.Run("a Staging run again on the same template (a crash between its writes)", func(t *testing.T) {
		require.NoError(t, r.patchUpgradeStatus(ctx, latestCluster(t, fc, c), func(us *neo4jv1beta1.UpgradeStatus) {
			us.Phase = upgradePhaseStaging
			us.StepStartTime = &now
		}))
		_, err := r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		assert.Equal(t, int32(2), *statefulSetPartition(live()), "the partition is not raised over the server being replaced")
		assert.Equal(t, int32(2), *latestCluster(t, fc, c).Status.UpgradeStatus.CurrentPartition)
	})

	t.Run("a cached copy from before a retarget", func(t *testing.T) {
		const later = "2026.11.0-enterprise"
		retargeted := latestCluster(t, fc, c)
		retargeted.Spec.Image.Tag = later
		require.NoError(t, fc.Update(ctx, retargeted))
		require.NoError(t, r.patchUpgradeStatus(ctx, latestCluster(t, fc, c), func(us *neo4jv1beta1.UpgradeStatus) {
			us.Phase, us.TargetVersion, us.StepStartTime = upgradePhaseStaging, later, &now
		}))
		_, err := r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		require.Equal(t, "neo4j:"+later, serverImage(live()))

		// The pass that reads the cluster from before the retarget stages
		// nothing: its spec names the earlier target.
		_, err = r.stepUpgradeStaging(ctx, stale.DeepCopy())
		require.NoError(t, err)
		assert.Equal(t, "neo4j:"+later, serverImage(live()), "the earlier target is not written back")
	})
}

// Every server frozen (a partition at the replica count) and server-2 down on
// another image than the servers that are up — recreated from a revision the
// walk had not put it on: the upgrade goes on towards a tag that is not
// server-2's, and Staging replaces it first. A server down on the version the
// others run is down for another reason, and waits.
func TestHeldUpgradeRepair_AServerDownOnAnotherImageWithEveryServerFrozen(t *testing.T) {
	ctx := context.Background()
	frozen := func() *appsv1.StatefulSet {
		sts := heldSTS()
		sts.Spec.Template.Spec.Containers[0].Image = "neo4j:" + repairTag // staged already
		sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(3))
		sts.Status.ReadyReplicas = 2
		return sts
	}
	failed := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		c, _ := repairFixture()
		c.Spec.UpgradeStrategy.AutoPauseOnFailure = false
		c.Status.UpgradeStatus.Phase = upgradePhaseFailed
		c.Status.UpgradeStatus.TargetVersion = repairTag
		return c
	}

	t.Run("goes on, and Staging replaces server-2 first", func(t *testing.T) {
		c, sts := failed(), frozen()
		r, fc, _ := repairReconciler(t, c, sts,
			heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
			downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{}))
		down := r.heldUpgradeRepair(ctx, latestCluster(t, fc, c))
		require.NotNil(t, down)
		assert.Equal(t, int32(2), down.partition)
		require.True(t, r.isUpgradeRequired(ctx, latestCluster(t, fc, c)))
		_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		_, err = r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		assert.Equal(t, int32(2), *statefulSetPartition(storedServerSTS(t, fc)))
	})

	for name, tc := range map[string]struct {
		mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		down   *corev1.Pod
	}{
		"Paused, waiting for the user": {func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.UpgradeStrategy.AutoPauseOnFailure = true
			c.Status.UpgradeStatus.Phase = upgradePhasePaused
		}, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})},
		"down on the version the others run": {nil, downServerPod(2, "neo4j:"+holdRunning, pullBackOff(), corev1.ContainerState{})},
		"down on the tag asked for":          {nil, downServerPod(2, "neo4j:"+repairTag, pullBackOff(), corev1.ContainerState{})},
	} {
		t.Run(name, func(t *testing.T) {
			c := failed()
			if tc.mutate != nil {
				tc.mutate(c)
			}
			r, fc, _ := repairReconciler(t, c, frozen(),
				heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true), tc.down)
			assert.Nil(t, r.heldUpgradeRepair(ctx, latestCluster(t, fc, c)))
		})
	}
}

// Staging waits while the StatefulSet created again has not got its held
// template back, and writes it back itself once it can: the normal pass,
// which would, does not run while the machine is active.
func TestStepUpgradeStaging_WritesTheHeldTemplateBackFirst(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	require.NoError(t, r.orphanDeleteStatefulSet(ctx, "default", "c-server"))
	grown := latestCluster(t, fc, c)
	grown.Spec.Storage.Size = "2Gi"
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))

	staging := latestCluster(t, fc, c)
	staging.Spec.Image.Tag = holdTarget
	require.NoError(t, fc.Update(ctx, staging))
	now := metav1.Now()
	require.NoError(t, r.patchUpgradeStatus(ctx, latestCluster(t, fc, c), func(us *neo4jv1beta1.UpgradeStatus) {
		us.Phase, us.TargetVersion, us.StepStartTime = upgradePhaseStaging, holdTarget, &now
	}))

	_, err := r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase, "it waits")
	assert.Contains(t, emu.sts().Annotations, recreateRestorePartitionAnnotation, "the controller has not recorded the revision yet")

	emu.observe()
	_, err = r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	sts := emu.sts()
	assert.NotContains(t, sts.Annotations, recreateRestorePartitionAnnotation)
	assert.Equal(t, "neo4j:"+neverPublished, sts.Spec.Template.Spec.Containers[0].Image, "the held template, back")
	emu.observe()

	_, err = r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, upgradePhaseRolling, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)
	assert.Equal(t, "neo4j:"+holdTarget, emu.sts().Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, int32(2), *statefulSetPartition(emu.sts()), "server-2, down, is replaced first")
}

// The pass that deleted the StatefulSet for an expansion creates it again as
// soon as the delete has gone through, from the uncached read.
func TestWaitStatefulSetGone(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "c-server", Namespace: "default"}
	scheme := makeUpgradeScheme()
	gone := fake.NewClientBuilder().WithScheme(scheme).Build()
	assert.True(t, waitStatefulSetGone(ctx, gone, gone, key))

	there := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "c-server", Namespace: "default"}}).Build()
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	assert.False(t, waitStatefulSetGone(short, gone, there, key), "the API server still has it")
	assert.False(t, waitStatefulSetGone(short, there, gone, key), "the cache still has it")
}

// jsonRevisionData is a ControllerRevision's data for template, as the
// StatefulSet controller stores it.
func jsonRevisionData(template corev1.PodTemplateSpec) (runtime.RawExtension, error) {
	raw, err := json.Marshal(map[string]any{"spec": map[string]any{"template": template}})
	return runtime.RawExtension{Raw: raw}, err
}
