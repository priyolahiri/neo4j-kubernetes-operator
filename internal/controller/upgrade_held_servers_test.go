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

package controller_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// The servers of a cluster whose upgrade stopped, and of one whose StatefulSet
// a storage expansion deleted, against the API server (#538). envtest runs no
// StatefulSet controller, no kubelet and no Neo4j: each spec writes the
// StatefulSet and the pods as they would be, and drives the normal pass with
// its own reconciler (the clusters are stand-ins the suite's controller leaves
// alone).
var _ = Describe("Neo4jEnterpriseCluster whose upgrade holds its servers", func() {
	const (
		running = "2026.08.1-enterprise" // what every server ran before the upgrade
		target  = "2026.09.0-enterprise" // the stopped upgrade's target, on server-2 only
		fixed   = "2026.09.1-enterprise" // the tag the user moves on to
	)
	var key types.NamespacedName

	AfterEach(func() {
		ctx := context.Background()
		for i := 0; i < 4; i++ {
			_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-server-%d", key.Name, i), Namespace: key.Namespace}},
				client.GracePeriodSeconds(0))
		}
		deleteCluster(ctx, key)
	})

	// newCluster creates a stand-in cluster on tag, with status.
	newCluster := func(tag string, mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)) *neo4jv1beta1.Neo4jEnterpriseCluster {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("held-servers-%d", time.Now().UnixNano()),
				Namespace:  "default",
				Labels:     standInLabels(),
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				UpgradeStrategy:        &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key = types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
		Expect(k8sClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, []byte(
			`{"spec":{"monitoring":{"enabled":false}}}`)))).To(Succeed())
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		mutate(cluster)
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
		return cluster
	}

	// serverPod creates server pod ordinal on image, Ready or failing to pull.
	serverPod := func(ordinal int, image string, ready bool) {
		ctx := context.Background()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-server-%d", key.Name, ordinal), Namespace: key.Namespace,
				Labels: map[string]string{"neo4j.com/cluster": key.Name, "neo4j.com/clustering": "true"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: image}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		status := corev1.ConditionTrue
		state := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		if !ready {
			status = corev1.ConditionFalse
			state = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}
		}
		pod.Status = corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "neo4j", Image: image, Ready: ready, State: state}},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// heldStatefulSet writes the server StatefulSet a stopped upgrade to
	// stoppedOn leaves: its template, partition 2, ready replicas as given.
	heldStatefulSet := func(stoppedOn string, ready int32) {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		cluster.Spec.Image.Tag = stoppedOn
		sts := resources.BuildServerStatefulSetForEnterprise(cluster)
		partition := int32(2)
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(sts), latest); err != nil {
				return err
			}
			latest.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: latest.Generation, Replicas: 3, ReadyReplicas: ready,
				CurrentReplicas: 2, UpdatedReplicas: 1,
				CurrentRevision: sts.Name + "-before", UpdateRevision: sts.Name + "-target",
			}
			return k8sClient.Status().Update(ctx, latest)
		})).To(Succeed())
	}

	// heldOn is the record of an upgrade to stoppedOn this operator paused
	// after releasing server-2, which never started on it.
	heldOn := func(stoppedOn string) func(*neo4jv1beta1.Neo4jEnterpriseCluster) {
		return func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			now := metav1.Now()
			partition := int32(2)
			c.Status.Phase = "Ready"
			c.Status.Version = running
			c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
				Phase: "Paused", PreviousVersion: running, TargetVersion: stoppedOn, StartedTargetVersion: running,
				StartTime: &now, StepStartTime: &now, CurrentPartition: &partition,
				LastError: "timed out waiting for server ordinal 2 to roll and rejoin the cluster",
				Progress:  &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
			}
		}
	}
	held := heldOn(target)

	reconciler := func() *controller.Neo4jEnterpriseClusterReconciler {
		c := mgr.GetClient()
		return &controller.Neo4jEnterpriseClusterReconciler{
			Client: c, Scheme: scheme.Scheme, Recorder: record.NewFakeRecorder(100),
			Validator: validation.NewClusterValidator(c), ConfigMapManager: controller.NewConfigMapManager(c),
			RequeueAfter: time.Second,
		}
	}

	// waitForCache waits until the manager's cache has what this spec wrote.
	waitForCache := func(check func(*neo4jv1beta1.Neo4jEnterpriseCluster) bool, pods int) {
		ctx := context.Background()
		c := mgr.GetClient()
		Eventually(func() bool {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := c.Get(ctx, key, got); err != nil || !check(got) {
				return false
			}
			list := &corev1.PodList{}
			if err := c.List(ctx, list, client.InNamespace(key.Namespace), client.MatchingLabels{"neo4j.com/cluster": key.Name}); err != nil {
				return false
			}
			return len(list.Items) == pods
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
	}

	stsKey := func() types.NamespacedName {
		return types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace}
	}

	livePartition := func() int32 {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(context.Background(), stsKey(), sts)).To(Succeed())
		Expect(sts.Spec.UpdateStrategy.RollingUpdate).NotTo(BeNil())
		Expect(sts.Spec.UpdateStrategy.RollingUpdate.Partition).NotTo(BeNil())
		return *sts.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	// operations.md id 163: servers added while the upgrade holds its
	// partition would start on its target; a drain would run mid-upgrade.
	DescribeTable("a change of spec.topology.servers waits while the upgrade holds its partition",
		func(servers int) {
			ctx := context.Background()
			newCluster(target, held)
			heldStatefulSet(target, 3)
			for i := 0; i < 3; i++ {
				serverPod(i, "neo4j:"+map[bool]string{true: target, false: running}[i == 2], true)
			}
			Expect(k8sClient.Patch(ctx, &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}},
				client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"spec":{"topology":{"servers":%d}}}`, servers))))).To(Succeed())
			waitForCache(func(c *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
				return c.Spec.Topology.Servers == int32(servers) && c.Status.UpgradeStatus != nil
			}, 3)
			r := reconciler()

			for pass := 0; pass < 2; pass++ {
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			}

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, stsKey(), sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)), "the servers stay as they are until the upgrade completes")
			Expect(livePartition()).To(Equal(int32(2)))
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
			Expect(got.Annotations).NotTo(HaveKey(controller.ScaleDownDrainingServersAnnotation), "no drain starts")
			var deferred *metav1.Condition
			for i := range got.Status.Conditions {
				if got.Status.Conditions[i].Type == controller.ConditionTypeTopologyChangeDeferred {
					deferred = &got.Status.Conditions[i]
				}
			}
			Expect(deferred).NotTo(BeNil())
			Expect(deferred.Status).To(Equal(metav1.ConditionTrue))
			Expect(deferred.Message).To(ContainSubstring(fmt.Sprintf("spec.topology.servers is %d; the server StatefulSet keeps 3 servers until the upgrade to %s", servers, target)))
		},
		Entry("a scale-up", 4),
		Entry("a scale-down", 2),
	)

	// operations.md id 162: changing the tag is the way to go on from a Paused
	// upgrade, and did nothing while the server it restarted could not start.
	// And server-2 never ran the target it could not pull, so an earlier fixed
	// tag is no downgrade: the #562 walk had 2026.09.0-enterprise refused after
	// a pause on 2026.09.9-enterprise (operations.md id 149).
	DescribeTable("replaces the servers that cannot start on the target when the tag moves on",
		func(stoppedOn, moveTo string) {
			ctx := context.Background()
			newCluster(stoppedOn, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				heldOn(stoppedOn)(c)
				c.Status.Conditions = []metav1.Condition{
					{Type: controller.ConditionTypeClusterFormed, Status: metav1.ConditionTrue, Reason: controller.ConditionReasonFormationComplete,
						LastTransitionTime: metav1.Now()},
					{Type: controller.ConditionTypeDegraded, Status: metav1.ConditionTrue, Reason: controller.ConditionReasonServersUnavailable,
						Message: "2 of 3 servers available", LastTransitionTime: metav1.Now()},
				}
			})
			heldStatefulSet(stoppedOn, 2)
			serverPod(0, "neo4j:"+running, true)
			serverPod(1, "neo4j:"+running, true)
			serverPod(2, "neo4j:"+stoppedOn, false)
			// A patch: an Update would send monitoring.enabled: false as nothing,
			// and the CRD default turns it back on (operations.md id 150).
			Expect(k8sClient.Patch(ctx, &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}},
				client.RawPatch(types.MergePatchType, []byte(`{"spec":{"image":{"tag":"`+moveTo+`"}}}`)))).To(Succeed())
			waitForCache(func(c *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
				return c.Spec.Image.Tag == moveTo && len(c.Status.Conditions) == 2 &&
					c.Spec.Monitoring != nil && !c.Spec.Monitoring.Enabled
			}, 3)
			r := reconciler()

			// Taken up, staged above server-2, then rolling from there.
			Eventually(func() string {
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
				Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
				Expect(got.Status.Phase).NotTo(Equal("Failed"), "the tag is not refused: %s", got.Status.Message)
				return got.Status.UpgradeStatus.Phase
			}, 15*time.Second, 200*time.Millisecond).Should(Equal("Rolling"))

			Expect(serverImage(ctx, stsKey())).To(Equal("neo4j:"+moveTo), "the new template reaches server-2")
			Expect(livePartition()).To(Equal(int32(2)), "server-2 is replaced first; server-0 and server-1 wait for the walk")
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
			Expect(got.Status.UpgradeStatus.TargetVersion).To(Equal(moveTo))
			Expect(*got.Status.UpgradeStatus.CurrentPartition).To(Equal(int32(2)))
			Expect(got.Status.UpgradeStatus.StartedTargetVersion).To(Equal(running), "server-2 has run nothing newer")
		},
		Entry("a later fixed tag", target, fixed),
		Entry("an earlier fixed tag, after a pause on one that was never published", "2026.09.9-enterprise", target),
	)

	// A storage expansion orphan-deletes the StatefulSet; the next pass creates
	// it again. While an image change is held (the cluster is not Ready), the
	// StatefulSet created again keeps the image the servers run.
	It("creates the StatefulSet a storage expansion deleted on the image the servers run", func() {
		ctx := context.Background()
		newCluster(target, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.Phase = "Forming"
			c.Status.Version = running
		})
		for i := 0; i < 3; i++ {
			serverPod(i, "neo4j:"+running, i != 2)
		}
		waitForCache(func(c *neo4jv1beta1.Neo4jEnterpriseCluster) bool { return c.Status.Phase == "Forming" }, 3)
		r := reconciler()

		_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})

		Eventually(func() error {
			return k8sClient.Get(ctx, stsKey(), &appsv1.StatefulSet{})
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(serverImage(ctx, stsKey())).To(Equal("neo4j:"+running),
			"the held change to %s waits for the upgrade, not a roll of every server", target)
		err := k8sClient.Get(ctx, key, &neo4jv1beta1.Neo4jEnterpriseCluster{})
		Expect(errors.IsNotFound(err)).To(BeFalse())
	})
})
