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

// A Paused or Failed upgrade keeps the StatefulSet partition where the upgrade
// state machine left it (operations.md id 149). The normal pass applies the
// builder's StatefulSet, whose partition is unset; it used to keep the live
// partition only while the machine was active, so the pass after a Paused or
// Failed upgrade set it to 0 and the StatefulSet controller restarted every
// server the machine had not rolled onto the target image, with only the
// Kubernetes readiness probe between them — none of the machine's gates.
//
// envtest runs no StatefulSet controller and no Neo4j: the spec writes the
// StatefulSet the machine leaves when server-2 has rolled (partition 2) and
// drives the normal pass with its own reconciler.
var _ = Describe("Neo4jEnterpriseCluster whose upgrade is Paused or Failed", func() {
	const (
		running = "2026.08.1-enterprise" // status.version: what every server ran before the upgrade
		target  = "2026.09.0-enterprise" // the upgrade's target, rolled onto server-2 only
	)
	var key types.NamespacedName

	AfterEach(func() { deleteCluster(context.Background(), key) })

	heldAt := func(phase string, partition int32) *controller.Neo4jEnterpriseClusterReconciler {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("held-%d", time.Now().UnixNano()),
				Namespace:  "default",
				Labels:     standInLabels(),
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: target},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				UpgradeStrategy:        &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: phase == "Paused"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key = types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
		// A typed false is omitted and the CRD defaults it to true: patch it.
		pause := "false"
		if phase == "Paused" {
			pause = "true"
		}
		Expect(k8sClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, []byte(
			`{"spec":{"upgradeStrategy":{"autoPauseOnFailure":`+pause+`},"monitoring":{"enabled":false}}}`)))).To(Succeed())

		// The server StatefulSet as Staging and the walk left it: the target
		// template, server-2 rolled onto it, server-0 and server-1 still on
		// the version they ran.
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		sts := resources.BuildServerStatefulSetForEnterprise(cluster)
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition}
		Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("neo4j:" + target))
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(sts), latest); err != nil {
				return err
			}
			latest.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: latest.Generation,
				Replicas:           3,
				ReadyReplicas:      3,
				CurrentReplicas:    partition,
				UpdatedReplicas:    3 - partition,
				CurrentRevision:    sts.Name + "-before",
				UpdateRevision:     sts.Name + "-target",
			}
			return k8sClient.Status().Update(ctx, latest)
		})).To(Succeed())

		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		now := metav1.Now()
		cluster.Status.Phase = phase
		cluster.Status.Version = running
		cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
			Phase: phase, PreviousVersion: running, TargetVersion: target,
			StartTime: &now, StepStartTime: &now, CurrentPartition: &partition,
			LastError: "timed out waiting for the databases on server ordinal 2 to catch up (d07)",
			Progress:  &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: partition},
		}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		// The manager's client, as in production: the ConfigMap's owner
		// reference is built from the cluster's type, which a cached read
		// carries. Wait until its cache has seen what this spec wrote.
		c := mgr.GetClient()
		Eventually(func() string {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := c.Get(ctx, key, got); err != nil || got.Status.UpgradeStatus == nil {
				return ""
			}
			return got.Status.UpgradeStatus.Phase
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(phase))
		Eventually(func() int32 {
			got := &appsv1.StatefulSet{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(sts), got); err != nil {
				return 0
			}
			return got.Status.ReadyReplicas
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(int32(3)))

		return &controller.Neo4jEnterpriseClusterReconciler{
			Client:           c,
			Scheme:           scheme.Scheme,
			Recorder:         record.NewFakeRecorder(100),
			Validator:        validation.NewClusterValidator(c),
			ConfigMapManager: controller.NewConfigMapManager(c),
			RequeueAfter:     time.Second,
		}
	}

	livePartition := func() *int32 {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace}, sts)).To(Succeed())
		if sts.Spec.UpdateStrategy.RollingUpdate == nil {
			return nil
		}
		return sts.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	DescribeTable("the normal pass keeps the partition, so no server the upgrade has not rolled restarts onto the target",
		func(phase string) {
			ctx := context.Background()
			r := heldAt(phase, 2)

			for pass := 0; pass < 2; pass++ {
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			}

			Expect(livePartition()).To(HaveValue(Equal(int32(2))),
				"server-0 and server-1 must stay on %s until the upgrade resumes", running)
			Expect(serverImage(ctx, types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace})).
				To(Equal("neo4j:" + target))

			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
			Expect(got.Status.UpgradeStatus.Phase).To(Equal(phase), "the upgrade stays where it stopped")
			Expect(got.Status.Version).To(Equal(running), "nothing verified a new version")

			// Only the rolling is held: the rest of the pass ran.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: key.Name + "-config", Namespace: key.Namespace}, &corev1.ConfigMap{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: key.Name + "-client", Namespace: key.Namespace}, &corev1.Service{})).To(Succeed())
		},
		Entry("Paused (autoPauseOnFailure)", "Paused"),
		Entry("Failed", "Failed"),
	)

	// status.version still names the version the servers ran before the
	// upgrade, so it alone let the tag go back to it; server-2 runs the target,
	// and going back would roll it back.
	It("refuses moving the tag back once the upgrade has restarted a server, and keeps holding", func() {
		ctx := context.Background()
		r := heldAt("Paused", 2)
		Eventually(func() error {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(ctx, key, got); err != nil {
				return err
			}
			got.Spec.Image.Tag = running
			return k8sClient.Update(ctx, got)
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
		Eventually(func() string {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			_ = mgr.GetClient().Get(ctx, key, got)
			return got.Spec.Image.Tag
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(running))

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred(), "a spec the user must fix is not a reconcile error")

		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal("Failed"))
		Expect(got.Status.Message).To(ContainSubstring(
			"downgrades are not supported: the upgrade to " + target + " (status.upgradeStatus.targetVersion) has already restarted servers on it"))
		Expect(got.Status.UpgradeStatus.Phase).To(Equal("Paused"))
		Expect(livePartition()).To(HaveValue(Equal(int32(2))))
		Expect(serverImage(ctx, types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace})).
			To(Equal("neo4j:"+target), "server-2 is not rolled back")
	})
})
