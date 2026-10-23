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

// The v1.22.0 journey (2026-10-17): a 3-server cluster on 5.26.31-enterprise
// was moved to 5.26-enterprise, which floated to 5.26.32 that day. The upgrade
// paused after server-2 restarted on 5.26.32; setting the tag back to
// 5.26.31-enterprise was accepted, and the operator rolled server-2 back down.
// A tag without a patch says no version, so the operator records what the
// servers report (status.upgradeStatus.resolvedTargetVersion,
// status.runningVersion) and holds the tag to it (operations.md id 153).
//
// envtest runs no StatefulSet controller and no Neo4j: the spec writes the
// StatefulSet and the status the operator would have recorded, and drives a
// real Reconcile with its own reconciler. That the API server keeps the two
// new status fields is part of what it checks: a CRD without them prunes them.
var _ = Describe("Neo4jEnterpriseCluster on a tag without a patch", func() {
	const (
		pinned   = "5.26.31-enterprise"
		floating = "5.26-enterprise"
		resolved = "5.26.32"
	)
	var key types.NamespacedName

	AfterEach(func() { deleteCluster(context.Background(), key) })

	// setUp creates a cluster on spec tag, with the server StatefulSet on
	// image at partition, every server ready, and status as given; it returns
	// a reconciler on the manager's client once its cache has caught up.
	setUp := func(tag, image string, partition int32, status neo4jv1beta1.Neo4jEnterpriseClusterStatus) *controller.Neo4jEnterpriseClusterReconciler {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("floating-%d", time.Now().UnixNano()),
				Namespace:  "default",
				Labels:     standInLabels(),
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: image},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				UpgradeStrategy:        &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true, UpgradeTimeout: "30s"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key = types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
		Expect(k8sClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType,
			[]byte(`{"spec":{"monitoring":{"enabled":false}}}`)))).To(Succeed())

		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		sts := resources.BuildServerStatefulSetForEnterprise(cluster)
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(sts), latest); err != nil {
				return err
			}
			current, update := sts.Name+"-before", sts.Name+"-target"
			if partition == 0 {
				current = update
			}
			latest.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: latest.Generation, Replicas: 3, ReadyReplicas: 3,
				CurrentReplicas: partition, UpdatedReplicas: 3 - partition,
				CurrentRevision: current, UpdateRevision: update,
			}
			return k8sClient.Status().Update(ctx, latest)
		})).To(Succeed())

		// The spec tag the user sets, then the status the operator recorded.
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(ctx, key, latest); err != nil {
				return err
			}
			latest.Spec.Image.Tag = tag
			return k8sClient.Update(ctx, latest)
		})).To(Succeed())
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(ctx, key, latest); err != nil {
				return err
			}
			latest.Status = status
			return k8sClient.Status().Update(ctx, latest)
		})).To(Succeed())

		// The API server keeps what the operator records: the CRD has the fields.
		stored := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, stored)).To(Succeed())
		Expect(stored.Status.RunningVersion).To(Equal(status.RunningVersion))
		if status.UpgradeStatus != nil {
			Expect(stored.Status.UpgradeStatus.ResolvedTargetVersion).To(Equal(status.UpgradeStatus.ResolvedTargetVersion))
		}

		c := mgr.GetClient()
		Eventually(func() string {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := c.Get(ctx, key, got); err != nil {
				return ""
			}
			return got.Spec.Image.Tag + "/" + got.Status.Version
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(tag + "/" + status.Version))
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

	stsKey := func() types.NamespacedName {
		return types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace}
	}
	livePartition := func() int32 {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(context.Background(), stsKey(), sts)).To(Succeed())
		return *sts.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	It("refuses the tag set back below what a Paused upgrade's restarted server runs, and rolls nothing", func() {
		ctx := context.Background()
		partition := int32(2)
		now := metav1.Now()
		r := setUp(pinned, floating, partition, neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase: "Ready", Version: pinned,
			UpgradeStatus: &neo4jv1beta1.UpgradeStatus{
				Phase: "Paused", PreviousVersion: pinned, TargetVersion: floating, ResolvedTargetVersion: resolved,
				StartTime: &now, StepStartTime: &now, CurrentPartition: &partition,
				LastError: "timed out waiting for server ordinal 2 to roll and rejoin the cluster",
				Progress:  &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
			},
		})

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred(), "a spec the user must fix is not a reconcile error")

		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal("Failed"))
		Expect(got.Status.Message).To(ContainSubstring(
			"the upgrade to " + floating + " (status.upgradeStatus.targetVersion) has already restarted servers on it, " +
				"and they run " + resolved + " (status.upgradeStatus.resolvedTargetVersion)"))
		Expect(got.Status.UpgradeStatus.Phase).To(Equal("Paused"), "no upgrade towards the lower tag starts")
		Expect(got.Status.UpgradeStatus.TargetVersion).To(Equal(floating))
		Expect(livePartition()).To(Equal(partition))
		Expect(serverImage(ctx, stsKey())).To(Equal("neo4j:"+floating), "server-2 is not rolled back")
	})

	DescribeTable("after the upgrade completed on the floating tag, the tag is checked against what the servers report",
		func(tag string, refused bool) {
			ctx := context.Background()
			now := metav1.Now()
			r := setUp(tag, floating, 0, neo4jv1beta1.Neo4jEnterpriseClusterStatus{
				Phase: "Ready", Version: floating, RunningVersion: resolved,
				UpgradeStatus: &neo4jv1beta1.UpgradeStatus{
					Phase: "Completed", PreviousVersion: pinned, TargetVersion: floating, ResolvedTargetVersion: resolved,
					StartTime: &now, CompletionTime: &now,
					Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3},
				},
			})

			_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})

			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
			if refused {
				Expect(got.Status.Phase).To(Equal("Failed"))
				Expect(got.Status.Message).To(ContainSubstring(
					"downgrades are not supported: the servers run " + resolved + " (status.runningVersion, what " +
						floating + " in status.version resolved to)"))
				Expect(serverImage(ctx, stsKey())).To(Equal("neo4j:"+floating), "no server is rolled down")
				Expect(got.Status.UpgradeStatus.Phase).To(Equal("Completed"))
				return
			}
			Expect(got.Status.Message).NotTo(ContainSubstring("validation failed"))
			Expect(got.Status.Message).NotTo(ContainSubstring("downgrades are not supported"))
		},
		Entry("a patch below what they run is refused", pinned, true),
		Entry("the patch they run is accepted", "5.26.32-enterprise", false),
	)
})
