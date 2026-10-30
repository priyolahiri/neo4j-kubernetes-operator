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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// operations.md id 229: a raise right after a drain started the removed
// servers again on their old volumes, as the servers Neo4j had dropped
// (v1.24.0 journey, #619 S1). Through a real Reconcile: the replicas stay
// where the drain left them while a removed server's volume is there, and
// the raise goes through once it is gone.
var _ = Describe("Neo4jEnterpriseCluster raised while a removed server's volume is there", func() {
	var key types.NamespacedName

	AfterEach(func() {
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-server-%d", key.Name, i), Namespace: key.Namespace}},
				client.GracePeriodSeconds(0))
		}
		_ = k8sClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + key.Name + "-server-3", Namespace: key.Namespace}})
		deleteCluster(ctx, key)
	})

	It("keeps the replicas until the volume is gone, then adds the servers", func() {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("raise-hold-%d", time.Now().UnixNano()),
				Namespace:  "default",
				Labels:     standInLabels(),
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key = types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
		Expect(k8sClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, []byte(
			`{"spec":{"monitoring":{"enabled":false}}}`)))).To(Succeed())
		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		cluster.Status.Phase = "Ready"
		cluster.Status.Version = "2026.08.1-enterprise"
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		// The StatefulSet the drain left at 3 replicas, its servers' pods,
		// and server-3's volume, which the StatefulSet controller handed to
		// the removed pod for the garbage collector.
		sts := resources.BuildServerStatefulSetForEnterprise(cluster)
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		for i := 0; i < 3; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-server-%d", key.Name, i), Namespace: key.Namespace,
					Labels: map[string]string{"neo4j.com/cluster": key.Name, "neo4j.com/clustering": "true"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:2026.08.1-enterprise"}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		}
		volume := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data-" + key.Name + "-server-3", Namespace: key.Namespace,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: key.Name + "-server-3", UID: "removed-pod"}},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}
		Expect(k8sClient.Create(ctx, volume)).To(Succeed())

		// Raised to 5.
		Expect(k8sClient.Patch(ctx, &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}},
			client.RawPatch(types.MergePatchType, []byte(`{"spec":{"topology":{"servers":5}}}`)))).To(Succeed())
		Eventually(func() int32 {
			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := mgr.GetClient().Get(ctx, key, got); err != nil {
				return 0
			}
			return got.Spec.Topology.Servers
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(int32(5)))
		c := mgr.GetClient()
		r := &controller.Neo4jEnterpriseClusterReconciler{
			Client: c, Scheme: scheme.Scheme, Recorder: record.NewFakeRecorder(100),
			Validator: validation.NewClusterValidator(c), ConfigMapManager: controller.NewConfigMapManager(c),
			RequeueAfter: 30 * time.Second, APIReader: k8sClient,
		}

		var res ctrl.Result
		for pass := 0; pass < 2; pass++ {
			res, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		}
		replicas := func() int32 {
			got := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: key.Name + "-server", Namespace: key.Namespace}, got)).To(Succeed())
			return *got.Spec.Replicas
		}
		Expect(replicas()).To(Equal(int32(3)), "no server is added while server-3's volume is there")
		Expect(res.RequeueAfter).To(BeNumerically("<=", 5*time.Second), "it looks again soon")
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		var deferred *metav1.Condition
		for i := range got.Status.Conditions {
			if got.Status.Conditions[i].Type == controller.ConditionTypeTopologyChangeDeferred {
				deferred = &got.Status.Conditions[i]
			}
		}
		Expect(deferred).NotTo(BeNil())
		Expect(deferred.Reason).To(Equal(controller.ConditionReasonWaitsForRemovedServers))
		Expect(deferred.Message).To(ContainSubstring("spec.topology.servers is 5; the server StatefulSet keeps 3 servers until"))

		// The collector deletes the volume: the raise goes through.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(volume), volume)).To(Succeed())
		if len(volume.Finalizers) > 0 {
			volume.Finalizers = nil
			Expect(k8sClient.Update(ctx, volume)).To(Succeed())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, volume))).To(Succeed())
		Eventually(func() bool {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(volume), &corev1.PersistentVolumeClaim{}) != nil
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(replicas()).To(Equal(int32(5)))
	})
})
