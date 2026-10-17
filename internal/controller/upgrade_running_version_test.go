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
	"strings"
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
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Upgrades and the version a deployment runs (operations.md id 144). envtest
// runs no StatefulSet controller and no Neo4j, so these specs write the
// StatefulSet status a rollout would leave, and drive their own reconciler for
// stand-in clusters, which the suite's cluster controller leaves alone.

// rollStatefulSet writes the status the StatefulSet controller writes once
// every replica runs the current template and is ready (ready = replicas), or
// one where none is ready (ready = 0).
func rollStatefulSet(ctx context.Context, key types.NamespacedName, ready int32) {
	Expect(tryRollStatefulSet(ctx, key, ready)).To(Succeed())
}

func tryRollStatefulSet(ctx context.Context, key types.NamespacedName, ready int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, key, sts); err != nil {
			return err
		}
		replicas := int32(1)
		if sts.Spec.Replicas != nil {
			replicas = *sts.Spec.Replicas
		}
		rev := fmt.Sprintf("%s-%d", sts.Name, sts.Generation)
		sts.Status = appsv1.StatefulSetStatus{
			ObservedGeneration: sts.Generation,
			Replicas:           replicas,
			ReadyReplicas:      ready,
			CurrentReplicas:    replicas,
			UpdatedReplicas:    replicas,
			CurrentRevision:    rev,
			UpdateRevision:     rev,
		}
		return k8sClient.Status().Update(ctx, sts)
	})
}

func serverImage(ctx context.Context, key types.NamespacedName) string {
	sts := &appsv1.StatefulSet{}
	Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
	return sts.Spec.Template.Spec.Containers[0].Image
}

// deleteCluster removes a cluster these specs created, finalizers first.
func deleteCluster(ctx context.Context, key types.NamespacedName) {
	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := k8sClient.Get(ctx, key, latest); err == nil {
		if len(latest.GetFinalizers()) > 0 {
			latest.SetFinalizers([]string{})
			_ = k8sClient.Update(ctx, latest)
		}
		_ = k8sClient.Delete(ctx, latest)
	}
	_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: key.Name + "-server", Namespace: key.Namespace}})
}

var _ = Describe("Neo4jEnterpriseCluster with upgradeStrategy Recreate", func() {
	const (
		running = "2026.08.1-enterprise"
		target  = "2026.09.0-enterprise"
	)
	var key types.NamespacedName

	AfterEach(func() { deleteCluster(context.Background(), key) })

	// Recreate made handleRollingUpgrade return before the state machine
	// started, and Reconcile returns what it returns: the "regular
	// reconciliation" it logged never ran. A Ready cluster kept its old image,
	// and nothing else in its spec was applied while the tag differed.
	It("upgrades a Ready cluster through the rolling-upgrade state machine", func() {
		ctx := context.Background()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("recreate-%d", time.Now().UnixNano()),
				Namespace:  "default",
				Labels:     standInLabels(),
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: target},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				UpgradeStrategy:        &neo4jv1beta1.UpgradeStrategySpec{Strategy: "Recreate"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key = types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
		// The CRD defaults both to true, and a typed false is omitted: set
		// them with a patch, so the step needs no Neo4j to talk to.
		Expect(k8sClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType,
			[]byte(`{"spec":{"upgradeStrategy":{"preUpgradeHealthCheck":false},"monitoring":{"enabled":false}}}`)))).To(Succeed())

		// The server StatefulSet of a Ready cluster on the running version.
		three := int32(3)
		selector := map[string]string{"neo4j.com/cluster": cluster.Name, "neo4j.com/server-name": "server"}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-server", Namespace: "default"},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    &three,
				ServiceName: cluster.Name + "-headless",
				Selector:    &metav1.LabelSelector{MatchLabels: selector},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: selector},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:" + running}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		rollStatefulSet(ctx, client.ObjectKeyFromObject(sts), 3)

		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		cluster.Status.Phase = neo4jv1beta1.PhaseReady
		cluster.Status.Version = running
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		rec := record.NewFakeRecorder(50)
		r := &controller.Neo4jEnterpriseClusterReconciler{
			Client:       k8sClient,
			Scheme:       scheme.Scheme,
			Recorder:     rec,
			Validator:    validation.NewClusterValidator(k8sClient),
			RequeueAfter: time.Second,
		}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.UpgradeStatus).NotTo(BeNil(), "the upgrade did not start")
		Expect(got.Status.UpgradeStatus.Phase).To(Equal("Staging"))
		Expect(got.Status.UpgradeStatus.PreviousVersion).To(Equal(running))
		Expect(got.Status.UpgradeStatus.TargetVersion).To(Equal(target))

		var started, warned bool
		for len(rec.Events) > 0 {
			e := <-rec.Events
			started = started || strings.HasPrefix(e, "Normal "+controller.EventReasonUpgradeStarted+" ")
			warned = warned || (strings.HasPrefix(e, "Warning "+controller.EventReasonValidationWarning+" ") &&
				strings.Contains(e, "spec.upgradeStrategy.strategy"))
		}
		Expect(started).To(BeTrue(), "UpgradeStarted is announced")
		Expect(warned).To(BeTrue(), "the ValidationWarning says Recreate does what RollingUpgrade does")
	})
})

var _ = Describe("Neo4jEnterpriseStandalone image tag against the version it runs", func() {
	const (
		running = "2026.09.0-enterprise"
		earlier = "2026.08.1-enterprise"
	)
	var standalone *neo4jv1beta1.Neo4jEnterpriseStandalone

	AfterEach(func() {
		ctx := context.Background()
		if standalone == nil {
			return
		}
		latest := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(standalone), latest); err == nil {
			latest.SetFinalizers(nil)
			_ = k8sClient.Update(ctx, latest)
			_ = k8sClient.Delete(ctx, latest)
		}
		standalone = nil
	})

	// The standalone controller ran ValidateCreate only; the update-only method
	// with the image rule was never called, and that rule allowed everything
	// anyway. A tag below the version the standalone ran went straight into its
	// StatefulSet, and Neo4j does not support downgrades. The suite's own
	// standalone controller reconciles this one.
	It("refuses a tag below the version the standalone runs and leaves the server on it", func() {
		ctx := context.Background()
		adminSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("testpassword")},
		}
		if err := k8sClient.Create(ctx, adminSecret); err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
		standalone = &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("sa-downgrade-%d", time.Now().UnixNano()), Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: running},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				Auth:                   &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"},
			},
		}
		Expect(k8sClient.Create(ctx, standalone)).To(Succeed())
		key := client.ObjectKeyFromObject(standalone)

		By("running the first version")
		Eventually(func() error {
			return k8sClient.Get(ctx, key, &appsv1.StatefulSet{})
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		Expect(serverImage(ctx, key)).To(Equal("neo4j:" + running))
		// Rolled again on each poll: the controller may still be writing the
		// template it just created, which the StatefulSet controller would
		// observe and roll in turn.
		Eventually(func() string {
			_ = tryRollStatefulSet(ctx, key, 1)
			got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			_ = k8sClient.Get(ctx, key, got)
			return got.Status.Version
		}, 30*time.Second, 500*time.Millisecond).Should(Equal(running))

		By("moving the tag to an earlier version")
		Eventually(func() error {
			got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			if err := k8sClient.Get(ctx, key, got); err != nil {
				return err
			}
			got.Spec.Image.Tag = earlier
			return k8sClient.Update(ctx, got)
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		Eventually(func() string {
			got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			_ = k8sClient.Get(ctx, key, got)
			return got.Status.Phase + ": " + got.Status.Message
		}, 30*time.Second, 200*time.Millisecond).Should(And(
			HavePrefix("Failed: Validation failed: "),
			ContainSubstring("downgrades are not supported: the standalone runs "+running+" (status.version)")))

		// The server keeps the image it runs, and status.version says so.
		Consistently(func() string { return serverImage(ctx, key) }, 2*time.Second, 200*time.Millisecond).
			Should(Equal("neo4j:" + running))
		got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.Version).To(Equal(running))
	})
})
