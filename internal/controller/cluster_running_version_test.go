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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// The cluster's update-only validation read the cluster twice and compared the
// two copies, so its version rule never fired (operations.md id 139). The one
// version check left was the upgrade machine's, made when an upgrade starts —
// and a target changed while an upgrade is in flight re-stages without one, so
// a downgrade below what the servers run went straight to the StatefulSet.
// The rule now compares spec.image.tag with status.version on every pass.
//
// The suite's cluster controller runs without a validator and leaves stand-in
// clusters alone, so these specs drive their own reconciler, with one, against
// the real API server.
var _ = Describe("Neo4jEnterpriseCluster image tag against the version it runs", func() {
	const (
		running = "2026.08.1-enterprise" // status.version: what every server was verified to run
		target  = "2026.09.0-enterprise" // the upgrade in flight
	)
	var (
		cluster *neo4jv1beta1.Neo4jEnterpriseCluster
		rec     *record.FakeRecorder
		r       *controller.Neo4jEnterpriseClusterReconciler
	)

	// midUpgrade creates a cluster whose upgrade to target is rolling, with the
	// spec already edited to tag.
	midUpgrade := func(tag string) types.NamespacedName {
		ctx := context.Background()
		cluster = &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("tag-vs-running-%d", time.Now().UnixNano()),
				Namespace: "default",
				Labels:    standInLabels(),
				// Accepted on an earlier pass, as any cluster mid-upgrade was.
				Finalizers: []string{controller.ClusterFinalizer},
			},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

		Expect(k8sClient.Get(ctx, key, cluster)).To(Succeed())
		now := metav1.Now()
		three := int32(3)
		cluster.Status.Phase = "Forming" // not formed, so no diagnostics are attempted
		cluster.Status.Version = running
		cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
			Phase: "Rolling", PreviousVersion: running, TargetVersion: target,
			StartTime: &now, StepStartTime: &now, CurrentPartition: &three,
		}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		rec = record.NewFakeRecorder(50)
		r = &controller.Neo4jEnterpriseClusterReconciler{
			Client:       k8sClient,
			Scheme:       scheme.Scheme,
			Recorder:     rec,
			Validator:    validation.NewClusterValidator(k8sClient),
			RequeueAfter: time.Second,
		}
		return key
	}

	AfterEach(func() {
		if cluster != nil {
			ctx := context.Background()
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, latest); err == nil {
				if len(latest.GetFinalizers()) > 0 {
					_ = releaseFinalizers(ctx, latest)
				}
				_ = k8sClient.Delete(ctx, latest)
			}
			cluster = nil
		}
	})

	It("refuses a target below the version the servers run, and leaves the upgrade where it was", func() {
		ctx := context.Background()
		key := midUpgrade("2025.10.0-enterprise")

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred(), "a spec the user must fix is not a reconcile error")

		// Not re-staged to the downgrade: the servers keep the template they
		// were rolling to.
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.UpgradeStatus).NotTo(BeNil())
		Expect(got.Status.UpgradeStatus.TargetVersion).To(Equal(target))
		Expect(got.Status.UpgradeStatus.Phase).To(Equal("Rolling"))

		Expect(got.Status.Phase).To(Equal("Failed"))
		Expect(got.Status.Message).To(HavePrefix("Update validation failed: "))
		Expect(got.Status.Message).To(ContainSubstring("spec.image.tag"))
		Expect(got.Status.Message).To(ContainSubstring(running + " (status.version)"))
		Expect(res.RequeueAfter).To(Equal(time.Second), "refused again on the validation requeue until the tag is fixed")

		var refused bool
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.HasPrefix(e, "Warning "+controller.EventReasonValidationFailed+" Cluster update validation failed: ") {
				refused = true
			}
		}
		Expect(refused).To(BeTrue(), "the refusal is announced as a ValidationFailed event")
	})

	It("lets a target change at or above the version the servers run re-stage the upgrade", func() {
		ctx := context.Background()
		key := midUpgrade("2026.10.0-enterprise")

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Status.Phase).NotTo(Equal("Failed"), got.Status.Message)
		Expect(got.Status.UpgradeStatus.Phase).To(Equal("Staging"))
		Expect(got.Status.UpgradeStatus.TargetVersion).To(Equal("2026.10.0-enterprise"))

		// Announced once, by the write that records the new target.
		var retargeted []string
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.HasPrefix(e, "Normal "+controller.EventReasonUpgradeRetargeted+" ") {
				retargeted = append(retargeted, e)
			}
		}
		Expect(retargeted).To(ConsistOf("Normal " + controller.EventReasonUpgradeRetargeted +
			" Rolling upgrade retargeted from " + target + " to 2026.10.0-enterprise during Rolling; the new target is staged"))
	})
})
