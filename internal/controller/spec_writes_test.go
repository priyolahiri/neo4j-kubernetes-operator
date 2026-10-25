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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The specs in this package write the operator's CRs as the operator does: a
// merge patch of what the spec changed, never a whole-object Update
// (docs/knowledge/operations.md ids 150 and 205). An Update sends the object
// as the Go types encode it. A false the CRD defaults to true is left out and
// the API server defaults it back to true, so a spec that Updates a cluster to
// change one field goes on to test a cluster nobody applied.

// patchCR applies mutate to obj and writes only what mutate changed. The patch
// carries obj's resourceVersion, so a concurrent write is a Conflict, as it is
// for an Update, and a retry loop around it still means what it did.
//
// A merge patch from Go cannot set such a field to false either: the false is
// left out of the patch too, as a removal, and the API server defaults it
// again. A spec that needs the false writes it as JSON (patchJSON).
func patchCR(ctx context.Context, obj client.Object, mutate func()) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("cannot copy %T to patch it", obj)
	}
	mutate()
	return k8sClient.Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// patchJSON writes a JSON merge patch as written, so a false in it is stored.
func patchJSON(ctx context.Context, obj client.Object, patch string) error {
	return k8sClient.Patch(ctx, obj, client.RawPatch(types.MergePatchType, []byte(patch)))
}

// releaseFinalizers removes every finalizer from obj, whatever this copy holds
// and whatever was written since: the finalizer half of an AfterEach cleanup.
func releaseFinalizers(ctx context.Context, obj client.Object) error {
	return patchJSON(ctx, obj, `{"metadata":{"finalizers":null}}`)
}

var _ = Describe("A spec's write to one of the operator's CRs", func() {
	key := types.NamespacedName{Name: "spec-write-keeps-false", Namespace: "default"}

	AfterEach(func() {
		ctx := context.Background()
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := k8sClient.Get(ctx, key, latest); err == nil {
			_ = releaseFinalizers(ctx, latest)
			_ = k8sClient.Delete(ctx, latest)
		}
	})

	// apply creates the cluster from JSON, as kubectl apply does, so the Go
	// types never see it on its way in.
	apply := func(ctx context.Context, monitoring bool) {
		Expect(k8sClient.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": neo4jv1beta1.GroupVersion.String(),
			"kind":       "Neo4jEnterpriseCluster",
			"metadata":   map[string]any{"name": key.Name, "namespace": key.Namespace},
			"spec": map[string]any{
				"image":      map[string]any{"repo": "neo4j", "tag": "5.26.0-enterprise"},
				"storage":    map[string]any{"className": "standard", "size": "1Gi"},
				"topology":   map[string]any{"servers": int64(3)},
				"monitoring": map[string]any{"enabled": monitoring},
			},
		}})).To(Succeed())
	}
	monitoringEnabled := func(ctx context.Context) bool {
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		Expect(got.Spec.Monitoring).NotTo(BeNil())
		return got.Spec.Monitoring.Enabled
	}
	// edit writes one change to the stored cluster, re-reading it on a Conflict
	// (the cluster controller writes it too).
	edit := func(ctx context.Context, write func(*neo4jv1beta1.Neo4jEnterpriseCluster) error) {
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := k8sClient.Get(ctx, key, latest); err != nil {
				return err
			}
			return write(latest)
		})).To(Succeed())
	}

	It("keeps a false the CRD defaults to true, which a whole-object Update turns back on", func() {
		ctx := context.Background()
		apply(ctx, false)
		Expect(monitoringEnabled(ctx)).To(BeFalse(), "applied as JSON, the false is stored")

		edit(ctx, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			return patchCR(ctx, c, func() { metav1.SetMetaDataAnnotation(&c.ObjectMeta, "test/edited", "patch") })
		})
		Expect(monitoringEnabled(ctx)).To(BeFalse(), "a patch sends what the spec changed, and nothing else")

		edit(ctx, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			metav1.SetMetaDataAnnotation(&c.ObjectMeta, "test/edited", "update")
			return k8sClient.Update(ctx, c) // cr-update-ok: shows the trap patchCR avoids
		})
		Expect(monitoringEnabled(ctx)).To(BeTrue(),
			"the Update left the false out and the API server defaulted it; if this fails, the CRD no longer defaults monitoring.enabled and the spec above proves nothing")
	})

	It("sets such a field to false only from JSON", func() {
		ctx := context.Background()
		apply(ctx, true)

		edit(ctx, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			return patchCR(ctx, c, func() { c.Spec.Monitoring.Enabled = false })
		})
		Expect(monitoringEnabled(ctx)).To(BeTrue(), "the patch removes the field and the API server defaults it again")

		edit(ctx, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			return patchJSON(ctx, c, `{"spec":{"monitoring":{"enabled":false}}}`)
		})
		Expect(monitoringEnabled(ctx)).To(BeFalse(), "the JSON says false, and false is stored")
	})
})
