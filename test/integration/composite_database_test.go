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

package integration_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Composite database API and validation.
//
// These start no Neo4j deployment: they cover CRD admission and validator
// behaviour, which is version-independent, and every CR names a clusterRef
// that does not exist so nothing reaches a live server. Core tier — every PR,
// both anchors, seconds. The behaviour that DOES need a server (create,
// constituents, cascade delete) is covered by the deployed spec below and by
// the pre-release journey.
var _ = Describe("Composite Database — API and validation", Label("core"), Serial, func() {
	var (
		ctx           context.Context
		testNamespace string
		composite     *neo4jv1beta1.Neo4jCompositeDatabase
	)

	BeforeEach(func() {
		ctx = context.Background()
		testNamespace = createTestNamespace("composite")
	})

	AfterEach(func() {
		if composite != nil {
			if len(composite.GetFinalizers()) > 0 {
				composite.SetFinalizers([]string{})
				_ = k8sClient.Update(ctx, composite)
			}
			_ = k8sClient.Delete(ctx, composite)
			composite = nil
		}
		if testNamespace != "" {
			cleanupCustomResourcesInNamespace(testNamespace)
		}
	})

	newComposite := func(name string, constituents ...neo4jv1beta1.CompositeConstituent) *neo4jv1beta1.Neo4jCompositeDatabase {
		return &neo4jv1beta1.Neo4jCompositeDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{
				ClusterRef: "does-not-exist-yet",
				Constituents: append([]neo4jv1beta1.CompositeConstituent{},
					constituents...),
			},
		}
	}

	It("accepts a well-formed composite and goes Pending on a nonexistent cluster ref", func() {
		composite = newComposite("cineasts",
			neo4jv1beta1.CompositeConstituent{Name: "latest", TargetDatabase: "movies-latest"})
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		// A composite whose deployment does not exist yet is an ordinary
		// apply-everything-together state, not a spec error.
		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: composite.Name, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Pending"))
	})

	// The CRD schema carries the name pattern, so this is rejected by the API
	// server before the operator sees it. Underscores are illegal in Neo4j
	// database names — the server's own words are "use simple ascii
	// characters, numbers, dots and dashes".
	It("rejects an underscore in the composite name at admission", func() {
		bad := newComposite("bad-name",
			neo4jv1beta1.CompositeConstituent{Name: "latest", TargetDatabase: "movies"})
		bad.Spec.Name = "has_underscore"
		err := k8sClient.Create(ctx, bad)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.name"))
	})

	It("rejects a composite with no constituents", func() {
		bad := newComposite("empty")
		err := k8sClient.Create(ctx, bad)
		Expect(err).To(HaveOccurred(), "constituents is required with at least one entry")
	})

	// `<composite>.<constituent>` is how a constituent is addressed, so a dot
	// in either half breaks the namespacing.
	It("rejects a dot in a constituent name at admission", func() {
		bad := newComposite("cineasts",
			neo4jv1beta1.CompositeConstituent{Name: "a.b", TargetDatabase: "movies"})
		Expect(k8sClient.Create(ctx, bad)).ToNot(Succeed())
	})

	It("reports Failed when a constituent targets its own composite", func() {
		composite = newComposite("cineasts",
			neo4jv1beta1.CompositeConstituent{Name: "self", TargetDatabase: "cineasts"})
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: composite.Name, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Failed"))
		Expect(composite.Status.Message).To(ContainSubstring("own composite"))
	})

	It("reports Failed on duplicate constituent names", func() {
		composite = newComposite("cineasts",
			neo4jv1beta1.CompositeConstituent{Name: "latest", TargetDatabase: "movies-a"},
			neo4jv1beta1.CompositeConstituent{Name: "latest", TargetDatabase: "movies-b"})
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: composite.Name, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Failed"))
	})

	It("defaults enforceConstituents, wait and deletionPolicy", func() {
		composite = newComposite("cineasts",
			neo4jv1beta1.CompositeConstituent{Name: "latest", TargetDatabase: "movies-latest"})
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		fetched := &neo4jv1beta1.Neo4jCompositeDatabase{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: composite.Name, Namespace: testNamespace,
		}, fetched)).To(Succeed())

		Expect(fetched.Spec.EnforceConstituents).ToNot(BeNil())
		Expect(*fetched.Spec.EnforceConstituents).To(BeTrue(),
			"spec is authoritative by default, same posture as Neo4jRole.enforcePrivileges")
		Expect(fetched.Spec.Wait).ToNot(BeNil())
		Expect(*fetched.Spec.Wait).To(BeTrue())
		Expect(fetched.Spec.DeletionPolicy).To(Equal("Delete"))
	})
})
