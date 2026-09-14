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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Composite databases against a real server.
//
// Extended tier: it stands up a Neo4j deployment, which the core lane must not
// do for a feature this cheap to break in cheaper ways. What it covers is
// exactly the behaviour no fake can reach — the Cypher the operator emits
// being accepted, the constituents actually appearing in SHOW DATABASE, and
// the cascade delete leaving target databases alone.
//
// Every assertion here mirrors something established by hand against 5.26.30
// and 2026.08.1 during the design spike; this is what keeps those findings
// true as the code moves.
var _ = Describe("Composite Database (deployed)", Label("extended"), Serial, func() {
	const (
		password  = "compositeTest123"
		targetOne = "movies-latest"
		targetTwo = "movies-upcoming"
	)

	var (
		ctx            context.Context
		testNamespace  string
		standaloneName string
		podName        string
		standalone     *neo4jv1beta1.Neo4jEnterpriseStandalone
		composite      *neo4jv1beta1.Neo4jCompositeDatabase
	)

	cypher := func(query string) (string, error) {
		out, err := execOut(ctx, podName, testNamespace,
			"cypher-shell", "-a", "bolt://localhost:7687", "-u", "neo4j", "-p", password,
			"--non-interactive", query)
		return string(out), err
	}

	// cypherOn runs against a named session database. A composite's
	// constituent is reachable ONLY from a session on the composite itself:
	//
	//	42N04: Failed to access database identified by `cineasts.latest` while
	//	       connected to session database `neo4j`.
	//	       Connect to `cineasts.latest` directly.
	//
	// That last sentence is the server's advice and it does not work —
	// connecting to the constituent gives 22N51, graph reference not found.
	// Connect to the COMPOSITE and USE the constituent from there.
	cypherOn := func(database, query string) (string, error) {
		out, err := execOut(ctx, podName, testNamespace,
			"cypher-shell", "-a", "bolt://localhost:7687", "-u", "neo4j", "-p", password,
			"--non-interactive", "-d", database, query)
		return string(out), err
	}

	BeforeEach(func() {
		ctx = context.Background()
		testNamespace = createTestNamespace("composite-deployed")
		standaloneName = fmt.Sprintf("composite-sa-%d", GinkgoRandomSeed())
		podName = standaloneName + "-0"

		By("Deploying a standalone")
		standalone = createBasicStandalone(standaloneName, testNamespace)
		standalone.Spec.Auth = &neo4jv1beta1.AuthSpec{AdminSecret: "neo4j-admin-secret"}
		applyCIOptimizationsStandalone(standalone)
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: testNamespace},
			Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte(password)},
			Type:       corev1.SecretTypeOpaque,
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, standalone)).To(Succeed())

		Eventually(func() bool {
			s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: standaloneName, Namespace: testNamespace,
			}, s); err != nil {
				return false
			}
			return s.Status.Ready
		}, clusterTimeout, interval).Should(BeTrue(), "standalone should become Ready")
		waitForBoltReady(ctx, podName, testNamespace, password, 5*time.Minute)

		By("Creating the constituent target databases")
		for _, db := range []string{targetOne, targetTwo} {
			_, err := cypher(fmt.Sprintf("CREATE DATABASE `%s` IF NOT EXISTS WAIT", db))
			Expect(err).ToNot(HaveOccurred())
		}
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
		if standalone != nil {
			if len(standalone.GetFinalizers()) > 0 {
				standalone.SetFinalizers([]string{})
				_ = k8sClient.Update(ctx, standalone)
			}
			_ = k8sClient.Delete(ctx, standalone)
			standalone = nil
		}
		if testNamespace != "" {
			cleanupCustomResourcesInNamespace(testNamespace)
		}
	})

	It("creates the composite and its constituents, enforces the set, and cascades on delete", func() {
		compositeName := "cineasts"

		By("Applying a composite with two constituents")
		composite = &neo4jv1beta1.Neo4jCompositeDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: compositeName, Namespace: testNamespace},
			Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{
				ClusterRef: standaloneName,
				Constituents: []neo4jv1beta1.CompositeConstituent{
					{Name: "latest", TargetDatabase: targetOne},
					{Name: "upcoming", TargetDatabase: targetTwo},
				},
			},
		}
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: compositeName, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Ready"))

		By("Checking the server reports type=composite with both constituents")
		out, err := cypher(fmt.Sprintf(
			"SHOW DATABASE `%s` YIELD name, type, constituents", compositeName))
		Expect(err).ToNot(HaveOccurred())
		Expect(out).To(ContainSubstring(`"composite"`))
		Expect(out).To(ContainSubstring(compositeName + ".latest"))
		Expect(out).To(ContainSubstring(compositeName + ".upcoming"))

		By("Checking status.observedConstituents is read back from the server")
		Expect(composite.Status.ObservedConstituents).To(ConsistOf(
			compositeName+".latest", compositeName+".upcoming"))

		By("Querying through the composite")
		queried, err := cypherOn(compositeName,
			fmt.Sprintf("USE `%s`.latest MATCH (n) RETURN count(n) AS n", compositeName))
		Expect(err).ToNot(HaveOccurred(),
			"a constituent must be queryable from a session on its composite")
		Expect(queried).To(ContainSubstring("n"))

		By("Checking graph.names() reports the constituents from the composite session")
		names, err := cypherOn(compositeName, "RETURN graph.names() AS names")
		Expect(err).ToNot(HaveOccurred())
		Expect(names).To(ContainSubstring(compositeName + ".latest"))

		By("Confirming a constituent is NOT reachable from another database's session")
		_, err = cypher(fmt.Sprintf("USE `%s`.latest MATCH (n) RETURN count(n) AS n", compositeName))
		Expect(err).To(HaveOccurred(),
			"cross-composite access from the default database must be refused (42N04)")

		By("Removing a constituent from spec — enforceConstituents defaults true, so it goes")
		Eventually(func() error {
			latest := &neo4jv1beta1.Neo4jCompositeDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: compositeName, Namespace: testNamespace,
			}, latest); err != nil {
				return err
			}
			latest.Spec.Constituents = []neo4jv1beta1.CompositeConstituent{
				{Name: "latest", TargetDatabase: targetOne},
			}
			return k8sClient.Update(ctx, latest)
		}, clusterTimeout, interval).Should(Succeed())

		// Returning (string, error): Gomega fails the assertion outright on a
		// non-nil error rather than matching against the value. Swallowing the
		// error and returning its text instead would make this assertion PASS
		// whenever cypher-shell failed — "error: …" does not contain
		// ".upcoming" either — which is a green test for a broken server.
		Eventually(func() (string, error) {
			return cypher(fmt.Sprintf(
				"SHOW DATABASE `%s` YIELD constituents", compositeName))
		}, clusterTimeout, interval).ShouldNot(ContainSubstring(compositeName+".upcoming"),
			"a constituent absent from spec must be dropped when enforceConstituents is true")

		By("Deleting the CR — CASCADE removes the aliases, never the target databases")
		Expect(k8sClient.Delete(ctx, composite)).To(Succeed())
		Eventually(func() (bool, error) {
			out, err := cypher("SHOW DATABASES YIELD name, type RETURN name, type")
			if err != nil {
				return false, err
			}
			return !strings.Contains(out, `"`+compositeName+`"`), nil
		}, clusterTimeout, interval).Should(BeTrue(), "the composite should be dropped")

		out, err = cypher("SHOW DATABASES YIELD name, type RETURN name, type")
		Expect(err).ToNot(HaveOccurred())
		Expect(out).To(ContainSubstring(targetOne),
			"CASCADE ALIASES removes the constituent ALIASES — the target databases must survive")
		Expect(out).To(ContainSubstring(targetTwo))
		composite = nil
	})

	// The trap the design spike found, and the reason the composite and its
	// constituents live in one CR. `CREATE ALIAS x.y` succeeds with no
	// composite `x`; the composite can then never be created (42N87), and the
	// server's error never mentions ordering.
	It("reports a blocked namespace when a dotted alias already squats it", func() {
		compositeName := "blocked"

		By("Creating a dotted alias by hand, with no composite behind it")
		_, err := cypher(fmt.Sprintf(
			"CREATE ALIAS `%s`.squatter FOR DATABASE `%s`", compositeName, targetOne))
		Expect(err).ToNot(HaveOccurred(),
			"Neo4j accepts this even though no composite of that name exists — that is the trap")

		composite = &neo4jv1beta1.Neo4jCompositeDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: compositeName, Namespace: testNamespace},
			Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{
				ClusterRef: standaloneName,
				Constituents: []neo4jv1beta1.CompositeConstituent{
					{Name: "latest", TargetDatabase: targetOne},
				},
			},
		}
		Expect(k8sClient.Create(ctx, composite)).To(Succeed())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: compositeName, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Failed"))

		// The whole value is the message: the server's own 42N87 says only
		// that two names conflict, never that ordering caused it or which
		// alias to remove.
		Expect(composite.Status.Message).To(ContainSubstring("squatter"))
		Expect(composite.Status.Message).To(ContainSubstring("occupies its namespace"))

		By("Dropping the squatter — the composite then converges on its own")
		_, err = cypher(fmt.Sprintf("DROP ALIAS `%s`.squatter IF EXISTS FOR DATABASE", compositeName))
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() string {
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: compositeName, Namespace: testNamespace,
			}, composite); err != nil {
				return ""
			}
			return composite.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Ready"))
	})
})
