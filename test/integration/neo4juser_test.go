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

// These tests verify Neo4jUser end-to-end against a live cluster:
//  1. A Neo4jUser created with a password Secret + role binding ends up
//     visible via SHOW USERS with the right roles.
//  2. Rotating the password Secret causes the controller to re-issue
//     ALTER USER SET PASSWORD; status.passwordLastRotated advances.
//  3. Deleting the Neo4jUser drops the user from Neo4j (deletionPolicy
//     defaults to Delete).

var _ = Describe("Neo4jUser end-to-end", Label("core"), func() {
	const (
		testTimeout = time.Second * 600
	)

	var (
		testCtx     context.Context
		namespace   *corev1.Namespace
		user        *neo4jv1beta1.Neo4jUser
		creds       *corev1.Secret
		clusterName string
		adminPass   string
		userPass    string
		newUserPass string
	)

	BeforeEach(func() {
		testCtx = context.Background()

		if !isOperatorRunning() {
			Skip("Operator must be running in the cluster for integration tests")
		}

		userPass = randomPassword(18)
		newUserPass = randomPassword(18)

		// Reuse the shared native-auth cluster (see shared_cluster_test.go).
		var nsName string
		clusterName, nsName, adminPass = useSharedNativeCluster(testCtx)
		namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}

		creds = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "appuser-creds", Namespace: nsName},
			Data:       map[string][]byte{"password": []byte(userPass)},
		}
		Expect(k8sClient.Create(testCtx, creds)).To(Succeed())
	})

	AfterEach(func() {
		// Shared cluster torn down in AfterSuite — delete only this spec's CRs.
		if user != nil {
			if len(user.GetFinalizers()) > 0 {
				user.SetFinalizers([]string{})
				_ = k8sClient.Update(testCtx, user)
			}
			_ = k8sClient.Delete(testCtx, user)
			user = nil
		}
		if creds != nil {
			_ = k8sClient.Delete(testCtx, creds)
			creds = nil
		}
	})

	// Regression: a Neo4jUser must recover when its recorded password hash is
	// lost while the Neo4j user already exists with the desired password.
	//
	// This used to livelock permanently. The hash is written to status only at
	// the end of a fully successful pass, so any interruption before that —
	// operator restart, update conflict, or the post-create SHOW USERS racing
	// the system database's eventual consistency — left it empty. The next
	// reconcile then saw "password needs rotating", issued ALTER USER ... SET
	// PASSWORD with the value already stored, and Neo4j rejects same-password
	// ALTER as a hard ArgumentError. The hash was therefore never recorded and
	// the loop repeated forever (CI: PR #337, 630s spec timeout).
	//
	// Clearing the hash directly reproduces the trapped state deterministically,
	// without needing to win the original race. Before the fix this spec hangs
	// until timeout; after it, the user returns to Ready.
	It("recovers when the applied password hash is lost", SpecTimeout(testTimeout), func(ctx SpecContext) {
		By("Creating a Neo4jUser and waiting for it to become Ready")
		user = &neo4jv1beta1.Neo4jUser{
			ObjectMeta: metav1.ObjectMeta{Name: "hashloss", Namespace: namespace.Name},
			Spec: neo4jv1beta1.Neo4jUserSpec{
				ClusterRef:        clusterName,
				Username:          "hashloss",
				PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "appuser-creds"},
			},
		}
		Expect(k8sClient.Create(ctx, user)).To(Succeed())

		key := types.NamespacedName{Name: "hashloss", Namespace: namespace.Name}
		Eventually(func() string {
			u := &neo4jv1beta1.Neo4jUser{}
			if err := k8sClient.Get(ctx, key, u); err != nil {
				return ""
			}
			return u.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Ready"))

		By("Clearing status.passwordSecretHash to simulate an interrupted reconcile")
		Eventually(func() error {
			u := &neo4jv1beta1.Neo4jUser{}
			if err := k8sClient.Get(ctx, key, u); err != nil {
				return err
			}
			u.Status.PasswordSecretHash = ""
			u.Status.Phase = "Pending"
			return k8sClient.Status().Update(ctx, u)
		}, time.Minute, interval).Should(Succeed())

		By("Expecting the user to reconcile back to Ready rather than livelock")
		// The controller now re-issues ALTER USER with the password already in
		// place. Neo4j rejects it; the fix classifies that rejection as the
		// desired state already holding, so the pass completes and re-records
		// the hash instead of failing forever.
		Eventually(func() string {
			u := &neo4jv1beta1.Neo4jUser{}
			if err := k8sClient.Get(ctx, key, u); err != nil {
				return ""
			}
			return u.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Ready"),
			"Neo4jUser livelocked: it re-ALTERs to the same password and never recovers")

		By("Confirming the hash was re-recorded, so the loop cannot repeat")
		u := &neo4jv1beta1.Neo4jUser{}
		Expect(k8sClient.Get(ctx, key, u)).To(Succeed())
		Expect(u.Status.PasswordSecretHash).ToNot(BeEmpty())
	})

	It("creates, rotates and drops a user", SpecTimeout(testTimeout), func(ctx SpecContext) {
		By("Creating a Neo4jUser bound to the reader role")
		user = &neo4jv1beta1.Neo4jUser{
			ObjectMeta: metav1.ObjectMeta{Name: "appuser", Namespace: namespace.Name},
			Spec: neo4jv1beta1.Neo4jUserSpec{
				ClusterRef:        clusterName,
				Username:          "appuser",
				PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "appuser-creds"},
				Roles:             []string{"reader"},
			},
		}
		Expect(k8sClient.Create(ctx, user)).To(Succeed())

		By("Waiting for Neo4jUser status.phase=Ready")
		Eventually(func() string {
			u := &neo4jv1beta1.Neo4jUser{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "appuser", Namespace: namespace.Name}, u); err != nil {
				return ""
			}
			return u.Status.Phase
		}, clusterTimeout, interval).Should(Equal("Ready"))

		By("Verifying SHOW USERS via cypher-shell")
		podName := fmt.Sprintf("%s-server-0", clusterName)
		Eventually(func() string {
			cmd, cancel := boundedExec(ctx, podName, namespace.Name,
				"cypher-shell", "--format", "plain", "-u", "neo4j", "-p", adminPass,
				"SHOW USERS YIELD user, roles WHERE user = 'appuser' RETURN user, roles",
			)
			defer cancel()
			out, err := cmd.CombinedOutput()
			if err != nil {
				// Surface the failure mode so the Eventually's last-observed value
				// in the timeout message includes the cause, not an empty string.
				return fmt.Sprintf("kubectl exec/cypher-shell failed: %v; output: %s", err, string(out))
			}
			return string(out)
		}, clusterTimeout, interval).Should(SatisfyAll(
			ContainSubstring("appuser"),
			ContainSubstring("reader"),
		))

		By("Verifying the appuser can authenticate with the original password")
		Eventually(func() error {
			cmd, cancel := boundedExec(ctx, podName, namespace.Name,
				"cypher-shell", "--format", "plain", "-u", "appuser", "-p", userPass,
				"RETURN 1",
			)
			defer cancel()
			out, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("cypher-shell auth as appuser failed: %w; output: %s", err, string(out))
			}
			return nil
		}, clusterTimeout, interval).Should(Succeed())

		By("Capturing initial passwordSecretHash")
		initial := &neo4jv1beta1.Neo4jUser{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "appuser", Namespace: namespace.Name}, initial)).To(Succeed())
		Expect(initial.Status.PasswordSecretHash).ToNot(BeEmpty())
		initialHash := initial.Status.PasswordSecretHash

		By("Rotating the password Secret")
		rotated := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "appuser-creds", Namespace: namespace.Name}, rotated)).To(Succeed())
		rotated.Data["password"] = []byte(newUserPass)
		Expect(k8sClient.Update(ctx, rotated)).To(Succeed())

		By("Waiting for the controller to apply the new password")
		Eventually(func() string {
			u := &neo4jv1beta1.Neo4jUser{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "appuser", Namespace: namespace.Name}, u); err != nil {
				return ""
			}
			return u.Status.PasswordSecretHash
		}, clusterTimeout, interval).ShouldNot(Equal(initialHash))

		By("Verifying the appuser can authenticate with the new password")
		Eventually(func() error {
			cmd, cancel := boundedExec(ctx, podName, namespace.Name,
				"cypher-shell", "--format", "plain", "-u", "appuser", "-p", newUserPass,
				"RETURN 1",
			)
			defer cancel()
			out, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("cypher-shell auth as appuser (rotated password) failed: %w; output: %s", err, string(out))
			}
			return nil
		}, clusterTimeout, interval).Should(Succeed())

		By("Deleting the Neo4jUser CR")
		Expect(k8sClient.Delete(ctx, user)).To(Succeed())
		user = nil // AfterEach should not double-delete

		By("Waiting for the user to disappear from SHOW USERS")
		Eventually(func() bool {
			cmd, cancel := boundedExec(ctx, podName, namespace.Name,
				"cypher-shell", "--format", "plain", "-u", "neo4j", "-p", adminPass,
				"SHOW USERS YIELD user WHERE user = 'appuser' RETURN count(*) AS n",
			)
			defer cancel()
			out, err := cmd.CombinedOutput()
			if err != nil {
				GinkgoWriter.Printf("cypher-shell SHOW USERS failed: %v; output: %s\n", err, string(out))
				return false
			}
			return cypherShellLastValueIsZero(out)
		}, clusterTimeout, interval).Should(BeTrue(), "DROP USER must remove appuser from SHOW USERS")
	})
})

// cypherShellLastValueIsZero parses output from `cypher-shell --format plain`
// for a single-column count query and returns true when the last non-empty
// line equals "0". Plain format emits:
//
//	n
//	<count>
//
// The header row is unreliable to split on (some cypher-shell versions wrap
// it differently across newlines), so we just take the last non-empty line.
//
// Empty input is treated as "not zero" rather than panicking on the slice
// access — cypher-shell can in principle emit nothing during a transient
// connection blip, and the calling Eventually loop will retry.
func cypherShellLastValueIsZero(out []byte) bool {
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return false
	}
	lines := strings.Split(trimmed, "\n")
	return strings.TrimSpace(lines[len(lines)-1]) == "0"
}
