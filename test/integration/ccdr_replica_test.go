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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Cross-cluster replication specs.
//
// Labelled "extended" rather than "core" because hosting a replica requires
// Neo4j 2026.08+, which is above the current CI anchor CalVer — the same
// treatment property sharding gets. These run on manual dispatch.
//
// What is covered here is the part that does NOT need two clusters: CRD
// admission, validator behaviour, and the version gate. The full end-to-end
// walk (upstream chain → replica → alias → promotion) needs two Neo4j
// deployments and lives in the manual pre-release journey, because the project
// runs one Enterprise deployment at a time.
var _ = Describe("Cross-Cluster Replication — API and validation", Label("extended"), Serial, func() {
	var (
		ctx           context.Context
		testNamespace string
		replica       *neo4jv1beta1.Neo4jReplicaDatabase
		alias         *neo4jv1beta1.Neo4jDatabaseAlias
	)

	BeforeEach(func() {
		ctx = context.Background()
		testNamespace = createTestNamespace("ccdr")
	})

	AfterEach(func() {
		if replica != nil {
			if len(replica.GetFinalizers()) > 0 {
				replica.SetFinalizers([]string{})
				_ = k8sClient.Update(ctx, replica)
			}
			_ = k8sClient.Delete(ctx, replica)
			replica = nil
		}
		if alias != nil {
			if len(alias.GetFinalizers()) > 0 {
				alias.SetFinalizers([]string{})
				_ = k8sClient.Update(ctx, alias)
			}
			_ = k8sClient.Delete(ctx, alias)
			alias = nil
		}
		if testNamespace != "" {
			cleanupCustomResourcesInNamespace(testNamespace)
		}
	})

	It("accepts a backup-mode replica and rejects a mutation of spec.source", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo-replica",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:    neo4jv1beta1.ReplicaSourceModeBackup,
					PullURI: "s3://bucket/chain/",
					SeedURI: "s3://bucket/chain/foo.backup",
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		// spec.source carries an apiserver-side CEL transition rule, because
		// Neo4j cannot re-point an existing replica — honouring a change would
		// mean dropping and re-seeding the database.
		fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: replica.Name, Namespace: testNamespace,
		}, fetched)).To(Succeed())

		fetched.Spec.Source.PullURI = "s3://bucket/a-different-chain/"
		err := k8sClient.Update(ctx, fetched)
		Expect(err).To(HaveOccurred(), "spec.source must be immutable")
		Expect(err.Error()).To(ContainSubstring("immutable"))
	})

	It("accepts network mode and goes Pending on a nonexistent cluster ref", func() {
		// Network mode is a fully supported path (this session's work) — a
		// syntactically valid address must be ACCEPTED, not rejected. The
		// only reason this particular CR can't proceed is that its
		// clusterRef doesn't exist yet, which is an ordinary Pending/retry
		// condition, never Failed.
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "net-replica",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:      neo4jv1beta1.ReplicaSourceModeNetwork,
					Addresses: []string{"upstream-0.example.com:6000"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal("Pending"))
	})

	It("rejects network mode with both addresses and upstreamClusterRef set", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "net-replica-both",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
					Addresses:          []string{"upstream-0.example.com:6000"},
					UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{Name: "prod-cluster"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal(neo4jv1beta1.ReplicaPhaseFailed))

		fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: replica.Name, Namespace: testNamespace,
		}, fetched)).To(Succeed())
		Expect(fetched.Status.Message).To(ContainSubstring("mutually exclusive"))
	})

	It("rejects network mode with neither addresses nor upstreamClusterRef set", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "net-replica-neither",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode: neo4jv1beta1.ReplicaSourceModeNetwork,
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal(neo4jv1beta1.ReplicaPhaseFailed))
	})

	It("rejects an upstreamClusterRef with an empty name", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "net-replica-empty-ref",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
					UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal(neo4jv1beta1.ReplicaPhaseFailed))
	})

	It("rejects backup mode with both pullURI and upstreamBackupRef set", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup-replica-both",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
					PullURI:           "s3://bucket/chain/",
					UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{Name: "foo-chain"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal(neo4jv1beta1.ReplicaPhaseFailed))

		fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: replica.Name, Namespace: testNamespace,
		}, fetched)).To(Succeed())
		Expect(fetched.Status.Message).To(ContainSubstring("mutually exclusive"))
	})

	It("rejects an upstreamBackupRef with an empty name", func() {
		replica = &neo4jv1beta1.Neo4jReplicaDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup-replica-empty-ref",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
				ClusterRef:       "does-not-exist-yet",
				UpstreamDatabase: "foo",
				Source: neo4jv1beta1.ReplicaSourceSpec{
					Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
					UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{},
				},
			},
		}
		Expect(k8sClient.Create(ctx, replica)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jReplicaDatabase{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: replica.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal(neo4jv1beta1.ReplicaPhaseFailed))
	})

	It("accepts an alias whose target database does not exist yet", func() {
		// An alias and the replica it fronts are normally applied together, so
		// a missing target must be Pending rather than Failed.
		alias = &neo4jv1beta1.Neo4jDatabaseAlias{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jDatabaseAliasSpec{
				ClusterRef:     "does-not-exist-yet",
				TargetDatabase: "foo-replica",
			},
		}
		Expect(k8sClient.Create(ctx, alias)).To(Succeed())

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jDatabaseAlias{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: alias.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal("Pending"))
	})

	It("rejects an alias that targets its own name", func() {
		bad := &neo4jv1beta1.Neo4jDatabaseAlias{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "selfref",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jDatabaseAliasSpec{
				ClusterRef:     "does-not-exist-yet",
				TargetDatabase: "selfref",
			},
		}
		Expect(k8sClient.Create(ctx, bad)).To(Succeed())
		defer func() {
			bad.SetFinalizers([]string{})
			_ = k8sClient.Update(ctx, bad)
			_ = k8sClient.Delete(ctx, bad)
		}()

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jDatabaseAlias{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: bad.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal("Failed"))
	})

	It("rejects a replication-source backup that would break its own chain", func() {
		// R2 + R4: retention prunes a differential's parent; no schedule means
		// the chain never advances.
		backup := &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "chain-source",
				Namespace: testNamespace,
			},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				Mode:        "replication-source",
				InstanceRef: "does-not-exist-yet",
				Database:    "foo",
				Retention:   &neo4jv1beta1.RetentionPolicy{MaxCount: 5},
				Storage: neo4jv1beta1.StorageLocation{
					Type: "s3", Bucket: "b", Path: "p",
				},
			},
		}
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())
		defer func() {
			backup.SetFinalizers([]string{})
			_ = k8sClient.Update(ctx, backup)
			_ = k8sClient.Delete(ctx, backup)
		}()

		Eventually(func() string {
			fetched := &neo4jv1beta1.Neo4jBackup{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: backup.Name, Namespace: testNamespace,
			}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, 300*time.Second, 2*time.Second).Should(Equal("Invalid"),
			fmt.Sprintf("replication-source backup %q should be rejected", backup.Name))
	})
})
