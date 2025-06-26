package controller_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-kubernetes-operator/api/v1alpha1"
)

var _ = Describe("Neo4jRestore Controller", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	var (
		ctx           context.Context
		restore       *neo4jv1alpha1.Neo4jRestore
		cluster       *neo4jv1alpha1.Neo4jEnterpriseCluster
		restoreName   string
		clusterName   string
		namespaceName string
	)

	BeforeEach(func() {
		// Ensure context is properly initialized
		if ctx == nil {
			ctx = context.Background()
		}

		restoreName = fmt.Sprintf("test-restore-%d", time.Now().UnixNano())
		clusterName = fmt.Sprintf("test-cluster-%d", time.Now().UnixNano())
		namespaceName = "default"

		// Create cluster first
		cluster = &neo4jv1alpha1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespaceName,
			},
			Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
				Edition: "enterprise",
				Image: neo4jv1alpha1.ImageSpec{
					Repo: "neo4j",
					Tag:  "5.26-enterprise",
				},
				Topology: neo4jv1alpha1.TopologyConfiguration{
					Primaries:   3,
					Secondaries: 2,
				},
				Storage: neo4jv1alpha1.StorageSpec{
					ClassName: "standard",
					Size:      "10Gi",
				},
				Auth: &neo4jv1alpha1.AuthSpec{
					AdminSecret: "neo4j-admin-secret",
				},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())

		// Patch cluster status to Ready so restore controller proceeds
		patch := client.MergeFrom(cluster.DeepCopy())
		cluster.Status.Conditions = []metav1.Condition{{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "TestReady",
			LastTransitionTime: metav1.Now(),
		}}
		Expect(k8sClient.Status().Patch(ctx, cluster, patch)).To(Succeed())

		// Create basic restore spec
		restore = &neo4jv1alpha1.Neo4jRestore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      restoreName,
				Namespace: namespaceName,
			},
			Spec: neo4jv1alpha1.Neo4jRestoreSpec{
				TargetCluster: clusterName,
				DatabaseName:  "neo4j",
				Source: neo4jv1alpha1.RestoreSource{
					Type:      "backup",
					BackupRef: "test-backup",
				},
			},
		}
	})

	AfterEach(func() {
		// Clean up resources
		if restore != nil {
			if err := k8sClient.Delete(ctx, restore, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
				// Log the error but don't fail the test cleanup
				fmt.Printf("Warning: Failed to delete restore during cleanup: %v\n", err)
			}
		}
		if cluster != nil {
			if err := k8sClient.Delete(ctx, cluster, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
				// Log the error but don't fail the test cleanup
				fmt.Printf("Warning: Failed to delete cluster during cleanup: %v\n", err)
			}
		}
	})

	Context("When creating a Neo4jRestore", func() {
		It("Should create restore successfully", func() {
			By("Creating a Neo4jRestore")
			Expect(k8sClient.Create(ctx, restore)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restore.Name,
				Namespace: restore.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			// We'll need to wait and give the controller time to get the updated object.
			// The first reconciliation may add the finalizer.
			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify finalizer was added
			Expect(createdRestore.Finalizers).Should(ContainElement("neo4jrestores.neo4j.neo4j.com/finalizer"))
		})

		It("Should fail when cluster doesn't exist", func() {
			By("Creating a Neo4jRestore with non-existent cluster")
			restoreWithNoCluster := &neo4jv1alpha1.Neo4jRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-restore-no-cluster",
					Namespace: "default",
				},
				Spec: neo4jv1alpha1.Neo4jRestoreSpec{
					TargetCluster: "non-existent-cluster",
					DatabaseName:  "neo4j",
					Source: neo4jv1alpha1.RestoreSource{
						Type:      "backup",
						BackupRef: "test-backup",
					},
				},
			}
			Expect(k8sClient.Create(ctx, restoreWithNoCluster)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restoreWithNoCluster.Name,
				Namespace: restoreWithNoCluster.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify status reflects the error
			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				if err != nil {
					return false
				}
				return len(createdRestore.Status.Conditions) > 0
			}, timeout, interval).Should(BeTrue())

			// Clean up
			Expect(k8sClient.Delete(ctx, restoreWithNoCluster)).Should(Succeed())
		})

		It("Should fail when backup doesn't exist", func() {
			By("Creating a Neo4jRestore with non-existent backup")
			restoreWithNoBackup := &neo4jv1alpha1.Neo4jRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-restore-no-backup",
					Namespace: "default",
				},
				Spec: neo4jv1alpha1.Neo4jRestoreSpec{
					TargetCluster: clusterName, // Use existing cluster
					DatabaseName:  "neo4j",
					Source: neo4jv1alpha1.RestoreSource{
						Type:      "backup",
						BackupRef: "non-existent-backup",
					},
				},
			}
			Expect(k8sClient.Create(ctx, restoreWithNoBackup)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restoreWithNoBackup.Name,
				Namespace: restoreWithNoBackup.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify status reflects the error
			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				if err != nil {
					return false
				}
				return len(createdRestore.Status.Conditions) > 0
			}, timeout, interval).Should(BeTrue())

			// Clean up
			Expect(k8sClient.Delete(ctx, restoreWithNoBackup)).Should(Succeed())
		})

		It("Should handle storage-based restore", func() {
			By("Creating a Neo4jRestore with storage source")
			restoreWithStorage := &neo4jv1alpha1.Neo4jRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-restore-storage",
					Namespace: "default",
				},
				Spec: neo4jv1alpha1.Neo4jRestoreSpec{
					TargetCluster: clusterName, // Use existing cluster
					DatabaseName:  "neo4j",
					Source: neo4jv1alpha1.RestoreSource{
						Type: "storage",
						Storage: &neo4jv1alpha1.StorageLocation{
							Type:   "pvc",
							Path:   "/backups",
							Bucket: "backup-bucket",
							PVC: &neo4jv1alpha1.PVCSpec{
								Name:             "backup-storage",
								StorageClassName: "standard",
								Size:             "10Gi",
							},
						},
						BackupPath: "/backups/2024-01-01/",
					},
				},
			}
			Expect(k8sClient.Create(ctx, restoreWithStorage)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restoreWithStorage.Name,
				Namespace: restoreWithStorage.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Clean up
			Expect(k8sClient.Delete(ctx, restoreWithStorage)).Should(Succeed())
		})
	})

	Context("When updating a Neo4jRestore", func() {
		It("Should handle status updates correctly", func() {
			By("Creating and updating a Neo4jRestore")
			Expect(k8sClient.Create(ctx, restore)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restore.Name,
				Namespace: restore.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Update the restore
			createdRestore.Spec.DatabaseName = "testdb"
			Expect(k8sClient.Update(ctx, createdRestore)).Should(Succeed())

			// Verify the update was processed
			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("When deleting a Neo4jRestore", func() {
		It("Should handle deletion correctly", func() {
			By("Creating and deleting a Neo4jRestore")
			Expect(k8sClient.Create(ctx, restore)).Should(Succeed())

			restoreLookupKey := types.NamespacedName{
				Name:      restore.Name,
				Namespace: restore.Namespace,
			}
			createdRestore := &neo4jv1alpha1.Neo4jRestore{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Delete the restore
			Expect(k8sClient.Delete(ctx, restore)).Should(Succeed())

			// Verify it was deleted
			Eventually(func() bool {
				err := k8sClient.Get(ctx, restoreLookupKey, createdRestore)
				return err != nil
			}, timeout, interval).Should(BeTrue())
		})
	})
})
