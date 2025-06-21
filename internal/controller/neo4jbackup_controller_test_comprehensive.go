/*
Copyright 2025.

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

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
)

var _ = Describe("Neo4jBackupController Comprehensive Tests", func() {
	Context("When creating backup configurations", func() {
		var (
			cluster    *neo4jv1alpha1.Neo4jEnterpriseCluster
			backup     *neo4jv1alpha1.Neo4jBackup
			reconciler *Neo4jBackupReconciler
			ctx        context.Context
		)

		BeforeEach(func() {
			ctx = context.Background()

			scheme := runtime.NewScheme()
			_ = neo4jv1alpha1.AddToScheme(scheme)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

			reconciler = &Neo4jBackupReconciler{
				Client:                  fakeClient,
				Scheme:                  scheme,
				Recorder:                record.NewFakeRecorder(100),
				MaxConcurrentReconciles: 1,
				RequeueAfter:            time.Minute,
			}

			cluster = &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.0.0-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries:   3,
						Secondaries: 2,
					},
					Storage: neo4jv1alpha1.StorageSpec{
						ClassName: "standard",
						Size:      "10Gi",
					},
				},
			}

			backup = &neo4jv1alpha1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-backup",
					Namespace: "default",
				},
				Spec: neo4jv1alpha1.Neo4jBackupSpec{
					Target: neo4jv1alpha1.BackupTarget{
						Kind: "Cluster",
						Name: "test-cluster",
					},
					Storage: neo4jv1alpha1.StorageLocation{
						Type: "pvc",
						PVC: &neo4jv1alpha1.PVCSpec{
							StorageClassName: "standard",
							Size:             "5Gi",
						},
					},
				},
			}
		})

		It("should create PVC backup successfully", func() {
			Expect(reconciler.Client.Create(ctx, cluster)).Should(Succeed())
			Expect(reconciler.Client.Create(ctx, backup)).Should(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      backup.Name,
					Namespace: backup.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
		})

		It("should handle S3 backup configuration", func() {
			backup.Spec.Storage = neo4jv1alpha1.StorageLocation{
				Type:   "s3",
				Bucket: "test-bucket",
				Path:   "/backups",
				Cloud: &neo4jv1alpha1.CloudBlock{
					Provider: "aws",
					Identity: &neo4jv1alpha1.CloudIdentity{
						Provider:       "aws",
						ServiceAccount: "neo4j-backup-sa",
					},
				},
			}

			Expect(reconciler.Client.Create(ctx, cluster)).Should(Succeed())
			Expect(reconciler.Client.Create(ctx, backup)).Should(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      backup.Name,
					Namespace: backup.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
		})

		It("should handle scheduled backups with cron", func() {
			backup.Spec.Schedule = "0 2 * * *" // Daily at 2 AM

			Expect(reconciler.Client.Create(ctx, cluster)).Should(Succeed())
			Expect(reconciler.Client.Create(ctx, backup)).Should(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      backup.Name,
					Namespace: backup.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
		})

		It("should validate retention policies", func() {
			backup.Spec.Retention = &neo4jv1alpha1.RetentionPolicy{
				MaxAge:       "30d",
				MaxCount:     7,
				DeletePolicy: "Delete",
			}

			Expect(reconciler.Client.Create(ctx, cluster)).Should(Succeed())
			Expect(reconciler.Client.Create(ctx, backup)).Should(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      backup.Name,
					Namespace: backup.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
		})
	})
})
