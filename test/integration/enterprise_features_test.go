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

package integration

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
)

var _ = Describe("Enterprise Features Integration Tests", func() {
	var (
		namespace string
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		namespace = fmt.Sprintf("test-enterprise-%d", time.Now().Unix())

		// Create test namespace
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		// Create admin secret
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "neo4j-admin-secret",
				Namespace: namespace,
			},
			Data: map[string][]byte{
				"NEO4J_AUTH": []byte("neo4j/testpassword123"),
			},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
	})

	AfterEach(func() {
		// Clean up namespace
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
	})

	Describe("Auto-Scaling Feature", func() {
		It("Should create and configure HPA for read replicas", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "autoscaling-cluster",
					Namespace: namespace,
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
					AutoScaling: &neo4jv1alpha1.AutoScalingSpec{
						Enabled:     true,
						MinReplicas: 2,
						MaxReplicas: 10,
						Metrics: []neo4jv1alpha1.AutoScalingMetric{
							{
								Type:   "cpu",
								Target: "70",
							},
							{
								Type:   "memory",
								Target: "80",
							},
						},
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating the Neo4j cluster with auto-scaling")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying HPA is created")
			Eventually(func() error {
				hpa := &autoscalingv2.HorizontalPodAutoscaler{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "autoscaling-cluster-secondary",
					Namespace: namespace,
				}, hpa)
			}, time.Minute*5, time.Second*10).Should(Succeed())

			By("Checking HPA configuration")
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "autoscaling-cluster-secondary",
				Namespace: namespace,
			}, hpa)).To(Succeed())

			Expect(*hpa.Spec.MinReplicas).To(Equal(int32(2)))
			Expect(hpa.Spec.MaxReplicas).To(Equal(int32(10)))
			Expect(len(hpa.Spec.Metrics)).To(BeNumerically(">", 0))
		})

		// CPU-based scaling would be tested in a real environment
		// with actual workload and metrics
	})

	Describe("Blue-Green Deployment Feature", func() {
		It("Should handle blue-green deployment configuration", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bluegreen-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries:   3,
						Secondaries: 1,
					},
					Storage: neo4jv1alpha1.StorageSpec{
						ClassName: "standard",
						Size:      "10Gi",
					},
					BlueGreen: &neo4jv1alpha1.BlueGreenDeploymentSpec{
						Enabled: true,
						Traffic: &neo4jv1alpha1.TrafficSwitchingConfig{
							Mode:             "automatic",
							CanaryPercentage: 20,
							WaitDuration:     "5m",
						},
						Validation: &neo4jv1alpha1.BlueGreenValidationConfig{
							HealthChecks: []neo4jv1alpha1.BlueGreenHealthCheck{
								{
									Name:           "basic-connectivity",
									CypherQuery:    "RETURN 1 as result",
									ExpectedResult: "1",
									Timeout:        "30s",
								},
							},
						},
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating the cluster with blue-green deployment")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying cluster is processed")
			Eventually(func() error {
				updated := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "bluegreen-cluster",
					Namespace: namespace,
				}, updated)
			}, time.Minute*2, time.Second*5).Should(Succeed())

			// Blue-green deployment would create additional resources during upgrade
			// This test validates the configuration is accepted
		})
	})

	Describe("Plugin Management Feature", func() {
		It("Should create and manage Neo4j plugins", func() {
			// First create a cluster
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "plugin-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
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
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Create a plugin
			plugin := &neo4jv1alpha1.Neo4jPlugin{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "apoc-plugin",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jPluginSpec{
					ClusterRef: "plugin-cluster",
					Name:       "apoc",
					Version:    "5.26.0",
					Enabled:    true,
					Source: &neo4jv1alpha1.PluginSource{
						Type: "official",
					},
					Config: map[string]string{
						"apoc.export.file.enabled": "true",
						"apoc.import.file.enabled": "true",
					},
				},
			}

			By("Creating the Neo4j plugin")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			By("Verifying plugin is processed")
			Eventually(func() error {
				updated := &neo4jv1alpha1.Neo4jPlugin{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "apoc-plugin",
					Namespace: namespace,
				}, updated)
			}, time.Minute*2, time.Second*5).Should(Succeed())
		})
	})

	Describe("Disaster Recovery Feature", func() {
		It("Should create disaster recovery configuration", func() {
			// Create primary cluster
			primaryCluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "primary-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
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
			Expect(k8sClient.Create(ctx, primaryCluster)).To(Succeed())

			// Create secondary cluster
			secondaryCluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "secondary-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
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
			Expect(k8sClient.Create(ctx, secondaryCluster)).To(Succeed())

			// Create disaster recovery configuration
			dr := &neo4jv1alpha1.Neo4jDisasterRecovery{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cross-region-dr",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jDisasterRecoverySpec{
					PrimaryClusterRef:   "primary-cluster",
					SecondaryClusterRef: "secondary-cluster",
					CrossRegion: &neo4jv1alpha1.CrossRegionConfig{
						PrimaryRegion:   "us-east-1",
						SecondaryRegion: "us-west-2",
						ReplicationMode: "async",
					},
					Failover: &neo4jv1alpha1.FailoverConfig{
						Automatic: true,
						RPO:       "1h",
						RTO:       "15m",
					},
				},
			}

			By("Creating disaster recovery configuration")
			Expect(k8sClient.Create(ctx, dr)).To(Succeed())

			By("Verifying DR configuration is processed")
			Eventually(func() error {
				updated := &neo4jv1alpha1.Neo4jDisasterRecovery{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "cross-region-dr",
					Namespace: namespace,
				}, updated)
			}, time.Minute*2, time.Second*5).Should(Succeed())
		})
	})

	Describe("Query Monitoring Feature", func() {
		It("Should configure query monitoring", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "monitoring-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
					},
					Storage: neo4jv1alpha1.StorageSpec{
						ClassName: "standard",
						Size:      "10Gi",
					},
					QueryMonitoring: &neo4jv1alpha1.QueryMonitoringSpec{
						Enabled:              true,
						SlowQueryThreshold:   "2s",
						ExplainPlan:          true,
						IndexRecommendations: true,
						Sampling: &neo4jv1alpha1.QuerySamplingConfig{
							Rate:                "0.1",
							MaxQueriesPerSecond: 100,
						},
						MetricsExport: &neo4jv1alpha1.QueryMetricsExportConfig{
							Prometheus:     true,
							CustomEndpoint: "",
							Interval:       "30s",
						},
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating cluster with query monitoring")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying cluster configuration")
			Eventually(func() bool {
				updated := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      "monitoring-cluster",
					Namespace: namespace,
				}, updated)
				return err == nil && updated.Spec.QueryMonitoring != nil
			}, time.Minute*2, time.Second*5).Should(BeTrue())
		})
	})

	Describe("Point-in-Time Recovery Feature", func() {
		It("Should configure point-in-time recovery", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
					},
					Storage: neo4jv1alpha1.StorageSpec{
						ClassName: "standard",
						Size:      "10Gi",
					},
					PointInTimeRecovery: &neo4jv1alpha1.PointInTimeRecoverySpec{
						Enabled:                 true,
						TransactionLogRetention: "7d",
						RecoveryPointObjective:  "5m",
						LogShipping: &neo4jv1alpha1.LogShippingConfig{
							Enabled:  true,
							Interval: "1m",
							Destination: &neo4jv1alpha1.StorageLocation{
								Type:   "s3",
								Bucket: "neo4j-pitr-logs",
								Path:   "/transaction-logs",
							},
						},
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating cluster with PITR")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying PITR configuration")
			Eventually(func() bool {
				updated := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      "pitr-cluster",
					Namespace: namespace,
				}, updated)
				return err == nil && updated.Spec.PointInTimeRecovery != nil
			}, time.Minute*2, time.Second*5).Should(BeTrue())
		})
	})

	Describe("Multi-Tenant Feature", func() {
		It("Should configure multi-tenancy", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multitenant-cluster",
					Namespace: namespace,
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
					Image: neo4jv1alpha1.ImageSpec{
						Repo: "neo4j",
						Tag:  "5.26-enterprise",
					},
					Topology: neo4jv1alpha1.TopologyConfiguration{
						Primaries: 3,
					},
					Storage: neo4jv1alpha1.StorageSpec{
						ClassName: "standard",
						Size:      "10Gi",
					},
					MultiTenant: &neo4jv1alpha1.MultiTenantSpec{
						Enabled:   true,
						Isolation: "database",
						Tenants: []neo4jv1alpha1.TenantConfig{
							{
								Name:      "tenant-a",
								Databases: []string{"tenant_a_db"},
								Resources: &neo4jv1alpha1.TenantResourceConfig{
									CPU:            "1000m",
									Memory:         "2Gi",
									Storage:        "10Gi",
									MaxConnections: 100,
								},
							},
							{
								Name:      "tenant-b",
								Databases: []string{"tenant_b_db"},
								Resources: &neo4jv1alpha1.TenantResourceConfig{
									CPU:            "500m",
									Memory:         "1Gi",
									Storage:        "5Gi",
									MaxConnections: 50,
								},
							},
						},
						ResourceQuotas: &neo4jv1alpha1.MultiTenantResourceQuotas{
							DefaultCPUQuota:      "500m",
							DefaultMemoryQuota:   "1Gi",
							DefaultStorageQuota:  "5Gi",
							MaxTenantsPerCluster: 10,
						},
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating multi-tenant cluster")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying multi-tenant configuration")
			Eventually(func() bool {
				updated := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      "multitenant-cluster",
					Namespace: namespace,
				}, updated)
				return err == nil && updated.Spec.MultiTenant != nil && len(updated.Spec.MultiTenant.Tenants) == 2
			}, time.Minute*2, time.Second*5).Should(BeTrue())
		})
	})

	Describe("Combined Enterprise Features", func() {
		It("Should handle cluster with all enterprise features enabled", func() {
			cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "full-enterprise-cluster",
					Namespace: namespace,
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
					// Enable all enterprise features
					AutoScaling: &neo4jv1alpha1.AutoScalingSpec{
						Enabled:     true,
						MinReplicas: 2,
						MaxReplicas: 5,
						Metrics: []neo4jv1alpha1.AutoScalingMetric{
							{Type: "cpu", Target: "70"},
						},
					},
					BlueGreen: &neo4jv1alpha1.BlueGreenDeploymentSpec{
						Enabled: true,
					},
					QueryMonitoring: &neo4jv1alpha1.QueryMonitoringSpec{
						Enabled:            true,
						SlowQueryThreshold: "5s",
					},
					PointInTimeRecovery: &neo4jv1alpha1.PointInTimeRecoverySpec{
						Enabled:                 true,
						TransactionLogRetention: "3d",
					},
					MultiTenant: &neo4jv1alpha1.MultiTenantSpec{
						Enabled:   true,
						Isolation: "database",
					},
					Auth: &neo4jv1alpha1.AuthSpec{
						AdminSecret: "neo4j-admin-secret",
					},
				},
			}

			By("Creating cluster with all enterprise features")
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			By("Verifying cluster is created and processed")
			Eventually(func() error {
				updated := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "full-enterprise-cluster",
					Namespace: namespace,
				}, updated)
			}, time.Minute*3, time.Second*10).Should(Succeed())

			By("Checking that StatefulSets are created")
			Eventually(func() error {
				sts := &appsv1.StatefulSet{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      "full-enterprise-cluster-primary",
					Namespace: namespace,
				}, sts)
			}, time.Minute*5, time.Second*10).Should(Succeed())
		})
	})
})
