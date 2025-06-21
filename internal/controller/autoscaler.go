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
	"fmt"
	"strconv"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
)

// AutoScaler manages auto-scaling for Neo4j read replicas
type AutoScaler struct {
	client.Client
}

// NewAutoScaler creates a new auto-scaler instance
func NewAutoScaler(client client.Client) *AutoScaler {
	return &AutoScaler{
		Client: client,
	}
}

// ReconcileAutoScaling handles auto-scaling for a cluster
func (a *AutoScaler) ReconcileAutoScaling(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx).WithName("autoscaler")

	if cluster.Spec.AutoScaling == nil || !cluster.Spec.AutoScaling.Enabled {
		logger.Info("Auto-scaling not enabled for cluster")
		return nil
	}

	// Ensure secondary StatefulSet exists
	if cluster.Spec.Topology.Secondaries == 0 {
		logger.Info("No secondary replicas configured, skipping auto-scaling")
		return nil
	}

	// Get or create HPA
	hpa, err := a.ensureHPA(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to ensure HPA: %w", err)
	}

	// Update HPA based on cluster configuration
	if err := a.updateHPA(ctx, cluster, hpa); err != nil {
		return fmt.Errorf("failed to update HPA: %w", err)
	}

	logger.Info("Auto-scaling reconciliation completed")
	return nil
}

func (a *AutoScaler) ensureHPA(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	hpaName := fmt.Sprintf("%s-secondary-hpa", cluster.Name)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}

	err := a.Get(ctx, types.NamespacedName{
		Name:      hpaName,
		Namespace: cluster.Namespace,
	}, hpa)

	if err != nil {
		// Create new HPA
		hpa = a.buildHPA(cluster)
		if err := a.Create(ctx, hpa); err != nil {
			return nil, fmt.Errorf("failed to create HPA: %w", err)
		}
		return hpa, nil
	}

	return hpa, nil
}

func (a *AutoScaler) buildHPA(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) *autoscalingv2.HorizontalPodAutoscaler {
	hpaName := fmt.Sprintf("%s-secondary-hpa", cluster.Name)
	targetName := fmt.Sprintf("%s-secondary", cluster.Name)

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hpaName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "neo4j",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": "autoscaler",
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       targetName,
			},
			MinReplicas: &cluster.Spec.AutoScaling.MinReplicas,
			MaxReplicas: cluster.Spec.AutoScaling.MaxReplicas,
			Metrics:     a.buildHPAMetrics(cluster),
		},
	}

	return hpa
}

func (a *AutoScaler) buildHPAMetrics(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) []autoscalingv2.MetricSpec {
	var metrics []autoscalingv2.MetricSpec

	for _, metric := range cluster.Spec.AutoScaling.Metrics {
		switch metric.Type {
		case "cpu":
			targetValue, _ := strconv.Atoi(metric.Target)
			metrics = append(metrics, autoscalingv2.MetricSpec{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: func(i int32) *int32 { return &i }(int32(targetValue)),
					},
				},
			})

		case "memory":
			targetValue, _ := strconv.Atoi(metric.Target)
			metrics = append(metrics, autoscalingv2.MetricSpec{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceMemory,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: func(i int32) *int32 { return &i }(int32(targetValue)),
					},
				},
			})

		case "query_latency":
			targetQuantity := resource.MustParse(metric.Target)
			metrics = append(metrics, autoscalingv2.MetricSpec{
				Type: autoscalingv2.PodsMetricSourceType,
				Pods: &autoscalingv2.PodsMetricSource{
					Metric: autoscalingv2.MetricIdentifier{
						Name: "neo4j_query_latency_p99",
					},
					Target: autoscalingv2.MetricTarget{
						Type:         autoscalingv2.AverageValueMetricType,
						AverageValue: &targetQuantity,
					},
				},
			})
		}
	}

	// Default CPU metric if none specified
	if len(metrics) == 0 {
		metrics = append(metrics, autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: func(i int32) *int32 { return &i }(70),
				},
			},
		})
	}

	return metrics
}

func (a *AutoScaler) updateHPA(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	// Update HPA spec based on current cluster configuration
	hpa.Spec.MinReplicas = &cluster.Spec.AutoScaling.MinReplicas
	hpa.Spec.MaxReplicas = cluster.Spec.AutoScaling.MaxReplicas
	hpa.Spec.Metrics = a.buildHPAMetrics(cluster)

	return a.Update(ctx, hpa)
}
