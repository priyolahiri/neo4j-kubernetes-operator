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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
	neo4jclient "github.com/neo4j-labs/neo4j-operator/internal/neo4j"
)

// BlueGreenDeploymentManager manages blue-green deployments for Neo4j clusters
type BlueGreenDeploymentManager struct {
	client.Client
}

// NewBlueGreenDeploymentManager creates a new blue-green deployment manager
func NewBlueGreenDeploymentManager(client client.Client) *BlueGreenDeploymentManager {
	return &BlueGreenDeploymentManager{
		Client: client,
	}
}

// BlueGreenState represents the current state of blue-green deployment
type BlueGreenState struct {
	ActiveColor    string                    `json:"activeColor"`
	InactiveColor  string                    `json:"inactiveColor"`
	Phase          string                    `json:"phase"`
	StartTime      *metav1.Time              `json:"startTime,omitempty"`
	LastSwitchTime *metav1.Time              `json:"lastSwitchTime,omitempty"`
	Validation     *BlueGreenValidationState `json:"validation,omitempty"`
	Traffic        *BlueGreenTrafficState    `json:"traffic,omitempty"`
}

// BlueGreenValidationState tracks validation status
type BlueGreenValidationState struct {
	Status        string                      `json:"status"`
	Results       []BlueGreenValidationResult `json:"results,omitempty"`
	StartTime     *metav1.Time                `json:"startTime,omitempty"`
	CompletedTime *metav1.Time                `json:"completedTime,omitempty"`
}

// BlueGreenValidationResult represents a validation result
type BlueGreenValidationResult struct {
	Name      string      `json:"name"`
	Status    string      `json:"status"`
	Message   string      `json:"message,omitempty"`
	Timestamp metav1.Time `json:"timestamp"`
}

// BlueGreenTrafficState tracks traffic routing state
type BlueGreenTrafficState struct {
	BlueTrafficPercentage  int32        `json:"blueTrafficPercentage"`
	GreenTrafficPercentage int32        `json:"greenTrafficPercentage"`
	LastUpdate             *metav1.Time `json:"lastUpdate,omitempty"`
}

// ReconcileBlueGreenDeployment handles blue-green deployment for a cluster
func (bg *BlueGreenDeploymentManager) ReconcileBlueGreenDeployment(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx).WithName("bluegreen")

	if cluster.Spec.BlueGreen == nil || !cluster.Spec.BlueGreen.Enabled {
		logger.Info("Blue-green deployment not enabled for cluster")
		return nil
	}

	// Get current blue-green state
	state, err := bg.getCurrentState(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get current state: %w", err)
	}

	logger.Info("Current blue-green state", "activeColor", state.ActiveColor, "phase", state.Phase)

	// Handle based on current phase
	switch state.Phase {
	case "Stable":
		// Check if upgrade is needed
		if bg.isUpgradeNeeded(ctx, cluster, state) {
			return bg.startBlueGreenDeployment(ctx, cluster, state)
		}
	case "Deploying":
		return bg.continueDeployment(ctx, cluster, state)
	case "Validating":
		return bg.continueValidation(ctx, cluster, state)
	case "CanaryTesting":
		return bg.continueCanaryTesting(ctx, cluster, state)
	case "Switching":
		return bg.continueSwitching(ctx, cluster, state)
	case "RollingBack":
		return bg.continueRollback(ctx, cluster, state)
	default:
		// Initialize to stable state
		return bg.initializeStableState(ctx, cluster)
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) getCurrentState(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) (*BlueGreenState, error) {
	// Implementation would fetch state from annotations or ConfigMap
	state := &BlueGreenState{
		ActiveColor:   "blue",
		InactiveColor: "green",
		Phase:         "Stable",
	}

	return state, nil
}

func (bg *BlueGreenDeploymentManager) isUpgradeNeeded(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) bool {
	// Check if cluster spec has changed requiring deployment
	// This would compare current running version with desired version
	return false
}

func (bg *BlueGreenDeploymentManager) startBlueGreenDeployment(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	logger.Info("Starting blue-green deployment", "inactiveColor", state.InactiveColor)

	// Update state
	state.Phase = "Deploying"
	now := metav1.Now()
	state.StartTime = &now

	// Deploy to inactive environment
	if err := bg.deployToInactive(ctx, cluster, state); err != nil {
		return fmt.Errorf("failed to deploy to inactive environment: %w", err)
	}

	// Save state
	if err := bg.saveState(ctx, cluster, state); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) deployToInactive(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	// Create inactive StatefulSet
	sts := bg.buildInactiveStatefulSet(cluster, state.InactiveColor)

	if err := bg.Create(ctx, sts); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create inactive StatefulSet: %w", err)
	}

	// Create inactive Service
	svc := bg.buildInactiveService(cluster, state.InactiveColor)

	if err := bg.Create(ctx, svc); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create inactive Service: %w", err)
	}

	logger.Info("Inactive environment deployed", "color", state.InactiveColor)
	return nil
}

func (bg *BlueGreenDeploymentManager) buildInactiveStatefulSet(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, color string) *appsv1.StatefulSet {
	name := fmt.Sprintf("%s-%s", cluster.Name, color)

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "neo4j",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": "database",
				"deployment.color":            color,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &cluster.Spec.Topology.Primaries,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/instance": cluster.Name,
					"deployment.color":           color,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "neo4j",
						"app.kubernetes.io/instance":  cluster.Name,
						"app.kubernetes.io/component": "database",
						"deployment.color":            color,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "neo4j",
							Image: cluster.Spec.Image.Repo + ":" + cluster.Spec.Image.Tag,
							// Add other container configuration
						},
					},
				},
			},
		},
	}
}

func (bg *BlueGreenDeploymentManager) buildInactiveService(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, color string) *corev1.Service {
	name := fmt.Sprintf("%s-%s", cluster.Name, color)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "neo4j",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": "database",
				"deployment.color":            color,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/instance": cluster.Name,
				"deployment.color":           color,
			},
			Ports: []corev1.ServicePort{
				{
					Name: "bolt",
					Port: 7687,
				},
				{
					Name: "http",
					Port: 7474,
				},
				{
					Name: "https",
					Port: 7473,
				},
			},
		},
	}
}

func (bg *BlueGreenDeploymentManager) continueDeployment(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	// Check if inactive deployment is ready
	ready, err := bg.isInactiveDeploymentReady(ctx, cluster, state.InactiveColor)
	if err != nil {
		return fmt.Errorf("failed to check deployment readiness: %w", err)
	}

	if ready {
		logger.Info("Inactive deployment is ready, starting validation")
		state.Phase = "Validating"
		return bg.saveState(ctx, cluster, state)
	}

	logger.Info("Inactive deployment not ready yet")
	return nil
}

func (bg *BlueGreenDeploymentManager) isInactiveDeploymentReady(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, color string) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := bg.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-%s", cluster.Name, color),
		Namespace: cluster.Namespace,
	}, sts)
	if err != nil {
		return false, err
	}

	return sts.Status.ReadyReplicas == sts.Status.Replicas, nil
}

func (bg *BlueGreenDeploymentManager) continueValidation(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	// Run validation checks
	if state.Validation == nil {
		state.Validation = &BlueGreenValidationState{
			Status:    "Running",
			StartTime: func() *metav1.Time { t := metav1.Now(); return &t }(),
		}
	}

	// Perform health checks
	if err := bg.performHealthChecks(ctx, cluster, state); err != nil {
		logger.Error(err, "Health checks failed")
		state.Validation.Status = "Failed"
		state.Phase = "RollingBack"
		return bg.saveState(ctx, cluster, state)
	}

	// Run custom validations
	if err := bg.runCustomValidations(ctx, cluster, state); err != nil {
		logger.Error(err, "Custom validations failed")
		state.Validation.Status = "Failed"
		state.Phase = "RollingBack"
		return bg.saveState(ctx, cluster, state)
	}

	// All validations passed
	logger.Info("All validations passed")
	state.Validation.Status = "Passed"
	state.Validation.CompletedTime = func() *metav1.Time { t := metav1.Now(); return &t }()

	// Start canary testing or full switch based on configuration
	if cluster.Spec.BlueGreen.Traffic != nil && cluster.Spec.BlueGreen.Traffic.CanaryPercentage > 0 {
		state.Phase = "CanaryTesting"
	} else {
		state.Phase = "Switching"
	}

	return bg.saveState(ctx, cluster, state)
}

func (bg *BlueGreenDeploymentManager) performHealthChecks(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	// Create Neo4j client for inactive environment
	neo4jClient, err := bg.createInactiveNeo4jClient(ctx, cluster, state.InactiveColor)
	if err != nil {
		return fmt.Errorf("failed to create Neo4j client: %w", err)
	}
	defer neo4jClient.Close()

	// Perform basic health check
	healthy, err := neo4jClient.IsClusterHealthy(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if !healthy {
		return fmt.Errorf("cluster is not healthy")
	}

	// Run specific health checks if configured
	if cluster.Spec.BlueGreen.Validation != nil {
		for _, check := range cluster.Spec.BlueGreen.Validation.HealthChecks {
			if err := bg.runHealthCheck(ctx, neo4jClient, check); err != nil {
				return fmt.Errorf("health check %s failed: %w", check.Name, err)
			}
		}
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) runHealthCheck(ctx context.Context, client *neo4jclient.Client, check neo4jv1alpha1.BlueGreenHealthCheck) error {
	if check.CypherQuery != "" {
		// Run Cypher query check
		result, err := client.ExecuteQuery(ctx, check.CypherQuery)
		if err != nil {
			return err
		}

		if check.ExpectedResult != "" && result != check.ExpectedResult {
			return fmt.Errorf("unexpected result: got %s, expected %s", result, check.ExpectedResult)
		}
	}

	if check.HTTPEndpoint != "" {
		// Perform HTTP check
		// Implementation would make HTTP request to endpoint
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) runCustomValidations(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	if cluster.Spec.BlueGreen.Validation == nil {
		return nil
	}

	for _, validation := range cluster.Spec.BlueGreen.Validation.CustomValidation {
		if err := bg.runCustomValidation(ctx, cluster, validation, state.InactiveColor); err != nil {
			return fmt.Errorf("custom validation %s failed: %w", validation.Name, err)
		}
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) runCustomValidation(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, validation neo4jv1alpha1.BlueGreenCustomValidation, color string) error {
	// Create validation Job
	job := bg.buildValidationJob(cluster, validation, color)

	if err := bg.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create validation job: %w", err)
	}

	// Wait for job completion
	return bg.waitForJobCompletion(ctx, job)
}

func (bg *BlueGreenDeploymentManager) buildValidationJob(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, validation neo4jv1alpha1.BlueGreenCustomValidation, color string) *batchv1.Job {
	jobName := fmt.Sprintf("%s-%s-validation-%s", cluster.Name, color, validation.Name)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: cluster.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: validation.JobTemplate.BackoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "validation",
							Image:   validation.JobTemplate.Container.Image,
							Command: validation.JobTemplate.Container.Command,
							Args:    validation.JobTemplate.Container.Args,
							Env:     bg.convertEnvVars(validation.JobTemplate.Container.Env),
						},
					},
				},
			},
		},
	}
}

func (bg *BlueGreenDeploymentManager) convertEnvVars(envVars []neo4jv1alpha1.EnvVar) []corev1.EnvVar {
	var result []corev1.EnvVar
	for _, env := range envVars {
		converted := corev1.EnvVar{
			Name:  env.Name,
			Value: env.Value,
		}
		if env.ValueFrom != nil {
			converted.ValueFrom = &corev1.EnvVarSource{}
			if env.ValueFrom.SecretKeyRef != nil {
				converted.ValueFrom.SecretKeyRef = &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: env.ValueFrom.SecretKeyRef.Name,
					},
					Key: env.ValueFrom.SecretKeyRef.Key,
				}
			}
			if env.ValueFrom.ConfigMapKeyRef != nil {
				converted.ValueFrom.ConfigMapKeyRef = &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: env.ValueFrom.ConfigMapKeyRef.Name,
					},
					Key: env.ValueFrom.ConfigMapKeyRef.Key,
				}
			}
		}
		result = append(result, converted)
	}
	return result
}

func (bg *BlueGreenDeploymentManager) waitForJobCompletion(ctx context.Context, job *batchv1.Job) error {
	// Implementation would wait for job to complete and check result
	return nil
}

func (bg *BlueGreenDeploymentManager) continueCanaryTesting(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	// Route canary percentage of traffic to inactive environment
	canaryPercentage := cluster.Spec.BlueGreen.Traffic.CanaryPercentage

	if err := bg.routeTraffic(ctx, cluster, state, canaryPercentage); err != nil {
		return fmt.Errorf("failed to route canary traffic: %w", err)
	}

	// Wait for configured duration
	waitDuration, _ := time.ParseDuration(cluster.Spec.BlueGreen.Traffic.WaitDuration)

	// Check if enough time has passed
	if state.Traffic != nil && state.Traffic.LastUpdate != nil {
		elapsed := time.Since(state.Traffic.LastUpdate.Time)
		if elapsed < waitDuration {
			logger.Info("Waiting for canary testing period", "elapsed", elapsed, "required", waitDuration)
			return nil
		}
	}

	// Canary testing completed successfully, proceed to full switch
	logger.Info("Canary testing completed successfully")
	state.Phase = "Switching"
	return bg.saveState(ctx, cluster, state)
}

func (bg *BlueGreenDeploymentManager) routeTraffic(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState, percentage int32) error {
	// Implementation would update load balancer or ingress configuration
	// to route the specified percentage of traffic to the inactive environment

	if state.Traffic == nil {
		state.Traffic = &BlueGreenTrafficState{}
	}

	if state.ActiveColor == "blue" {
		state.Traffic.BlueTrafficPercentage = 100 - percentage
		state.Traffic.GreenTrafficPercentage = percentage
	} else {
		state.Traffic.BlueTrafficPercentage = percentage
		state.Traffic.GreenTrafficPercentage = 100 - percentage
	}

	now := metav1.Now()
	state.Traffic.LastUpdate = &now

	return nil
}

func (bg *BlueGreenDeploymentManager) continueSwitching(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	// Switch all traffic to inactive environment
	if err := bg.switchTraffic(ctx, cluster, state); err != nil {
		return fmt.Errorf("failed to switch traffic: %w", err)
	}

	// Update active/inactive colors
	state.ActiveColor, state.InactiveColor = state.InactiveColor, state.ActiveColor
	now := metav1.Now()
	state.LastSwitchTime = &now

	// Cleanup old environment
	if err := bg.cleanupOldEnvironment(ctx, cluster, state.InactiveColor); err != nil {
		logger.Error(err, "Failed to cleanup old environment")
	}

	// Update state to stable
	state.Phase = "Stable"
	logger.Info("Blue-green deployment completed successfully", "newActiveColor", state.ActiveColor)

	return bg.saveState(ctx, cluster, state)
}

func (bg *BlueGreenDeploymentManager) switchTraffic(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	// Implementation would update the main service selector to point to the new active environment
	mainService := &corev1.Service{}
	err := bg.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, mainService)
	if err != nil {
		return fmt.Errorf("failed to get main service: %w", err)
	}

	// Update selector to point to new active color
	mainService.Spec.Selector["deployment.color"] = state.InactiveColor

	return bg.Update(ctx, mainService)
}

func (bg *BlueGreenDeploymentManager) cleanupOldEnvironment(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, oldColor string) error {
	// Cleanup old StatefulSet
	sts := &appsv1.StatefulSet{}
	err := bg.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-%s", cluster.Name, oldColor),
		Namespace: cluster.Namespace,
	}, sts)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if err == nil {
		if err := bg.Delete(ctx, sts); err != nil {
			return fmt.Errorf("failed to delete old StatefulSet: %w", err)
		}
	}

	// Cleanup old Service
	svc := &corev1.Service{}
	err = bg.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-%s", cluster.Name, oldColor),
		Namespace: cluster.Namespace,
	}, svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if err == nil {
		if err := bg.Delete(ctx, svc); err != nil {
			return fmt.Errorf("failed to delete old Service: %w", err)
		}
	}

	return nil
}

func (bg *BlueGreenDeploymentManager) continueRollback(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	logger := log.FromContext(ctx)

	logger.Info("Rolling back deployment", "activeColor", state.ActiveColor)

	// Cleanup failed inactive environment
	if err := bg.cleanupOldEnvironment(ctx, cluster, state.InactiveColor); err != nil {
		logger.Error(err, "Failed to cleanup failed environment")
	}

	// Reset state to stable
	state.Phase = "Stable"
	state.Validation = nil
	state.Traffic = nil

	logger.Info("Rollback completed")
	return bg.saveState(ctx, cluster, state)
}

func (bg *BlueGreenDeploymentManager) initializeStableState(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	state := &BlueGreenState{
		ActiveColor:   "blue",
		InactiveColor: "green",
		Phase:         "Stable",
	}

	return bg.saveState(ctx, cluster, state)
}

func (bg *BlueGreenDeploymentManager) saveState(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, state *BlueGreenState) error {
	// Implementation would save state to cluster annotations or ConfigMap
	return nil
}

func (bg *BlueGreenDeploymentManager) createInactiveNeo4jClient(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, color string) (*neo4jclient.Client, error) {
	// Implementation would create a Neo4j client pointing to the inactive environment
	return neo4jclient.NewClientForEnterprise(cluster, bg.Client, "neo4j-admin-secret")
}
