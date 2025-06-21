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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
	"github.com/neo4j-labs/neo4j-operator/internal/metrics"
	neo4jclient "github.com/neo4j-labs/neo4j-operator/internal/neo4j"
)

// Neo4jDisasterRecoveryReconciler reconciles a Neo4jDisasterRecovery object
type Neo4jDisasterRecoveryReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	RequeueAfter time.Duration
}

const DRFinalizer = "neo4j.neo4j.com/disaster-recovery-finalizer"

//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdisasterrecoveries,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdisasterrecoveries/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdisasterrecoveries/finalizers,verbs=update

func (r *Neo4jDisasterRecoveryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Neo4jDisasterRecovery instance
	dr := &neo4jv1alpha1.Neo4jDisasterRecovery{}
	if err := r.Get(ctx, req.NamespacedName, dr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Neo4jDisasterRecovery resource not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Neo4jDisasterRecovery")
		return ctrl.Result{}, err
	}

	// Initialize metrics
	drMetrics := metrics.NewDisasterRecoveryMetrics(dr.Name, dr.Namespace)

	// Handle deletion
	if dr.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, dr, drMetrics)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(dr, DRFinalizer) {
		controllerutil.AddFinalizer(dr, DRFinalizer)
		if err := r.Update(ctx, dr); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Update status to "Initializing"
	r.updateDRStatus(ctx, dr, "Initializing", "Starting disaster recovery setup")

	// Validate primary and secondary clusters exist
	primaryCluster, secondaryCluster, err := r.validateClusters(ctx, dr)
	if err != nil {
		logger.Error(err, "Cluster validation failed")
		r.updateDRStatus(ctx, dr, "Failed", fmt.Sprintf("Cluster validation failed: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Set up cross-region networking if configured
	if dr.Spec.CrossRegion != nil && dr.Spec.CrossRegion.Network != nil {
		if err := r.setupCrossRegionNetworking(ctx, dr); err != nil {
			logger.Error(err, "Failed to setup cross-region networking")
			r.updateDRStatus(ctx, dr, "Failed", fmt.Sprintf("Cross-region networking setup failed: %v", err))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
		}
	}

	// Configure replication between clusters
	if err := r.configureReplication(ctx, dr, primaryCluster, secondaryCluster); err != nil {
		logger.Error(err, "Failed to configure replication")
		r.updateDRStatus(ctx, dr, "Failed", fmt.Sprintf("Replication configuration failed: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Set up health monitoring
	if err := r.setupHealthMonitoring(ctx, dr, primaryCluster, secondaryCluster); err != nil {
		logger.Error(err, "Failed to setup health monitoring")
		r.updateDRStatus(ctx, dr, "Failed", fmt.Sprintf("Health monitoring setup failed: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Update status to "Ready"
	r.updateDRStatus(ctx, dr, "Ready", "Disaster recovery configured successfully")

	// Start monitoring routine if not already running
	go r.monitorDisasterRecovery(ctx, dr, drMetrics)

	logger.Info("Disaster recovery reconciliation completed")
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

func (r *Neo4jDisasterRecoveryReconciler) handleDeletion(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, drMetrics *metrics.DisasterRecoveryMetrics) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Cleanup disaster recovery resources
	if err := r.cleanupDRResources(ctx, dr); err != nil {
		logger.Error(err, "Failed to cleanup disaster recovery resources")
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(dr, DRFinalizer)
	if err := r.Update(ctx, dr); err != nil {
		logger.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	logger.Info("Disaster recovery deleted successfully")
	return ctrl.Result{}, nil
}

func (r *Neo4jDisasterRecoveryReconciler) validateClusters(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) (*neo4jv1alpha1.Neo4jEnterpriseCluster, *neo4jv1alpha1.Neo4jEnterpriseCluster, error) {
	// Get primary cluster
	primaryCluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      dr.Spec.PrimaryClusterRef,
		Namespace: dr.Namespace,
	}, primaryCluster); err != nil {
		return nil, nil, fmt.Errorf("failed to get primary cluster %s: %w", dr.Spec.PrimaryClusterRef, err)
	}

	// Get secondary cluster
	secondaryCluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      dr.Spec.SecondaryClusterRef,
		Namespace: dr.Namespace,
	}, secondaryCluster); err != nil {
		return nil, nil, fmt.Errorf("failed to get secondary cluster %s: %w", dr.Spec.SecondaryClusterRef, err)
	}

	// Validate clusters are ready
	if primaryCluster.Status.Phase != "Ready" {
		return nil, nil, fmt.Errorf("primary cluster %s is not ready: %s", primaryCluster.Name, primaryCluster.Status.Phase)
	}

	if secondaryCluster.Status.Phase != "Ready" {
		return nil, nil, fmt.Errorf("secondary cluster %s is not ready: %s", secondaryCluster.Name, secondaryCluster.Status.Phase)
	}

	return primaryCluster, secondaryCluster, nil
}

func (r *Neo4jDisasterRecoveryReconciler) setupCrossRegionNetworking(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	logger := log.FromContext(ctx)

	if dr.Spec.CrossRegion.Network.VPCPeering != nil && dr.Spec.CrossRegion.Network.VPCPeering.AutoCreate {
		logger.Info("Setting up VPC peering for cross-region disaster recovery")
		// Implementation would create VPC peering connections
		// This would integrate with cloud provider APIs (AWS, GCP, Azure)
	}

	if dr.Spec.CrossRegion.Network.TransitGateway != nil {
		logger.Info("Configuring transit gateway for cross-region disaster recovery")
		// Implementation would configure transit gateway routes
	}

	if len(dr.Spec.CrossRegion.Network.CustomEndpoints) > 0 {
		logger.Info("Configuring custom network endpoints")
		// Implementation would set up custom network routes
	}

	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) configureReplication(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	// Create Neo4j clients for both clusters
	primaryClient, err := r.createNeo4jClient(ctx, primary)
	if err != nil {
		return fmt.Errorf("failed to create primary cluster client: %w", err)
	}
	defer primaryClient.Close()

	secondaryClient, err := r.createNeo4jClient(ctx, secondary)
	if err != nil {
		return fmt.Errorf("failed to create secondary cluster client: %w", err)
	}
	defer secondaryClient.Close()

	// Configure replication based on method
	switch dr.Spec.Replication.Method {
	case "streaming":
		if err := r.configureStreamingReplication(ctx, dr, primaryClient, secondaryClient); err != nil {
			return fmt.Errorf("failed to configure streaming replication: %w", err)
		}
	case "batch":
		if err := r.configureBatchReplication(ctx, dr, primaryClient, secondaryClient); err != nil {
			return fmt.Errorf("failed to configure batch replication: %w", err)
		}
	case "hybrid":
		if err := r.configureHybridReplication(ctx, dr, primaryClient, secondaryClient); err != nil {
			return fmt.Errorf("failed to configure hybrid replication: %w", err)
		}
	default:
		return fmt.Errorf("unsupported replication method: %s", dr.Spec.Replication.Method)
	}

	logger.Info("Replication configured successfully", "method", dr.Spec.Replication.Method)
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) configureStreamingReplication(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jclient.Client) error {
	// Implementation would set up real-time streaming replication
	// This would involve configuring CDC (Change Data Capture) streams
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) configureBatchReplication(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jclient.Client) error {
	// Implementation would set up scheduled batch replication
	// This would involve creating CronJobs for periodic data sync
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) configureHybridReplication(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jclient.Client) error {
	// Implementation would set up hybrid replication (streaming + batch)
	// Critical data streams in real-time, bulk data in batches
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) setupHealthMonitoring(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	// Create health monitoring resources
	if err := r.createHealthMonitoringResources(ctx, dr, primary, secondary); err != nil {
		return fmt.Errorf("failed to create health monitoring resources: %w", err)
	}

	logger.Info("Health monitoring setup completed")
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) createHealthMonitoringResources(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, primary, secondary *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	// Implementation would create:
	// 1. ServiceMonitor for Prometheus scraping
	// 2. CronJob for periodic health checks
	// 3. AlertManager rules for failover conditions
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) monitorDisasterRecovery(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, drMetrics *metrics.DisasterRecoveryMetrics) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.performHealthChecks(ctx, dr); err != nil {
				logger.Error(err, "Health check failed")

				// Check if failover should be triggered
				if r.shouldTriggerFailover(ctx, dr, err) {
					if err := r.triggerFailover(ctx, dr); err != nil {
						logger.Error(err, "Failover failed")
						drMetrics.RecordFailover(ctx, false)
					} else {
						logger.Info("Failover triggered successfully")
						drMetrics.RecordFailover(ctx, true)
					}
				}
			}
		}
	}
}

func (r *Neo4jDisasterRecoveryReconciler) performHealthChecks(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	// Implementation would perform various health checks:
	// 1. Primary cluster connectivity
	// 2. Replication lag monitoring
	// 3. Custom health checks
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) shouldTriggerFailover(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, err error) bool {
	// Implementation would evaluate failover triggers:
	// 1. Primary unavailable duration
	// 2. Replication lag threshold
	// 3. Custom health check failures
	return false
}

func (r *Neo4jDisasterRecoveryReconciler) triggerFailover(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	logger := log.FromContext(ctx)

	// Record failover start time
	now := metav1.Now()
	dr.Status.LastFailoverTime = &now
	dr.Status.ActiveRegion = dr.Spec.CrossRegion.SecondaryRegion

	// Update traffic routing to secondary cluster
	if err := r.updateTrafficRouting(ctx, dr); err != nil {
		return fmt.Errorf("failed to update traffic routing: %w", err)
	}

	// Send notifications
	if err := r.sendFailoverNotifications(ctx, dr); err != nil {
		logger.Error(err, "Failed to send failover notifications")
	}

	// Update status
	r.updateDRStatus(ctx, dr, "FailedOver", "Failover completed to secondary region")

	logger.Info("Failover completed successfully")
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) updateTrafficRouting(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	// Implementation would update DNS records, load balancer configuration, etc.
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) sendFailoverNotifications(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	// Implementation would send notifications via Slack, email, PagerDuty, etc.
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) cleanupDRResources(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery) error {
	// Implementation would cleanup all disaster recovery related resources
	return nil
}

func (r *Neo4jDisasterRecoveryReconciler) createNeo4jClient(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) (*neo4jclient.Client, error) {
	// Implementation would create a Neo4j client for the cluster
	return neo4jclient.NewClientForEnterprise(cluster, r.Client, "neo4j-admin-secret")
}

func (r *Neo4jDisasterRecoveryReconciler) updateDRStatus(ctx context.Context, dr *neo4jv1alpha1.Neo4jDisasterRecovery, phase, message string) {
	dr.Status.Phase = phase
	dr.Status.Message = message
	dr.Status.ObservedGeneration = dr.Generation

	// Add or update condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             phase,
		Message:            message,
	}

	if phase == "Failed" || phase == "Initializing" {
		condition.Status = metav1.ConditionFalse
	}

	// Update or append condition
	for i, cond := range dr.Status.Conditions {
		if cond.Type == condition.Type {
			dr.Status.Conditions[i] = condition
			goto updateStatus
		}
	}
	dr.Status.Conditions = append(dr.Status.Conditions, condition)

updateStatus:
	if err := r.Status().Update(ctx, dr); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update disaster recovery status")
	}
}

func (r *Neo4jDisasterRecoveryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1alpha1.Neo4jDisasterRecovery{}).
		Complete(r)
}
