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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
	neo4jclient "github.com/neo4j-labs/neo4j-operator/internal/neo4j"
)

// Neo4jPluginReconciler reconciles a Neo4jPlugin object
type Neo4jPluginReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	RequeueAfter time.Duration
}

const PluginFinalizer = "neo4j.neo4j.com/plugin-finalizer"

//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins/finalizers,verbs=update

func (r *Neo4jPluginReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Neo4jPlugin instance
	plugin := &neo4jv1alpha1.Neo4jPlugin{}
	if err := r.Get(ctx, req.NamespacedName, plugin); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Neo4jPlugin resource not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Neo4jPlugin")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if plugin.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, plugin)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(plugin, PluginFinalizer) {
		controllerutil.AddFinalizer(plugin, PluginFinalizer)
		if err := r.Update(ctx, plugin); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Get target cluster
	cluster, err := r.getTargetCluster(ctx, plugin)
	if err != nil {
		logger.Error(err, "Failed to get target cluster")
		r.updatePluginStatus(ctx, plugin, "Failed", fmt.Sprintf("Target cluster not found: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Update status to "Installing"
	r.updatePluginStatus(ctx, plugin, "Installing", "Installing plugin")

	// Install plugin
	if err := r.installPlugin(ctx, plugin, cluster); err != nil {
		logger.Error(err, "Failed to install plugin")
		r.updatePluginStatus(ctx, plugin, "Failed", fmt.Sprintf("Plugin installation failed: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Configure plugin
	if err := r.configurePlugin(ctx, plugin, cluster); err != nil {
		logger.Error(err, "Failed to configure plugin")
		r.updatePluginStatus(ctx, plugin, "Failed", fmt.Sprintf("Plugin configuration failed: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Update status to "Ready"
	r.updatePluginStatus(ctx, plugin, "Ready", "Plugin installed and configured successfully")

	logger.Info("Plugin reconciliation completed")
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

func (r *Neo4jPluginReconciler) handleDeletion(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Uninstall plugin
	if err := r.uninstallPlugin(ctx, plugin); err != nil {
		logger.Error(err, "Failed to uninstall plugin")
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, err
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(plugin, PluginFinalizer)
	if err := r.Update(ctx, plugin); err != nil {
		logger.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	logger.Info("Plugin deleted successfully")
	return ctrl.Result{}, nil
}

func (r *Neo4jPluginReconciler) getTargetCluster(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) (*neo4jv1alpha1.Neo4jEnterpriseCluster, error) {
	cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      plugin.Spec.ClusterRef,
		Namespace: plugin.Namespace,
	}, cluster); err != nil {
		return nil, fmt.Errorf("failed to get cluster %s: %w", plugin.Spec.ClusterRef, err)
	}

	if cluster.Status.Phase != "Ready" {
		return nil, fmt.Errorf("cluster %s is not ready: %s", cluster.Name, cluster.Status.Phase)
	}

	return cluster, nil
}

func (r *Neo4jPluginReconciler) installPlugin(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	// Create Neo4j client
	neo4jClient, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, "neo4j-admin-secret")
	if err != nil {
		return fmt.Errorf("failed to create Neo4j client: %w", err)
	}
	defer neo4jClient.Close()

	// Download plugin if needed
	if err := r.downloadPlugin(ctx, plugin); err != nil {
		return fmt.Errorf("failed to download plugin: %w", err)
	}

	// Install dependencies first
	for _, dep := range plugin.Spec.Dependencies {
		if err := r.installDependency(ctx, plugin, cluster, dep); err != nil {
			if !dep.Optional {
				return fmt.Errorf("failed to install required dependency %s: %w", dep.Name, err)
			}
			logger.Error(err, "Failed to install optional dependency", "dependency", dep.Name)
		}
	}

	// Install the plugin
	if err := r.performPluginInstallation(ctx, plugin, cluster, neo4jClient); err != nil {
		return fmt.Errorf("failed to perform plugin installation: %w", err)
	}

	logger.Info("Plugin installed successfully", "plugin", plugin.Spec.Name, "version", plugin.Spec.Version)
	return nil
}

func (r *Neo4jPluginReconciler) downloadPlugin(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {

	if plugin.Spec.Source == nil {
		// Use default source (official Neo4j plugin repository)
		return r.downloadFromOfficialRepository(ctx, plugin)
	}

	switch plugin.Spec.Source.Type {
	case "official":
		return r.downloadFromOfficialRepository(ctx, plugin)
	case "community":
		return r.downloadFromCommunityRepository(ctx, plugin)
	case "custom":
		return r.downloadFromCustomRepository(ctx, plugin)
	case "url":
		return r.downloadFromURL(ctx, plugin)
	default:
		return fmt.Errorf("unsupported source type: %s", plugin.Spec.Source.Type)
	}
}

func (r *Neo4jPluginReconciler) downloadFromOfficialRepository(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would download from official Neo4j plugin repository
	log.FromContext(ctx).Info("Downloading plugin from official repository", "plugin", plugin.Spec.Name)
	return nil
}

func (r *Neo4jPluginReconciler) downloadFromCommunityRepository(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would download from community repository
	log.FromContext(ctx).Info("Downloading plugin from community repository", "plugin", plugin.Spec.Name)
	return nil
}

func (r *Neo4jPluginReconciler) downloadFromCustomRepository(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would download from custom registry
	log.FromContext(ctx).Info("Downloading plugin from custom repository", "plugin", plugin.Spec.Name)
	return nil
}

func (r *Neo4jPluginReconciler) downloadFromURL(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would download plugin from direct URL
	log.FromContext(ctx).Info("Downloading plugin from URL", "plugin", plugin.Spec.Name, "url", plugin.Spec.Source.URL)

	// Verify checksum if provided
	if plugin.Spec.Source.Checksum != "" {
		if err := r.verifyChecksum(ctx, plugin); err != nil {
			return fmt.Errorf("checksum verification failed: %w", err)
		}
	}

	return nil
}

func (r *Neo4jPluginReconciler) verifyChecksum(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would verify the downloaded plugin's checksum
	return nil
}

func (r *Neo4jPluginReconciler) installDependency(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, dep neo4jv1alpha1.PluginDependency) error {
	logger := log.FromContext(ctx)

	logger.Info("Installing plugin dependency", "dependency", dep.Name, "constraint", dep.VersionConstraint)

	// Create dependency plugin resource
	depPlugin := &neo4jv1alpha1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-dep", plugin.Name, dep.Name),
			Namespace: plugin.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "neo4j-plugin",
				"app.kubernetes.io/instance":  plugin.Spec.ClusterRef,
				"app.kubernetes.io/component": "dependency",
				"neo4j.plugin/parent":         plugin.Name,
			},
		},
		Spec: neo4jv1alpha1.Neo4jPluginSpec{
			ClusterRef: plugin.Spec.ClusterRef,
			Name:       dep.Name,
			Version:    dep.VersionConstraint,
			Enabled:    true,
		},
	}

	// Create or update dependency plugin
	if err := r.Create(ctx, depPlugin); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create dependency plugin: %w", err)
	}

	// Wait for dependency to be ready
	return r.waitForPluginReady(ctx, depPlugin)
}

func (r *Neo4jPluginReconciler) waitForPluginReady(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Implementation would wait for plugin to be in Ready state
	return nil
}

func (r *Neo4jPluginReconciler) performPluginInstallation(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster, neo4jClient *neo4jclient.Client) error {
	logger := log.FromContext(ctx)

	// Copy plugin to Neo4j plugins directory
	if err := r.copyPluginToCluster(ctx, plugin, cluster); err != nil {
		return fmt.Errorf("failed to copy plugin to cluster: %w", err)
	}

	// Restart Neo4j instances to load plugin
	if err := r.restartNeo4jInstances(ctx, cluster); err != nil {
		return fmt.Errorf("failed to restart Neo4j instances: %w", err)
	}

	// Wait for cluster to be ready after restart
	if err := r.waitForClusterReady(ctx, cluster); err != nil {
		return fmt.Errorf("cluster not ready after plugin installation: %w", err)
	}

	// Verify plugin is loaded
	if err := r.verifyPluginLoaded(ctx, neo4jClient, plugin); err != nil {
		return fmt.Errorf("plugin verification failed: %w", err)
	}

	logger.Info("Plugin installation completed successfully")
	return nil
}

func (r *Neo4jPluginReconciler) copyPluginToCluster(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	// Implementation would:
	// 1. Create a Job or init container to copy plugin jar to plugins directory
	// 2. Update the StatefulSet to mount the plugin
	log.FromContext(ctx).Info("Copying plugin to cluster", "plugin", plugin.Spec.Name)
	return nil
}

func (r *Neo4jPluginReconciler) restartNeo4jInstances(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	// Implementation would trigger a rolling restart of Neo4j instances
	log.FromContext(ctx).Info("Restarting Neo4j instances to load plugin")
	return nil
}

func (r *Neo4jPluginReconciler) waitForClusterReady(ctx context.Context, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	// Implementation would wait for cluster to return to Ready state
	return nil
}

func (r *Neo4jPluginReconciler) verifyPluginLoaded(ctx context.Context, neo4jClient *neo4jclient.Client, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Verify plugin is loaded by calling CALL dbms.components() or similar
	components, err := neo4jClient.GetLoadedComponents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get loaded components: %w", err)
	}

	for _, component := range components {
		if component.Name == plugin.Spec.Name {
			return nil
		}
	}

	return fmt.Errorf("plugin %s not found in loaded components", plugin.Spec.Name)
}

func (r *Neo4jPluginReconciler) configurePlugin(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	logger := log.FromContext(ctx)

	if len(plugin.Spec.Config) == 0 {
		logger.Info("No plugin configuration provided")
		return nil
	}

	// Create Neo4j client
	neo4jClient, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, "neo4j-admin-secret")
	if err != nil {
		return fmt.Errorf("failed to create Neo4j client: %w", err)
	}
	defer neo4jClient.Close()

	// Configure plugin settings
	for key, value := range plugin.Spec.Config {
		if err := neo4jClient.SetConfiguration(ctx, key, value); err != nil {
			return fmt.Errorf("failed to set configuration %s=%s: %w", key, value, err)
		}
	}

	// Apply security configuration if specified
	if plugin.Spec.Security != nil {
		if err := r.applySecurityConfiguration(ctx, neo4jClient, plugin); err != nil {
			return fmt.Errorf("failed to apply security configuration: %w", err)
		}
	}

	logger.Info("Plugin configuration applied successfully")
	return nil
}

func (r *Neo4jPluginReconciler) applySecurityConfiguration(ctx context.Context, neo4jClient *neo4jclient.Client, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	security := plugin.Spec.Security

	// Configure allowed procedures
	if len(security.AllowedProcedures) > 0 {
		if err := neo4jClient.SetAllowedProcedures(ctx, security.AllowedProcedures); err != nil {
			return fmt.Errorf("failed to set allowed procedures: %w", err)
		}
	}

	// Configure denied procedures
	if len(security.DeniedProcedures) > 0 {
		if err := neo4jClient.SetDeniedProcedures(ctx, security.DeniedProcedures); err != nil {
			return fmt.Errorf("failed to set denied procedures: %w", err)
		}
	}

	// Configure sandbox mode
	if security.Sandbox {
		if err := neo4jClient.EnableSandboxMode(ctx, security.Sandbox); err != nil {
			return fmt.Errorf("failed to enable sandbox mode: %w", err)
		}
	}

	return nil
}

func (r *Neo4jPluginReconciler) uninstallPlugin(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	logger := log.FromContext(ctx)

	// Get target cluster
	cluster, err := r.getTargetCluster(ctx, plugin)
	if err != nil {
		// If cluster is not found, consider plugin already uninstalled
		if errors.IsNotFound(err) {
			logger.Info("Target cluster not found, considering plugin uninstalled")
			return nil
		}
		return fmt.Errorf("failed to get target cluster: %w", err)
	}

	// Remove plugin from cluster
	if err := r.removePluginFromCluster(ctx, plugin, cluster); err != nil {
		return fmt.Errorf("failed to remove plugin from cluster: %w", err)
	}

	// Clean up plugin dependencies
	if err := r.cleanupDependencies(ctx, plugin); err != nil {
		logger.Error(err, "Failed to cleanup dependencies")
	}

	logger.Info("Plugin uninstalled successfully")
	return nil
}

func (r *Neo4jPluginReconciler) removePluginFromCluster(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) error {
	// Implementation would:
	// 1. Remove plugin jar from plugins directory
	// 2. Restart Neo4j instances
	// 3. Verify plugin is no longer loaded
	log.FromContext(ctx).Info("Removing plugin from cluster", "plugin", plugin.Spec.Name)
	return nil
}

func (r *Neo4jPluginReconciler) cleanupDependencies(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin) error {
	// Clean up dependency plugins created for this plugin
	dependencyPlugins := &neo4jv1alpha1.Neo4jPluginList{}
	if err := r.List(ctx, dependencyPlugins, client.InNamespace(plugin.Namespace), client.MatchingLabels{
		"neo4j.plugin/parent": plugin.Name,
	}); err != nil {
		return fmt.Errorf("failed to list dependency plugins: %w", err)
	}

	for _, depPlugin := range dependencyPlugins.Items {
		if err := r.Delete(ctx, &depPlugin); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete dependency plugin %s: %w", depPlugin.Name, err)
		}
	}

	return nil
}

func (r *Neo4jPluginReconciler) updatePluginStatus(ctx context.Context, plugin *neo4jv1alpha1.Neo4jPlugin, phase, message string) {
	plugin.Status.Phase = phase
	plugin.Status.Message = message
	plugin.Status.ObservedGeneration = plugin.Generation

	// Add or update condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             phase,
		Message:            message,
	}

	if phase == "Failed" || phase == "Installing" {
		condition.Status = metav1.ConditionFalse
	}

	// Update or append condition
	for i, cond := range plugin.Status.Conditions {
		if cond.Type == condition.Type {
			plugin.Status.Conditions[i] = condition
			goto updateStatus
		}
	}
	plugin.Status.Conditions = append(plugin.Status.Conditions, condition)

updateStatus:
	if err := r.Status().Update(ctx, plugin); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update plugin status")
	}
}

func (r *Neo4jPluginReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1alpha1.Neo4jPlugin{}).
		Complete(r)
}
