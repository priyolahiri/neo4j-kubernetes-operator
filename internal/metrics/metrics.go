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

package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// Metric subsystem
	subsystem = "neo4j_operator"

	// Common labels
	LabelClusterName = "cluster_name"
	LabelNamespace   = "namespace"
	LabelOperation   = "operation"
	LabelResult      = "result"
	LabelPhase       = "phase"
	LabelRole        = "role"
)

var (
	// Tracer for OTEL tracing
	tracer = otel.Tracer("neo4j-operator")
	meter  = otel.Meter("neo4j-operator")

	// Prometheus metrics

	// Cluster metrics
	clusterReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "cluster_replicas_total",
			Help:      "Total number of Neo4j cluster replicas by role",
		},
		[]string{LabelClusterName, LabelNamespace, LabelRole},
	)

	clusterHealthy = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "cluster_healthy",
			Help:      "Whether the Neo4j cluster is healthy (1 = healthy, 0 = unhealthy)",
		},
		[]string{LabelClusterName, LabelNamespace},
	)

	// Reconciliation metrics
	reconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "reconcile_total",
			Help:      "Total number of reconciliation attempts",
		},
		[]string{LabelClusterName, LabelNamespace, LabelOperation, LabelResult},
	)

	reconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "reconcile_duration_seconds",
			Help:      "Time spent on reconciliation operations",
			Buckets:   []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{LabelClusterName, LabelNamespace, LabelOperation},
	)

	// Upgrade metrics
	upgradeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "upgrade_total",
			Help:      "Total number of upgrade attempts",
		},
		[]string{LabelClusterName, LabelNamespace, LabelResult},
	)

	upgradeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "upgrade_duration_seconds",
			Help:      "Time spent on upgrade operations",
			Buckets:   []float64{60.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0},
		},
		[]string{LabelClusterName, LabelNamespace, LabelPhase},
	)

	// Backup metrics
	backupTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "backup_total",
			Help:      "Total number of backup attempts",
		},
		[]string{LabelClusterName, LabelNamespace, LabelResult},
	)

	backupDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "backup_duration_seconds",
			Help:      "Time spent on backup operations",
			Buckets:   []float64{60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0},
		},
		[]string{LabelClusterName, LabelNamespace},
	)

	backupSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "backup_size_bytes",
			Help:      "Size of the latest backup in bytes",
		},
		[]string{LabelClusterName, LabelNamespace},
	)

	// Cypher execution metrics
	cypherTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "cypher_executions_total",
			Help:      "Total number of Cypher statement executions",
		},
		[]string{LabelClusterName, LabelNamespace, LabelOperation, LabelResult},
	)

	cypherDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "cypher_execution_duration_seconds",
			Help:      "Time spent executing Cypher statements",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]string{LabelClusterName, LabelNamespace, LabelOperation},
	)

	// Security metrics
	securityOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "security_operations_total",
			Help:      "Total number of security operations (user, role, grant)",
		},
		[]string{LabelClusterName, LabelNamespace, LabelOperation, LabelResult},
	)
)

func init() {
	// Register Prometheus metrics
	metrics.Registry.MustRegister(
		clusterReplicas,
		clusterHealthy,
		reconcileTotal,
		reconcileDuration,
		upgradeTotal,
		upgradeDuration,
		backupTotal,
		backupDuration,
		backupSize,
		cypherTotal,
		cypherDuration,
		securityOperationTotal,
		// New feature metrics
		disasterRecoveryStatus,
		failoverTotal,
		replicationLag,
		autoScalerEnabled,
		scaleEventsTotal,
		readReplicaCount,
	)
}

// ReconcileMetrics provides methods for recording reconciliation metrics
type ReconcileMetrics struct {
	clusterName string
	namespace   string
}

// NewReconcileMetrics creates a new ReconcileMetrics instance
func NewReconcileMetrics(clusterName, namespace string) *ReconcileMetrics {
	return &ReconcileMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordReconcile records a reconciliation operation with tracing
func (m *ReconcileMetrics) RecordReconcile(ctx context.Context, operation string, duration time.Duration, success bool) {
	result := "success"
	if !success {
		result = "failure"
	}

	// Record Prometheus metrics
	reconcileTotal.WithLabelValues(m.clusterName, m.namespace, operation, result).Inc()
	reconcileDuration.WithLabelValues(m.clusterName, m.namespace, operation).Observe(duration.Seconds())

	// Add span attributes if tracing is enabled
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
			attribute.String("result", result),
			attribute.Float64("duration.seconds", duration.Seconds()),
		)
	}
}

// StartReconcileSpan starts a new tracing span for reconciliation
func (m *ReconcileMetrics) StartReconcileSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "reconcile."+operation,
		trace.WithAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
		))
}

// ClusterMetrics provides methods for recording cluster-related metrics
type ClusterMetrics struct {
	clusterName string
	namespace   string
}

// NewClusterMetrics creates a new ClusterMetrics instance
func NewClusterMetrics(clusterName, namespace string) *ClusterMetrics {
	return &ClusterMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordClusterReplicas records the number of replicas by role
func (m *ClusterMetrics) RecordClusterReplicas(primaries, secondaries int32) {
	clusterReplicas.WithLabelValues(m.clusterName, m.namespace, "primary").Set(float64(primaries))
	clusterReplicas.WithLabelValues(m.clusterName, m.namespace, "secondary").Set(float64(secondaries))
}

// RecordClusterHealth records cluster health status
func (m *ClusterMetrics) RecordClusterHealth(healthy bool) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	clusterHealthy.WithLabelValues(m.clusterName, m.namespace).Set(value)
}

// UpgradeMetrics provides methods for recording upgrade-related metrics
type UpgradeMetrics struct {
	clusterName string
	namespace   string
}

// NewUpgradeMetrics creates a new UpgradeMetrics instance
func NewUpgradeMetrics(clusterName, namespace string) *UpgradeMetrics {
	return &UpgradeMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordUpgrade records an upgrade operation
func (m *UpgradeMetrics) RecordUpgrade(ctx context.Context, success bool, totalDuration time.Duration) {
	result := "success"
	if !success {
		result = "failure"
	}

	upgradeTotal.WithLabelValues(m.clusterName, m.namespace, result).Inc()

	// Add span attributes
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("result", result),
			attribute.Float64("total.duration.seconds", totalDuration.Seconds()),
		)
	}
}

// RecordUpgradePhase records duration for a specific upgrade phase
func (m *UpgradeMetrics) RecordUpgradePhase(phase string, duration time.Duration) {
	upgradeDuration.WithLabelValues(m.clusterName, m.namespace, phase).Observe(duration.Seconds())
}

// StartUpgradeSpan starts a new tracing span for upgrade operations
func (m *UpgradeMetrics) StartUpgradeSpan(ctx context.Context, phase string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "upgrade."+phase,
		trace.WithAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("phase", phase),
		))
}

// BackupMetrics provides methods for recording backup-related metrics
type BackupMetrics struct {
	clusterName string
	namespace   string
}

// NewBackupMetrics creates a new BackupMetrics instance
func NewBackupMetrics(clusterName, namespace string) *BackupMetrics {
	return &BackupMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordBackup records a backup operation
func (m *BackupMetrics) RecordBackup(ctx context.Context, success bool, duration time.Duration, sizeBytes int64) {
	result := "success"
	if !success {
		result = "failure"
	}

	backupTotal.WithLabelValues(m.clusterName, m.namespace, result).Inc()
	backupDuration.WithLabelValues(m.clusterName, m.namespace).Observe(duration.Seconds())

	if success && sizeBytes > 0 {
		backupSize.WithLabelValues(m.clusterName, m.namespace).Set(float64(sizeBytes))
	}

	// Add span attributes
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("result", result),
			attribute.Float64("duration.seconds", duration.Seconds()),
			attribute.Int64("size.bytes", sizeBytes),
		)
	}
}

// StartBackupSpan starts a new tracing span for backup operations
func (m *BackupMetrics) StartBackupSpan(ctx context.Context) (context.Context, trace.Span) {
	return tracer.Start(ctx, "backup",
		trace.WithAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
		))
}

// CypherMetrics provides methods for recording Cypher execution metrics
type CypherMetrics struct {
	clusterName string
	namespace   string
}

// NewCypherMetrics creates a new CypherMetrics instance
func NewCypherMetrics(clusterName, namespace string) *CypherMetrics {
	return &CypherMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordCypherExecution records a Cypher statement execution
func (m *CypherMetrics) RecordCypherExecution(ctx context.Context, operation string, duration time.Duration, success bool) {
	result := "success"
	if !success {
		result = "failure"
	}

	cypherTotal.WithLabelValues(m.clusterName, m.namespace, operation, result).Inc()
	cypherDuration.WithLabelValues(m.clusterName, m.namespace, operation).Observe(duration.Seconds())

	// Add span attributes
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
			attribute.String("result", result),
			attribute.Float64("duration.seconds", duration.Seconds()),
		)
	}
}

// StartCypherSpan starts a new tracing span for Cypher execution
func (m *CypherMetrics) StartCypherSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "cypher."+operation,
		trace.WithAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
		))
}

// SecurityMetrics provides methods for recording security operation metrics
type SecurityMetrics struct {
	clusterName string
	namespace   string
}

// NewSecurityMetrics creates a new SecurityMetrics instance
func NewSecurityMetrics(clusterName, namespace string) *SecurityMetrics {
	return &SecurityMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordSecurityOperation records a security operation (user, role, grant)
func (m *SecurityMetrics) RecordSecurityOperation(ctx context.Context, operation string, success bool) {
	result := "success"
	if !success {
		result = "failure"
	}

	securityOperationTotal.WithLabelValues(m.clusterName, m.namespace, operation, result).Inc()

	// Add span attributes
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
			attribute.String("result", result),
		)
	}
}

// StartSecuritySpan starts a new tracing span for security operations
func (m *SecurityMetrics) StartSecuritySpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "security."+operation,
		trace.WithAttributes(
			attribute.String("cluster.name", m.clusterName),
			attribute.String("namespace", m.namespace),
			attribute.String("operation", operation),
		))
}

// Enhanced metrics for new features

var (
	// Disaster Recovery metrics
	disasterRecoveryStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "disaster_recovery_status",
			Help:      "Status of disaster recovery setup (1=ready, 0=not ready)",
		},
		[]string{LabelClusterName, LabelNamespace, "primary_region", "secondary_region"},
	)

	failoverTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "failover_total",
			Help:      "Total number of failovers performed",
		},
		[]string{LabelClusterName, LabelNamespace, LabelResult},
	)

	replicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "replication_lag_seconds",
			Help:      "Replication lag in seconds",
		},
		[]string{LabelClusterName, LabelNamespace, "primary_region", "secondary_region"},
	)

	// Auto-scaling metrics
	autoScalerEnabled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "autoscaler_enabled",
			Help:      "Status of auto-scaler (1=enabled, 0=disabled)",
		},
		[]string{LabelClusterName, LabelNamespace},
	)

	scaleEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "scale_events_total",
			Help:      "Total number of scale events",
		},
		[]string{LabelClusterName, LabelNamespace, "direction"},
	)

	readReplicaCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "read_replica_count",
			Help:      "Current number of read replicas",
		},
		[]string{LabelClusterName, LabelNamespace},
	)
)

// DisasterRecoveryMetrics provides methods for recording disaster recovery metrics
type DisasterRecoveryMetrics struct {
	clusterName string
	namespace   string
}

// NewDisasterRecoveryMetrics creates a new DisasterRecoveryMetrics instance
func NewDisasterRecoveryMetrics(clusterName, namespace string) *DisasterRecoveryMetrics {
	return &DisasterRecoveryMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordFailover records a failover event
func (m *DisasterRecoveryMetrics) RecordFailover(ctx context.Context, success bool) {
	result := "failure"
	if success {
		result = "success"
	}
	failoverTotal.WithLabelValues(m.clusterName, m.namespace, result).Inc()
}

// AutoScalingMetrics provides methods for recording auto-scaling metrics
type AutoScalingMetrics struct {
	clusterName string
	namespace   string
}

// NewAutoScalingMetrics creates a new AutoScalingMetrics instance
func NewAutoScalingMetrics(clusterName, namespace string) *AutoScalingMetrics {
	return &AutoScalingMetrics{
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// RecordScalingEvent records a scaling event
func (m *AutoScalingMetrics) RecordScalingEvent(ctx context.Context, currentReplicas, desiredReplicas int32) {
	readReplicaCount.WithLabelValues(m.clusterName, m.namespace).Set(float64(currentReplicas))

	if desiredReplicas > currentReplicas {
		scaleEventsTotal.WithLabelValues(m.clusterName, m.namespace, "up").Inc()
	} else if desiredReplicas < currentReplicas {
		scaleEventsTotal.WithLabelValues(m.clusterName, m.namespace, "down").Inc()
	}
}

// RecordManualScale records a manual scaling operation
func (m *AutoScalingMetrics) RecordManualScale(ctx context.Context, replicas int32) {
	readReplicaCount.WithLabelValues(m.clusterName, m.namespace).Set(float64(replicas))
}
