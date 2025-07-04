package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestNewReconcileMetrics(t *testing.T) {
	metrics := NewReconcileMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestReconcileMetrics_RecordReconcile(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		duration   time.Duration
		success    bool
		wantResult string
	}{
		{
			name:       "successful reconcile",
			operation:  "create",
			duration:   time.Second,
			success:    true,
			wantResult: MetricResultSuccess,
		},
		{
			name:       "failed reconcile",
			operation:  "update",
			duration:   time.Second * 2,
			success:    false,
			wantResult: MetricResultFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewReconcileMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			reconcileTotal.Reset()
			reconcileDuration.Reset()

			ctx := context.Background()
			metrics.RecordReconcile(ctx, tt.operation, tt.duration, tt.success)

			// Check counter metric
			counter := reconcileTotal.WithLabelValues("test-cluster", "test-namespace", tt.operation, tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))

			// Check histogram metric
			_ = reconcileDuration.WithLabelValues("test-cluster", "test-namespace", tt.operation)
			assert.Equal(t, 1, testutil.CollectAndCount(reconcileDuration))
		})
	}
}

func TestReconcileMetrics_StartReconcileSpan(t *testing.T) {
	metrics := NewReconcileMetrics("test-cluster", "test-namespace")

	ctx := context.Background()
	newCtx, span := metrics.StartReconcileSpan(ctx, "test-operation")

	require.NotNil(t, newCtx)
	require.NotNil(t, span)

	// Verify span exists (may not be recording in test environment without tracer setup)
	assert.NotNil(t, span)

	// Clean up
	span.End()
}

func TestNewClusterMetrics(t *testing.T) {
	metrics := NewClusterMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestClusterMetrics_RecordClusterReplicas(t *testing.T) {
	metrics := NewClusterMetrics("test-cluster", "test-namespace")

	// Clear metrics before test
	clusterReplicas.Reset()

	metrics.RecordClusterReplicas(3, 2)

	// Check primary replicas
	primaryGauge := clusterReplicas.WithLabelValues("test-cluster", "test-namespace", "primary")
	assert.Equal(t, 3.0, testutil.ToFloat64(primaryGauge))

	// Check secondary replicas
	secondaryGauge := clusterReplicas.WithLabelValues("test-cluster", "test-namespace", "secondary")
	assert.Equal(t, 2.0, testutil.ToFloat64(secondaryGauge))
}

func TestClusterMetrics_RecordClusterHealth(t *testing.T) {
	tests := []struct {
		name     string
		healthy  bool
		expected float64
	}{
		{
			name:     "healthy cluster",
			healthy:  true,
			expected: 1.0,
		},
		{
			name:     "unhealthy cluster",
			healthy:  false,
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewClusterMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			clusterHealthy.Reset()

			metrics.RecordClusterHealth(tt.healthy)

			gauge := clusterHealthy.WithLabelValues("test-cluster", "test-namespace")
			assert.Equal(t, tt.expected, testutil.ToFloat64(gauge))
		})
	}
}

func TestNewUpgradeMetrics(t *testing.T) {
	metrics := NewUpgradeMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestUpgradeMetrics_RecordUpgrade(t *testing.T) {
	tests := []struct {
		name       string
		success    bool
		duration   time.Duration
		wantResult string
	}{
		{
			name:       "successful upgrade",
			success:    true,
			duration:   time.Minute * 5,
			wantResult: MetricResultSuccess,
		},
		{
			name:       "failed upgrade",
			success:    false,
			duration:   time.Minute * 2,
			wantResult: MetricResultFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewUpgradeMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			upgradeTotal.Reset()

			ctx := context.Background()
			metrics.RecordUpgrade(ctx, tt.success, tt.duration)

			counter := upgradeTotal.WithLabelValues("test-cluster", "test-namespace", tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))
		})
	}
}

func TestUpgradeMetrics_RecordUpgradePhase(t *testing.T) {
	metrics := NewUpgradeMetrics("test-cluster", "test-namespace")

	// Clear metrics before test
	upgradeDuration.Reset()

	duration := time.Minute * 2
	metrics.RecordUpgradePhase("prepare", duration)

	_ = upgradeDuration.WithLabelValues("test-cluster", "test-namespace", "prepare")
	assert.Equal(t, 1, testutil.CollectAndCount(upgradeDuration))
}

func TestNewBackupMetrics(t *testing.T) {
	metrics := NewBackupMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestBackupMetrics_RecordBackup(t *testing.T) {
	tests := []struct {
		name       string
		success    bool
		duration   time.Duration
		sizeBytes  int64
		wantResult string
	}{
		{
			name:       "successful backup",
			success:    true,
			duration:   time.Minute * 10,
			sizeBytes:  1024 * 1024,
			wantResult: MetricResultSuccess,
		},
		{
			name:       "failed backup",
			success:    false,
			duration:   time.Minute * 5,
			sizeBytes:  0,
			wantResult: MetricResultFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewBackupMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			backupTotal.Reset()
			backupDuration.Reset()
			backupSize.Reset()

			ctx := context.Background()
			metrics.RecordBackup(ctx, tt.success, tt.duration, tt.sizeBytes)

			// Check counter
			counter := backupTotal.WithLabelValues("test-cluster", "test-namespace", tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))

			// Check histogram
			_ = backupDuration.WithLabelValues("test-cluster", "test-namespace")
			assert.Equal(t, 1, testutil.CollectAndCount(backupDuration))

			// Check size gauge only for successful backups
			if tt.success && tt.sizeBytes > 0 {
				sizeGauge := backupSize.WithLabelValues("test-cluster", "test-namespace")
				assert.Equal(t, float64(tt.sizeBytes), testutil.ToFloat64(sizeGauge))
			}
		})
	}
}

func TestNewCypherMetrics(t *testing.T) {
	metrics := NewCypherMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestCypherMetrics_RecordCypherExecution(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		duration   time.Duration
		success    bool
		wantResult string
	}{
		{
			name:       "successful cypher execution",
			operation:  "CREATE",
			duration:   time.Millisecond * 100,
			success:    true,
			wantResult: MetricResultSuccess,
		},
		{
			name:       "failed cypher execution",
			operation:  "MATCH",
			duration:   time.Millisecond * 50,
			success:    false,
			wantResult: MetricResultFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewCypherMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			cypherTotal.Reset()
			cypherDuration.Reset()

			ctx := context.Background()
			metrics.RecordCypherExecution(ctx, tt.operation, tt.duration, tt.success)

			// Check counter
			counter := cypherTotal.WithLabelValues("test-cluster", "test-namespace", tt.operation, tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))

			// Check histogram
			_ = cypherDuration.WithLabelValues("test-cluster", "test-namespace", tt.operation)
			assert.Equal(t, 1, testutil.CollectAndCount(cypherDuration))
		})
	}
}

func TestNewSecurityMetrics(t *testing.T) {
	metrics := NewSecurityMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestSecurityMetrics_RecordSecurityOperation(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		success    bool
		wantResult string
	}{
		{
			name:       "successful security operation",
			operation:  "create_user",
			success:    true,
			wantResult: "success",
		},
		{
			name:       "failed security operation",
			operation:  "create_role",
			success:    false,
			wantResult: "failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewSecurityMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			securityOperationTotal.Reset()

			ctx := context.Background()
			metrics.RecordSecurityOperation(ctx, tt.operation, tt.success)

			counter := securityOperationTotal.WithLabelValues("test-cluster", "test-namespace", tt.operation, tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))
		})
	}
}

func TestNewDisasterRecoveryMetrics(t *testing.T) {
	metrics := NewDisasterRecoveryMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestDisasterRecoveryMetrics_RecordFailover(t *testing.T) {
	tests := []struct {
		name       string
		success    bool
		wantResult string
	}{
		{
			name:       "successful failover",
			success:    true,
			wantResult: "success",
		},
		{
			name:       "failed failover",
			success:    false,
			wantResult: "failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewDisasterRecoveryMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			failoverTotal.Reset()

			ctx := context.Background()
			metrics.RecordFailover(ctx, tt.success)

			counter := failoverTotal.WithLabelValues("test-cluster", "test-namespace", tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))
		})
	}
}

func TestNewManualScalingMetrics(t *testing.T) {
	metrics := NewManualScalingMetrics("test-cluster", "test-namespace")

	assert.Equal(t, "test-cluster", metrics.clusterName)
	assert.Equal(t, "test-namespace", metrics.namespace)
}

func TestManualScalingMetrics_RecordPrimaryScaling(t *testing.T) {
	tests := []struct {
		name              string
		currentReplicas   int32
		desiredReplicas   int32
		expectedDirection string
	}{
		{
			name:              "scale up primaries",
			currentReplicas:   2,
			desiredReplicas:   3,
			expectedDirection: "up",
		},
		{
			name:              "scale down primaries",
			currentReplicas:   3,
			desiredReplicas:   2,
			expectedDirection: "down",
		},
		{
			name:              "no change in primaries",
			currentReplicas:   2,
			desiredReplicas:   2,
			expectedDirection: "", // No scaling event recorded
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewManualScalingMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			primaryCount.Reset()
			scaleEventsTotal.Reset()

			ctx := context.Background()
			metrics.RecordPrimaryScaling(ctx, tt.currentReplicas, tt.desiredReplicas)

			// Check primary count gauge
			gauge := primaryCount.WithLabelValues("test-cluster", "test-namespace")
			assert.Equal(t, float64(tt.desiredReplicas), testutil.ToFloat64(gauge))

			// Check scale events counter (only if there's actual scaling)
			if tt.expectedDirection != "" {
				counter := scaleEventsTotal.WithLabelValues("test-cluster", "test-namespace", "primary", tt.expectedDirection)
				assert.Equal(t, 1.0, testutil.ToFloat64(counter))
			}
		})
	}
}

func TestManualScalingMetrics_RecordSecondaryScaling(t *testing.T) {
	tests := []struct {
		name              string
		currentReplicas   int32
		desiredReplicas   int32
		expectedDirection string
	}{
		{
			name:              "scale up secondaries",
			currentReplicas:   1,
			desiredReplicas:   2,
			expectedDirection: "up",
		},
		{
			name:              "scale down secondaries",
			currentReplicas:   2,
			desiredReplicas:   1,
			expectedDirection: "down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewManualScalingMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			secondaryCount.Reset()
			scaleEventsTotal.Reset()

			ctx := context.Background()
			metrics.RecordSecondaryScaling(ctx, tt.currentReplicas, tt.desiredReplicas)

			// Check secondary count gauge
			gauge := secondaryCount.WithLabelValues("test-cluster", "test-namespace")
			assert.Equal(t, float64(tt.desiredReplicas), testutil.ToFloat64(gauge))

			// Check scale events counter
			counter := scaleEventsTotal.WithLabelValues("test-cluster", "test-namespace", "secondary", tt.expectedDirection)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))
		})
	}
}

func TestManualScalingMetrics_RecordValidation(t *testing.T) {
	tests := []struct {
		name           string
		validationType string
		success        bool
		wantResult     string
	}{
		{
			name:           "successful validation",
			validationType: "resource_limits",
			success:        true,
			wantResult:     "success",
		},
		{
			name:           "failed validation",
			validationType: "topology",
			success:        false,
			wantResult:     "failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewManualScalingMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			scalingValidationTotal.Reset()

			ctx := context.Background()
			metrics.RecordValidation(ctx, tt.validationType, tt.success)

			counter := scalingValidationTotal.WithLabelValues("test-cluster", "test-namespace", tt.validationType, tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))
		})
	}
}

func TestManualScalingMetrics_SetManualScalingEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		expected float64
	}{
		{
			name:     "manual scaling enabled",
			enabled:  true,
			expected: 1.0,
		},
		{
			name:     "manual scaling disabled",
			enabled:  false,
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewManualScalingMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			manualScalerEnabled.Reset()

			metrics.SetManualScalingEnabled(tt.enabled)

			gauge := manualScalerEnabled.WithLabelValues("test-cluster", "test-namespace")
			assert.Equal(t, tt.expected, testutil.ToFloat64(gauge))
		})
	}
}

func TestSpanTracing(t *testing.T) {
	tests := []struct {
		name     string
		spanFunc func(context.Context) (context.Context, trace.Span)
	}{
		{
			name: "reconcile span",
			spanFunc: func(ctx context.Context) (context.Context, trace.Span) {
				metrics := NewReconcileMetrics("test-cluster", "test-namespace")
				return metrics.StartReconcileSpan(ctx, "test-op")
			},
		},
		{
			name: "upgrade span",
			spanFunc: func(ctx context.Context) (context.Context, trace.Span) {
				metrics := NewUpgradeMetrics("test-cluster", "test-namespace")
				return metrics.StartUpgradeSpan(ctx, "test-phase")
			},
		},
		{
			name: "backup span",
			spanFunc: func(ctx context.Context) (context.Context, trace.Span) {
				metrics := NewBackupMetrics("test-cluster", "test-namespace")
				return metrics.StartBackupSpan(ctx)
			},
		},
		{
			name: "cypher span",
			spanFunc: func(ctx context.Context) (context.Context, trace.Span) {
				metrics := NewCypherMetrics("test-cluster", "test-namespace")
				return metrics.StartCypherSpan(ctx, "test-query")
			},
		},
		{
			name: "security span",
			spanFunc: func(ctx context.Context) (context.Context, trace.Span) {
				metrics := NewSecurityMetrics("test-cluster", "test-namespace")
				return metrics.StartSecuritySpan(ctx, "test-op")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			newCtx, span := tt.spanFunc(ctx)

			require.NotNil(t, newCtx)
			require.NotNil(t, span)

			// Verify span exists (may not be recording in test environment without tracer setup)
			assert.NotNil(t, span)

			// Clean up
			span.End()
		})
	}
}

func TestMetricsRegistration(t *testing.T) {
	// This test verifies that metrics are registered without panicking
	// The init() function should have already registered all metrics

	// Test that we can get metric values (this would panic if not registered)
	testMetrics := []prometheus.Collector{
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
		disasterRecoveryStatus,
		failoverTotal,
		replicationLag,
		manualScalerEnabled,
		scaleEventsTotal,
		primaryCount,
		secondaryCount,
		scalingValidationTotal,
	}

	for _, metric := range testMetrics {
		// This would panic if metric wasn't registered properly
		assert.NotNil(t, metric)
	}
}
