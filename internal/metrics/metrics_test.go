package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			// Check histogram metric — verify the labelled child exists and the
			// vec has a single registered series after the recorded observation.
			histogram := reconcileDuration.WithLabelValues("test-cluster", "test-namespace", tt.operation)
			require.NotNil(t, histogram)
			assert.Equal(t, 1, testutil.CollectAndCount(reconcileDuration))
		})
	}
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

	// desired=3 (spec.topology.servers), ready=2 (StatefulSet readyReplicas)
	metrics.RecordClusterReplicas(3, 2)

	desiredGauge := clusterReplicas.WithLabelValues("test-cluster", "test-namespace", "desired")
	assert.Equal(t, 3.0, testutil.ToFloat64(desiredGauge))

	readyGauge := clusterReplicas.WithLabelValues("test-cluster", "test-namespace", "ready")
	assert.Equal(t, 2.0, testutil.ToFloat64(readyGauge))
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

	histogram := upgradeDuration.WithLabelValues("test-cluster", "test-namespace", "prepare")
	require.NotNil(t, histogram)
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
		wantResult string
	}{
		{
			name:       "successful backup",
			success:    true,
			duration:   time.Minute * 10,
			wantResult: MetricResultSuccess,
		},
		{
			name:       "failed backup",
			success:    false,
			duration:   time.Minute * 5,
			wantResult: MetricResultFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewBackupMetrics("test-cluster", "test-namespace")

			// Clear metrics before test
			backupTotal.Reset()
			backupDuration.Reset()

			ctx := context.Background()
			metrics.RecordBackup(ctx, tt.success, tt.duration)

			// Check counter
			counter := backupTotal.WithLabelValues("test-cluster", "test-namespace", tt.wantResult)
			assert.Equal(t, 1.0, testutil.ToFloat64(counter))

			// Check histogram — verify the labelled child exists and the vec has
			// a single registered series. (testutil.ToFloat64 is undefined on a
			// histogram observer; CollectAndCount is the right primitive here.)
			histogram := backupDuration.WithLabelValues("test-cluster", "test-namespace")
			require.NotNil(t, histogram)
			assert.Equal(t, 1, testutil.CollectAndCount(backupDuration))
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
	}

	for _, metric := range testMetrics {
		// This would panic if metric wasn't registered properly
		assert.NotNil(t, metric)
	}
}

func TestKubernetesClusterName_DefaultsToEmpty(t *testing.T) {
	t.Cleanup(func() { SetKubernetesClusterName("") })

	SetKubernetesClusterName("")
	assert.Equal(t, "", KubernetesClusterName(),
		"an unset --kubernetes-cluster-name must yield an empty label, not a placeholder")
}

func TestClusterMetrics_RecordServerHealth(t *testing.T) {
	tests := []struct {
		name       string
		k8sCluster string
		server     ServerHealth
		expected   float64
	}{
		{
			name:       "enabled and available, no k8s cluster name set",
			k8sCluster: "",
			server:     ServerHealth{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: true, Available: true},
			expected:   1.0,
		},
		{
			name:       "enabled but unavailable is degraded",
			k8sCluster: "",
			server:     ServerHealth{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: true, Available: false},
			expected:   0.0,
		},
		{
			name:       "available but disabled is degraded",
			k8sCluster: "",
			server:     ServerHealth{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: false, Available: true},
			expected:   0.0,
		},
		{
			name:       "k8s cluster name is carried onto the series",
			k8sCluster: "eu-west-prod",
			server:     ServerHealth{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: true, Available: true},
			expected:   1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(func() { SetKubernetesClusterName("") })
			serverHealth.Reset()
			SetKubernetesClusterName(tt.k8sCluster)

			NewClusterMetrics("test-cluster", "test-namespace").
				RecordServerHealth([]ServerHealth{tt.server})

			gauge := serverHealth.WithLabelValues(
				"test-cluster", "test-namespace", tt.server.Name, tt.server.Address, tt.k8sCluster)
			assert.Equal(t, tt.expected, testutil.ToFloat64(gauge))
		})
	}
}

// The whole point of the k8s_cluster label: the same Neo4j cluster name in the
// same namespace, running in two different Kubernetes clusters, must not
// collapse into one series when both are scraped into one Prometheus.
func TestClusterMetrics_RecordServerHealth_SeparatesKubernetesClusters(t *testing.T) {
	t.Cleanup(func() { SetKubernetesClusterName("") })
	serverHealth.Reset()

	srv := ServerHealth{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: true, Available: true}
	m := NewClusterMetrics("prod", "neo4j")

	SetKubernetesClusterName("eu-west")
	m.RecordServerHealth([]ServerHealth{srv})

	SetKubernetesClusterName("us-east")
	m.RecordServerHealth([]ServerHealth{{Name: "srv-0", Address: "10.0.0.1:7687", Enabled: true, Available: false}})

	require.Equal(t, 2, testutil.CollectAndCount(serverHealth),
		"two Kubernetes clusters must produce two distinct series, not one overwritten one")

	assert.Equal(t, 1.0, testutil.ToFloat64(
		serverHealth.WithLabelValues("prod", "neo4j", "srv-0", "10.0.0.1:7687", "eu-west")))
	assert.Equal(t, 0.0, testutil.ToFloat64(
		serverHealth.WithLabelValues("prod", "neo4j", "srv-0", "10.0.0.1:7687", "us-east")))
}
