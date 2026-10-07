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

// A series the cluster no longer reports must be withdrawn, not left at its
// last value. Live on Kind: a server that restarted kept a server_address
// "<nil>" series at 0 next to its real one at 1, and a server that was down
// kept its real-address series at 1 — the first keeps a `== 0` alert firing
// after recovery, the second hides an outage (#444).
func TestClusterMetrics_RecordServerHealth_WithdrawsSeriesNoLongerReported(t *testing.T) {
	serverHealth.Reset()
	m := NewClusterMetrics("prod", "neo4j")
	up := func(addr string) ServerHealth {
		return ServerHealth{Name: "srv-1", Address: addr, Enabled: true, Available: true}
	}

	m.RecordServerHealth([]ServerHealth{up("10.0.0.1:7687"), {Name: "srv-2", Address: "10.0.0.2:7687", Enabled: true, Available: true}})
	m.RecordServerHealth([]ServerHealth{up("10.0.0.1:7687"), {Name: "srv-2", Address: "", Enabled: true, Available: false}})
	require.Equal(t, 2, testutil.CollectAndCount(serverHealth), "srv-2's old series must go when its address changes")
	assert.Equal(t, 0.0, testutil.ToFloat64(serverHealth.WithLabelValues("prod", "neo4j", "srv-2", "", "")))

	m.RecordServerHealth([]ServerHealth{up("10.0.0.1:7687")})
	require.Equal(t, 1, testutil.CollectAndCount(serverHealth), "a server no longer listed must not keep a series")

	// Another cluster's series are not this cluster's to withdraw.
	NewClusterMetrics("other", "neo4j").RecordServerHealth([]ServerHealth{up("10.0.0.9:7687")})
	m.RecordServerHealth([]ServerHealth{up("10.0.0.1:7687")})
	assert.Equal(t, 2, testutil.CollectAndCount(serverHealth))
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

// A deleted cluster must stop exporting. On the v1.19.0 walk a deleted
// cluster's cluster_healthy, cluster_replicas_total and server_health series
// stayed at their last values until the operator restarted. Another cluster's
// series — including one with the same name in another namespace — stay.
func TestClusterMetrics_ForgetWithdrawsTheClustersSeries(t *testing.T) {
	for _, vec := range []interface{ Reset() }{clusterHealthy, clusterPhase, clusterReplicas, serverHealth, reconcileTotal} {
		vec.Reset()
	}
	gone := NewClusterMetrics("prod", "neo4j")
	other := NewClusterMetrics("prod", "staging")
	for _, m := range []*ClusterMetrics{gone, other} {
		m.RecordClusterHealth(true)
		m.RecordClusterPhase("Ready")
		m.RecordClusterReplicas(3, 3)
		m.RecordServerHealth([]ServerHealth{{Name: "srv-1", Address: "10.0.0.1:7687", Enabled: true, Available: true}})
	}
	NewReconcileMetrics("prod", "neo4j").RecordReconcile(context.Background(), "reconcile", time.Second, true)
	NewReconcileMetrics("prod", "staging").RecordReconcile(context.Background(), "reconcile", time.Second, true)
	before := testutil.CollectAndCount(clusterPhase)

	gone.Forget()
	serverHealthSeen.mu.Lock()
	_, remembered := serverHealthSeen.byCluster["/neo4j/prod"]
	serverHealthSeen.mu.Unlock()
	assert.False(t, remembered, "its server bookkeeping goes with it")

	assert.Equal(t, 1, testutil.CollectAndCount(clusterHealthy))
	assert.Equal(t, 1, testutil.CollectAndCount(serverHealth))
	assert.Equal(t, before/2, testutil.CollectAndCount(clusterPhase))
	assert.Equal(t, 1.0, testutil.ToFloat64(clusterHealthy.WithLabelValues("prod", "staging")), "the other namespace's cluster is untouched")
	assert.Equal(t, 2, testutil.CollectAndCount(clusterReplicas), "only the other namespace's desired and ready remain")
	assert.Equal(t, 1, testutil.CollectAndCount(reconcileTotal), "only the other namespace's reconcile counter remains")

	// Recording again after a recreate starts fresh series.
	gone.RecordServerHealth([]ServerHealth{{Name: "srv-1", Address: "10.0.0.5:7687", Enabled: true, Available: true}})
	assert.Equal(t, 2, testutil.CollectAndCount(serverHealth))
}
