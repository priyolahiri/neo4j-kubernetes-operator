package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The kind list comes from the scheme, not a literal, so a CRD added to
// api/v1beta1 shows up in `status` for free. This pins that wiring — and that
// the List kinds the scheme also registers are filtered out, since listing
// "Neo4jDatabaseList" would ask the API server for Neo4jDatabaseListList.
func TestRegisteredNeo4jKinds_FromSchemeAndExcludesListKinds(t *testing.T) {
	kinds := registeredNeo4jKinds(testClient(t))
	require.NotEmpty(t, kinds)

	var names []string
	for _, k := range kinds {
		names = append(names, k.Kind)
		assert.False(t, strings.HasSuffix(k.Kind, "List"), "%s is a List kind and must be filtered out", k.Kind)
		assert.Equal(t, neo4jv1beta1.GroupVersion.Group, k.Group)
	}

	joined := strings.Join(names, ",")
	for _, want := range []string{"Neo4jEnterpriseCluster", "Neo4jDatabase", "Neo4jUser", "Neo4jBackup"} {
		assert.Contains(t, joined, want)
	}
	// The Aura suite is in the same group and must be included — it was absent
	// from docs/index.md for its whole life, which is the kind of omission a
	// scheme-derived list cannot make.
	assert.Contains(t, joined, "AuraInstance")
}

func TestCollectStatus_ReadsPhaseReadyAndMessageGenerically(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "prod", Namespace: "neo4j",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-3 * time.Hour)),
			},
			Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
				Phase: "Ready", Ready: true,
			},
		},
		&neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "analytics", Namespace: "neo4j"},
			Status: neo4jv1beta1.Neo4jDatabaseStatus{
				Phase: "Failed", Message: "topology requires 3 primaries, cluster has 2 servers",
			},
		},
	)

	rows, err := collectStatus(context.Background(), c, "neo4j")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	byKind := map[string]resourceStatus{}
	for _, r := range rows {
		byKind[r.kind] = r
	}

	cluster := byKind["Neo4jEnterpriseCluster"]
	assert.Equal(t, "Ready", cluster.phase)
	assert.Equal(t, "true", cluster.ready)
	assert.Equal(t, "3h", cluster.age)
	assert.True(t, cluster.healthy())

	db := byKind["Neo4jDatabase"]
	assert.Equal(t, "Failed", db.phase)
	assert.False(t, db.healthy())
	assert.Contains(t, db.message, "topology requires 3 primaries")
}

func TestCollectStatus_NamespaceScoping(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "team-a"}},
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "team-b"}},
	)

	scoped, err := collectStatus(context.Background(), c, "team-a")
	require.NoError(t, err)
	require.Len(t, scoped, 1)
	assert.Equal(t, "a", scoped[0].name)

	all, err := collectStatus(context.Background(), c, "")
	require.NoError(t, err)
	assert.Len(t, all, 2, "an empty namespace must mean all namespaces")
}

// An unrecognised phase must NOT be reported as a problem. The Aura kinds
// mirror Aura's own status vocabulary, which Neo4j can extend without a version
// bump — the same reasoning the project's ArgoCD health checks use.
func TestResourceStatus_UnknownPhaseIsNotAlarming(t *testing.T) {
	cases := []struct {
		phase   string
		ready   string
		healthy bool
	}{
		{"Ready", "true", true},
		{"Running", "-", true},
		{"SomePhaseThisBinaryPredates", "-", true},
		{"Failed", "-", false},
		{"Degraded", "-", false},
		{"Ready", "false", false},
	}
	for _, tc := range cases {
		t.Run(tc.phase+"/"+tc.ready, func(t *testing.T) {
			r := resourceStatus{phase: tc.phase, ready: tc.ready}
			assert.Equal(t, tc.healthy, r.healthy())
		})
	}
}

func TestHumanAge(t *testing.T) {
	now := time.Now()
	assert.Equal(t, "30s", humanAge(now.Add(-30*time.Second)))
	assert.Equal(t, "5m", humanAge(now.Add(-5*time.Minute)))
	assert.Equal(t, "2h", humanAge(now.Add(-2*time.Hour)))
	assert.Equal(t, "3d", humanAge(now.Add(-72*time.Hour)))
	assert.Equal(t, "-", humanAge(time.Time{}))
}

// renderTo captures what the user actually sees, so the table layout and the
// message block are asserted rather than assumed.
func renderTo(t *testing.T, rows []resourceStatus, allNamespaces, problemsOnly bool, ns string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	renderStatus(rows, f, allNamespaces, problemsOnly, ns)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b)
}

func TestRenderStatus_TableAndMessages(t *testing.T) {
	rows := []resourceStatus{
		{kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Ready", ready: "true", age: "3h"},
		{kind: "Neo4jDatabase", name: "analytics", phase: "Failed", ready: "-", age: "5m",
			message: "topology requires 3 primaries, cluster has 2 servers"},
	}

	out := renderTo(t, rows, false, false, "neo4j")
	assert.Contains(t, out, "KIND")
	assert.Contains(t, out, "Neo4jEnterpriseCluster")
	assert.Contains(t, out, "analytics")
	// The message is what people act on, so it must appear in full below the
	// table rather than be truncated into a column.
	assert.Contains(t, out, "topology requires 3 primaries, cluster has 2 servers")
	assert.NotContains(t, out, "NAMESPACE", "the namespace column is only for --all-namespaces")
}

func TestRenderStatus_ProblemsOnlyAndEmptyStates(t *testing.T) {
	healthy := []resourceStatus{{kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Ready", ready: "true"}}

	out := renderTo(t, healthy, false, true, "neo4j")
	assert.Contains(t, out, "look healthy",
		"--problems with nothing wrong should say so, not print an empty table")

	out = renderTo(t, nil, false, false, "neo4j")
	assert.Contains(t, out, `no Neo4j resources found in namespace "neo4j"`)

	out = renderTo(t, nil, true, false, "")
	assert.Contains(t, out, "any namespace")
}

func TestRenderStatus_AllNamespacesAddsTheColumn(t *testing.T) {
	rows := []resourceStatus{{kind: "Neo4jDatabase", namespace: "team-a", name: "db", phase: "Ready", ready: "-"}}
	out := renderTo(t, rows, true, false, "")
	assert.Contains(t, out, "NAMESPACE")
	assert.Contains(t, out, "team-a")
}

// A run with nothing to report must not print a separator followed by nothing.
func TestRenderStatus_NoTrailingBlankWhenNothingToSay(t *testing.T) {
	rows := []resourceStatus{
		{kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Ready", ready: "true", age: "1h"},
	}
	out := renderTo(t, rows, false, false, "neo4j")
	assert.False(t, strings.HasSuffix(out, "\n\n"), "unexpected trailing blank line: %q", out)
}

// Pending is not unhealthy, but its message is the one line that says what to
// do next, so it must still be surfaced — with a marker distinct from an error.
func TestRenderStatus_PendingMessagesAreShownDistinctly(t *testing.T) {
	rows := []resourceStatus{
		{kind: "Neo4jUser", name: "reporting", phase: "Pending", ready: "-", age: "2m",
			message: `waiting for password Secret "reporting-pw"`},
	}
	out := renderTo(t, rows, false, false, "neo4j")
	assert.Contains(t, out, `… Neo4jUser/reporting: waiting for password Secret "reporting-pw"`)
	assert.NotContains(t, out, "✗ Neo4jUser/reporting", "pending must not be rendered as an error")
}
