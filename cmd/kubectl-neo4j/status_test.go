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
		{"Invalid", "-", false},
	}
	for _, tc := range cases {
		t.Run(tc.phase+"/"+tc.ready, func(t *testing.T) {
			r := resourceStatus{phase: tc.phase, ready: tc.ready}
			assert.Equal(t, tc.healthy, r.healthy())
		})
	}
}

// Invalid is the operator rejecting the spec: nothing was created and nothing
// will be until the manifest changes. It is the most actionable phase there is,
// and --problems used to hide it, because the unhealthy list was written from
// memory instead of from the phase vocabulary.
//
// The expectation is spelled out per phase over the WHOLE shared vocabulary, so
// adding a phase to api/v1beta1/phases.go without deciding whether it is a
// problem fails here instead of being silently treated as healthy.
func TestResourceStatus_EveryPhaseInTheVocabularyIsClassified(t *testing.T) {
	problems := map[string]bool{
		neo4jv1beta1.PhaseFailed:   true,
		neo4jv1beta1.PhaseError:    true,
		neo4jv1beta1.PhaseInvalid:  true,
		neo4jv1beta1.PhaseDegraded: true,
		neo4jv1beta1.PhaseUnknown:  true,
	}
	notProblems := map[string]bool{
		neo4jv1beta1.PhaseReady:      true,
		neo4jv1beta1.PhaseInstalled:  true,
		neo4jv1beta1.PhaseCompleted:  true,
		neo4jv1beta1.PhaseSuspended:  true, // a deliberate pause, not a fault
		neo4jv1beta1.PhasePending:    true,
		neo4jv1beta1.PhaseValidating: true,
		neo4jv1beta1.PhaseCreating:   true,
		neo4jv1beta1.PhaseForming:    true,
		neo4jv1beta1.PhaseInstalling: true,
		neo4jv1beta1.PhaseRunning:    true,
		neo4jv1beta1.PhaseWaiting:    true,
		neo4jv1beta1.PhaseUpgrading:  true,
		neo4jv1beta1.PhaseExpanding:  true,
	}
	for _, phase := range neo4jv1beta1.AllPhases {
		require.True(t, problems[phase] != notProblems[phase],
			"phase %q is in api/v1beta1.AllPhases but this test does not say whether --problems should show it", phase)
		r := resourceStatus{phase: phase, ready: "-"}
		assert.Equal(t, notProblems[phase], r.healthy(), "phase %q", phase)
	}

	// Kind-specific phases outside the shared set: none of these is a fault.
	for _, phase := range []string{
		"Scheduled", "Seeding", "Replicating", "Promoted", "Promoting",
		"Restoring", "Submitting", "Submitted", "Staging", "Rolling", "Paused",
	} {
		assert.True(t, resourceStatus{phase: phase}.healthy(), "phase %q must not be flagged", phase)
	}
}

// status.ready is `omitempty`, so the operator never serialises a false: it is
// "true" or absent (shown as "-"), and most kinds have no such field at all.
// A test on ready == "false" could therefore never match a real resource, and
// is gone. Readiness is the phase's job.
func TestResourceStatus_ReadyColumnDoesNotDecideHealth(t *testing.T) {
	assert.True(t, resourceStatus{phase: "Ready", ready: "-"}.healthy())
	assert.True(t, resourceStatus{phase: "Forming", ready: "-"}.healthy())
	// Were a false ever stored, a cluster that is still forming is "not ready"
	// by definition and is not a problem: the phase already says so.
	assert.True(t, resourceStatus{phase: "Forming", ready: "false"}.healthy())
}

// End to end through the real read path: an Invalid Neo4jBackup (the operator
// rejected its spec) must appear under --problems with its message, while a
// Scheduled one and a Completed one must not be listed as problems.
func TestStatusProblems_ReportsInvalidAndNotScheduledOrCompleted(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "neo4j"},
			Status: neo4jv1beta1.Neo4jBackupStatus{
				Phase: "Invalid", Message: "Invalid backup spec: PVC storage requires spec.storage.pvc.name",
			},
		},
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "neo4j"},
			Status:     neo4jv1beta1.Neo4jBackupStatus{Phase: "Completed"},
		},
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "sched", Namespace: "neo4j"},
			Status:     neo4jv1beta1.Neo4jBackupStatus{Phase: "Scheduled"},
		},
	)
	rows, err := collectStatus(context.Background(), c, "neo4j")
	require.NoError(t, err)

	out := renderTo(t, rows, false, true, "neo4j")
	assert.Contains(t, out, "bad")
	assert.Contains(t, out, "Invalid")
	assert.Contains(t, out, "✗ Neo4jBackup/bad: Invalid backup spec")
	assert.NotContains(t, out, "done")
	assert.NotContains(t, out, "sched")
	assert.NotContains(t, out, "look healthy")
}

// --problems asks for "only what needs attention". The message block under the
// table used to be built from ALL rows, so a Pending resource's "…" line
// appeared under --problems with no row above it to explain what it belonged
// to — and a run where one resource failed also advertised an unrelated
// Pending one.
func TestRenderStatus_ProblemsOnlyMessageBlockFollowsTheFilter(t *testing.T) {
	rows := []resourceStatus{
		{kind: "Neo4jDatabase", name: "analytics", phase: "Failed", ready: "-", age: "5m",
			message: "topology requires 3 primaries, cluster has 2 servers"},
		{kind: "Neo4jUser", name: "reporting", phase: "Pending", ready: "-", age: "2m",
			message: `waiting for password Secret "reporting-pw"`},
		{kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Ready", ready: "true", age: "3h",
			message: "cluster is ready"},
	}

	problems := renderTo(t, rows, false, true, "neo4j")
	assert.Contains(t, problems, "✗ Neo4jDatabase/analytics: topology requires 3 primaries")
	assert.NotContains(t, problems, "reporting", "a Pending resource is not shown under --problems, nor is its message")
	assert.NotContains(t, problems, "cluster is ready")

	// Without the filter the Pending line is still shown, as before.
	all := renderTo(t, rows, false, false, "neo4j")
	assert.Contains(t, all, `… Neo4jUser/reporting: waiting for password Secret "reporting-pw"`)
}

// With nothing flagged, --problems says so and prints no message block at all,
// even though a Pending resource has a message.
func TestRenderStatus_ProblemsOnlyWithOnlyPendingSaysHealthy(t *testing.T) {
	rows := []resourceStatus{
		{kind: "Neo4jUser", name: "reporting", phase: "Pending", ready: "-",
			message: `waiting for password Secret "reporting-pw"`},
	}
	out := renderTo(t, rows, false, true, "neo4j")
	assert.Contains(t, out, "all 1 Neo4j resource(s) look healthy")
	assert.NotContains(t, out, "reporting-pw")
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
