package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
)

// Every entry in conditionGuidance is a CLAIM ABOUT OPERATOR BEHAVIOUR, and
// this package does not own that behaviour. Keying the map off the operator's
// exported constants makes a rename a compile error; this test covers the other
// direction — a condition type ADDED to the operator without guidance here.
//
// The list is explicit rather than reflective because Go cannot enumerate
// package constants at runtime. Same arrangement, and same reason, as
// TestDevControllerDefaultCoversRegistry in cmd/main_test.go.
func TestConditionGuidanceCoversEveryConditionType(t *testing.T) {
	all := []string{
		controller.ConditionTypeAvailable,
		controller.ConditionTypeProgressing,
		controller.ConditionTypeDegraded,
		controller.ConditionTypeReady,
		controller.ConditionTypeServersHealthy,
		controller.ConditionTypeDatabasesHealthy,
		controller.ConditionTypeServersPendingDrain,
		controller.ConditionTypeRolesSynced,
		controller.ConditionTypePasswordSynced,
		controller.ConditionTypePendingDependencies,
		controller.ConditionTypePrivilegesSynced,
		controller.ConditionTypePrivilegesResolve,
		controller.ConditionTypeUserNotFound,
		controller.ConditionTypeCrossClusterProxySecure,
		controller.ConditionTypeOIDCProviderConfigured,
		controller.ConditionTypeAuthRuleVersionTooOld,
		controller.ConditionTypeClusterNotReady,
	}

	for _, ct := range all {
		g, ok := conditionGuidance[ct]
		assert.True(t, ok, "condition %q has no guidance — add one, or the CLI silently says nothing about it", ct)
		if ok {
			assert.NotEmpty(t, g.meaning, "%s: meaning must not be empty", ct)
			assert.NotEmpty(t, g.action, "%s: action must not be empty — guidance without an action is a glossary, not help", ct)
		}
	}
	assert.Len(t, conditionGuidance, len(all),
		"conditionGuidance has entries not in this list — either the operator gained a condition (update the list) or an entry explains something that no longer exists")
}

// Guidance for a terminal, irreversible state must say so. Promotion cannot be
// undone, and a user reading this after a failover needs to know that before
// they delete anything.
func TestPhaseGuidance_PromotedWarnsItIsTerminal(t *testing.T) {
	g, ok := phaseGuidance[neo4jv1beta1.ReplicaPhasePromoted]
	require.True(t, ok)
	assert.Contains(t, strings.ToLower(g.meaning), "irreversible")
	assert.Contains(t, g.action, "NOT drop",
		"deleting a promoted replica CR does not drop the database — the user must be told")
}

func TestExplainTerm_CaseInsensitiveAndCoversBothMaps(t *testing.T) {
	out := captureExplain(t, func(f *os.File) bool { return explainTerm("servershealthy", f) })
	assert.Contains(t, out, "ServersHealthy (condition)")
	assert.Contains(t, out, "→ ")

	out = captureExplain(t, func(f *os.File) bool { return explainTerm("Replicating", f) })
	assert.Contains(t, out, "Replicating (phase)")
}

func TestExplainTerm_UnknownReturnsFalse(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer f.Close()
	assert.False(t, explainTerm("NotAThing", f))
}

func TestExplainResource_RendersPhaseAndConditions(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase: "Degraded",
			Conditions: []metav1.Condition{{
				Type: controller.ConditionTypeServersHealthy, Status: metav1.ConditionFalse,
				Reason: "ServerUnavailable", Message: "server prod-server-1 is not available",
			}},
		},
	})

	out := captureExplainErr(t, func(f *os.File) error {
		return explainResource(context.Background(), c, "neo4j", "Neo4jEnterpriseCluster/prod", f)
	})

	assert.Contains(t, out, "phase: Degraded")
	assert.Contains(t, out, "✗ ServersHealthy = False")
	assert.Contains(t, out, "server prod-server-1 is not available")
	assert.Contains(t, out, "OOMKilled", "the guidance should point at the most common real cause")
}

// An unknown phase must be admitted, not guessed at — this binary may simply
// predate it, and inventing an explanation would be worse than saying nothing.
func TestExplainResource_UnknownPhaseIsAdmittedNotGuessed(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status:     neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "SomeFuturePhase"},
	})

	out := captureExplainErr(t, func(f *os.File) error {
		return explainResource(context.Background(), c, "neo4j", "Neo4jEnterpriseCluster/prod", f)
	})
	assert.Contains(t, out, "no guidance for this phase")
	assert.Contains(t, out, "newer than this CLI")
}

func TestExplainResource_UnknownKindIsAClearError(t *testing.T) {
	c := testClient(t)
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer f.Close()

	err = explainResource(context.Background(), c, "neo4j", "NotAKind/x", f)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a Neo4j resource kind")
}

func captureExplain(t *testing.T, fn func(*os.File) bool) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	fn(f)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b)
}

func captureExplainErr(t *testing.T, fn func(*os.File) error) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	require.NoError(t, fn(f))
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b)
}

// The shared phase vocabulary must be explainable in full. Before this, the
// map held only the replica phases, so `explain` met `Ready` — the phase every
// healthy resource reports — with "no guidance for this phase — it may be
// newer than this CLI", pointing the reader at a version mismatch that did not
// exist. Adding a phase without guidance now fails here instead.
func TestEveryPhaseConstantHasGuidance(t *testing.T) {
	for _, phase := range neo4jv1beta1.AllPhases {
		g, ok := phaseGuidance[phase]
		require.True(t, ok, "phase %q has no guidance in explain", phase)
		assert.NotEmpty(t, g.meaning, "phase %q has an empty meaning", phase)
		assert.NotEmpty(t, g.action, "phase %q has an empty action", phase)
	}
}

// A condition's REASON is the most specific thing a status carries and the
// least guessable. explain read type, status and message and dropped the
// reason, so the one token worth looking up was the one it could not explain.
func TestExplainTerm_ExplainsReasons(t *testing.T) {
	for _, reason := range []string{
		controller.EventReasonCompositeNameBlocked,
		controller.ReasonGraphPrivilegeOnComposite,
	} {
		out := captureExplain(t, func(f *os.File) bool { return explainTerm(reason, f) })
		assert.Contains(t, out, reason)
		assert.Contains(t, out, "(reason)")
	}

	// Case-insensitive, like conditions and phases.
	out := captureExplain(t, func(f *os.File) bool {
		return explainTerm("compositedatabasenameblocked", f)
	})
	assert.Contains(t, out, "42N87", "the ordering trap is the whole point of this entry")
}

// The reason a composite is blocked is the case that motivated the dimension:
// Neo4j's own 42N87 never mentions ordering, so the reason string is the only
// route to the explanation.
func TestExplainResource_SurfacesTheConditionReason(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "cineasts", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jCompositeDatabaseStatus{
			Phase: "Failed",
			Conditions: []metav1.Condition{{
				Type:    "Ready",
				Status:  metav1.ConditionFalse,
				Reason:  controller.EventReasonCompositeNameBlocked,
				Message: "alias cineasts.latest already exists; drop it first",
			}},
		},
	})

	out := captureExplainErr(t, func(f *os.File) error {
		return explainResource(context.Background(), c, "neo4j", "Neo4jCompositeDatabase/cineasts", f)
	})

	assert.Contains(t, out, "alias cineasts.latest already exists")
	assert.Contains(t, out, "reason "+controller.EventReasonCompositeNameBlocked)
	assert.Contains(t, out, "DROP ALIAS", "the guidance has to say what to actually do")
}

// A reason with no entry must leave the output alone rather than print an
// empty explanation — most reasons restate their condition and need none.
func TestExplainResource_UnknownReasonAddsNothing(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "cineasts", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jCompositeDatabaseStatus{
			Phase: "Ready",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "CompositeDatabaseReady",
			}},
		},
	})

	out := captureExplainErr(t, func(f *os.File) error {
		return explainResource(context.Background(), c, "neo4j", "Neo4jCompositeDatabase/cineasts", f)
	})
	assert.NotContains(t, out, "reason CompositeDatabaseReady:")
}

func TestExplainList_IncludesReasons(t *testing.T) {
	out := captureExplain(t, func(f *os.File) bool { renderExplainList(f); return true })
	assert.Contains(t, out, "Reasons:")
	assert.Contains(t, out, controller.EventReasonCompositeNameBlocked)
	assert.Contains(t, out, "reasons this CLI does not know about")
}
