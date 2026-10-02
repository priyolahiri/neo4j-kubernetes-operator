package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestFindCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllGood"},
		{Type: "ServersHealthy", Status: metav1.ConditionTrue, Reason: "Healthy"},
	}

	t.Run("found", func(t *testing.T) {
		c := findCondition(conditions, "Ready")
		assert.NotNil(t, c)
		assert.Equal(t, "AllGood", c.Reason)
	})

	t.Run("not found", func(t *testing.T) {
		c := findCondition(conditions, "NonExistent")
		assert.Nil(t, c)
	})

	t.Run("empty slice", func(t *testing.T) {
		c := findCondition(nil, "Ready")
		assert.Nil(t, c)
	})
}

func TestUpsertCondition(t *testing.T) {
	t.Run("update existing", func(t *testing.T) {
		conditions := []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Pending"},
		}
		updated := upsertCondition(conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Done",
		})
		assert.Len(t, updated, 1)
		assert.Equal(t, metav1.ConditionTrue, updated[0].Status)
		assert.Equal(t, "Done", updated[0].Reason)
	})

	t.Run("append new", func(t *testing.T) {
		conditions := []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionTrue},
		}
		updated := upsertCondition(conditions, metav1.Condition{
			Type: "ServersHealthy", Status: metav1.ConditionTrue,
		})
		assert.Len(t, updated, 2)
	})

	t.Run("empty slice", func(t *testing.T) {
		updated := upsertCondition(nil, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue,
		})
		assert.Len(t, updated, 1)
	})
}

// A CronJob-backed Neo4jBackup never leaves the Scheduled phase — that IS its
// healthy resting state — so the Ready condition must say so. It used to fall
// through PhaseToConditionStatus's default and report Unknown/Pending, which
// made `kubectl wait --for=condition=Ready` and Flux health assessment hang on
// a perfectly healthy scheduled backup.
func TestPhaseToConditionStatus_ScheduledIsHealthyAndWaiting(t *testing.T) {
	status, reason := PhaseToConditionStatus("Scheduled")
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, ConditionReasonBackupScheduled, reason)
	assert.Equal(t, "BackupScheduled", reason, "the reason is user-visible in `kubectl describe`")
}

// The shared phase vocabulary must stay the single source for what the
// controllers set. If a controller starts setting a phase that AllPhases does
// not list, `kubectl neo4j explain` answers "may be newer than this CLI" and
// this classification silently defaults — so the resting phases are pinned.
func TestAllPhases_ListsEveryRestingPhaseTheControllersSet(t *testing.T) {
	assert.Contains(t, neo4jv1beta1.AllPhases, "Scheduled",
		"Neo4jBackup sets Scheduled for CronJob-backed backups; it must be in the shared vocabulary")
}

// Pin the rest of the classification table so a future edit to one arm cannot
// quietly move another phase.
func TestPhaseToConditionStatus_Table(t *testing.T) {
	cases := []struct {
		phase  string
		status metav1.ConditionStatus
		reason string
	}{
		{neo4jv1beta1.PhaseReady, metav1.ConditionTrue, ConditionReasonReady},
		{neo4jv1beta1.PhaseInstalled, metav1.ConditionTrue, ConditionReasonReady},
		{neo4jv1beta1.PhaseCompleted, metav1.ConditionTrue, ConditionReasonBackupSucceeded},
		{neo4jv1beta1.PhaseFailed, metav1.ConditionFalse, ConditionReasonFailed},
		{neo4jv1beta1.PhaseDegraded, metav1.ConditionFalse, ConditionReasonFailed},
		{neo4jv1beta1.PhaseSuspended, metav1.ConditionFalse, ConditionReasonFailed},
		{neo4jv1beta1.PhaseInvalid, metav1.ConditionFalse, ConditionReasonFailed},
		{neo4jv1beta1.PhaseError, metav1.ConditionFalse, ConditionReasonFailed},
		{neo4jv1beta1.PhaseUpgrading, metav1.ConditionUnknown, ConditionReasonUpgrading},
		{neo4jv1beta1.PhaseExpanding, metav1.ConditionUnknown, ConditionReasonStorageExpanding},
		{neo4jv1beta1.PhaseForming, metav1.ConditionUnknown, ConditionReasonForming},
		{neo4jv1beta1.PhaseCreating, metav1.ConditionUnknown, ConditionReasonForming},
		{neo4jv1beta1.PhasePending, metav1.ConditionUnknown, ConditionReasonPending},
		{neo4jv1beta1.PhaseWaiting, metav1.ConditionUnknown, ConditionReasonPending},
		{neo4jv1beta1.PhaseRunning, metav1.ConditionUnknown, ConditionReasonPending},
		{neo4jv1beta1.PhaseUnknown, metav1.ConditionUnknown, ConditionReasonPending},
	}
	for _, tc := range cases {
		status, reason := PhaseToConditionStatus(tc.phase)
		assert.Equal(t, tc.status, status, "phase %q", tc.phase)
		assert.Equal(t, tc.reason, reason, "phase %q", tc.phase)
	}
}
