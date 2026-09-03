// Package controller provides shared condition type and reason constants for all Neo4j operator controllers.
package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Standard condition types following Kubernetes API conventions.
// Flux health checks use the "Ready" condition type automatically.
const (
	ConditionTypeAvailable   = "Available"
	ConditionTypeProgressing = "Progressing"
	ConditionTypeDegraded    = "Degraded"
	ConditionTypeReady       = "Ready"

	// ConditionTypeServersHealthy indicates all Neo4j servers in the cluster
	// are in Enabled state and Available health.
	ConditionTypeServersHealthy = "ServersHealthy"

	// ConditionTypeDatabasesHealthy indicates all expected user databases are online.
	ConditionTypeDatabasesHealthy = "DatabasesHealthy"

	// ConditionTypeServersPendingDrain indicates the cluster still has Neo4j
	// servers registered beyond spec.topology.servers (i.e. a scale-down whose
	// removed servers have not yet been deallocated + dropped). True means
	// databases may be under-replicated on those servers; the operator does not
	// yet auto-drain them (#173).
	ConditionTypeServersPendingDrain = "ServersPendingDrain"
)

// Reason constants for the Ready condition across all CRDs.
const (
	ConditionReasonReady           = "ClusterReady"
	ConditionReasonForming         = "ClusterForming"
	ConditionReasonFailed          = "ReconciliationFailed"
	ConditionReasonUpgrading       = "UpgradeInProgress"
	ConditionReasonPending         = "Pending"
	ConditionReasonDatabaseReady   = "DatabaseReady"
	ConditionReasonDatabaseFailed  = "DatabaseCreationFailed"
	ConditionReasonBackupSucceeded = "BackupSucceeded"
	ConditionReasonBackupFailed    = "BackupFailed"
	ConditionReasonRestoreComplete = "RestoreCompleted"
	ConditionReasonRestoreFailed   = "RestoreFailed"
	ConditionReasonPluginInstalled = "PluginInstalled"
	ConditionReasonPluginFailed    = "PluginInstallFailed"

	ConditionReasonStorageExpanding       = "StorageExpanding"
	ConditionReasonAllServersHealthy      = "AllServersHealthy"
	ConditionReasonServerDegraded         = "ServerDegraded"
	ConditionReasonAllDatabasesOnline     = "AllDatabasesOnline"
	ConditionReasonDatabaseOffline        = "DatabaseOffline"
	ConditionReasonDiagnosticsUnavailable = "DiagnosticsUnavailable"
	ConditionReasonServersPendingDrain    = "ServersPendingDrain"
	ConditionReasonNoServersPendingDrain  = "NoServersPendingDrain"
	ConditionReasonScaleDownBlocked       = "ScaleDownBlocked"
	// ConditionReasonScaleDownDeferredByUpgrade — a topology shrink was
	// requested while a rolling upgrade is mid-flight; the drain starts
	// after the upgrade completes (#173/#174 mutual exclusion).
	ConditionReasonScaleDownDeferredByUpgrade = "ScaleDownDeferredByUpgrade"
)

// SetReadyCondition sets the standard "Ready" condition on a conditions slice.
// It preserves LastTransitionTime when status and reason are unchanged.
// Returns true if the condition was changed (new or status/reason changed).
func SetReadyCondition(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) bool {
	existing := findCondition(*conditions, ConditionTypeReady)
	if existing != nil && existing.Status == status && existing.Reason == reason {
		// Only update generation and message — preserve LastTransitionTime
		existing.ObservedGeneration = generation
		existing.Message = message
		return false
	}
	newCond := metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             status,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
	*conditions = upsertCondition(*conditions, newCond)
	return true
}

// SetNamedCondition upserts any named condition type on a conditions slice.
// It preserves LastTransitionTime when status and reason are unchanged.
// Returns true if the condition changed.
func SetNamedCondition(conditions *[]metav1.Condition, condType string, generation int64, status metav1.ConditionStatus, reason, message string) bool {
	existing := findCondition(*conditions, condType)
	if existing != nil && existing.Status == status && existing.Reason == reason {
		existing.ObservedGeneration = generation
		existing.Message = message
		return false
	}
	newCond := metav1.Condition{
		Type:               condType,
		Status:             status,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
	*conditions = upsertCondition(*conditions, newCond)
	return true
}

// PhaseToConditionStatus maps a phase string to a ConditionStatus and Ready condition reason.
func PhaseToConditionStatus(phase string) (metav1.ConditionStatus, string) {
	switch phase {
	case neo4jv1beta1.PhaseReady, neo4jv1beta1.PhaseInstalled:
		return metav1.ConditionTrue, ConditionReasonReady
	case neo4jv1beta1.PhaseCompleted:
		return metav1.ConditionTrue, ConditionReasonBackupSucceeded
	case neo4jv1beta1.PhaseFailed, neo4jv1beta1.PhaseDegraded,
		neo4jv1beta1.PhaseSuspended, neo4jv1beta1.PhaseInvalid, neo4jv1beta1.PhaseError:
		return metav1.ConditionFalse, ConditionReasonFailed
	case neo4jv1beta1.PhaseUpgrading:
		return metav1.ConditionUnknown, ConditionReasonUpgrading
	case neo4jv1beta1.PhaseExpanding:
		return metav1.ConditionUnknown, ConditionReasonStorageExpanding
	case neo4jv1beta1.PhaseForming, neo4jv1beta1.PhaseCreating:
		return metav1.ConditionUnknown, ConditionReasonForming
	case neo4jv1beta1.PhaseInstalling, neo4jv1beta1.PhaseRunning,
		neo4jv1beta1.PhaseValidating, neo4jv1beta1.PhasePending, neo4jv1beta1.PhaseWaiting:
		return metav1.ConditionUnknown, ConditionReasonPending
	default:
		return metav1.ConditionUnknown, ConditionReasonPending
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func upsertCondition(conditions []metav1.Condition, cond metav1.Condition) []metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == cond.Type {
			conditions[i] = cond
			return conditions
		}
	}
	return append(conditions, cond)
}
