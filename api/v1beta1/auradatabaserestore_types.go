/*
Copyright 2025 Priyo Lahiri.

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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AuraDatabaseRestoreSpec restores a database on a managed Neo4j Aura instance
// from one of its per-database backups (Aura API v2beta1). One-shot, in place.
// BETA / best-effort.
//
// +kubebuilder:validation:XValidation:rule="has(self.backupId) != has(self.backupRef)",message="set exactly one of backupId or backupRef"
type AuraDatabaseRestoreSpec struct {
	// DatabaseRef is the AuraDatabase (same namespace) to restore in place.
	// +kubebuilder:validation:Required
	DatabaseRef string `json:"databaseRef"`

	// BackupID is the Aura backup ID to restore from. Mutually exclusive with
	// backupRef.
	// +optional
	BackupID string `json:"backupId,omitempty"`

	// BackupRef resolves the backup ID from an AuraDatabaseBackup (same
	// namespace). Mutually exclusive with backupId.
	// +optional
	BackupRef string `json:"backupRef,omitempty"`

	// ManagementPolicies restricts which actions the operator may take.
	// +kubebuilder:validation:items:Enum=Observe;Create;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraDatabaseRestoreStatus is the observed state of the restore.
type AuraDatabaseRestoreStatus struct {
	// Phase mirrors the restore outcome: Restoring while preparing, Submitting
	// while the request is in flight, then Submitted (the TERMINAL success phase)
	// or Error. It is never Completed — Aura restores asynchronously and its
	// v2beta1 database endpoint exposes no status, so completion cannot be
	// observed and is not claimed. A CR found in Submitting means the operator
	// stopped mid-request and the outcome is unknown; it is not retried, because
	// repeating a restore overwrites the database again.
	// +optional
	Phase string `json:"phase,omitempty"`

	// StartedAt is when the restore was submitted.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// FinishedAt is when the restore reached a terminal state.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auradbrestore
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraDatabaseRestore restores a database in place from a per-database backup on
// a Neo4j Aura instance (Aura API v2beta1). BETA / best-effort — see
// docs/design/aura-orchestration.md.
type AuraDatabaseRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraDatabaseRestoreSpec   `json:"spec,omitempty"`
	Status AuraDatabaseRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraDatabaseRestoreList contains a list of AuraDatabaseRestore.
type AuraDatabaseRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraDatabaseRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraDatabaseRestore{}, &AuraDatabaseRestoreList{})
}
