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

// AuraDatabaseBackupSpec takes an on-demand backup of a database on a managed
// Neo4j Aura instance via the Aura API v2beta1 (per-database backups exist only
// on multi-database tiers). BETA / best-effort.
type AuraDatabaseBackupSpec struct {
	// DatabaseRef is the AuraDatabase (same namespace) to back up. Credentials,
	// organization, project, and instance are resolved from it.
	// +kubebuilder:validation:Required
	DatabaseRef string `json:"databaseRef"`

	// ManagementPolicies restricts which actions the operator may take.
	// +kubebuilder:validation:items:Enum=Observe;Create;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraDatabaseBackupStatus is the observed state of the backup.
type AuraDatabaseBackupStatus struct {
	// BackupID is the Aura-assigned backup ID (mirrored into the
	// neo4j.com/external-backup-id annotation so the backup is taken once).
	// +optional
	BackupID string `json:"backupId,omitempty"`

	// Phase mirrors the reconcile outcome (Pending, Completed, Error).
	// +optional
	Phase string `json:"phase,omitempty"`

	// Timestamp is the backup's timestamp as reported by Aura.
	// +optional
	Timestamp string `json:"timestamp,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Exportable reports whether Aura can export/download this backup. Only
	// populated once the backup has been read back (the create response carries
	// just an ID).
	// +optional
	Exportable bool `json:"exportable,omitempty"`

	// LastSyncedTime is when the backup was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auradbbackup
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseRef`
// +kubebuilder:printcolumn:name="BackupID",type=string,JSONPath=`.status.backupId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraDatabaseBackup takes an on-demand per-database backup on a Neo4j Aura
// instance via the Aura API v2beta1. BETA / best-effort — see
// docs/design/aura-orchestration.md.
type AuraDatabaseBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraDatabaseBackupSpec   `json:"spec,omitempty"`
	Status AuraDatabaseBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraDatabaseBackupList contains a list of AuraDatabaseBackup.
type AuraDatabaseBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraDatabaseBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraDatabaseBackup{}, &AuraDatabaseBackupList{})
}
