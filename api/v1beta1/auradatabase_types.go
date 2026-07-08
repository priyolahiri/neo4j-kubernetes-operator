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

// AuraDatabaseSpec manages a database on a managed Neo4j Aura instance via the
// Aura API v2beta1.
//
// BETA / best-effort — v2beta1 is an unstable beta and the database create body
// is not fully schema'd upstream. Aura manages replication/topology per tier, so
// this CRD has no topology knob. For databases on a self-managed cluster, use
// Neo4jDatabase instead.
type AuraDatabaseSpec struct {
	// InstanceRef is the AuraInstance (same namespace) that hosts the database.
	// Credentials, organization, and project are resolved from it.
	// +kubebuilder:validation:Required
	InstanceRef string `json:"instanceRef"`

	// Name is the database name in Aura (defaults to metadata.name).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Name string `json:"name,omitempty"`

	// OrganizationID overrides the organization resolved from the instance's
	// AuraProviderConfig (v2beta1 is org-scoped).
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// DeletionPolicy controls what happens to the Aura database when this CR is
	// deleted: Delete (default; drop it) or Orphan (leave it in place).
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// ManagementPolicies restricts which actions the operator may take.
	// +kubebuilder:validation:items:Enum=Observe;Create;Update;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraDatabaseStatus is the observed state of the database.
type AuraDatabaseStatus struct {
	// DatabaseID is the Aura-assigned database ID (mirrored into the
	// neo4j.com/external-database-id annotation for idempotent create + adopt).
	// +optional
	DatabaseID string `json:"databaseId,omitempty"`

	// Phase mirrors the reconcile outcome (Pending, Ready, Error).
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the database was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auradb
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.spec.instanceRef`
// +kubebuilder:printcolumn:name="DatabaseID",type=string,JSONPath=`.status.databaseId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraDatabase manages a database on a Neo4j Aura instance via the Aura API
// v2beta1. BETA / best-effort — see the type doc and docs/design/aura-orchestration.md.
type AuraDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraDatabaseSpec   `json:"spec,omitempty"`
	Status AuraDatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraDatabaseList contains a list of AuraDatabase.
type AuraDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraDatabase{}, &AuraDatabaseList{})
}
