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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AuraProjectMemberSpec manages the project-level role of an existing Aura
// console user (identified by email) via the Aura API v2beta1.
//
// This is Aura PLATFORM identity (project access), NOT an in-database Neo4j user.
// It reconciles the role of a user who is already a project member; to bring a
// new person in, create an AuraInvite. BETA / best-effort.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
type AuraProjectMemberSpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults)
	// in the same namespace. Mutually exclusive with credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// OrganizationID is the Aura organization. If empty, the referenced
	// AuraProviderConfig's defaultOrganizationId is used.
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// ProjectID is the Aura project (API tenant_id). If empty, the referenced
	// AuraProviderConfig's defaultProjectId is used.
	// +optional
	ProjectID string `json:"projectId,omitempty"`

	// Email identifies the project member whose role is managed.
	// +kubebuilder:validation:Required
	Email string `json:"email"`

	// Role is the desired project-level role.
	// +kubebuilder:validation:Enum=PROJECT_ADMIN;PROJECT_MEMBER;PROJECT_VIEWER;METRICS_READER
	// +kubebuilder:validation:Required
	Role string `json:"role"`

	// DeletionPolicy controls what happens when this CR is deleted: Orphan
	// (default; leave the member's access untouched) or Delete (remove them from
	// the project).
	// +kubebuilder:validation:Enum=Orphan;Delete
	// +kubebuilder:default=Orphan
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// ManagementPolicies restricts which actions the operator may take.
	// +kubebuilder:validation:items:Enum=Observe;Update;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraProjectMemberStatus is the observed state of the membership.
type AuraProjectMemberStatus struct {
	// UserID is the Aura user ID resolved from the email.
	// +optional
	UserID string `json:"userId,omitempty"`

	// Phase mirrors the reconcile outcome (Pending, Ready, NotAMember, Error).
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the membership was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auraprojmember
// +kubebuilder:printcolumn:name="Email",type=string,JSONPath=`.spec.email`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraProjectMember manages an Aura console user's project role via the Aura API
// v2beta1. BETA / best-effort — see docs/design/aura-orchestration.md.
type AuraProjectMember struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraProjectMemberSpec   `json:"spec,omitempty"`
	Status AuraProjectMemberStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraProjectMemberList contains a list of AuraProjectMember.
type AuraProjectMemberList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraProjectMember `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraProjectMember{}, &AuraProjectMemberList{})
}
