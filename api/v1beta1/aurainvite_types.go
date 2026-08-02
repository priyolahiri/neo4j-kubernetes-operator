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

// AuraInviteSpec invites an email to a Neo4j Aura organization (or a project
// within it) with a role, via the Aura API v2beta1.
//
// This is how a new person is granted Aura console access; an existing member's
// role is managed with AuraOrganizationMember / AuraProjectMember. BETA /
// best-effort.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
// +kubebuilder:validation:XValidation:rule="!self.role.startsWith('namespace-') || has(self.projectId)",message="a namespace-* role is a project-scoped invite and requires projectId"
// +kubebuilder:validation:XValidation:rule="!self.role.startsWith('organization-') || !has(self.projectId)",message="an organization-* role is an org-level invite; do not set projectId (use a namespace-* role to scope an invite to a project)"
// +kubebuilder:validation:XValidation:rule="!self.role.startsWith('namespace-') || has(self.organizationRole)",message="organizationRole is required when role is a namespace-* (project-scoped) role: Aura requires an organization role on every invite, so a project-only invite is impossible"
type AuraInviteSpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults)
	// in the same namespace. Mutually exclusive with credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// OrganizationID is the Aura organization to invite into. If empty, the
	// referenced AuraProviderConfig's defaultOrganizationId is used.
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// ProjectID optionally scopes the invite to a project (a project-member
	// invite). Omit for an organization-level invite.
	// +optional
	ProjectID string `json:"projectId,omitempty"`

	// Email is the invitee's email address.
	// +kubebuilder:validation:Required
	Email string `json:"email"`

	// Role is the role to grant on acceptance, using the Aura API's own v2beta1
	// invite vocabulary.
	//
	// Use an organization-* role for an organization-level invite (leave
	// projectId empty), or a namespace-* role for a project-scoped invite
	// (projectId set). The namespace-* spelling is the API's: the invite body
	// names project roles `namespace-*` even though the project-members endpoint
	// names the same concepts `project-*`.
	// +kubebuilder:validation:Enum=organization-owner;organization-admin;organization-member;namespace-admin;namespace-member;namespace-viewer;namespace-metrics-integration-reader
	// +kubebuilder:validation:Required
	Role string `json:"role"`

	// OrganizationRole grants the organization-level role on acceptance.
	//
	// REQUIRED alongside a namespace-* (project-scoped) role: Aura demands at
	// least one organization role on EVERY invite — an empty `roles` list is
	// rejected outright — so a project-only invite is not expressible. Verified
	// live 2026-08-01. (This field previously said "leave it empty … and let Aura
	// apply its own default"; there is no such default, and doing so was a 400.)
	// +kubebuilder:validation:Enum=organization-owner;organization-admin;organization-member
	// +optional
	OrganizationRole string `json:"organizationRole,omitempty"`

	// DeletionPolicy controls what happens to a still-pending invite when this CR
	// is deleted: Delete (default; revoke it) or Orphan (leave it).
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// ManagementPolicies restricts which actions the operator may take.
	// +kubebuilder:validation:items:Enum=Observe;Create;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraInviteStatus is the observed state of the invite.
type AuraInviteStatus struct {
	// InviteID is the Aura-assigned invite ID (mirrored into the
	// neo4j.com/external-invite-id annotation for idempotent create + adopt).
	// +optional
	InviteID string `json:"inviteId,omitempty"`

	// Phase mirrors the reconcile outcome (Pending, Sent, Accepted, Error).
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the invite was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aurainvite
// +kubebuilder:printcolumn:name="Email",type=string,JSONPath=`.spec.email`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraInvite invites a user to a Neo4j Aura organization or project via the Aura
// API v2beta1. BETA / best-effort — see docs/design/aura-orchestration.md.
type AuraInvite struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraInviteSpec   `json:"spec,omitempty"`
	Status AuraInviteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraInviteList contains a list of AuraInvite.
type AuraInviteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraInvite `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraInvite{}, &AuraInviteList{})
}
