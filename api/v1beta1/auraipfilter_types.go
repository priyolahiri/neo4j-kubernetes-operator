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

// AuraIPFilterSpec configures a Neo4j Aura network IP filter (CIDR allowlist).
//
// BETA: IP filtering is only available on the Aura API v2beta1 surface, which is
// an unstable beta (breaking changes allowed without a version bump). This CRD
// is best-effort and its behaviour may change to track the API — see
// docs/knowledge/operations.md and docs/design/aura-orchestration.md.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.instanceRef) || (has(self.instanceRef) && self.instanceRef == oldSelf.instanceRef)",message="instanceRef is immutable once set"
type AuraIPFilterSpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults)
	// in the same namespace. Mutually exclusive with credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// OrganizationID is the Aura organization ID (v2beta1 hierarchy). If empty,
	// the referenced AuraProviderConfig's defaultOrganizationId is used.
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// ProjectID is the Aura project (API tenant_id). If empty, the referenced
	// AuraProviderConfig's defaultProjectId is used.
	// +optional
	ProjectID string `json:"projectId,omitempty"`

	// InstanceRef optionally scopes the filter to a single AuraInstance (same
	// namespace) — Aura permits at most one IP filter per instance. Omit for a
	// project-wide filter. Immutable once set.
	// +optional
	InstanceRef string `json:"instanceRef,omitempty"`

	// Name is the filter's display name in Aura (defaults to metadata.name).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Name string `json:"name,omitempty"`

	// Region the filter applies to, when required by the provider.
	// +optional
	Region string `json:"region,omitempty"`

	// CIDRs is the allowlist of source ranges in CIDR notation (e.g.
	// "203.0.113.0/24"). At least one is required.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:Required
	CIDRs []string `json:"cidrs"`

	// DeletionPolicy controls what happens to the Aura filter when this CR is
	// deleted: Orphan (default; leave the filter in place — deleting it would
	// open network access) or Delete (remove it from Aura).
	// +kubebuilder:validation:Enum=Orphan;Delete
	// +kubebuilder:default=Orphan
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// ManagementPolicies restricts which actions the operator may take.
	// Default ["*"] = full management.
	// +kubebuilder:validation:items:Enum=Observe;Create;Update;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraIPFilterStatus is the observed state of the filter.
type AuraIPFilterStatus struct {
	// FilterID is the Aura-assigned IP-filter ID (mirrored into the
	// neo4j.com/external-ipfilter-id annotation, the operator's source of truth
	// for idempotent create + adopt).
	// +optional
	FilterID string `json:"filterId,omitempty"`

	// Phase mirrors the Aura filter status (Pending, Ready, Updating, Error).
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the filter was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auraipf
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.spec.instanceRef`
// +kubebuilder:printcolumn:name="FilterID",type=string,JSONPath=`.status.filterId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraIPFilter manages a Neo4j Aura network IP filter (CIDR allowlist) via the
// Aura API v2beta1. BETA / best-effort — see the type doc and
// docs/design/aura-orchestration.md.
type AuraIPFilter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraIPFilterSpec   `json:"spec,omitempty"`
	Status AuraIPFilterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraIPFilterList contains a list of AuraIPFilter.
type AuraIPFilterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraIPFilter `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraIPFilter{}, &AuraIPFilterList{})
}
