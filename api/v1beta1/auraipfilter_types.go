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

// AuraIPFilterAllowEntry is one allow-listed source range. The Aura v2beta1 API
// splits CIDR notation into an address plus a prefix length (e.g.
// "203.0.113.0/24" → address "203.0.113.0", prefixLen 24).
type AuraIPFilterAllowEntry struct {
	// Address is the IP address of the CIDR (e.g. "203.0.113.0").
	// +kubebuilder:validation:Required
	Address string `json:"address"`

	// PrefixLen is the CIDR prefix length (0–32 for IPv4, up to 128 for IPv6).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=128
	// +kubebuilder:validation:Required
	PrefixLen int32 `json:"prefixLen"`

	// Description is an optional human-friendly label for this entry.
	// +optional
	Description string `json:"description,omitempty"`
}

// AuraIPFilterSpec configures a Neo4j Aura network IP filter (allowlist).
//
// BETA: IP filtering is on the Aura API v2beta1 (an unstable beta). IP filters
// are ORGANIZATION-scoped and applied to instances; this CRD models the common
// case of protecting one or more AuraInstances. See the type fields and
// docs/design/aura-orchestration.md.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
type AuraIPFilterSpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults)
	// in the same namespace. Mutually exclusive with credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// OrganizationID is the Aura organization that owns the filter (filters are
	// org-scoped in v2beta1). If empty, the referenced AuraProviderConfig's
	// defaultOrganizationId is used.
	// +optional
	OrganizationID string `json:"organizationId,omitempty"`

	// Name is the filter's display name in Aura (defaults to metadata.name).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Name string `json:"name,omitempty"`

	// Description is an optional human-friendly description of the filter.
	// +optional
	Description string `json:"description,omitempty"`

	// AllowList is the set of source ranges permitted by the filter. At least
	// one entry is required.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1000
	// +kubebuilder:validation:Required
	AllowList []AuraIPFilterAllowEntry `json:"allowList"`

	// InstanceRefs names the AuraInstances (same namespace) the filter applies
	// to; the operator resolves each to its Aura instance ID (the API's
	// filtered_entities.instances). Omit for a filter you attach out of band.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=100
	InstanceRefs []string `json:"instanceRefs,omitempty"`

	// FilteringDisabled turns the filter off without deleting it (the API's
	// filtering_disabled). Default false (the filter is enforced).
	// +optional
	FilteringDisabled bool `json:"filteringDisabled,omitempty"`

	// DeletionPolicy controls what happens to the Aura filter when this CR is
	// deleted: Orphan (default; leave it in place — deleting it opens access) or
	// Delete (remove it from Aura).
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

	// Phase mirrors the reconcile outcome (Pending, Ready, Error).
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
// +kubebuilder:printcolumn:name="FilterID",type=string,JSONPath=`.status.filterId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraIPFilter manages a Neo4j Aura network IP filter (allowlist) via the Aura
// API v2beta1. BETA / best-effort — see the type doc and
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
