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

// AuraCustomerManagedKeySpec registers a customer-managed encryption key (CMK)
// with Aura. The key material lives in the customer's own cloud KMS; Aura stores
// only a reference to it. Once the key reaches status Ready, its Aura-assigned ID
// (surfaced in status.customerManagedKeyId) is referenced from an AuraInstance's
// spec.customerManagedKeyId to encrypt that instance with the customer's key.
//
// Every placement field is immutable: a CMK is permanently bound to one cloud
// provider, region, instance type, and KMS key. Change → create a new CMK.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) != has(self.credentialsSecretRef)",message="set exactly one of providerConfigRef or credentialsSecretRef"
// +kubebuilder:validation:XValidation:rule="self.cloudProvider == oldSelf.cloudProvider",message="cloudProvider is immutable"
// +kubebuilder:validation:XValidation:rule="self.region == oldSelf.region",message="region is immutable"
// +kubebuilder:validation:XValidation:rule="self.instanceType == oldSelf.instanceType",message="instanceType is immutable"
// +kubebuilder:validation:XValidation:rule="self.keyId == oldSelf.keyId",message="keyId is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.projectId) || (has(self.projectId) && self.projectId == oldSelf.projectId)",message="projectId is immutable once set"
type AuraCustomerManagedKeySpec struct {
	// ProviderConfigRef selects the AuraProviderConfig (credentials + defaults)
	// in the same namespace. Mutually exclusive with credentialsSecretRef.
	// +optional
	ProviderConfigRef *corev1.LocalObjectReference `json:"providerConfigRef,omitempty"`

	// CredentialsSecretRef is a single-account shortcut when no
	// AuraProviderConfig is used. Mutually exclusive with providerConfigRef.
	// +optional
	CredentialsSecretRef *AuraCredentialsSecretRef `json:"credentialsSecretRef,omitempty"`

	// ProjectID is the Aura project (API tenant_id) the key is scoped to.
	// Immutable. If empty, the referenced AuraProviderConfig's defaultProjectId
	// is used.
	// +optional
	ProjectID string `json:"projectId,omitempty"`

	// Name is the CMK's display name in Aura (defaults to metadata.name).
	// +kubebuilder:validation:MaxLength=30
	// +optional
	Name string `json:"name,omitempty"`

	// CloudProvider hosting the key. Immutable. Must match the instances that
	// will use it.
	// +kubebuilder:validation:Enum=aws;gcp;azure
	// +kubebuilder:validation:Required
	CloudProvider string `json:"cloudProvider"`

	// Region the key applies to (e.g. europe-west1). Immutable.
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// InstanceType the key can encrypt. Customer-managed keys are only supported
	// for the dedicated tiers. Immutable.
	// +kubebuilder:validation:Enum=enterprise-db;enterprise-ds
	// +kubebuilder:validation:Required
	InstanceType string `json:"instanceType"`

	// KeyID is the cloud provider KMS key resource identifier Aura will use — an
	// AWS KMS key ARN, a GCP KMS key resource name, or an Azure Key Vault key
	// URL. The customer must grant Aura access to this key out of band. Immutable.
	// +kubebuilder:validation:Required
	KeyID string `json:"keyId"`

	// DeletionPolicy controls what happens to the Aura-registered key when this
	// CR is deleted: Orphan (default; leave the key registered in Aura) or Delete
	// (deregister it — fails while any instance still uses it).
	// +kubebuilder:validation:Enum=Orphan;Delete
	// +kubebuilder:default=Orphan
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// ManagementPolicies restricts which actions the operator may take on the
	// key. Default ["*"] = full management.
	// +kubebuilder:validation:items:Enum=Observe;Create;Update;Delete;*
	// +kubebuilder:default={"*"}
	// +optional
	ManagementPolicies []string `json:"managementPolicies,omitempty"`
}

// AuraCustomerManagedKeyStatus is the observed state of the key.
type AuraCustomerManagedKeyStatus struct {
	// CustomerManagedKeyID is the Aura-assigned key ID. Reference this value from
	// an AuraInstance's spec.customerManagedKeyId. It is also mirrored into the
	// neo4j.com/external-cmk-id annotation (the operator's source of truth for
	// idempotent create + adopt).
	// +optional
	CustomerManagedKeyID string `json:"customerManagedKeyId,omitempty"`

	// Phase mirrors the Aura key status: Pending, Ready, Deleting, Error.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Conditions: Ready (key usable) and Synced (operator reconciled spec).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncedTime is when the key was last observed from the Aura API.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auracmk
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.cloudProvider`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="KeyID",type=string,JSONPath=`.status.customerManagedKeyId`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraCustomerManagedKey registers a customer-managed encryption key with Aura
// for use by dedicated-tier AuraInstances. See docs/design/aura-orchestration.md.
type AuraCustomerManagedKey struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraCustomerManagedKeySpec   `json:"spec,omitempty"`
	Status AuraCustomerManagedKeyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraCustomerManagedKeyList contains a list of AuraCustomerManagedKey.
type AuraCustomerManagedKeyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraCustomerManagedKey `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraCustomerManagedKey{}, &AuraCustomerManagedKeyList{})
}
