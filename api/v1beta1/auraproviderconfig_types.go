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

// AuraCredentialsSecretRef references a Kubernetes Secret holding the Neo4j
// Aura API OAuth client credentials (client-credentials grant). The Secret can
// be populated by any means, including External Secrets Operator.
type AuraCredentialsSecretRef struct {
	// Name of the Secret holding the Aura API client credentials.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// ClientIDKey is the Secret key holding the OAuth client ID.
	// +kubebuilder:default=clientId
	// +optional
	ClientIDKey string `json:"clientIdKey,omitempty"`

	// ClientSecretKey is the Secret key holding the OAuth client secret.
	// +kubebuilder:default=clientSecret
	// +optional
	ClientSecretKey string `json:"clientSecretKey,omitempty"`
}

// AuraProviderConfigSpec configures access to a Neo4j Aura account. It holds the
// API credentials and account-level defaults, and is referenced by AuraInstance
// (and other Aura resources) via spec.providerConfigRef. The controller owns the
// per-credential OAuth token cache and the Aura API rate limiter keyed by this
// config, so all resources sharing a provider config share one token and one
// rate-limit budget (25/125 req/min per credential).
type AuraProviderConfigSpec struct {
	// CredentialsSecretRef references the Secret with the OAuth client
	// credentials. The Secret must be in the same namespace as this config.
	// +kubebuilder:validation:Required
	CredentialsSecretRef AuraCredentialsSecretRef `json:"credentialsSecretRef"`

	// DefaultProjectID is the Aura project (API tenant_id) used by resources
	// that reference this config without specifying their own projectId.
	// +optional
	DefaultProjectID string `json:"defaultProjectId,omitempty"`

	// BaseURL overrides the Aura API base URL (default https://api.neo4j.io/v1).
	// Intended for testing against a fake API server; leave empty in production.
	// +optional
	BaseURL string `json:"baseUrl,omitempty"`
}

// AuraProviderConfigStatus reports whether the credentials are usable.
type AuraProviderConfigStatus struct {
	// Conditions: Ready=True once an access token was successfully obtained with
	// the referenced credentials (reason CredentialsValidated).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=auracfg
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.defaultProjectId`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AuraProviderConfig holds Neo4j Aura API credentials and account defaults,
// referenced by Aura resources via spec.providerConfigRef.
type AuraProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuraProviderConfigSpec   `json:"spec,omitempty"`
	Status AuraProviderConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AuraProviderConfigList contains a list of AuraProviderConfig.
type AuraProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuraProviderConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuraProviderConfig{}, &AuraProviderConfigList{})
}
