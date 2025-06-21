/*
Copyright 2025.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Neo4jUserSpec defines the desired state of Neo4jUser
type Neo4jUserSpec struct {
	// +kubebuilder:validation:Required
	// Reference to the Neo4j cluster
	ClusterRef string `json:"clusterRef"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]{2,30}$`
	// Username for the Neo4j user
	Username string `json:"username"`

	// +kubebuilder:validation:Required
	// Secret containing the user password
	PasswordSecret PasswordSecretRef `json:"passwordSecret"`

	// Roles assigned to the user
	Roles []string `json:"roles,omitempty"`

	// Whether user must change password on first login
	// +kubebuilder:default=false
	MustChangePassword bool `json:"mustChangePassword,omitempty"`

	// Whether the user account is suspended
	// +kubebuilder:default=false
	Suspended bool `json:"suspended,omitempty"`

	// Additional user properties
	Properties map[string]string `json:"properties,omitempty"`
}

// PasswordSecretRef references a secret containing the user password
type PasswordSecretRef struct {
	// +kubebuilder:validation:Required
	// Name of the secret
	Name string `json:"name"`

	// +kubebuilder:default=password
	// Key within the secret containing the password
	Key string `json:"key,omitempty"`
}

// Neo4jUserStatus defines the observed state of Neo4jUser
type Neo4jUserStatus struct {
	// Conditions represent the current state of the user
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the current phase of the user
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the current state
	Message string `json:"message,omitempty"`

	// CreationTime shows when the user was created
	CreationTime *metav1.Time `json:"creationTime,omitempty"`

	// LastLogin shows when the user last logged in
	LastLogin *metav1.Time `json:"lastLogin,omitempty"`

	// PasswordChangeRequired indicates if password change is required
	PasswordChangeRequired bool `json:"passwordChangeRequired,omitempty"`

	// Applied roles
	AppliedRoles []string `json:"appliedRoles,omitempty"`

	// ObservedGeneration reflects the generation of the most recently observed Neo4jUser
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=`.spec.username`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Roles",type=string,JSONPath=`.spec.roles`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jUser is the Schema for the neo4jusers API
type Neo4jUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jUserSpec   `json:"spec,omitempty"`
	Status Neo4jUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jUserList contains a list of Neo4jUser
type Neo4jUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jUser{}, &Neo4jUserList{})
}
