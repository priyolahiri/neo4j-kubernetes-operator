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

// Neo4jGrantSpec defines the desired state of Neo4jGrant
type Neo4jGrantSpec struct {
	// +kubebuilder:validation:Required
	// Reference to the Neo4j cluster
	ClusterRef string `json:"clusterRef"`

	// +kubebuilder:validation:Required
	// Target user or role for the grant
	Target GrantTarget `json:"target"`

	// +kubebuilder:validation:Required
	// Privilege statements to execute
	Statements []string `json:"statements"`

	// Privilege rules (alternative format)
	PrivilegeRules []PrivilegeRule `json:"privilegeRules,omitempty"`

	// +kubebuilder:validation:Enum=error;ignore;replace
	// +kubebuilder:default=error
	// What to do when a privilege doesn't match existing state
	WhenNotMatched string `json:"whenNotMatched,omitempty"`

	// Description of the grant
	Description string `json:"description,omitempty"`
}

// GrantTarget defines the target of a privilege grant
type GrantTarget struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=User;Role
	// Type of target (User or Role)
	Kind string `json:"kind"`

	// +kubebuilder:validation:Required
	// Name of the target user or role
	Name string `json:"name"`
}

// PrivilegeRule defines a privilege rule for granting or revoking
type PrivilegeRule struct {
	// Role name to grant/revoke privilege to/from
	RoleName string `json:"roleName,omitempty"`

	// User name to grant/revoke privilege to/from
	UserName string `json:"userName,omitempty"`

	// Privilege action (GRANT or REVOKE)
	Action string `json:"action,omitempty"`

	// Database or resource the privilege applies to
	Resource string `json:"resource,omitempty"`

	// Specific privilege type
	Privilege string `json:"privilege,omitempty"`

	// The graph/database the privilege applies to
	Graph string `json:"graph,omitempty"`

	// Qualifier for the privilege (e.g., label, property)
	Qualifier string `json:"qualifier,omitempty"`
}

// Neo4jGrantStatus defines the observed state of Neo4jGrant
type Neo4jGrantStatus struct {
	// Conditions represent the current state of the grant
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the current phase of the grant
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the current state
	Message string `json:"message,omitempty"`

	// ObservedGeneration reflects the generation observed by the controller
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Applied statements
	AppliedStatements []string `json:"appliedStatements,omitempty"`

	// Failed statements with error messages
	FailedStatements []FailedStatement `json:"failedStatements,omitempty"`

	// Last execution time
	LastExecutionTime *metav1.Time `json:"lastExecutionTime,omitempty"`

	// Hash of the current statements for change detection
	StatementsHash string `json:"statementsHash,omitempty"`
}

// FailedStatement represents a failed privilege statement
type FailedStatement struct {
	// The statement that failed
	Statement string `json:"statement"`

	// Error message
	Error string `json:"error"`

	// Timestamp of the failure
	Timestamp metav1.Time `json:"timestamp"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.kind`
// +kubebuilder:printcolumn:name="Statements",type=integer,JSONPath=`.spec.statements.length`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jGrant is the Schema for the neo4jgrants API
type Neo4jGrant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jGrantSpec   `json:"spec,omitempty"`
	Status Neo4jGrantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jGrantList contains a list of Neo4jGrant
type Neo4jGrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jGrant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jGrant{}, &Neo4jGrantList{})
}
