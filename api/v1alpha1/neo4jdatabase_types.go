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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// Neo4jDatabaseSpec defines the desired state of Neo4jDatabase
type Neo4jDatabaseSpec struct {
	// +kubebuilder:validation:Required
	// Reference to the Neo4j cluster
	ClusterRef string `json:"clusterRef"`

	// +kubebuilder:validation:Required
	// Database name
	Name string `json:"name"`

	// Database creation options
	Options map[string]string `json:"options,omitempty"`

	// Initial data import configuration
	InitialData *InitialDataSpec `json:"initialData,omitempty"`
}

// InitialDataSpec defines initial data import configuration
type InitialDataSpec struct {
	// Source type for initial data
	// +kubebuilder:validation:Enum=cypher;dump;csv
	Source string `json:"source,omitempty"`

	// Cypher statements for initial data
	CypherStatements []string `json:"cypherStatements,omitempty"`

	// Configuration map reference containing data
	ConfigMapRef string `json:"configMapRef,omitempty"`

	// Secret reference containing sensitive data
	SecretRef string `json:"secretRef,omitempty"`

	// Storage location for data files
	Storage *StorageLocation `json:"storage,omitempty"`
}

// Neo4jDatabaseStatus defines the observed state of Neo4jDatabase
type Neo4jDatabaseStatus struct {
	// Conditions represent the current state of the database
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the current phase of the database
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the current state
	Message string `json:"message,omitempty"`

	// ObservedGeneration reflects the generation observed by the controller
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DataImported indicates whether initial data has been imported
	DataImported *bool `json:"dataImported,omitempty"`

	// CreationTime shows when the database was created
	CreationTime *metav1.Time `json:"creationTime,omitempty"`

	// Size shows the current database size
	Size string `json:"size,omitempty"`

	// LastBackupTime shows when the last backup was taken
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jDatabase is the Schema for the neo4jdatabases API
type Neo4jDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jDatabaseSpec   `json:"spec,omitempty"`
	Status Neo4jDatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jDatabaseList contains a list of Neo4jDatabase
type Neo4jDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jDatabase{}, &Neo4jDatabaseList{})
}
