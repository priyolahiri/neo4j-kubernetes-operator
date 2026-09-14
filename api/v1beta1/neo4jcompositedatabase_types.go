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

// Neo4jCompositeDatabaseSpec defines a composite database — a single query
// endpoint over several constituent databases.
//
// A composite stores no data of its own. It has no topology, no store, no
// indexes or constraints, and no seeding: it is an access layer, and its
// content is entirely the constituent aliases declared below. That is why this
// is a separate Kind rather than a flag on Neo4jDatabase, whose spec is almost
// wholly topology, options, storage and seeding — every one of which is
// meaningless here.
//
// Scope: LOCAL constituents only, i.e. aliases targeting databases in the same
// DBMS. Remote constituents (`... AT '<url>' USER ... PASSWORD ...`) are NOT
// modelled, and the reason is a server prerequisite rather than a modelling
// choice: creating one fails with
//
//	Failed to create alias for remote database: the required setting(s)
//	[dbms.security.keystore.path, dbms.security.keystore.password] are missing
//
// so remote constituents need a provisioned keystore on every server before a
// single one can exist. That is its own piece of work.
type Neo4jCompositeDatabaseSpec struct {
	// ClusterRef is the Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone in
	// the same namespace that hosts this composite and its constituents.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Name is the composite database name in Neo4j. Defaults to metadata.name
	// when empty.
	//
	// Neo4j database names accept only ASCII letters, digits, dots and dashes —
	// underscores are rejected by the server, so they are rejected here.
	// A dot is legal in a name but reserved in practice: `a.b` is how a
	// constituent of composite `a` is addressed, so a composite whose own name
	// contains a dot cannot have constituents.
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9.\-]*$`
	// +optional
	Name string `json:"name,omitempty"`

	// Constituents are the databases this composite exposes. Each becomes an
	// alias `<composite>.<name>` pointing at TargetDatabase.
	//
	// Declared inline rather than as separate CRs because a composite IS its
	// constituents — it has no other content — and because ordering is not
	// optional: an alias in the `<composite>.` namespace created BEFORE the
	// composite exists silently becomes an ordinary dotted alias, and then
	// permanently blocks the composite from being created at all:
	//
	//	42N87: The database or alias name `x` conflicts with the name `x.y`
	//	       of an existing database or alias.
	//
	// Owning both in one reconcile is what makes that ordering guaranteed.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Constituents []CompositeConstituent `json:"constituents"`

	// EnforceConstituents makes spec.constituents authoritative: a constituent
	// alias present in Neo4j but absent here is dropped on the next reconcile.
	//
	// Default true, same posture as Neo4jRole.enforcePrivileges. Set false to
	// leave out-of-band constituents alone, in which case this CR only ever
	// adds.
	// +kubebuilder:default=true
	// +optional
	EnforceConstituents *bool `json:"enforceConstituents,omitempty"`

	// DefaultCypherLanguage sets the composite's default Cypher version.
	//
	// CalVer only. On the 5.26 LTS the clause does not parse — `CREATE
	// COMPOSITE DATABASE` there accepts only IF NOT EXISTS / WAIT / NOWAIT /
	// OPTIONS, and `ALTER DATABASE ... SET` accepts only OPTION, ACCESS READ
	// and TOPOLOGY. The validator rejects this field on a 5.26 deployment
	// rather than letting the server produce a syntax error.
	//
	// It is the ONLY property of a composite that can be altered after
	// creation.
	// +kubebuilder:validation:Enum="5";"25"
	// +optional
	DefaultCypherLanguage string `json:"defaultCypherLanguage,omitempty"`

	// Wait makes creation block until the composite is available, via the
	// Cypher WAIT clause.
	// +kubebuilder:default=true
	// +optional
	Wait *bool `json:"wait,omitempty"`

	// DeletionPolicy controls what happens in Neo4j when this CR is deleted.
	//
	//	Delete (default): DROP COMPOSITE DATABASE ... CASCADE ALIASES.
	//	                  CASCADE is required — a plain drop is REFUSED while
	//	                  constituents exist. It removes the constituent
	//	                  ALIASES only; their target databases are untouched.
	//	Retain:           leave the composite in place, release the finalizer.
	// +kubebuilder:validation:Enum=Delete;Retain
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// CompositeConstituent is one database exposed through the composite.
type CompositeConstituent struct {
	// Name is the constituent's name within the composite. The resulting alias
	// is `<composite>.<name>`, which is also how queries address it
	// (`USE <composite>.<name>`).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9\-]*$`
	Name string `json:"name"`

	// TargetDatabase is the database in this same DBMS that the constituent
	// resolves to.
	//
	// It does not have to exist yet — the controller reports Pending and
	// retries, so a composite and its targets can be applied together.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetDatabase string `json:"targetDatabase"`
}

// Neo4jCompositeDatabaseStatus is the observed state.
type Neo4jCompositeDatabaseStatus struct {
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is one of Pending, Creating, Ready, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ObservedConstituents is the constituent list read back from the server
	// (`SHOW DATABASE <name> YIELD constituents`), fully qualified as
	// `<composite>.<name>`. It is what the DBMS actually exposes, which is not
	// always what spec asked for — an out-of-band alias shows up here when
	// enforceConstituents is false.
	// +optional
	ObservedConstituents []string `json:"observedConstituents,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=n4jcomposite;n4jcomposites,categories=neo4j
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Constituents",type=integer,JSONPath=`.status.observedConstituents[*]`,priority=1
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jCompositeDatabase is the Schema for the neo4jcompositedatabases API.
type Neo4jCompositeDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jCompositeDatabaseSpec   `json:"spec,omitempty"`
	Status Neo4jCompositeDatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jCompositeDatabaseList contains a list of Neo4jCompositeDatabase.
type Neo4jCompositeDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jCompositeDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jCompositeDatabase{}, &Neo4jCompositeDatabaseList{})
}
