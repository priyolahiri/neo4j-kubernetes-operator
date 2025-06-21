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

// Neo4jDisasterRecoverySpec defines the desired state of Neo4jDisasterRecovery
type Neo4jDisasterRecoverySpec struct {
	// +kubebuilder:validation:Required
	// Primary cluster reference
	PrimaryClusterRef string `json:"primaryClusterRef"`

	// +kubebuilder:validation:Required
	// Secondary cluster reference
	SecondaryClusterRef string `json:"secondaryClusterRef"`

	// Cross-region configuration
	CrossRegion *CrossRegionConfig `json:"crossRegion,omitempty"`

	// Failover configuration
	Failover *FailoverConfig `json:"failover,omitempty"`

	// Replication configuration
	Replication *ReplicationConfig `json:"replication,omitempty"`

	// Health check configuration
	HealthCheck *DRHealthCheckConfig `json:"healthCheck,omitempty"`
}

// CrossRegionConfig defines cross-region disaster recovery settings
type CrossRegionConfig struct {
	// +kubebuilder:validation:Required
	// Primary region
	PrimaryRegion string `json:"primaryRegion"`

	// +kubebuilder:validation:Required
	// Secondary region
	SecondaryRegion string `json:"secondaryRegion"`

	// +kubebuilder:validation:Enum=async;sync
	// +kubebuilder:default=async
	// Replication mode
	ReplicationMode string `json:"replicationMode,omitempty"`

	// Network configuration for cross-region
	Network *CrossRegionNetworkConfig `json:"network,omitempty"`
}

// CrossRegionNetworkConfig defines network settings for cross-region replication
type CrossRegionNetworkConfig struct {
	// VPC peering configuration
	VPCPeering *VPCPeeringConfig `json:"vpcPeering,omitempty"`

	// Transit gateway configuration
	TransitGateway *TransitGatewayConfig `json:"transitGateway,omitempty"`

	// Custom network endpoints
	CustomEndpoints []NetworkEndpoint `json:"customEndpoints,omitempty"`
}

// VPCPeeringConfig defines VPC peering settings
type VPCPeeringConfig struct {
	// VPC ID in primary region
	PrimaryVPCID string `json:"primaryVpcId,omitempty"`

	// VPC ID in secondary region
	SecondaryVPCID string `json:"secondaryVpcId,omitempty"`

	// Auto-create peering connection
	AutoCreate bool `json:"autoCreate,omitempty"`
}

// TransitGatewayConfig defines transit gateway settings
type TransitGatewayConfig struct {
	// Transit Gateway ID
	GatewayID string `json:"gatewayId,omitempty"`

	// Route table ID
	RouteTableID string `json:"routeTableId,omitempty"`
}

// NetworkEndpoint defines a network endpoint
type NetworkEndpoint struct {
	// Endpoint name
	Name string `json:"name"`

	// Endpoint URL
	URL string `json:"url"`

	// Region
	Region string `json:"region"`
}

// FailoverConfig defines failover behavior
type FailoverConfig struct {
	// +kubebuilder:default=true
	// Enable automatic failover
	Automatic bool `json:"automatic,omitempty"`

	// +kubebuilder:default="1h"
	// Recovery Point Objective (max data loss)
	RPO string `json:"rpo,omitempty"`

	// +kubebuilder:default="15m"
	// Recovery Time Objective (max downtime)
	RTO string `json:"rto,omitempty"`

	// Failover triggers
	Triggers *FailoverTriggers `json:"triggers,omitempty"`

	// Notification configuration
	Notifications *DRNotificationConfig `json:"notifications,omitempty"`
}

// FailoverTriggers defines conditions that trigger failover
type FailoverTriggers struct {
	// Primary cluster unavailable for duration
	PrimaryUnavailableFor string `json:"primaryUnavailableFor,omitempty"`

	// Replication lag threshold
	ReplicationLagThreshold string `json:"replicationLagThreshold,omitempty"`

	// Custom health check failures
	HealthCheckFailures int32 `json:"healthCheckFailures,omitempty"`
}

// DRNotificationConfig defines disaster recovery notifications
type DRNotificationConfig struct {
	// Slack webhook URL
	SlackWebhook string `json:"slackWebhook,omitempty"`

	// Email configuration
	Email *EmailNotificationConfig `json:"email,omitempty"`

	// PagerDuty integration
	PagerDuty *PagerDutyConfig `json:"pagerDuty,omitempty"`
}

// EmailNotificationConfig defines email notification settings
type EmailNotificationConfig struct {
	// SMTP server
	SMTPServer string `json:"smtpServer,omitempty"`

	// Recipients
	Recipients []string `json:"recipients,omitempty"`

	// Sender email
	From string `json:"from,omitempty"`
}

// PagerDutyConfig defines PagerDuty integration
type PagerDutyConfig struct {
	// Integration key
	IntegrationKey string `json:"integrationKey,omitempty"`

	// Service ID
	ServiceID string `json:"serviceId,omitempty"`
}

// ReplicationConfig defines replication settings
type ReplicationConfig struct {
	// +kubebuilder:validation:Enum=streaming;batch;hybrid
	// +kubebuilder:default=streaming
	// Replication method
	Method string `json:"method,omitempty"`

	// Replication interval for batch mode
	Interval string `json:"interval,omitempty"`

	// Compression settings
	Compression *ReplicationCompressionConfig `json:"compression,omitempty"`

	// Encryption settings
	Encryption *ReplicationEncryptionConfig `json:"encryption,omitempty"`

	// Bandwidth throttling
	BandwidthLimit string `json:"bandwidthLimit,omitempty"`
}

// ReplicationCompressionConfig defines compression settings
type ReplicationCompressionConfig struct {
	// +kubebuilder:default=true
	// Enable compression
	Enabled bool `json:"enabled,omitempty"`

	// +kubebuilder:validation:Enum=gzip;lz4;zstd
	// +kubebuilder:default=lz4
	// Compression algorithm
	Algorithm string `json:"algorithm,omitempty"`

	// Compression level (1-9)
	Level int32 `json:"level,omitempty"`
}

// ReplicationEncryptionConfig defines replication encryption
type ReplicationEncryptionConfig struct {
	// +kubebuilder:default=true
	// Enable encryption
	Enabled bool `json:"enabled,omitempty"`

	// Encryption key secret reference
	KeySecret string `json:"keySecret,omitempty"`

	// +kubebuilder:validation:Enum=AES256;ChaCha20
	// +kubebuilder:default=AES256
	// Encryption algorithm
	Algorithm string `json:"algorithm,omitempty"`
}

// DRHealthCheckConfig defines disaster recovery health checks
type DRHealthCheckConfig struct {
	// Health check interval
	// +kubebuilder:default="30s"
	Interval string `json:"interval,omitempty"`

	// Health check timeout
	// +kubebuilder:default="10s"
	Timeout string `json:"timeout,omitempty"`

	// Failure threshold before triggering failover
	// +kubebuilder:default=3
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// Custom health check endpoints
	CustomChecks []CustomHealthCheck `json:"customChecks,omitempty"`
}

// CustomHealthCheck defines a custom health check
type CustomHealthCheck struct {
	// Check name
	Name string `json:"name"`

	// Cypher query to execute
	CypherQuery string `json:"cypherQuery,omitempty"`

	// HTTP endpoint to check
	HTTPEndpoint string `json:"httpEndpoint,omitempty"`

	// Expected result
	ExpectedResult string `json:"expectedResult,omitempty"`
}

// Neo4jDisasterRecoveryStatus defines the observed state of Neo4jDisasterRecovery
type Neo4jDisasterRecoveryStatus struct {
	// Conditions represent the current state of the disaster recovery
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the current phase
	Phase string `json:"phase,omitempty"`

	// Message provides additional information
	Message string `json:"message,omitempty"`

	// Current active region
	ActiveRegion string `json:"activeRegion,omitempty"`

	// Replication status
	ReplicationStatus *ReplicationStatus `json:"replicationStatus,omitempty"`

	// Last failover time
	LastFailoverTime *metav1.Time `json:"lastFailoverTime,omitempty"`

	// Health check results
	HealthCheckResults []HealthCheckResult `json:"healthCheckResults,omitempty"`

	// Observed generation
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// ReplicationStatus defines the status of replication
type ReplicationStatus struct {
	// Replication lag
	Lag string `json:"lag,omitempty"`

	// Last replication time
	LastReplicationTime *metav1.Time `json:"lastReplicationTime,omitempty"`

	// Bytes replicated
	BytesReplicated int64 `json:"bytesReplicated,omitempty"`

	// Replication rate
	ReplicationRate string `json:"replicationRate,omitempty"`

	// Replication errors
	Errors []string `json:"errors,omitempty"`
}

// HealthCheckResult defines a health check result
type HealthCheckResult struct {
	// Check name
	Name string `json:"name"`

	// Status
	Status string `json:"status"`

	// Last check time
	LastCheckTime metav1.Time `json:"lastCheckTime"`

	// Error message if failed
	Error string `json:"error,omitempty"`

	// Response time
	ResponseTime string `json:"responseTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.spec.primaryClusterRef`
// +kubebuilder:printcolumn:name="Secondary",type=string,JSONPath=`.spec.secondaryClusterRef`
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.activeRegion`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Neo4jDisasterRecovery is the Schema for the neo4jdisasterrecoveries API
type Neo4jDisasterRecovery struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Neo4jDisasterRecoverySpec   `json:"spec,omitempty"`
	Status Neo4jDisasterRecoveryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// Neo4jDisasterRecoveryList contains a list of Neo4jDisasterRecovery
type Neo4jDisasterRecoveryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Neo4jDisasterRecovery `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Neo4jDisasterRecovery{}, &Neo4jDisasterRecoveryList{})
}
