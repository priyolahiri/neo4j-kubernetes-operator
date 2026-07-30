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

// Methods that make Neo4jEnterpriseCluster and Neo4jEnterpriseStandalone
// interchangeable to the shared Aura Fleet Manager provisioning helper in
// internal/controller. Both CRs embed the same AuraFleetManagementSpec, so the
// provisioning logic (register deployment, mint token, collect telemetry) is
// written once against these accessors instead of being duplicated per
// controller — the same arrangement as SeedCredsTarget in seed_creds_target.go,
// and for the same reason: the api package must not import the controller
// package, so the accessors live here and the interface is declared where it is
// consumed.

// GetFleetSpec returns the cluster CR's spec.auraFleetManagement (may be nil).
func (c *Neo4jEnterpriseCluster) GetFleetSpec() *AuraFleetManagementSpec {
	return c.Spec.AuraFleetManagement
}

// GetFleetStatus returns the cluster CR's status.auraFleetManagement (may be nil).
func (c *Neo4jEnterpriseCluster) GetFleetStatus() *AuraFleetManagementStatus {
	return c.Status.AuraFleetManagement
}

// SetFleetStatus replaces the cluster CR's status.auraFleetManagement.
// In-place mutation is intentional — the caller writes the CR back.
func (c *Neo4jEnterpriseCluster) SetFleetStatus(s *AuraFleetManagementStatus) {
	c.Status.AuraFleetManagement = s
}

// GetFleetSpec returns the standalone CR's spec.auraFleetManagement (may be nil).
func (s *Neo4jEnterpriseStandalone) GetFleetSpec() *AuraFleetManagementSpec {
	return s.Spec.AuraFleetManagement
}

// GetFleetStatus returns the standalone CR's status.auraFleetManagement (may be nil).
func (s *Neo4jEnterpriseStandalone) GetFleetStatus() *AuraFleetManagementStatus {
	return s.Status.AuraFleetManagement
}

// SetFleetStatus replaces the standalone CR's status.auraFleetManagement.
func (s *Neo4jEnterpriseStandalone) SetFleetStatus(st *AuraFleetManagementStatus) {
	s.Status.AuraFleetManagement = st
}
