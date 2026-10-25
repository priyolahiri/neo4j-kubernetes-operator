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

package validation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A standalone's upgradeStrategy timeouts bound steps of a cluster's rolling
// upgrade, and nothing on the standalone's path reads them (#532). Setting one
// is the knowledge rule 102 case: a ValidationWarning, and still accepted.
func TestStandaloneValidator_NoEffectWarnings_UpgradeTimeouts(t *testing.T) {
	v := NewStandaloneValidator()

	for _, tc := range []struct {
		path string
		set  func(*neo4jv1beta1.UpgradeStrategySpec)
	}{
		{"spec.upgradeStrategy.upgradeTimeout", func(s *neo4jv1beta1.UpgradeStrategySpec) { s.UpgradeTimeout = "1h" }},
		{"spec.upgradeStrategy.healthCheckTimeout", func(s *neo4jv1beta1.UpgradeStrategySpec) { s.HealthCheckTimeout = "10m" }},
		{"spec.upgradeStrategy.stabilizationTimeout", func(s *neo4jv1beta1.UpgradeStrategySpec) { s.StabilizationTimeout = "not-a-duration" }},
	} {
		t.Run(tc.path, func(t *testing.T) {
			sa := validStandalone()
			sa.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{PostUpgradeHealthCheck: true}
			tc.set(sa.Spec.UpgradeStrategy)
			assert.Empty(t, v.ValidateCreate(sa), "the standalone must still be accepted")
			requireNoEffectWarning(t, v.NoEffectWarnings(sa), tc.path, "replaces its one pod")
			assert.Equal(t, 1, noEffectCount(v.NoEffectWarnings(sa)))
		})
	}

	// The API server stores the CRD's defaults wherever upgradeStrategy is
	// present, so an upgradeStrategy that names only a strategy arrives with
	// every timeout set. Those, and the same durations spelt otherwise, are
	// "never mentioned" and must stay silent.
	t.Run("the CRD defaults do not warn", func(t *testing.T) {
		manifest := map[string]any{
			"apiVersion": neo4jv1beta1.GroupVersion.String(),
			"kind":       "Neo4jEnterpriseStandalone",
			"metadata":   map[string]any{"name": "sa", "namespace": "default"},
			"spec": map[string]any{
				"acceptLicenseAgreement": "eval",
				"image":                  map[string]any{"repo": "neo4j", "tag": "2026.08.1-enterprise"},
				"storage":                map[string]any{"size": "1Gi"},
				"upgradeStrategy":        map[string]any{"strategy": "Recreate"},
			},
		}
		withCRDDefaults(t, "neo4j.neo4j.com_neo4jenterprisestandalones.yaml", manifest)
		raw, err := json.Marshal(manifest)
		require.NoError(t, err)
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		require.NoError(t, json.Unmarshal(raw, sa))
		require.Equal(t, "30m", sa.Spec.UpgradeStrategy.UpgradeTimeout, "the CRD default reached the object")
		assert.Zero(t, noEffectCount(v.NoEffectWarnings(sa)))

		sa.Spec.UpgradeStrategy.UpgradeTimeout = "1800s"
		sa.Spec.UpgradeStrategy.HealthCheckTimeout = "300s"
		assert.Zero(t, noEffectCount(v.NoEffectWarnings(sa)), "the default duration, spelt otherwise")
	})

	// postUpgradeHealthCheck is not read either, but false says that no check
	// runs after the upgrade, which is what happens, and true is the default.
	t.Run("postUpgradeHealthCheck does not warn", func(t *testing.T) {
		sa := validStandalone()
		sa.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{PostUpgradeHealthCheck: false}
		assert.Zero(t, noEffectCount(v.NoEffectWarnings(sa)))
	})

	// A cluster reads all three.
	t.Run("a cluster is never warned", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{
			UpgradeTimeout: "1h", HealthCheckTimeout: "10m", StabilizationTimeout: "10m",
		}
		assert.Zero(t, noEffectCount(NewClusterValidator(nil).NoEffectWarnings(cluster)))
	})
}
