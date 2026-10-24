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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ---------------------------------------------------------------------------
// TestValidateVersionUpgrade
// ---------------------------------------------------------------------------

func TestValidateVersionUpgrade(t *testing.T) {
	v := NewUpgradeValidator()

	cases := []struct {
		name     string
		current  string
		target   string
		wantErrs int
	}{
		// Valid upgrades
		{"SemVer patch upgrade", "5.26.0-enterprise", "5.26.3-enterprise", 0},
		{"SemVer minor upgrade rejected (5.27 does not exist)", "5.26.0-enterprise", "5.27.0-enterprise", 1},
		{"CalVer minor upgrade", "2025.01.0-enterprise", "2025.02.0-enterprise", 0},
		{"CalVer year upgrade", "2025.01.0-enterprise", "2026.01.0-enterprise", 0},
		{"SemVer to CalVer migration", "5.26.0-enterprise", "2025.01.0-enterprise", 0},
		// "All upgrades from earlier than 5.26 must first upgrade to Neo4j
		// 5.26 LTS": the step to 5.26 is the supported one, not a refusal.
		{"an earlier 5.x to 5.26", "5.25.1-enterprise", "5.26.3-enterprise", 0},

		// Nothing to compare with, or nothing that can be ordered.
		{"no verified version yet", "", "2025.01.0-enterprise", 0},
		{"unchanged", "2026.08.1-enterprise", "2026.08.1-enterprise", 0},
		{"recorded version does not parse", "latest", "2026.08.1-enterprise", 0},
		{"tag without a patch floats to the newest one", "5.26.12-enterprise", "5.26-enterprise", 0},
		{"from a floating tag to a named patch", "2026.08-enterprise", "2026.08.0-enterprise", 0},
		{"the same tag pinned to its digest", "2026.08.1-enterprise", "2026.08.1-enterprise@sha256:0123abcd", 0},
		{"a tag pinned to a digest names its patch", "5.26.31-enterprise", "5.26.30@sha256:0123abcd", 1},

		// Invalid upgrades
		{"CalVer to SemVer downgrade rejected", "2025.01.0-enterprise", "5.26.0-enterprise", 1},
		{"CalVer downgrade", "2025.02.0-enterprise", "2025.01.0-enterprise", 1},
		{"CalVer patch downgrade", "2026.08.1-enterprise", "2026.08.0-enterprise", 1},
		{"SemVer patch downgrade", "5.26.12-enterprise", "5.26.10-enterprise", 1},
		{"CalVer to SemVer downgrade always rejected", "2025.01.0-enterprise", "5.26.0-enterprise", 1},
		{"an earlier 5.x straight to CalVer", "5.25.1-enterprise", "2025.01.0-enterprise", 1},
		{"invalid version format", "5.26.0-enterprise", "not-a-version", 1},
		{"Neo4j 4.x not supported", "4.4.0-enterprise", "4.4.9-enterprise", 1},
		{"Neo4j 4.x to 5 is a migration, not an upgrade", "4.4.0-enterprise", "5.26.0-enterprise", 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			errs := v.ValidateVersionUpgrade(tc.current, tc.target)
			if len(errs) != tc.wantErrs {
				t.Errorf("ValidateVersionUpgrade(%q, %q): expected %d errors, got %d: %v",
					tc.current, tc.target, tc.wantErrs, len(errs), errs)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestValidateUpgradeStrategy
// ---------------------------------------------------------------------------

func TestValidateUpgradeStrategy(t *testing.T) {
	v := NewUpgradeValidator()

	negOne := int32(-1)
	validMaxUnavail := int32(1)

	cases := []struct {
		name     string
		strategy *neo4jv1beta1.UpgradeStrategySpec
		wantErrs int
	}{
		{
			name:     "nil strategy - no errors",
			strategy: nil, wantErrs: 0,
		},
		{
			name: "valid RollingUpgrade strategy",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{
				Strategy:                    "RollingUpgrade",
				UpgradeTimeout:              "30m",
				HealthCheckTimeout:          "5m",
				StabilizationTimeout:        "3m",
				MaxUnavailableDuringUpgrade: &validMaxUnavail,
			},
			wantErrs: 0,
		},
		{
			name:     "valid Recreate strategy",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{Strategy: "Recreate"}, wantErrs: 0,
		},
		{
			name:     "unknown strategy Blue-Green",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{Strategy: "Blue-Green"}, wantErrs: 1,
		},
		{
			name:     "invalid upgradeTimeout",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{UpgradeTimeout: "not-a-duration"}, wantErrs: 1,
		},
		{
			name:     "invalid healthCheckTimeout",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{HealthCheckTimeout: "not-a-duration"}, wantErrs: 1,
		},
		{
			name:     "invalid stabilizationTimeout",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{StabilizationTimeout: "not-a-duration"}, wantErrs: 1,
		},
		// Each parses, and each put every step's deadline at or before its
		// start: the upgrade failed at its first check.
		{
			name:     "upgradeTimeout of zero",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{UpgradeTimeout: "0s"}, wantErrs: 1,
		},
		{
			name:     "upgradeTimeout of a bare 0",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{UpgradeTimeout: "0"}, wantErrs: 1,
		},
		{
			name:     "negative healthCheckTimeout",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{HealthCheckTimeout: "-5m"}, wantErrs: 1,
		},
		{
			name:     "negative stabilizationTimeout",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{StabilizationTimeout: "-1h30m"}, wantErrs: 1,
		},
		{
			name: "all three at zero",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{
				UpgradeTimeout: "0s", HealthCheckTimeout: "0m", StabilizationTimeout: "0h",
			},
			wantErrs: 3,
		},
		// What time.ParseDuration accepts above zero still passes.
		{
			name: "positive durations in other spellings",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{
				UpgradeTimeout: "1h30m", HealthCheckTimeout: "+90s", StabilizationTimeout: "1.5m",
			},
			wantErrs: 0,
		},
		{
			name:     "a nanosecond is above zero",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{UpgradeTimeout: "1ns"}, wantErrs: 0,
		},
		{
			name: "maxUnavailableDuringUpgrade = -1",
			strategy: &neo4jv1beta1.UpgradeStrategySpec{
				MaxUnavailableDuringUpgrade: &negOne,
			},
			wantErrs: 1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					UpgradeStrategy:        tc.strategy,
				},
			}
			errs := v.ValidateUpgradeStrategy(cluster)
			if len(errs) != tc.wantErrs {
				t.Errorf("ValidateUpgradeStrategy(%q): expected %d errors, got %d: %v",
					tc.name, tc.wantErrs, len(errs), errs)
			}
		})
	}
}
