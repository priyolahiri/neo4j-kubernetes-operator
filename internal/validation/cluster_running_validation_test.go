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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The cluster's update-only rules compared the cluster with itself: the
// reconciler read the same object twice and passed both as "old" and "new"
// (operations.md id 139). The rules that need the running cluster now read it
// from status.version, which the operator stamps once it has verified every
// server runs that version.
func TestValidateWithWarnings_ChecksTheTagAgainstStatusVersion(t *testing.T) {
	validator := NewClusterValidator(nil)
	ctx := context.Background()

	cases := []struct {
		name      string
		running   string // status.version
		tag       string // spec.image.tag
		wantError bool
	}{
		{"never verified: nothing to compare", "", "2025.10.0-enterprise", false},
		{"unchanged", "2026.08.1-enterprise", "2026.08.1-enterprise", false},
		{"an upgrade", "2026.08.1-enterprise", "2026.09.0-enterprise", false},
		{"LTS to CalVer", "5.26.12-enterprise", "2026.08.1-enterprise", false},
		{"onto the floating tag of the same line", "5.26.12-enterprise", "5.26-enterprise", false},
		{"a CalVer downgrade", "2026.08.1-enterprise", "2025.10.0-enterprise", true},
		{"CalVer back to the LTS", "2026.08.1-enterprise", "5.26.12-enterprise", true},
		{"a patch downgrade", "5.26.12-enterprise", "5.26.10-enterprise", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := validClusterForWarnings()
			cluster.Spec.Image.Tag = tc.tag
			cluster.Status.Version = tc.running

			errs := validator.ValidateWithWarnings(ctx, cluster).Errors
			if !tc.wantError {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1, "%v", errs)
			assert.Equal(t, "spec.image.tag", errs[0].Field)
			assert.Contains(t, errs[0].Detail, "downgrades are not supported")
			assert.Contains(t, errs[0].Detail, tc.running+" (status.version)",
				"the refusal must say what the servers run, which is not in the spec the user edited")

			// The same spec on a cluster that does not run yet is accepted:
			// the rule needs the running version, and the spec rules alone
			// have nothing against it.
			assert.Empty(t, validator.ValidateCreateWithWarnings(ctx, cluster).Errors)
		})
	}

	// A tag that does not parse is the image rule's to report, once.
	t.Run("an unparseable tag is reported once", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.Image.Tag = "latest"
		cluster.Status.Version = "2026.08.1-enterprise"
		errs := validator.ValidateWithWarnings(ctx, cluster).Errors
		require.Len(t, errs, 1, "%v", errs)
		assert.Equal(t, "spec.image.tag", errs[0].Field)
		assert.NotContains(t, errs[0].Detail, "invalid version format")
	})
}

// The server count has no rule that needs the running cluster: at least two
// servers is a spec rule, and a scale-down is the drain's. A scale-down to two
// or more must not be refused by validation.
func TestValidateWithWarnings_LeavesTheServerCountToTheSpecRules(t *testing.T) {
	validator := NewClusterValidator(nil)
	cluster := validClusterForWarnings()
	cluster.Status.Version = cluster.Spec.Image.Tag
	cluster.Status.Replicas = &neo4jv1beta1.ReplicaStatus{Servers: 5}
	cluster.Spec.Topology.Servers = 3
	assert.Empty(t, validator.ValidateWithWarnings(context.Background(), cluster).Errors)

	cluster.Spec.Topology.Servers = 1
	errs := validator.ValidateWithWarnings(context.Background(), cluster).Errors
	require.Len(t, errs, 1, "%v", errs)
	assert.Equal(t, "spec.topology.servers", errs[0].Field)
	assert.True(t, strings.Contains(errs[0].Detail, "at least 2"), errs[0].Detail)
}

// spec.upgradeStrategy needs only the spec. It ran only on the update path,
// which the operator took on every pass and `kubectl neo4j validate` never
// did, so the CLI passed a timeout the operator then refused.
func TestValidateCreateWithWarnings_ChecksTheUpgradeStrategy(t *testing.T) {
	validator := NewClusterValidator(nil)
	for _, path := range []string{"upgradeTimeout", "healthCheckTimeout", "stabilizationTimeout"} {
		t.Run(path, func(t *testing.T) {
			cluster := validClusterForWarnings()
			strategy := &neo4jv1beta1.UpgradeStrategySpec{}
			switch path {
			case "upgradeTimeout":
				strategy.UpgradeTimeout = "5 minutes"
			case "healthCheckTimeout":
				strategy.HealthCheckTimeout = "5 minutes"
			case "stabilizationTimeout":
				strategy.StabilizationTimeout = "5 minutes"
			}
			cluster.Spec.UpgradeStrategy = strategy

			errs := validator.ValidateCreateWithWarnings(context.Background(), cluster).Errors
			require.Len(t, errs, 1, "%v", errs)
			assert.Equal(t, field.NewPath("spec", "upgradeStrategy", path).String(), errs[0].Field)
		})
	}

	t.Run("Go durations pass", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{
			UpgradeTimeout: "1h30m", HealthCheckTimeout: "5m", StabilizationTimeout: "90s",
		}
		assert.Empty(t, validator.ValidateCreateWithWarnings(context.Background(), cluster).Errors)
	})
}
