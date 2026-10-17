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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// status.version stays at the version the servers ran before an upgrade until
// the upgrade verifies the new one, so it alone allowed moving the tag back to
// it after an upgrade had rolled servers forward — or, mid-flight, to a target
// between the two. Those servers run the target: going below it rolls them
// back, which Neo4j does not support (operations.md id 149). Before the upgrade
// has restarted any server, going back to status.version is still allowed.
func TestValidateWithWarnings_ChecksTheTagAgainstAnUpgradeThatRestartedAServer(t *testing.T) {
	const (
		running = "2026.08.1-enterprise" // status.version
		target  = "2026.10.0-enterprise" // the upgrade's target
	)
	restarted := &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2}
	none := &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}
	upgrade := func(phase string, progress *neo4jv1beta1.UpgradeProgress, targetVersion string) *neo4jv1beta1.UpgradeStatus {
		return &neo4jv1beta1.UpgradeStatus{Phase: phase, PreviousVersion: running, TargetVersion: targetVersion, Progress: progress}
	}

	cases := []struct {
		name      string
		upgrade   *neo4jv1beta1.UpgradeStatus
		tag       string
		wantError bool
	}{
		{"Paused after restarting a server: back to status.version", upgrade("Paused", restarted, target), running, true},
		{"Failed after restarting a server: back to status.version", upgrade("Failed", restarted, target), running, true},
		{"Rolling: a new target between the two", upgrade("Rolling", restarted, target), "2026.09.0-enterprise", true},
		{"every server rolled, failed verifying", upgrade("Failed", &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3}, target), running, true},
		{"Paused before any server restarted: back to status.version", upgrade("Paused", none, target), running, false},
		{"Staging: back to status.version", upgrade("Staging", none, target), running, false},
		{"Paused after restarting a server: the same target", upgrade("Paused", restarted, target), target, false},
		{"Paused after restarting a server: a later target", upgrade("Paused", restarted, target), "2026.11.0-enterprise", false},
		{"a completed upgrade is status.version's rule", upgrade("Completed", &neo4jv1beta1.UpgradeProgress{Upgraded: 3}, running), running, false},
		{"no progress recorded (an older record)", upgrade("Paused", nil, target), running, false},
		{"a floating target and a patch of its line", upgrade("Paused", restarted, "2026.10-enterprise"), "2026.10.0-enterprise", false},
		{"a floating target and an earlier line", upgrade("Paused", restarted, "2026.10-enterprise"), "2026.09.2-enterprise", true},
	}
	validator := NewClusterValidator(nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := validClusterForWarnings()
			cluster.Spec.Image.Tag = tc.tag
			cluster.Status.Version = running
			cluster.Status.UpgradeStatus = tc.upgrade

			errs := validator.ValidateWithWarnings(context.Background(), cluster).Errors
			if !tc.wantError {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1, "%v", errs)
			assert.Equal(t, "spec.image.tag", errs[0].Field)
			assert.Contains(t, errs[0].Detail, "downgrades are not supported: the upgrade to "+tc.upgrade.TargetVersion+
				" (status.upgradeStatus.targetVersion) has already restarted servers on it")
		})
	}

	// A cluster upgraded before status.version was ever recorded is held to
	// the upgrade's target all the same.
	t.Run("no status.version", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.Image.Tag = running
		cluster.Status.UpgradeStatus = upgrade("Paused", restarted, target)
		errs := validator.ValidateWithWarnings(context.Background(), cluster).Errors
		require.Len(t, errs, 1, "%v", errs)
		assert.Contains(t, errs[0].Detail, "has already restarted servers on it")
	})

	// status.version's own refusal comes first, and is not repeated.
	t.Run("below status.version", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.Image.Tag = "2025.10.0-enterprise"
		cluster.Status.Version = running
		cluster.Status.UpgradeStatus = upgrade("Paused", restarted, target)
		errs := validator.ValidateWithWarnings(context.Background(), cluster).Errors
		require.Len(t, errs, 1, "%v", errs)
		assert.Contains(t, errs[0].Detail, running+" (status.version)")
	})
}
