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
	resolvedTo := func(us *neo4jv1beta1.UpgradeStatus, version string) *neo4jv1beta1.UpgradeStatus {
		us.ResolvedTargetVersion = version
		return us
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
		// A floating target says no patch, so a patch of its line is ordered
		// only against what a restarted server reports (operations.md id 153);
		// with nothing recorded it is not checked.
		{"a floating target and a patch of its line, nothing recorded", upgrade("Paused", restarted, "2026.10-enterprise"), "2026.10.0-enterprise", false},
		{"a floating target and a patch of its line below what it resolved to", resolvedTo(upgrade("Paused", restarted, "2026.10-enterprise"), "2026.10.2"), "2026.10.0-enterprise", true},
		{"a floating target and the patch it resolved to", resolvedTo(upgrade("Paused", restarted, "2026.10-enterprise"), "2026.10.2"), "2026.10.2-enterprise", false},
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

// The v1.22.0 journey (2026-10-17): a cluster on 5.26.31-enterprise was moved
// to 5.26-enterprise, which floated to 5.26.32 that day. The upgrade paused
// after server-2 restarted on 5.26.32, the tag was set back to
// 5.26.31-enterprise, and it was accepted: a patch is not ordered against a
// tag without one, so the operator rolled server-2 down to 5.26.31. What the
// restarted server reports is recorded (status.upgradeStatus.
// resolvedTargetVersion), and the tag is held to it (operations.md id 153).
func TestValidateWithWarnings_ChecksTheTagAgainstWhatAFloatingTargetResolvedTo(t *testing.T) {
	const (
		running  = "5.26.31-enterprise" // status.version
		floating = "5.26-enterprise"    // the upgrade's target
		resolved = "5.26.32"            // what server-2 reports since it restarted on it
	)
	held := func(phase, resolvedTo string, progress *neo4jv1beta1.UpgradeProgress) *neo4jv1beta1.UpgradeStatus {
		return &neo4jv1beta1.UpgradeStatus{Phase: phase, PreviousVersion: running, TargetVersion: floating,
			ResolvedTargetVersion: resolvedTo, Progress: progress}
	}
	restarted := &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2}
	none := &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}

	cases := []struct {
		name      string
		upgrade   *neo4jv1beta1.UpgradeStatus
		tag       string
		wantError bool
	}{
		{"Paused: back to the patch the servers ran (the incident)", held("Paused", resolved, restarted), running, true},
		{"Failed: back to the patch the servers ran", held("Failed", resolved, restarted), running, true},
		{"Rolling: a new target back at the patch the servers ran", held("Rolling", resolved, restarted), running, true},
		{"Paused: the patch it resolved to", held("Paused", resolved, restarted), "5.26.32-enterprise", false},
		{"Paused: a later patch", held("Paused", resolved, restarted), "5.26.33-enterprise", false},
		{"Paused: the floating target itself", held("Paused", resolved, restarted), floating, false},
		{"Paused: a later line", held("Paused", resolved, restarted), "2025.10.0-enterprise", false},
		{"Paused before any server restarted: back to status.version", held("Paused", resolved, none), running, false},
		{"nothing recorded yet: not checked, as before", held("Paused", "", restarted), running, false},
		{"Completed: status.runningVersion's rule, not this one", held("Completed", resolved, &neo4jv1beta1.UpgradeProgress{Upgraded: 3}), floating, false},
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
			assert.Contains(t, errs[0].Detail, "downgrades are not supported: the upgrade to "+floating+
				" (status.upgradeStatus.targetVersion) has already restarted servers on it, and they run "+resolved+
				" (status.upgradeStatus.resolvedTargetVersion)")
			assert.Contains(t, errs[0].Detail, "Set the tag to "+resolved+" or later")
		})
	}
}

// After an upgrade to a tag without a patch completes, status.version names no
// patch either, so status.version alone let a patch of its line through — and
// rolled every server down. status.runningVersion records what the servers
// report, and the tag is held to it (operations.md id 153).
func TestValidateWithWarnings_ChecksTheTagAgainstTheVersionTheServersReport(t *testing.T) {
	const (
		floating = "5.26-enterprise" // status.version
		reported = "5.26.32"         // status.runningVersion
	)
	cases := []struct {
		name      string
		running   string
		tag       string
		wantError bool
	}{
		{"a patch below what they run", reported, "5.26.31-enterprise", true},
		{"an earlier line", reported, "5.25.0-enterprise", true},
		{"the patch they run", reported, "5.26.32-enterprise", false},
		{"a later patch", reported, "5.26.33-enterprise", false},
		{"the floating tag", reported, floating, false},
		{"CalVer", reported, "2025.10.0-enterprise", false},
		{"nothing read yet: not checked, as before", "", "5.26.31-enterprise", false},
	}
	validator := NewClusterValidator(nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := validClusterForWarnings()
			cluster.Spec.Image.Tag = tc.tag
			cluster.Status.Version = floating
			cluster.Status.RunningVersion = tc.running
			cluster.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: "Completed", TargetVersion: floating,
				PreviousVersion: "5.26.31-enterprise", Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3}}

			errs := validator.ValidateWithWarnings(context.Background(), cluster).Errors
			if !tc.wantError {
				assert.Empty(t, errs)
				return
			}
			require.NotEmpty(t, errs)
			assert.Equal(t, "spec.image.tag", errs[0].Field)
			if tc.tag == "5.25.0-enterprise" {
				return // refused by status.version's rule first: 5.25 is an earlier line
			}
			require.Len(t, errs, 1, "%v", errs)
			assert.Contains(t, errs[0].Detail, "downgrades are not supported: the servers run "+reported+
				" (status.runningVersion, what "+floating+" in status.version resolved to)")
			assert.Contains(t, errs[0].Detail, "Set the tag to "+reported+" or later")
		})
	}
}
