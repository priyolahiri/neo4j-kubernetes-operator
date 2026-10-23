package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The v1.22.0 journey (2026-10-17) set a cluster paused on its upgrade to
// 5.26-enterprise (then 5.26.32) back to 5.26.31-enterprise; it was accepted,
// and `validate --connect` said ok too. A tag without a patch says no version,
// so what the servers report is recorded in status, and --connect reads it from
// the live cluster as the operator does (operations.md id 153).
func TestValidate_Connected_ChecksTheTagAgainstWhatTheServersReport(t *testing.T) {
	manifest := func(tag string) string {
		return strings.Replace(runningClusterManifest, `tag: "2025.10.0-enterprise"`, `tag: "`+tag+`"`, 1)
	}

	t.Run("the incident: a paused upgrade whose floating target resolved to a later patch", func(t *testing.T) {
		live := runningCluster("5.26.31-enterprise")
		live.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
			Phase: "Paused", PreviousVersion: "5.26.31-enterprise", TargetVersion: "5.26-enterprise",
			ResolvedTargetVersion: "5.26.32",
			Progress:              &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
		}
		results, err := validateSource(writeManifest(t, manifest("5.26.31-enterprise")), testClient(t, live), "")
		require.NoError(t, err)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Equal(t, "spec.image.tag", results[0].findings[0].path)
		assert.Contains(t, results[0].findings[0].detail, "they run 5.26.32 (status.upgradeStatus.resolvedTargetVersion)")
	})

	t.Run("after the upgrade completed on the floating tag", func(t *testing.T) {
		live := runningCluster("5.26-enterprise")
		live.Status.RunningVersion = "5.26.32"

		results, err := validateSource(writeManifest(t, manifest("5.26.31-enterprise")), testClient(t, live), "")
		require.NoError(t, err)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Contains(t, results[0].findings[0].detail, "the servers run 5.26.32 (status.runningVersion")

		results, err = validateSource(writeManifest(t, manifest("5.26.32-enterprise")), testClient(t, live), "")
		require.NoError(t, err)
		assert.Empty(t, results[0].findings, "the patch the servers run")
	})

	t.Run("a standalone", func(t *testing.T) {
		live := runningStandalone("5.26-enterprise")
		live.Status.RunningVersion = "5.26.32"
		standalone := strings.Replace(runningStandaloneManifest, `tag: "2025.10.0-enterprise"`, `tag: "5.26.31-enterprise"`, 1)
		results, err := validateSource(writeManifest(t, standalone), testClient(t, live), "")
		require.NoError(t, err)
		require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
		assert.Contains(t, results[0].findings[0].detail, "the standalone runs 5.26.32 (status.runningVersion")
	})
}
