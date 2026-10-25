package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// The published Argo CD health checks judge a self-managed resource by the
// same phases this CLI does: every phase `status --problems` reports is
// Degraded, on every kind. A Neo4jDatabase in ValidationFailed was Progressing,
// so Argo CD showed its Application still syncing and a later sync wave waited
// for ever (#528). The Aura kinds key off their Ready condition and are left
// out: their phase is Aura's own, open vocabulary.
func TestArgoCDHealthChecks_EveryProblemPhaseIsDegraded(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "gitops", "argocd-health-checks.yaml"))
	require.NoError(t, err)
	var cm struct {
		Data map[string]string `json:"data"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &cm))

	const prefix = "resource.customizations.health.neo4j.neo4j.com_"
	phaseLiteral := regexp.MustCompile(`phase == "([A-Za-z]+)"`)
	checked := 0
	for key, lua := range cm.Data {
		kind := strings.TrimPrefix(key, prefix)
		if kind == key || strings.HasPrefix(kind, "Aura") {
			continue
		}
		checked++
		lines := strings.Split(lua, "\n")
		degraded := map[string]bool{}
		for i, line := range lines {
			if strings.TrimSpace(line) != `hs.status = "Degraded"` {
				continue
			}
			// The condition is the nearest if/elseif above, past comments.
			for j := i - 1; j >= 0; j-- {
				cond := strings.TrimSpace(lines[j])
				if strings.HasPrefix(cond, "--") {
					continue
				}
				for _, m := range phaseLiteral.FindAllStringSubmatch(cond, -1) {
					degraded[m[1]] = true
				}
				break
			}
		}
		for _, phase := range problemPhases {
			assert.True(t, degraded[phase], "%s: phase %q is a problem to `kubectl neo4j status` but not Degraded to Argo CD", kind, phase)
		}
	}
	assert.Equal(t, 15, checked, "the self-managed CRDs")
}

// A cluster whose upgrade stopped stays Ready (#565), so its health check reads
// status.upgradeStatus.phase within the Ready branch: a Failed upgrade is
// Degraded and a Paused one Suspended, both with the cluster's message, which
// names the stopped upgrade. Keyed on status.phase alone it reported both
// Healthy, and a GitOps user lost the only sign that the image change did not
// land (v1.22.0 reported a failed upgrade's cluster as Failed, so Degraded).
// Read by conditions, as the guard above reads the problem phases.
func TestArgoCDHealthChecks_AStoppedUpgradeIsNotHealthy(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "gitops", "argocd-health-checks.yaml"))
	require.NoError(t, err)
	var cm struct {
		Data map[string]string `json:"data"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &cm))
	lua := cm.Data["resource.customizations.health.neo4j.neo4j.com_Neo4jEnterpriseCluster"]
	require.NotEmpty(t, lua)
	lines := strings.Split(lua, "\n")
	trimmed := make([]string, len(lines))
	for i, l := range lines {
		trimmed[i] = strings.TrimSpace(l)
	}

	// upgrade is read from status.upgradeStatus.phase, guarded for a cluster
	// without an upgrade record.
	assert.Contains(t, trimmed, "if obj.status.upgradeStatus ~= nil then")
	assert.Contains(t, trimmed, "upgrade = obj.status.upgradeStatus.phase")

	// The Ready branch: from `if phase == "Ready" then` to the next branch on
	// phase.
	start, end := -1, len(trimmed)
	for i, l := range trimmed {
		if l == `if phase == "Ready" then` {
			start = i
		} else if start >= 0 && strings.HasPrefix(l, "elseif phase ==") {
			end = i
			break
		}
	}
	require.GreaterOrEqual(t, start, 0, "no Ready branch")
	ready := trimmed[start:end]

	// Each health state's nearest condition inside the Ready branch, and the
	// message set with it.
	stateUnder := func(state string) (cond, message string) {
		for i, l := range ready {
			if l != `hs.status = "`+state+`"` {
				continue
			}
			for j := i - 1; j >= 0; j-- {
				if !strings.HasPrefix(ready[j], "--") {
					cond = ready[j]
					break
				}
			}
			if i+1 < len(ready) {
				message = ready[i+1]
			}
			return cond, message
		}
		return "", ""
	}
	for upgrade, state := range map[string]string{"Failed": "Degraded", "Paused": "Suspended"} {
		cond, message := stateUnder(state)
		assert.Contains(t, cond, `upgrade == "`+upgrade+`"`, "a %s upgrade on a Ready cluster is %s", upgrade, state)
		assert.True(t, strings.HasPrefix(message, "hs.message = obj.status.message"), "%s: the cluster's message names the stopped upgrade", state)
	}
	cond, _ := stateUnder("Healthy")
	assert.Equal(t, "else", cond, "Healthy only when no upgrade stopped")
}
