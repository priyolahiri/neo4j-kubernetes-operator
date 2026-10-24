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
