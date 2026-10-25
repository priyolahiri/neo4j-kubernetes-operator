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

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A resource waiting for its deployment — Pending with ClusterNotFound, or a
// backup's Waiting — is Progressing to Argo CD on every self-managed kind, so
// an Application that applies a database or a user with its cluster syncs and
// waits, instead of reading Degraded until the cluster is there (operations.md
// id 182). Before #566 those kinds went ValidationFailed or Failed first,
// which this file's sibling test pins as Degraded.
func TestArgoCDHealthChecks_AWaitIsProgressing(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "gitops", "argocd-health-checks.yaml"))
	require.NoError(t, err)
	var cm struct {
		Data map[string]string `json:"data"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &cm))

	const prefix = "resource.customizations.health.neo4j.neo4j.com_"
	phaseLiteral := regexp.MustCompile(`phase == "([A-Za-z]+)"`)
	waits := []string{neo4jv1beta1.PhasePending, neo4jv1beta1.PhaseWaiting}
	checked := 0
	for key, lua := range cm.Data {
		kind := strings.TrimPrefix(key, prefix)
		if kind == key || strings.HasPrefix(kind, "Aura") {
			continue
		}
		checked++
		lines := strings.Split(lua, "\n")
		decided := map[string]string{} // phase -> the status a condition naming it sets
		elseProgressing := false
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "hs.status = ") {
				continue
			}
			status := strings.Trim(strings.TrimPrefix(trimmed, "hs.status = "), `"`)
			for j := i - 1; j >= 0; j-- {
				cond := strings.TrimSpace(lines[j])
				if strings.HasPrefix(cond, "--") {
					continue
				}
				if cond == "else" && status == "Progressing" {
					elseProgressing = true
				}
				for _, m := range phaseLiteral.FindAllStringSubmatch(cond, -1) {
					decided[m[1]] = status
				}
				break
			}
		}
		for _, phase := range waits {
			if status, ok := decided[phase]; ok {
				assert.Equal(t, "Progressing", status, "%s: phase %q", kind, phase)
				continue
			}
			assert.True(t, elseProgressing, "%s: phase %q falls to a branch that is not Progressing", kind, phase)
		}
	}
	assert.Equal(t, 15, checked, "the self-managed CRDs")
}
