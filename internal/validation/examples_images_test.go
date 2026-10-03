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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// ltsExampleTag is the 5.26 LTS line as CI pulls it: the integration matrix
// runs `5.26-enterprise`, which tracks the latest 5.26 patch.
const ltsExampleTag = "5.26-enterprise"

// The published examples must run the Neo4j versions CI validates: the 5.26
// LTS line as CI pulls it, or the CalVer release the extended suite pins. They
// drifted twice: at v1.14 the sharding examples pinned 2026.04 while CI ran
// 2026.06, and by v1.18 they pinned 2026.06 against 2026.08.1 — while every
// LTS example pinned 5.26.0, the first LTS patch, which is what Getting
// Started hands a new user. When the CI anchor moves, this fails until the
// examples move with it.
func TestPublishedExamplesUseTheImagesCIValidates(t *testing.T) {
	anchor := ciCalVerAnchor(t)
	allowed := map[string]bool{ltsExampleTag: true, anchor: true}

	for _, root := range []string{"../../examples", "../../config/samples"} {
		require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			if ext := filepath.Ext(path); ext != ".yaml" && ext != ".yml" {
				return nil
			}
			for i, doc := range splitYAMLDocuments(t, path) {
				var obj struct {
					Kind string `json:"kind"`
					Spec struct {
						Image struct {
							Tag string `json:"tag"`
						} `json:"image"`
					} `json:"spec"`
				}
				if yaml.Unmarshal(doc, &obj) != nil {
					continue // undecodable documents are TestPublishedManifestsPassOurOwnValidators' concern
				}
				if obj.Kind != "Neo4jEnterpriseCluster" && obj.Kind != "Neo4jEnterpriseStandalone" {
					continue
				}
				tag := obj.Spec.Image.Tag
				assert.True(t, allowed[tag],
					"%s document %d (%s) uses image tag %q; published examples use %q or the CalVer CI anchor %q",
					strings.TrimPrefix(path, "../../"), i+1, obj.Kind, tag, ltsExampleTag, anchor)
			}
			return nil
		}))
	}
}

// ciCalVerAnchor reads the CalVer image the extended suite runs by default.
func ciCalVerAnchor(t *testing.T) string {
	t.Helper()
	wf, err := os.ReadFile("../../.github/workflows/integration-tests.yml")
	require.NoError(t, err)
	m := regexp.MustCompile(`neo4j-version \|\| '([0-9]{4}\.[0-9.]+-enterprise)'`).FindSubmatch(wf)
	require.NotNil(t, m, "could not find the default neo4j-version in integration-tests.yml")
	return string(m[1])
}
