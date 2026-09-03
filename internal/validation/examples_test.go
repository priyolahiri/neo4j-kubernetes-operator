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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Every manifest we publish must pass the validators we ship.
//
// Three did not, and nothing caught it: examples/plugins used a
// securityPolicy value outside the supported set, an advanced sharding example
// asked for more heap and page cache than its own container limit allowed, and
// six Neo4jBackup documents under examples/end-to-end declared s3 storage with
// no cloud.provider. All of them were ACCEPTED by `kubectl apply
// --dry-run=server` — the CRD schema cannot see any of it — so a user
// following the docs would have applied them and watched the CR go Failed.
//
// This runs the offline validators over the two directories users copy from.
// Only kinds with offline validators are checked; the rest have nothing to
// assert here (see the skip taxonomy in `kubectl neo4j validate`).
func TestPublishedManifestsPassOurOwnValidators(t *testing.T) {
	roots := []string{"../../examples", "../../config/samples"}

	for _, root := range roots {
		require.DirExists(t, root)
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			if ext := filepath.Ext(path); ext != ".yaml" && ext != ".yml" {
				return nil
			}
			t.Run(strings.TrimPrefix(path, "../../"), func(t *testing.T) {
				for i, doc := range splitYAMLDocuments(t, path) {
					if errs := validateExampleDoc(t, doc); len(errs) > 0 {
						assert.Empty(t, errs,
							"document %d of %s fails the operator's own validators", i+1, path)
					}
				}
			})
			return nil
		})
		require.NoError(t, err)
	}
}

func splitYAMLDocuments(t *testing.T, path string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var docs [][]byte
	for _, doc := range strings.Split(string(raw), "\n---") {
		if strings.TrimSpace(stripComments(doc)) == "" {
			continue
		}
		docs = append(docs, []byte(doc))
	}
	return docs
}

func stripComments(doc string) string {
	var b strings.Builder
	for _, line := range strings.Split(doc, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// validateExampleDoc runs the validator for the document's Kind, if there is an
// offline one. Kinds whose validation needs a live cluster are skipped: this
// test is about manifests being self-consistent, not about references
// resolving.
func validateExampleDoc(t *testing.T, doc []byte) []string {
	t.Helper()

	var meta struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
	}
	if err := yaml.Unmarshal(doc, &meta); err != nil {
		return nil // not a Kubernetes object; other tooling covers those
	}
	if !strings.HasPrefix(meta.APIVersion, "neo4j.neo4j.com/") {
		return nil
	}

	var messages []string
	switch meta.Kind {
	case "Neo4jEnterpriseCluster":
		obj := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := yaml.Unmarshal(doc, obj); err != nil {
			return []string{"cannot decode: " + err.Error()}
		}
		for _, e := range NewClusterValidator(nil).ValidateCreateWithWarnings(context.Background(), obj).Errors {
			messages = append(messages, e.Error())
		}
	case "Neo4jEnterpriseStandalone":
		obj := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		if err := yaml.Unmarshal(doc, obj); err != nil {
			return []string{"cannot decode: " + err.Error()}
		}
		for _, e := range NewStandaloneValidator().ValidateCreate(obj) {
			messages = append(messages, e.Error())
		}
	case "Neo4jBackup":
		obj := &neo4jv1beta1.Neo4jBackup{}
		if err := yaml.Unmarshal(doc, obj); err != nil {
			return []string{"cannot decode: " + err.Error()}
		}
		for _, e := range NewBackupValidator().Validate(obj) {
			messages = append(messages, e.Error())
		}
	case "Neo4jPlugin":
		obj := &neo4jv1beta1.Neo4jPlugin{}
		if err := yaml.Unmarshal(doc, obj); err != nil {
			return []string{"cannot decode: " + err.Error()}
		}
		for _, e := range NewPluginValidator().Validate(obj).Errors {
			messages = append(messages, e.Error())
		}
	}
	return messages
}
