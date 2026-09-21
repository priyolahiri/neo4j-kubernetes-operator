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
	"testing"

	"github.com/stretchr/testify/assert"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The real typos that got through during the v1.16.0 verification journey.
// Each passed `validate` with "0 error(s)" and was then refused by the API
// server with a strict-decoding error.
func TestUnknownFieldPaths_TheTyposThatGotThrough(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		obj  any
		want []string
	}{
		{
			name: "spec.auth.secretRef (meant adminSecret)",
			doc: `
spec:
  auth:
    authenticationProviders: ["native"]
    secretRef: {name: s}
`,
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
			want: []string{"spec.auth.secretRef"},
		},
		{
			name: "spec.authentication (meant auth) — shipped in a published example",
			doc:  "spec:\n  authentication:\n    passwordSecretRef: {name: s}\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseCluster{},
			want: []string{"spec.authentication"},
		},
		{
			name: "spec.backupRef on a restore (it lives under source)",
			doc:  "spec:\n  instanceRef: c\n  backupRef: b\n",
			obj:  &neo4jv1beta1.Neo4jRestore{},
			want: []string{"spec.backupRef"},
		},
		{
			// The pinned "every error, not the first" contract applies here
			// too: one run must name all of them.
			name: "several at once, each with its full path, sorted",
			doc:  "spec:\n  nope: 1\n  auth: {alsoNope: 2}\n  image: {wrong: x}\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
			want: []string{"spec.auth.alsoNope", "spec.image.wrong", "spec.nope"},
		},
		{
			name: "a top-level unknown key",
			doc:  "spec: {}\nspecc: {}\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
			want: []string{"specc"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, unknownFieldPaths([]byte(tc.doc), tc.obj))
		})
	}
}

// False positives are worse than the gap: a check that flags legitimate
// manifests gets switched off. Every one of these is a real shape from the
// repo's own examples.
func TestUnknownFieldPaths_NoFalsePositives(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		obj  any
	}{
		{
			name: "apiVersion/kind come from the embedded TypeMeta",
			doc:  "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jEnterpriseStandalone\nspec: {}\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			// ObjectMeta is Kubernetes', not ours, and reflecting into it
			// would report perfectly legal keys.
			name: "metadata is not walked",
			doc:  "metadata:\n  name: x\n  labels: {team: db}\n  annotations: {a: b}\nspec: {}\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			// spec.config is map[string]string: every key is user data.
			name: "an open map accepts any key",
			doc:  "spec:\n  config:\n    server.memory.heap.max_size: 2G\n    dbms.security.auth_enabled: \"true\"\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			name: "a composite's remote driverSettings are an open map too",
			doc: `
spec:
  clusterRef: c
  constituents:
    - name: partner
      targetDatabase: movies
      remote:
        url: neo4j+s://x:7687
        driverSettings: {connection_timeout: 30s}
`,
			obj: &neo4jv1beta1.Neo4jCompositeDatabase{},
		},
		{
			name: "status is not walked",
			doc:  "spec: {}\nstatus:\n  phase: Ready\n  anythingAtAll: 1\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			name: "an empty document says nothing",
			doc:  "",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			name: "undecodable YAML is left to the caller's own decode",
			doc:  "spec: [this is: not: a map\n",
			obj:  &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Empty(t, unknownFieldPaths([]byte(tc.doc), tc.obj))
		})
	}
}

// A kind that validates but has no type here silently stops being checked for
// typos — the exact gap this was written to close.
func TestKindTypesCoversEveryValidator(t *testing.T) {
	for kind := range validators {
		newObj, ok := kindTypes[kind]
		assert.True(t, ok, "kind %q validates but has no kindTypes entry, so typos in it are invisible", kind)
		if ok {
			assert.NotNil(t, newObj(), "kindTypes[%q] must build an object", kind)
		}
	}
	for kind := range kindTypes {
		_, ok := validators[kind]
		assert.True(t, ok, "kindTypes has %q, which is not a validated kind", kind)
	}
}

// A kind whose validator needs a cluster is skipped offline — but the
// unknown-field check reads only the document and the Go type, so it must run
// anyway. It used to sit after the skip, which left the six cross-referencing
// kinds with no typo check at all: a bad `spec.propertyShards` on a
// Neo4jShardedDatabase passed `validate` and was then refused by the API
// server during the v1.16.0 journey.
func TestUnknownFieldsAreCheckedOnKindsThatSkipOffline(t *testing.T) {
	skipOffline := []string{}
	for kind, kv := range validators {
		if kv.needsClient {
			skipOffline = append(skipOffline, kind)
		}
	}
	assert.NotEmpty(t, skipOffline, "the premise of this test is that some kinds skip offline")

	for _, kind := range skipOffline {
		newObj, ok := kindTypes[kind]
		assert.True(t, ok, "%s skips offline and has no type, so typos in it are invisible", kind)
		if !ok {
			continue
		}
		doc := []byte("spec:\n  definitelyNotAField: 1\n")
		assert.Equal(t, []string{"spec.definitelyNotAField"}, unknownFieldPaths(doc, newObj()),
			"%s must still be typo-checked while its cross-reference rules are skipped", kind)
	}
}
