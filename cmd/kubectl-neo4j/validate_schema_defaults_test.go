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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A standalone's `tls: {}` is stored by the API server as mode cert-manager,
// and the operator then requires an issuerRef. validate judged the manifest
// without the CRD's defaults, read no mode, and passed it (#540). It now
// reports what the operator reports.
func TestValidate_AStandaloneTLSBlockWithoutAModeIsCertManager(t *testing.T) {
	errs := cliErrors(t, runningStandaloneManifest+`  tls: {}
`)
	require.Len(t, errs, 1)
	assert.True(t, strings.HasPrefix(errs[0], "spec.tls.issuerRef: Required value"), errs[0])
}

// The defaults reach every kind, nested blocks and all, as the API server
// stores them.
func TestWithSchemaDefaults_FillsWhatTheAPIServerStores(t *testing.T) {
	decode := func(t *testing.T, doc string, into any) {
		t.Helper()
		var meta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(doc), &meta))
		require.NoError(t, yaml.Unmarshal(withSchemaDefaults(meta.Kind, meta.APIVersion, []byte(doc)), into))
	}

	t.Run("standalone", func(t *testing.T) {
		var sa neo4jv1beta1.Neo4jEnterpriseStandalone
		decode(t, runningStandaloneManifest+`  tls:
    issuerRef: {name: ca}
  upgradeStrategy: {strategy: Recreate}
`, &sa)
		assert.Equal(t, "cert-manager", sa.Spec.TLS.Mode)
		require.NotNil(t, sa.Spec.TLS.StrictPeerValidation)
		assert.True(t, *sa.Spec.TLS.StrictPeerValidation)
		assert.Equal(t, "ClusterIssuer", sa.Spec.TLS.IssuerRef.Kind, "a default inside a nested block")
		assert.True(t, sa.Spec.UpgradeStrategy.PostUpgradeHealthCheck, "a bool the CRD defaults to true")
		assert.Equal(t, "30m", sa.Spec.UpgradeStrategy.UpgradeTimeout)
		assert.Equal(t, "Recreate", sa.Spec.UpgradeStrategy.Strategy, "a value the user wrote is kept")
		assert.Nil(t, sa.Spec.Service, "an absent block without a default stays absent")
	})

	t.Run("role", func(t *testing.T) {
		var role neo4jv1beta1.Neo4jRole
		decode(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRole
metadata: {name: analyst, namespace: neo4j}
spec: {clusterRef: prod, name: analyst}
`, &role)
		assert.True(t, role.Spec.EnforcePrivileges)
		assert.Equal(t, "Delete", role.Spec.DeletionPolicy)
	})

	t.Run("a value the user set to false stays false", func(t *testing.T) {
		var role neo4jv1beta1.Neo4jRole
		decode(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRole
metadata: {name: analyst, namespace: neo4j}
spec: {clusterRef: prod, name: analyst, enforcePrivileges: false}
`, &role)
		assert.False(t, role.Spec.EnforcePrivileges)
	})

	t.Run("a kind without an embedded CRD is left as it is", func(t *testing.T) {
		doc := []byte("apiVersion: v1\nkind: ConfigMap\n")
		assert.Equal(t, doc, withSchemaDefaults("ConfigMap", "v1", doc))
	})
}

// The walk is the API server's (structural defaulting): absent or disallowed
// null properties take a fresh copy of the default, defaults inside a
// defaulted block are applied too, and arrays and maps are walked element by
// element.
func TestApplyDefaults_Structural(t *testing.T) {
	schema := &schemaNode{}
	require.NoError(t, json.Unmarshal([]byte(`{
	  "properties": {
	    "block": {"default": {}, "properties": {"mode": {"default": "on"}}},
	    "nullable": {"default": "x", "nullable": true},
	    "notNullable": {"default": "y"},
	    "list": {"items": {"properties": {"kind": {"default": "item"}}}},
	    "byName": {"additionalProperties": {"properties": {"port": {"default": 7687}}}}
	  }
	}`), schema))

	obj := map[string]any{
		"nullable":    nil,
		"notNullable": nil,
		"list":        []any{map[string]any{}, map[string]any{"kind": "mine"}},
		"byName":      map[string]any{"a": map[string]any{}},
	}
	applyDefaults(obj, schema)
	out, err := json.Marshal(obj)
	require.NoError(t, err)
	assert.JSONEq(t, `{
	  "block": {"mode": "on"},
	  "nullable": null,
	  "notNullable": "y",
	  "list": [{"kind": "item"}, {"kind": "mine"}],
	  "byName": {"a": {"port": 7687}}
	}`, string(out))

	// Two absent blocks never share one default map.
	other := map[string]any{}
	applyDefaults(other, schema)
	otherBlock, ok := other["block"].(map[string]any)
	require.True(t, ok)
	otherBlock["mode"] = "changed"
	block, ok := obj["block"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "on", block["mode"])
}
