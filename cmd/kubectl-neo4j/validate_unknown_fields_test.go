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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		{
			// The walker used to stop at every list: only map children were
			// followed, so a typo one level inside any array of structs was
			// invisible and the API server was the first to notice.
			name: "a typo inside a list item (spec.topology.serverRoles[0])",
			doc: `
spec:
  topology:
    servers: 3
    serverRoles:
      - serverIndex: 0
        modeContraint: PRIMARY
`,
			obj:  &neo4jv1beta1.Neo4jEnterpriseCluster{},
			want: []string{"spec.topology.serverRoles[0].modeContraint"},
		},
		{
			// corev1.EnvVar comes from outside this repo, and nests a second
			// struct (valueFrom.secretKeyRef) under the list item.
			name: "typos in a list of a foreign type and in a struct nested under an item",
			doc: `
spec:
  env:
    - name: A
      vlaue: x
    - name: B
      valueFrom:
        secretKeyRef: {name: s, kee: k}
`,
			obj:  &neo4jv1beta1.Neo4jEnterpriseCluster{},
			want: []string{"spec.env[0].vlaue", "spec.env[1].valueFrom.secretKeyRef.kee"},
		},
		{
			name: "every bad item in a list is reported, each with its own index",
			doc: `
spec:
  topology:
    serverRoles:
      - {serverIndex: 0, nope: 1}
      - {serverIndex: 1, modeConstraint: NONE}
      - {serverIndex: 2, alsoNope: 1}
`,
			obj: &neo4jv1beta1.Neo4jEnterpriseCluster{},
			want: []string{
				"spec.topology.serverRoles[0].nope",
				"spec.topology.serverRoles[2].alsoNope",
			},
		},
		{
			name: "a typo in a struct nested under a list item (constituents[].remote)",
			doc: `
spec:
  clusterRef: c
  constituents:
    - name: partner
      remote: {urll: neo4j+s://x:7687}
`,
			obj:  &neo4jv1beta1.Neo4jCompositeDatabase{},
			want: []string{"spec.constituents[0].remote.urll"},
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
			// Walking into lists must not start judging scalars: a []string
			// has no fields to be unknown.
			name: "lists of scalars are left alone",
			doc: `
spec:
  auth:
    authenticationProviders: ["native", "oidc-x"]
`,
			obj: &neo4jv1beta1.Neo4jEnterpriseStandalone{},
		},
		{
			name: "correctly spelled list items, including a foreign type and a nested struct",
			doc: `
spec:
  topology:
    servers: 3
    serverRoles:
      - {serverIndex: 0, modeConstraint: PRIMARY}
      - {serverIndex: 1, modeConstraint: NONE}
  env:
    - name: A
      value: x
    - name: B
      valueFrom:
        secretKeyRef: {name: s, key: k}
`,
			obj: &neo4jv1beta1.Neo4jEnterpriseCluster{},
		},
		{
			// A map INSIDE a list item is still free-form: the item's struct is
			// walked, the map's keys are not.
			name: "an open map inside a list item still accepts any key",
			doc: `
spec:
  clusterRef: c
  constituents:
    - name: partner
      remote:
        url: neo4j+s://x:7687
        driverSettings: {connection_timeout: 30s, some.odd.key: x}
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

// EVERY Neo4j CRD kind must be typo-checked, not just the 13 with
// operator-side validators. "No validator" is not "no spelling": a
// Neo4jReplicaPromotion with spec.replicaDatabaseRef instead of
// spec.replicaRef passed validate and was refused by the API server during the
// v1.16.0 journey.
//
// Deriving the type from the scheme is what makes this hold for CRDs added
// later — a hand-written list would omit them silently.
func TestEveryRegisteredKindIsTypoChecked(t *testing.T) {
	kinds := 0
	for gvk := range neo4jScheme.AllKnownTypes() {
		if gvk.Group != neo4jv1beta1.GroupVersion.Group || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		// Skip the meta types the scheme registers alongside ours.
		if !strings.HasPrefix(gvk.Kind, "Neo4j") && !strings.HasPrefix(gvk.Kind, "Aura") {
			continue
		}
		kinds++
		obj := newObjectForKind(gvk.Kind)
		assert.NotNil(t, obj, "%s is a registered CRD kind with no type lookup", gvk.Kind)
		if obj == nil {
			continue
		}
		doc := []byte("spec:\n  definitelyNotAField: 1\n")
		assert.Equal(t, []string{"spec.definitelyNotAField"}, unknownFieldPaths(doc, obj),
			"%s must be typo-checked", gvk.Kind)
	}
	assert.GreaterOrEqual(t, kinds, 27, "expected every CRD kind to be covered")
}

// Kinds whose validators need a cluster are skipped offline; the typo check
// reads only the document and the type, so it must run anyway.
func TestUnknownFieldsAreCheckedOnKindsThatSkipOffline(t *testing.T) {
	skipOffline := []string{}
	for kind, kv := range validators {
		if kv.needsClient {
			skipOffline = append(skipOffline, kind)
		}
	}
	assert.NotEmpty(t, skipOffline, "the premise of this test is that some kinds skip offline")
	for _, kind := range skipOffline {
		obj := newObjectForKind(kind)
		assert.NotNil(t, obj, "%s skips offline and has no type, so typos in it are invisible", kind)
		if obj == nil {
			continue
		}
		doc := []byte("spec:\n  definitelyNotAField: 1\n")
		assert.Equal(t, []string{"spec.definitelyNotAField"}, unknownFieldPaths(doc, obj),
			"%s must still be typo-checked while its cross-reference rules are skipped", kind)
	}
}

// A kind that is not ours at all must not be claimed.
func TestNewObjectForKind_IgnoresForeignKinds(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Deployment", "NotAKind", ""} {
		assert.Nil(t, newObjectForKind(kind), "%q is not a Neo4j CRD kind", kind)
	}
}

// The strongest false-positive guard there is: take every CRD kind, fill in
// EVERY field reachable through its types (structs, pointers, slices of
// structs, maps), serialise it exactly as the API server would see it, and
// require the walker to find nothing wrong. A document built from the type
// itself cannot contain a misspelling, so any path reported here is the walker
// misreading a legal shape — an inline struct, a custom-marshalled type, a
// free-form map — rather than a typo.
//
// Walking into lists made this a live risk: the number of shapes the walker can
// get wrong went from "struct inside struct" to every slice in every kind.
func TestUnknownFieldPaths_FullyPopulatedKindsHaveNoFalsePositives(t *testing.T) {
	kinds := 0
	for gvk := range neo4jScheme.AllKnownTypes() {
		if gvk.Group != neo4jv1beta1.GroupVersion.Group || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		obj := newObjectForKind(gvk.Kind)
		if obj == nil {
			continue
		}
		kinds++
		populateForTest(reflect.ValueOf(obj).Elem(), map[reflect.Type]bool{})
		doc, err := json.Marshal(obj)
		require.NoError(t, err, gvk.Kind)
		assert.Empty(t, unknownFieldPaths(doc, newObjectForKind(gvk.Kind)),
			"%s: a document filled in from its own type must not report an unknown field", gvk.Kind)
	}
	assert.GreaterOrEqual(t, kinds, 27, "expected every CRD kind to be covered")
}

// The populate helper must really reach into lists, or the test above proves
// nothing about them.
func TestPopulateForTest_ReachesListItems(t *testing.T) {
	var c neo4jv1beta1.Neo4jEnterpriseCluster
	populateForTest(reflect.ValueOf(&c).Elem(), map[reflect.Type]bool{})
	require.NotEmpty(t, c.Spec.Topology.ServerRoles)
	require.NotEmpty(t, c.Spec.Env)
	doc, err := json.Marshal(&c)
	require.NoError(t, err)
	assert.Contains(t, string(doc), `"serverRoles":[{`)
	// ... and a typo planted into that same populated document is found.
	bad := strings.ReplaceAll(string(doc), `"modeConstraint"`, `"modeContraint"`)
	assert.Contains(t, unknownFieldPaths([]byte(bad), &neo4jv1beta1.Neo4jEnterpriseCluster{}),
		"spec.topology.serverRoles[0].modeContraint")
}

var (
	testMarshalerType   = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	testUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
)

// populateForTest sets v (and everything under it) to a non-zero value. Types
// that marshal themselves (Quantity, Time, IntOrString, RawExtension) are left
// at their zero value — their wire shape is theirs, not a struct's. onPath
// stops a recursive type from expanding forever.
func populateForTest(v reflect.Value, onPath map[reflect.Type]bool) {
	t := v.Type()
	if t.Implements(testMarshalerType) || reflect.PointerTo(t).Implements(testMarshalerType) ||
		reflect.PointerTo(t).Implements(testUnmarshalerType) {
		return
	}
	switch t.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(t.Elem()))
		populateForTest(v.Elem(), onPath)
	case reflect.Struct:
		if onPath[t] {
			return
		}
		onPath[t] = true
		defer delete(onPath, t)
		for i := 0; i < t.NumField(); i++ {
			if f := v.Field(i); f.CanSet() {
				populateForTest(f, onPath)
			}
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			v.SetBytes([]byte("x"))
			return
		}
		item := reflect.New(t.Elem()).Elem()
		populateForTest(item, onPath)
		v.Set(reflect.Append(reflect.MakeSlice(t, 0, 1), item))
	case reflect.Map:
		item := reflect.New(t.Elem()).Elem()
		populateForTest(item, onPath)
		m := reflect.MakeMap(t)
		m.SetMapIndex(reflect.ValueOf("some.free-form.key").Convert(t.Key()), item)
		v.Set(m)
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	}
}
