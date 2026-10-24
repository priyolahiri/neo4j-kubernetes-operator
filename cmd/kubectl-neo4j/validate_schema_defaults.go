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

// Schema defaults.
//
// The API server fills in every +kubebuilder:default the CRD declares before
// it stores an object, and the operator validates the stored object. This
// command validates the manifest, so without the same defaults it judged a
// different object: a standalone's `tls: {}` had no mode here, where the API
// server stores `mode: cert-manager` and the operator then requires an
// issuerRef; a `postUpgradeHealthCheck` left out read as false. The cluster
// had its own ApplyDefaults, which covers four of its schema defaults; no
// other kind had anything (#540).
//
// So the defaults come from the CRD itself, embedded in the binary from
// config/crd/bases, and are applied the way the API server applies them
// (k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting): for each
// object, every property with a default that is absent, or null where the
// schema does not allow null, gets a copy of the default; then each value is
// walked against its own schema, defaults included, so a defaulted block gets
// its own defaults. Arrays are walked item by item, maps value by value. That
// package is the API server's, and this module does not depend on it; the
// algorithm is short enough to carry here, and TestApplyDefaults_Structural
// pins it (operations.md id 150).

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	"github.com/priyolahiri/neo4j-kubernetes-operator/config/crd"
)

// schemaNode is the part of a structural OpenAPI v3 schema defaulting reads.
type schemaNode struct {
	Properties map[string]*schemaNode `json:"properties,omitempty"`
	Items      *schemaNode            `json:"items,omitempty"`
	// AdditionalProperties is a schema, or a bool this walk ignores.
	AdditionalProperties json.RawMessage `json:"additionalProperties,omitempty"`
	Default              json.RawMessage `json:"default,omitempty"`
	Nullable             bool            `json:"nullable,omitempty"`
}

// additional is the schema of a map's values, or nil.
func (s *schemaNode) additional() *schemaNode {
	if s == nil || len(s.AdditionalProperties) == 0 || s.AdditionalProperties[0] != '{' {
		return nil
	}
	var node schemaNode
	if err := json.Unmarshal(s.AdditionalProperties, &node); err != nil {
		return nil
	}
	return &node
}

type crdDocument struct {
	Spec struct {
		Names struct {
			Kind string `json:"kind"`
		} `json:"names"`
		Versions []struct {
			Name    string `json:"name"`
			Storage bool   `json:"storage"`
			Schema  struct {
				OpenAPIV3Schema *schemaNode `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

var (
	schemasOnce sync.Once
	// schemas maps kind, then API version name, to the version's schema.
	schemas map[string]map[string]*schemaNode
)

// loadSchemas reads the embedded CRDs once. A file that does not parse is left
// out: its kind is then validated without defaults, as it was before.
func loadSchemas() map[string]map[string]*schemaNode {
	schemasOnce.Do(func() {
		schemas = map[string]map[string]*schemaNode{}
		paths, _ := fs.Glob(crd.Bases, "bases/*.yaml") // the pattern is valid
		for _, path := range paths {
			raw, err := fs.ReadFile(crd.Bases, path)
			if err != nil {
				continue
			}
			var doc crdDocument
			if yaml.Unmarshal(raw, &doc) != nil || doc.Spec.Names.Kind == "" {
				continue
			}
			versions := map[string]*schemaNode{}
			for _, v := range doc.Spec.Versions {
				if v.Schema.OpenAPIV3Schema == nil {
					continue
				}
				versions[v.Name] = v.Schema.OpenAPIV3Schema
				if v.Storage {
					versions[""] = v.Schema.OpenAPIV3Schema
				}
			}
			schemas[doc.Spec.Names.Kind] = versions
		}
	})
	return schemas
}

// schemaFor is the schema of kind at apiVersion ("group/version"), or the
// storage version's when the version is not one the CRD serves.
func schemaFor(kind, apiVersion string) *schemaNode {
	versions := loadSchemas()[kind]
	if versions == nil {
		return nil
	}
	if i := strings.LastIndex(apiVersion, "/"); i >= 0 {
		if s := versions[apiVersion[i+1:]]; s != nil {
			return s
		}
	}
	return versions[""]
}

// withSchemaDefaults returns doc with its kind's CRD defaults filled in, as the
// API server would store it, in JSON (which every YAML decoder reads). When the
// kind has no embedded CRD or doc does not decode, doc is returned unchanged:
// its own decode reports what is wrong with it.
func withSchemaDefaults(kind, apiVersion string, doc []byte) []byte {
	schema := schemaFor(kind, apiVersion)
	if schema == nil {
		return doc
	}
	asJSON, err := yaml.YAMLToJSON(doc)
	if err != nil {
		return doc
	}
	dec := json.NewDecoder(bytes.NewReader(asJSON))
	dec.UseNumber() // integers stay exact through the round trip
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return doc
	}
	applyDefaults(obj, schema)
	out, err := json.Marshal(obj)
	if err != nil {
		return doc
	}
	return out
}

// applyDefaults is the API server's structural defaulting of x against s.
func applyDefaults(x any, s *schemaNode) {
	if s == nil {
		return
	}
	switch x := x.(type) {
	case map[string]any:
		for name, prop := range s.Properties {
			if prop == nil || len(prop.Default) == 0 {
				continue
			}
			if v, found := x[name]; !found || (v == nil && !prop.Nullable) {
				if value, ok := decodeDefault(prop.Default); ok {
					x[name] = value
				}
			}
		}
		additional := s.additional()
		for name, value := range x {
			if prop, found := s.Properties[name]; found {
				applyDefaults(value, prop)
				continue
			}
			if additional != nil {
				if value == nil && !additional.Nullable && len(additional.Default) > 0 {
					if d, ok := decodeDefault(additional.Default); ok {
						x[name] = d
						value = d
					}
				}
				applyDefaults(value, additional)
			}
		}
	case []any:
		for i := range x {
			if x[i] == nil && s.Items != nil && !s.Items.Nullable && len(s.Items.Default) > 0 {
				if d, ok := decodeDefault(s.Items.Default); ok {
					x[i] = d
				}
			}
			applyDefaults(x[i], s.Items)
		}
	}
}

// decodeDefault is a fresh copy of a schema default: each use decodes its own,
// so no two places in the object share a map or a slice.
func decodeDefault(raw json.RawMessage) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, false
	}
	return value, true
}
