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

// Package crd carries the generated CustomResourceDefinitions of the
// self-managed kinds (bases/neo4j.neo4j.com_neo4j*.yaml) into a binary, so a
// tool built from this module reads the schema the API server applies without
// a cluster. `kubectl neo4j validate` fills in the schema's defaults before it
// judges a manifest, as the API server does before the operator reads one
// (docs/knowledge/operations.md id 150).
//
// The YAML is generated (`make manifests`); this file is not, and only names
// it.
package crd

import "embed"

// Bases holds config/crd/bases/neo4j.neo4j.com_neo4j*.yaml, the CRDs of every
// kind with an operator-side validator. The Aura kinds have none.
//
//go:embed bases/neo4j.neo4j.com_neo4j*.yaml
var Bases embed.FS
