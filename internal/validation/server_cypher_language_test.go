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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateServerCypherLanguage(t *testing.T) {
	cfg := func(m map[string]string) ConfigAt { return ConfigAt{Path: field.NewPath("spec", "config"), Config: m} }
	key := "db.query.default_language"
	tests := []struct {
		name, spec, tag string
		legacy          []ConfigAt
		wantErr         string
	}{
		{name: "unset on the LTS", tag: "5.26-enterprise"},
		{name: "CYPHER_5 on the LTS is fine (and writes nothing)", spec: "CYPHER_5", tag: "5.26.31-enterprise"},
		{name: "CYPHER_25 on the LTS", spec: "CYPHER_25", tag: "5.26-enterprise", wantErr: "spec.serverDefaultCypherLanguage"},
		{name: "CYPHER_25 on CalVer", spec: "CYPHER_25", tag: "2026.08.1-enterprise"},
		{name: "the key on the LTS stops the server starting", tag: "5.26-enterprise",
			legacy: []ConfigAt{cfg(map[string]string{key: "CYPHER_5"})}, wantErr: "spec.config[db.query.default_language]"},
		{name: "the key alone on CalVer is the user's choice", tag: "2026.08.1-enterprise",
			legacy: []ConfigAt{cfg(map[string]string{key: "CYPHER_25"})}},
		{name: "field and key agreeing", spec: "CYPHER_25", tag: "2026.08.1-enterprise",
			legacy: []ConfigAt{cfg(map[string]string{key: "CYPHER_25"})}},
		{name: "field and key disagreeing", spec: "CYPHER_5", tag: "2026.08.1-enterprise",
			legacy: []ConfigAt{cfg(map[string]string{key: "CYPHER_25"})}, wantErr: "conflicts with spec.serverDefaultCypherLanguage=CYPHER_5"},
		{name: "disagreeing in propertySharding.config", spec: "CYPHER_5", tag: "2026.06-enterprise",
			legacy:  []ConfigAt{{Path: field.NewPath("spec", "propertySharding", "config"), Config: map[string]string{key: "CYPHER_25"}}},
			wantErr: "spec.propertySharding.config[db.query.default_language]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateServerCypherLanguage(tt.spec, tt.tag, tt.legacy...)
			if tt.wantErr == "" {
				assert.Empty(t, errs)
				return
			}
			assert.Contains(t, errs.ToAggregate().Error(), tt.wantErr)
		})
	}
}
