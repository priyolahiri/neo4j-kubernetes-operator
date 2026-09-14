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

package neo4j

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Constituents are addressed as `<composite>.<name>` everywhere — in SHOW
// DATABASE output, in a USE clause, and in a privilege. These two are the only
// place that spelling is decided.
func TestQualifyAndUnqualifyConstituent(t *testing.T) {
	assert.Equal(t, "cineasts.latest", QualifyConstituent("cineasts", "latest"))
	assert.Equal(t, "latest", UnqualifyConstituent("cineasts", "cineasts.latest"))

	// A name that is not in this composite's namespace comes back untouched,
	// so a caller filtering a mixed alias list cannot mistake it for one.
	assert.Equal(t, "other.latest", UnqualifyConstituent("cineasts", "other.latest"))

	// Constituent names may not contain dots (the validator enforces it), but
	// the target database name is a separate identifier that legitimately can.
	assert.Equal(t, "cineasts.a.b", QualifyConstituent("cineasts", "a.b"))
}

// The composite type string the server reports. Verified on 5.26.30 and
// 2026.08.1: SHOW DATABASES YIELD type returns exactly "composite".
func TestCompositeDatabaseTypeConstant(t *testing.T) {
	assert.Equal(t, "composite", CompositeDatabaseType)
}
