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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Knowledge rule 64: the destructive drop-and-recreate path fires only while
// Status.LastDestructiveRestoreGeneration is behind the spec's generation, and
// only for replaceExisting+force. Without the generation half, every reconcile
// of a replaceExisting CR would DROP DATABASE and re-seed it again, forever.
//
// This pins the controller's own predicate. The only other coverage was a local
// integration spec that skips in CI.
func TestDestructiveRestorePending_GatesOnGeneration(t *testing.T) {
	mk := func(replace, force bool, lastGen, gen int64) *neo4jv1beta1.Neo4jShardedDatabase {
		sd := &neo4jv1beta1.Neo4jShardedDatabase{}
		sd.Generation = gen
		sd.Spec.ReplaceExisting = replace
		sd.Spec.Force = force
		sd.Status.LastDestructiveRestoreGeneration = lastGen
		return sd
	}

	assert.True(t, destructiveRestorePending(mk(true, true, 0, 1)),
		"first reconcile of a replaceExisting+force CR: destructive restore is due")
	assert.True(t, destructiveRestorePending(mk(true, true, 1, 2)),
		"spec edited since the last restore (generation advanced): due again")
	assert.False(t, destructiveRestorePending(mk(true, true, 2, 2)),
		"already restored at this generation: must NOT re-drop on the next reconcile")
	assert.False(t, destructiveRestorePending(mk(true, true, 3, 2)),
		"stamp ahead of the generation is not due either")
	assert.False(t, destructiveRestorePending(mk(false, true, 0, 1)),
		"replaceExisting off: never destructive")
	assert.False(t, destructiveRestorePending(mk(true, false, 0, 1)),
		"force off: never destructive (the validator pairs the two)")
}
