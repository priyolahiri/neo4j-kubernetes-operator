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

package v1beta1

// The shared status.phase vocabulary.
//
// These were inline string literals in every controller, which made the set
// unknowable from outside: `kubectl neo4j explain` carried guidance only for
// the replica phases below (the only ones that HAD constants) and answered
// "no guidance for this phase — it may be newer than this CLI" for `Ready`,
// the phase every healthy resource in the product reports. That message sent
// users looking for a version mismatch that did not exist.
//
// Declaring the vocabulary here makes it one thing: controllers set these,
// PhaseToConditionStatus classifies these, and the CLI explains these — so a
// rename breaks the build instead of silently leaving one of the three behind.
const (
	// Terminal, healthy.
	PhaseReady     = "Ready"
	PhaseInstalled = "Installed"
	PhaseCompleted = "Completed"

	// Terminal, unhealthy.
	PhaseFailed    = "Failed"
	PhaseError     = "Error"
	PhaseInvalid   = "Invalid"
	PhaseDegraded  = "Degraded"
	PhaseSuspended = "Suspended"

	// In progress.
	PhasePending    = "Pending"
	PhaseValidating = "Validating"
	PhaseCreating   = "Creating"
	PhaseForming    = "Forming"
	PhaseInstalling = "Installing"
	PhaseRunning    = "Running"
	PhaseWaiting    = "Waiting"
	PhaseUpgrading  = "Upgrading"
	PhaseExpanding  = "Expanding"

	// Set when nothing has been able to determine the state.
	PhaseUnknown = "Unknown"
)

// AllPhases is the complete shared vocabulary, in the order above.
//
// It exists so consumers can be tested for COMPLETENESS rather than for the
// handful of values someone remembered: `kubectl neo4j explain` asserts it can
// explain every entry, so adding a phase without guidance fails a test rather
// than reaching a user mid-incident as an unexplained word.
//
// Kind-specific phases that are not part of this shared set (the replica and
// promotion phases in neo4jreplicadatabase_types.go and
// neo4jreplicapromotion_types.go, and the Aura phases, which mirror Aura's own
// open vocabulary) are deliberately not listed here.
var AllPhases = []string{
	PhaseReady,
	PhaseInstalled,
	PhaseCompleted,
	PhaseFailed,
	PhaseError,
	PhaseInvalid,
	PhaseDegraded,
	PhaseSuspended,
	PhasePending,
	PhaseValidating,
	PhaseCreating,
	PhaseForming,
	PhaseInstalling,
	PhaseRunning,
	PhaseWaiting,
	PhaseUpgrading,
	PhaseExpanding,
	PhaseUnknown,
}
