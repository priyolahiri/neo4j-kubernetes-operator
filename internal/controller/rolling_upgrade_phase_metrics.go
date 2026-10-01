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

// neo4j_operator_upgrade_duration_seconds{phase}: one observation per
// rolling-upgrade phase, taken when the phase ENDS.
//
// Exactly-once is a property of the status write, not of the metric call. The
// phase is read from the freshly fetched object inside the same optimistic-
// concurrency window as the write that changes it, and the observation is made
// only after that write succeeded. Reconciles repeat, caches go stale and the
// operator restarts mid-upgrade — but only one writer can move the persisted
// phase from X to Y, and a repeat finds Y already persisted ("phase did not
// change") and observes nothing. The duration comes from the persisted
// status.upgradeStatus.phaseStartTime, never from memory, so a restart in the
// middle of a phase still reports its full wall-clock time (downtime
// included).
//
// What is timed:
//
//   - Staging, Rolling, Stabilizing and Verifying, each when it ends. The last
//     step that leaves the chain is observed too — Verifying ending in
//     Completed, or whichever phase was active when the upgrade failed or
//     paused (so a Rolling timeout shows up as a Rolling observation of about
//     upgradeTimeout).
//   - Completed, Failed and Paused are NOT timed: they are terminal, so there
//     is no end to measure. Leaving them (the next attempt's Staging, or a
//     manual resume) observes nothing.
//   - Rolling is measured from its own phaseStartTime, not stepStartTime:
//     stepStartTime restarts on every partition advance, so it would report only
//     the last server's step.
//
// Resolution is one second (metav1.Time serialises to seconds).

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/metrics"
)

// endedUpgradePhase is one phase observation waiting to be recorded.
type endedUpgradePhase struct {
	phase    string
	duration time.Duration
}

// isTimedUpgradePhase reports whether a phase has a measurable duration.
func isTimedUpgradePhase(phase string) bool {
	switch phase {
	case upgradePhaseStaging, upgradePhaseRolling, upgradePhaseStabilizing, upgradePhaseVerifying:
		return true
	}
	return false
}

// stampUpgradePhaseTransition must be called on the freshly fetched
// UpgradeStatus after a mutation that started from prevPhase/prevPhaseStart.
// When the phase changed it restarts PhaseStartTime for the new phase and
// returns the phase that ended, or nil when there is nothing to observe: the
// phase did not change, the ended phase is terminal, or it carries no
// persisted phaseStartTime (an upgrade that began under an operator that did
// not write one — falling back to stepStartTime would under-report Rolling).
func stampUpgradePhaseTransition(
	us *neo4jv1beta1.UpgradeStatus,
	prevPhase string,
	prevPhaseStart *metav1.Time,
	now metav1.Time,
) *endedUpgradePhase {
	if us.Phase == prevPhase {
		return nil
	}
	us.PhaseStartTime = &now
	if !isTimedUpgradePhase(prevPhase) || prevPhaseStart == nil {
		return nil
	}
	d := now.Sub(prevPhaseStart.Time)
	if d < 0 {
		d = 0 // clock skew between the writer that stamped it and this one
	}
	return &endedUpgradePhase{phase: prevPhase, duration: d}
}

// snapshotPhaseStart copies the pointer's value so a mutate that replaces the
// whole struct cannot change what the transition is measured against.
func snapshotPhaseStart(t *metav1.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

// recordEndedUpgradePhase makes the observation. Recording is metrics-only and
// can never fail a reconcile.
func recordEndedUpgradePhase(m *metrics.UpgradeMetrics, ended *endedUpgradePhase) {
	if m == nil || ended == nil {
		return
	}
	m.RecordUpgradePhase(ended.phase, ended.duration)
}
