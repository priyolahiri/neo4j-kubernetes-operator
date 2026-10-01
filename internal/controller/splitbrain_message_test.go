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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The detector's ErrorMessage already leads with "Split-brain detected: …".
// The reconciler used to prefix it again for both the SplitBrainDetected event
// and the status message, so users read
// "Split-brain detected: Split-brain detected: 2 cluster groups found, …".
func TestSplitBrainMessage_IsNotPrefixedTwice(t *testing.T) {
	// The exact text the detector builds for a genuine split-brain.
	analysis := &SplitBrainAnalysis{
		IsSplitBrain: true,
		ErrorMessage: "Split-brain detected: 2 cluster groups found, 1 orphaned pods",
	}

	msg := splitBrainMessage(analysis)
	assert.Equal(t, "Split-brain detected: 2 cluster groups found, 1 orphaned pods", msg)
	assert.Equal(t, 1, strings.Count(msg, "Split-brain detected"), "prefix must appear exactly once")
}

func TestSplitBrainMessage_AddsThePrefixOnlyWhenMissing(t *testing.T) {
	// A future detector path that sets IsSplitBrain with a bare description
	// still reads as a split-brain, and an empty message is not blank.
	assert.Equal(t, "Split-brain detected: two views disagree",
		splitBrainMessage(&SplitBrainAnalysis{IsSplitBrain: true, ErrorMessage: "two views disagree"}))
	assert.Equal(t, "Split-brain detected",
		splitBrainMessage(&SplitBrainAnalysis{IsSplitBrain: true}))
}

// The event the user actually sees: one Warning, reason SplitBrainDetected,
// text carrying the prefix once. Uses a fake recorder.
func TestReportSplitBrainDetected_EmitsOneUndoubledWarning(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	r := &Neo4jEnterpriseClusterReconciler{Recorder: rec}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
	}
	analysis := &SplitBrainAnalysis{
		IsSplitBrain: true,
		ErrorMessage: "Split-brain detected: 2 cluster groups found, 1 orphaned pods",
	}

	statusMsg := r.reportSplitBrainDetected(cluster, analysis)

	require.Len(t, rec.Events, 1)
	got := <-rec.Events
	assert.Equal(t, corev1.EventTypeWarning+" "+EventReasonSplitBrainDetected+
		" Split-brain detected: 2 cluster groups found, 1 orphaned pods", got)
	assert.Equal(t, 1, strings.Count(got, "Split-brain detected"))

	// The status message returned to the caller reads the same way.
	assert.Equal(t, "Split-brain detected: 2 cluster groups found, 1 orphaned pods", statusMsg)
}
