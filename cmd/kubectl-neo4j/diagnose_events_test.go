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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
)

// readyClusterSince is a cluster whose Ready condition turned True ago ago.
func readyClusterSince(ago time.Duration) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase: neo4jv1beta1.PhaseReady,
			Conditions: []metav1.Condition{{
				Type: controller.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: controller.ConditionReasonReady,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-ago)),
			}},
		},
	}
}

func clusterWarning(name, reason string, ago time.Duration) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: "neo4j"},
		Type:           corev1.EventTypeWarning,
		Reason:         reason,
		Message:        fmt.Sprintf("%s message", reason),
		LastTimestamp:  metav1.NewTime(time.Now().Add(-ago)),
		InvolvedObject: corev1.ObjectReference{Kind: "Neo4jEnterpriseCluster", Name: "prod"},
	}
}

// diagnoseProd diagnoses Neo4jEnterpriseCluster/prod with objs and a healthy
// workload (one ready server pod), so the events are all there is to report.
func diagnoseProd(t *testing.T, objs ...client.Object) diagnosis {
	t.Helper()
	pod := serverPod("prod", "prod-server-0")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "neo4j", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	objs = append(objs, pod, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-server", Namespace: "neo4j"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptrInt32(1),
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: pod.Labels}},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
	})
	results, err := diagnoseNamespace(context.Background(), testClient(t, objs...), "neo4j", "Neo4jEnterpriseCluster/prod")
	require.NoError(t, err)
	require.Len(t, results, 1)
	return results[0]
}

// The v1.22.0 re-walk: diagnose on a healthy cluster marked a
// ClusterQuorumLost from 40 minutes earlier with ⚠, though the cluster had
// been Ready again for 35 (#553). A Warning from before the Ready condition
// last changed describes a state the cluster has left: it is counted, not
// marked, and a quiet run has nothing to say about the cluster.
func TestDiagnose_AWarningFromBeforeTheLastTransitionIsNotMarked(t *testing.T) {
	d := diagnoseProd(t, readyClusterSince(35*time.Minute),
		clusterWarning("e1", controller.EventReasonClusterQuorumLost, 40*time.Minute))
	assert.Empty(t, d.symptoms, joinSymptoms(d.symptoms))
	assert.Equal(t, 1, d.earlierWarnings)

	out, code := renderDiagnoseTo(t, []diagnosis{d}, "neo4j", false)
	assert.Equal(t, exitOK, code)
	assert.Contains(t, out, "nothing to report below the resource level")
	assert.Contains(t, out, "1 earlier Warning event(s), from before its Ready condition last changed 35m ago")
	assert.NotContains(t, out, markWarning)

	quiet, _ := renderDiagnoseTo(t, []diagnosis{d}, "neo4j", true)
	assert.Contains(t, quiet, "nothing to report")
	assert.NotContains(t, quiet, "Neo4jEnterpriseCluster/prod")
}

func TestDiagnose_WhichWarningsDescribeTheCurrentState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		since   time.Duration // Ready condition last changed this long ago
		reason  string
		seen    time.Duration // the Warning was last seen this long ago
		current bool
	}{
		{"raised after the transition", 35 * time.Minute, controller.EventReasonClusterDegraded, 10 * time.Minute, true},
		{"raised in the same pass as the transition, just before its status write", 10 * time.Minute, controller.EventReasonClusterQuorumLost, 10*time.Minute + 2*time.Second, true},
		{"raised before the transition", 10 * time.Minute, controller.EventReasonClusterQuorumLost, 12 * time.Minute, false},
		// An advisory is announced again every 30 minutes while it applies,
		// so one seen within that time may still apply.
		{"an advisory from before the transition, recent", 10 * time.Minute, controller.EventReasonValidationWarning, 25 * time.Minute, true},
		{"an advisory long unannounced, from before the transition", 10 * time.Minute, controller.EventReasonValidationWarning, 50 * time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := diagnoseProd(t, readyClusterSince(tc.since), clusterWarning("e1", tc.reason, tc.seen))
			if tc.current {
				require.Len(t, d.symptoms, 1)
				assert.Equal(t, markWarning, d.symptoms[0].mark)
				assert.Zero(t, d.earlierWarnings)
			} else {
				assert.Empty(t, d.symptoms)
				assert.Equal(t, 1, d.earlierWarnings)
			}
		})
	}

	// Nothing tells the states of an object without a Ready condition apart.
	t.Run("no Ready condition: every Warning is current", func(t *testing.T) {
		cluster := readyClusterSince(time.Minute)
		cluster.Status.Conditions = nil
		d := diagnoseProd(t, cluster, clusterWarning("e1", controller.EventReasonClusterQuorumLost, 50*time.Minute))
		assert.Len(t, d.symptoms, 1)
	})
}
