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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// diagnoseOneServer runs diagnose over a cluster with one server pod whose
// neo4j container has status cs, and returns that cluster's findings and the
// exit code the user's CI would see.
func diagnoseOneServer(t *testing.T, cs corev1.ContainerStatus) ([]symptom, int) {
	t.Helper()
	pod := serverPod("prod", "prod-server-0")
	pod.Status.Phase = corev1.PodRunning
	cs.Name = "neo4j"
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{cs}
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pod,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	_, code := renderDiagnoseTo(t, results, "neo4j", false)
	return symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"), code
}

func errorExit(code int32) *corev1.ContainerStateTerminated {
	return &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Error"}
}

// #619 S9: the kubelet says CrashLoopBackOff only while it waits to start the
// container again. Just before, at restartCount 2, the container is
// Terminated with reason Error (kubectl shows "Error"), and diagnose gave a ⚠
// for the previous run and exited 0. Three runs that died in a row are a
// crash-loop, whatever the kubelet calls it yet.
func TestDiagnose_ACrashLoopBeforeCrashLoopBackOffIsAFailure(t *testing.T) {
	symptoms, code := diagnoseOneServer(t, corev1.ContainerStatus{
		RestartCount: 2,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", Message: "Unrecognized setting",
		}},
		LastTerminationState: corev1.ContainerState{Terminated: errorExit(1)},
	})

	assert.Equal(t, exitProblems, code, joinSymptoms(symptoms))
	require.Len(t, symptoms, 1, "one finding for the container, as CrashLoopBackOff gets: %s", joinSymptoms(symptoms))
	s := symptoms[0]
	assert.Equal(t, markProblem, s.mark)
	assert.Equal(t, "container neo4j in prod-server-0", s.subject)
	assert.Equal(t, "crash-looping after 2 restart(s)", s.what)
	assert.Contains(t, s.detail, "exited 1 (Error)")
	assert.Contains(t, s.detail, "Unrecognized setting")
	assert.Contains(t, s.action, "kubectl logs prod-server-0 -c neo4j --previous")
}

// What is not a crash-loop stays as it was: one restart (a server that stopped
// once during a planned roll), and a container that died twice and has been
// running since.
func TestDiagnose_ARestartIsNotACrashLoop(t *testing.T) {
	t.Run("one restart, running again", func(t *testing.T) {
		symptoms, code := diagnoseOneServer(t, corev1.ContainerStatus{
			RestartCount:         1,
			Ready:                true,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: errorExit(143)},
		})
		assert.Equal(t, exitOK, code, joinSymptoms(symptoms))
		require.Len(t, symptoms, 1)
		assert.Equal(t, markWarning, symptoms[0].mark)
		assert.Equal(t, "previously exited 143 (Error)", symptoms[0].what)
	})

	t.Run("one restart, its second run just died", func(t *testing.T) {
		symptoms, code := diagnoseOneServer(t, corev1.ContainerStatus{
			RestartCount:         1,
			State:                corev1.ContainerState{Terminated: errorExit(1)},
			LastTerminationState: corev1.ContainerState{Terminated: errorExit(143)},
		})
		assert.Equal(t, exitOK, code, joinSymptoms(symptoms))
		assert.NotContains(t, joinSymptoms(symptoms), "crash-looping")
	})

	t.Run("restarted twice, running and ready since", func(t *testing.T) {
		symptoms, code := diagnoseOneServer(t, corev1.ContainerStatus{
			RestartCount:         2,
			Ready:                true,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: errorExit(1)},
		})
		assert.Equal(t, exitOK, code, joinSymptoms(symptoms))
		assert.NotContains(t, joinSymptoms(symptoms), "crash-looping")
	})

	t.Run("restarted twice, the latest run exited 0", func(t *testing.T) {
		symptoms, code := diagnoseOneServer(t, corev1.ContainerStatus{
			RestartCount:         2,
			State:                corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}},
			LastTerminationState: corev1.ContainerState{Terminated: errorExit(1)},
		})
		assert.Equal(t, exitOK, code, joinSymptoms(symptoms))
		assert.NotContains(t, joinSymptoms(symptoms), "crash-looping")
	})
}
