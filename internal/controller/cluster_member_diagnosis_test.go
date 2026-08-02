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

// Pinning tests for the member-pod diagnosis on the connectivity-failure path.
//
// Provenance: a release verification journey lost ~10 minutes to a cluster that
// sat at phase=Forming while all three servers were in CrashLoopBackOff. The only
// signal was a ConnectivityDegraded event reading
// "dial tcp ...:7687: connect: connection refused", which points at Bolt and
// networking. The real cause — "Initial heap size set to a larger value than the
// maximum heap size" — was visible only via
// `kubectl logs <pod> -c neo4j --previous`. Nothing on the CR routed anyone there.

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func diagCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "three-node-simple", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
	}
}

// diagPod builds a member pod carrying the labels ServerPodSelector matches, so
// the test exercises the same selector the production code uses rather than a
// hand-rolled one.
func diagPod(clusterName, podName string, status corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
			Labels:    resources.ServerPodSelector(clusterName),
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{status}},
	}
}

func diagReconciler(t *testing.T, objs ...runtime.Object) *Neo4jEnterpriseClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = neo4jv1beta1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
}

func TestDiagnoseUnhealthyMembers(t *testing.T) {
	cluster := diagCluster()

	t.Run("crash loop reports the previous exit reason, not just CrashLoopBackOff", func(t *testing.T) {
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
			},
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
					Reason:   "Error",
					Message:  "Initial heap size set to a larger value than the maximum heap size",
				},
			},
		})

		got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster)

		for _, want := range []string{
			"three-node-simple-server-0",
			"CrashLoopBackOff",
			"Initial heap size set to a larger value than the maximum heap size",
			"--previous",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("diagnosis missing %q\ngot: %s", want, got)
			}
		}
	})

	t.Run("OOMKilled is named", func(t *testing.T) {
		pod := diagPod(cluster.Name, "three-node-simple-server-1", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
			},
		})

		got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster)

		if !strings.Contains(got, "OOMKilled") || !strings.Contains(got, "137") {
			t.Errorf("expected the OOM kill to be named, got: %s", got)
		}
	})

	t.Run("a healthy cluster produces no noise", func(t *testing.T) {
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		})

		if got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster); got != "" {
			t.Errorf("expected no diagnosis for a ready pod, got: %s", got)
		}
	})

	t.Run("starting but not yet ready is not reported as a fault", func(t *testing.T) {
		// Every cluster passes through this state during formation; reporting it
		// would put a scary message on the CR for the normal path.
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		})

		if got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster); got != "" {
			t.Errorf("expected silence for a starting pod, got: %s", got)
		}
	})

	t.Run("ContainerCreating is not reported during normal formation", func(t *testing.T) {
		// Every healthy cluster shows this for the first seconds. Reporting it
		// would put "member pod not running" on the CR on the happy path.
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			},
		})

		if got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster); got != "" {
			t.Errorf("expected silence while the container is being created, got: %s", got)
		}
	})

	t.Run("ContainerCreating IS reported once the container has already died", func(t *testing.T) {
		// Same waiting reason, but a previous termination makes it a restart
		// loop rather than a first start.
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			},
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
			},
		})

		got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster)
		if !strings.Contains(got, "OOMKilled") {
			t.Errorf("expected the prior termination to be reported, got: %s", got)
		}
	})

	t.Run("a real image failure is reported immediately", func(t *testing.T) {
		pod := diagPod(cluster.Name, "three-node-simple-server-0", corev1.ContainerStatus{
			Name:  "neo4j",
			Ready: false,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"},
			},
		})

		got := diagReconciler(t, cluster, pod).diagnoseUnhealthyMembers(context.Background(), cluster)
		if !strings.Contains(got, "ErrImagePull") {
			t.Errorf("expected an image-pull failure to be reported without waiting for a termination, got: %s", got)
		}
	})

	t.Run("no pods at all yields nothing", func(t *testing.T) {
		if got := diagReconciler(t, cluster).diagnoseUnhealthyMembers(context.Background(), cluster); got != "" {
			t.Errorf("expected empty diagnosis with no pods, got: %s", got)
		}
	})

	t.Run("output is bounded and stable", func(t *testing.T) {
		objs := []runtime.Object{cluster}
		// Deliberately out of order to prove the output is sorted rather than
		// dependent on List order, which would make the event churn.
		for _, name := range []string{"server-3", "server-1", "server-0", "server-2"} {
			objs = append(objs, diagPod(cluster.Name, "three-node-simple-"+name, corev1.ContainerStatus{
				Name:  "neo4j",
				Ready: false,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			}))
		}
		r := diagReconciler(t, objs...)

		got := r.diagnoseUnhealthyMembers(context.Background(), cluster)
		if !strings.Contains(got, "(+1 more)") {
			t.Errorf("expected the report to be capped with an overflow count, got: %s", got)
		}
		if idx0, idx1 := strings.Index(got, "server-0"), strings.Index(got, "server-1"); idx0 > idx1 {
			t.Errorf("expected sorted pod order, got: %s", got)
		}
		if strings.Contains(got, "server-3") {
			t.Errorf("expected server-3 to be dropped by the cap, got: %s", got)
		}

		if second := r.diagnoseUnhealthyMembers(context.Background(), cluster); second != got {
			t.Errorf("diagnosis is not stable across calls:\n first: %s\nsecond: %s", got, second)
		}
	})
}

func TestTruncateForMessage(t *testing.T) {
	if got := truncateForMessage("  short  ", 20); got != "short" {
		t.Errorf("expected trimmed passthrough, got %q", got)
	}
	long := strings.Repeat("x", 50)
	got := truncateForMessage(long, 10)
	if len([]rune(got)) != 11 || !strings.HasSuffix(got, "…") {
		t.Errorf("expected a 10-char prefix plus an ellipsis, got %q (%d runes)", got, len([]rune(got)))
	}
	if !strings.HasPrefix(got, strings.Repeat("x", 10)) {
		t.Errorf("truncation must keep the LEADING text where the cause is, got %q", got)
	}
}
