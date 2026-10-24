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

// Pinning tests for #262: an image change while the cluster is not Ready must
// be HELD (never silently applied by the normal config-restart path), Ready
// must not be declared mid-rollout, and these guarantees are what makes the
// status.version backfill at Ready truthful.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func gateTestReconciler(t *testing.T, objs ...runtime.Object) *Neo4jEnterpriseClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = neo4jv1beta1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	return &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
}

func gateTestSTS(image string, replicas int32, currentRev, updateRev string, updated, ready int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "c-server", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: image}}},
			},
		},
		Status: appsv1.StatefulSetStatus{
			CurrentRevision: currentRev,
			UpdateRevision:  updateRev,
			UpdatedReplicas: updated,
			ReadyReplicas:   ready,
		},
	}
}

func gateTestCluster(tag, phase string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: phase},
	}
}

// The core #262 pin: image drift + cluster NOT Ready → the in-memory spec is
// pinned to the StatefulSet's current image, so the normal path cannot stomp
// the version change in as a config restart.
func TestHoldImageDriftUntilReady_PinsSpecWhileNotReady(t *testing.T) {
	sts := gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev1", 3, 3)
	cluster := gateTestCluster("2026.04-enterprise", "Forming")
	r := gateTestReconciler(t, sts, cluster)

	r.holdImageDriftUntilReady(context.Background(), cluster)

	if cluster.Spec.Image.Tag != "5.26.0-enterprise" || cluster.Spec.Image.Repo != "neo4j" {
		t.Errorf("spec must be pinned to the running image while not Ready; got %s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	}
}

func TestHoldImageDriftUntilReady_NoDriftNoTouch(t *testing.T) {
	sts := gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev1", 3, 3)
	cluster := gateTestCluster("5.26.0-enterprise", "Forming")
	r := gateTestReconciler(t, sts, cluster)
	r.holdImageDriftUntilReady(context.Background(), cluster)
	if cluster.Spec.Image.Tag != "5.26.0-enterprise" {
		t.Errorf("no drift: spec must be untouched; got %s", cluster.Spec.Image.Tag)
	}
}

func TestHoldImageDriftUntilReady_NoStatefulSetNoTouch(t *testing.T) {
	cluster := gateTestCluster("2026.04-enterprise", "")
	r := gateTestReconciler(t, cluster)
	r.holdImageDriftUntilReady(context.Background(), cluster)
	if cluster.Spec.Image.Tag != "2026.04-enterprise" {
		t.Errorf("initial install (no STS): spec must be untouched; got %s", cluster.Spec.Image.Tag)
	}
}

// Ready must not be declared while a rollout is in flight (#262: 'Ready' was
// reported over a mixed-version cluster mid-roll).
func TestServerStatefulSetFullyRolled(t *testing.T) {
	cases := []struct {
		name string
		sts  *appsv1.StatefulSet
		want bool
	}{
		{"fully rolled", gateTestSTS("neo4j:x", 3, "rev2", "rev2", 3, 3), true},
		{"revision mismatch (mid-roll)", gateTestSTS("neo4j:x", 3, "rev1", "rev2", 1, 3), false},
		{"updated lagging", gateTestSTS("neo4j:x", 3, "rev2", "rev2", 2, 3), false},
		{"ready lagging", gateTestSTS("neo4j:x", 3, "rev2", "rev2", 3, 2), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := gateTestCluster("x", "Forming")
			r := gateTestReconciler(t, tc.sts, cluster)
			got, detail := r.serverStatefulSetFullyRolled(context.Background(), cluster)
			if got != tc.want {
				t.Errorf("fullyRolled = %v (detail %q), want %v", got, detail, tc.want)
			}
		})
	}
}

// Escape hatch: a first deploy wedged on an unpullable image (0 ready
// replicas, every pod ImagePullBackOff) has no quorum for the hold to
// protect — the user's corrected tag must flow to the StatefulSet instead
// of being deferred until a Ready that can never come (found by the
// v1.12.1 fresh-eyes journey: a bad example tag permanently wedged the
// cluster even after fixing spec.image).
func TestHoldImageDriftUntilReady_ZeroReadyReplicasEscapesHold(t *testing.T) {
	sts := gateTestSTS("neo4j:2025.12.0-enterprise", 3, "rev1", "rev1", 0, 0)
	cluster := gateTestCluster("2026.04-enterprise", "Forming")
	r := gateTestReconciler(t, sts, cluster)

	r.holdImageDriftUntilReady(context.Background(), cluster)

	if cluster.Spec.Image.Tag != "2026.04-enterprise" {
		t.Errorf("with zero ready replicas the corrected image must NOT be held; got %s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	}
}

// A tag carrying a digest, which validation accepts, is split as the image was
// built: the hold used to split on the last colon and pinned the repo
// "neo4j:2026.08.1-enterprise@sha256" with the tag "0123…" (#531).
func TestHoldImageDriftUntilReady_ATagWithADigestIsPinnedWhole(t *testing.T) {
	const running = "2026.08.1-enterprise@sha256:0123abcd"
	sts := gateTestSTS("registry.example:5000/neo4j:"+running, 3, "rev1", "rev1", 3, 3)
	cluster := gateTestCluster("2026.09.0-enterprise", "Forming")
	cluster.Spec.Image.Repo = "registry.example:5000/neo4j"
	r := gateTestReconciler(t, sts, cluster)

	r.holdImageDriftUntilReady(context.Background(), cluster)

	if cluster.Spec.Image.Repo != "registry.example:5000/neo4j" || cluster.Spec.Image.Tag != running {
		t.Errorf("want registry.example:5000/neo4j : %s, got %s : %s", running, cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	}
}

// heldServerPod is a server pod of cluster "c", Ready or not, running image;
// a Ready one's neo4j container is running.
func heldServerPod(ordinal int, image string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	var state corev1.ContainerState
	if ready {
		status = corev1.ConditionTrue
		state.Running = &corev1.ContainerStateRunning{}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("c-server-%d", ordinal), Namespace: "default",
			Labels: map[string]string{"neo4j.com/cluster": "c", "neo4j.com/clustering": "true"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: image}}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "neo4j", Ready: ready, State: state}},
		},
	}
}

// A storage expansion orphan-deletes the server StatefulSet and the next pass
// creates it again. The hold found no StatefulSet and let the held image
// through, so the new StatefulSet rolled every server onto it, ungated, on a
// cluster that was not Ready (#538). It reads the orphaned pods instead.
func TestHoldImageDriftUntilReady_HoldsTheOrphanedServersImage(t *testing.T) {
	const running, target = "neo4j:2026.08.1-enterprise", "2026.09.0-enterprise"

	t.Run("every server on one image", func(t *testing.T) {
		cluster := gateTestCluster(target, "Forming")
		r := gateTestReconciler(t, cluster,
			heldServerPod(0, running, true), heldServerPod(1, running, true), heldServerPod(2, running, false))
		r.holdImageDriftUntilReady(context.Background(), cluster)
		if cluster.Spec.Image.Tag != "2026.08.1-enterprise" || cluster.Spec.Image.Repo != "neo4j" {
			t.Errorf("the image the orphaned servers run must be held; got %s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
		}
	})

	t.Run("a held upgrade: the highest ordinal carries the template it left", func(t *testing.T) {
		cluster := heldCluster(upgradePhasePaused, true)
		cluster.Spec.Image.Tag = "2026.10.0-enterprise" // moved on while the cluster was not Ready
		cluster.Status.Phase = "Forming"
		r := gateTestReconciler(t, cluster,
			heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true), heldServerPod(2, "neo4j:"+holdTarget, true))
		r.holdImageDriftUntilReady(context.Background(), cluster)
		if cluster.Spec.Image.Tag != holdTarget {
			t.Fatalf("want the held target %s, got %s", holdTarget, cluster.Spec.Image.Tag)
		}
		// So the StatefulSet created again keeps the partition the upgrade left.
		if p := partitionForNewServerStatefulSet(cluster, ptr.To(int32(3))); p == nil || *p != 2 {
			t.Errorf("the recreated StatefulSet must keep partition 2; got %v", p)
		}
	})

	t.Run("no server ready: the escape hatch lets the image through", func(t *testing.T) {
		cluster := gateTestCluster(target, "Forming")
		r := gateTestReconciler(t, cluster, heldServerPod(0, running, false), heldServerPod(1, running, false))
		r.holdImageDriftUntilReady(context.Background(), cluster)
		if cluster.Spec.Image.Tag != target {
			t.Errorf("with no ready server the image is not held; got %s", cluster.Spec.Image.Tag)
		}
	})

	t.Run("pods of another cluster are not this one's", func(t *testing.T) {
		cluster := gateTestCluster(target, "Forming")
		other := heldServerPod(0, running, true)
		other.Name, other.Labels["neo4j.com/cluster"] = "cc-server-0", "cc"
		r := gateTestReconciler(t, cluster, other)
		r.holdImageDriftUntilReady(context.Background(), cluster)
		if cluster.Spec.Image.Tag != target {
			t.Errorf("a new cluster's image is not held; got %s", cluster.Spec.Image.Tag)
		}
	})
}

// UpgradeDeferred is announced once a hold outlasts the pass that met it. On
// the pass after a refused tag is fixed, the phase is still the refusal's
// Failed: that pass holds the new image and writes Ready, and the next one
// starts the upgrade. It announced a deferral every time (#550).
func TestHoldImageDriftUntilReady_AnnouncesADeferralThatOutlastsAPass(t *testing.T) {
	ctx := context.Background()
	deferred := func(r *Neo4jEnterpriseClusterReconciler) int {
		rec, ok := r.Recorder.(*record.FakeRecorder)
		if !ok {
			t.Fatal("want a FakeRecorder")
		}
		n := 0
		for len(rec.Events) > 0 {
			if ev := <-rec.Events; strings.Contains(ev, EventReasonUpgradeDeferred) {
				n++
			}
		}
		return n
	}
	sts := gateTestSTS("neo4j:2026.08.1-enterprise", 3, "rev1", "rev1", 3, 3)
	pass := func(r *Neo4jEnterpriseClusterReconciler, tag string) {
		r.holdImageDriftUntilReady(ctx, gateTestCluster(tag, "Failed"))
	}

	r := gateTestReconciler(t, sts)
	pass(r, "2026.09.0-enterprise")
	if n := deferred(r); n != 0 {
		t.Fatalf("the first pass that holds a change announces nothing; got %d events", n)
	}
	// The cluster turned Ready and the upgrade started: nothing to announce.
	r.deferredUpgradeTargets.Delete("default/c")

	// A hold that lasts: announced on its second pass, once.
	pass(r, "2026.10.0-enterprise")
	pass(r, "2026.10.0-enterprise")
	pass(r, "2026.10.0-enterprise")
	if n := deferred(r); n != 1 {
		t.Fatalf("a lasting hold is announced once; got %d", n)
	}
	// Another target is another deferral.
	pass(r, "2026.11.0-enterprise")
	pass(r, "2026.11.0-enterprise")
	if n := deferred(r); n != 1 {
		t.Fatalf("a new target is announced once more; got %d", n)
	}

	// A change an upgrade the machine stopped already explains — its record
	// names the target, announced with UpgradeFailed — is not announced.
	held := heldCluster(upgradePhaseFailed, false)
	held.Status.Phase = "Forming"
	r = gateTestReconciler(t, gateTestSTS("neo4j:"+holdRunning, 3, "rev1", "rev1", 3, 3))
	for i := 0; i < 3; i++ {
		c := held.DeepCopy()
		r.holdImageDriftUntilReady(ctx, c)
		if c.Spec.Image.Tag != holdRunning {
			t.Fatalf("the change is still held; got %s", c.Spec.Image.Tag)
		}
	}
	if n := deferred(r); n != 0 {
		t.Fatalf("the stopped upgrade's own target is not announced as deferred; got %d", n)
	}
}
