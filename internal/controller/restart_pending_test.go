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
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestClusterDeferredSettings(t *testing.T) {
	lts := minimalCluster("c", "default")
	lts.Spec.Image.Tag = "5.26-enterprise"
	if got := clusterDeferredSettings(lts); len(got) != 1 || got[0].Name != resources.AsyncRaftChannelsSetting || got[0].Want != "true" {
		t.Errorf("5.26 must defer the async Raft channels setting, got %v", got)
	}
	calver := minimalCluster("c", "default")
	calver.Spec.Image.Tag = "2026.08.1-enterprise"
	if got := clusterDeferredSettings(calver); len(got) != 0 {
		t.Errorf("CalVer has it on by default; nothing to defer, got %v", got)
	}
	own := lts.DeepCopy()
	own.Spec.Config = map[string]string{resources.AsyncRaftChannelsSetting: "false"}
	if got := clusterDeferredSettings(own); len(got) != 0 {
		t.Errorf("a value set in spec.config is the user's, got %v", got)
	}
}

func TestSupportsAsyncRaftChannels(t *testing.T) {
	for version, want := range map[string]bool{
		"5.26.28": false, "5.26.29": true, "5.26.31": true,
		"2026.08.1": false, "5.25.0": false, "garbage": false,
	} {
		if got := supportsAsyncRaftChannels(version); got != want {
			t.Errorf("%s: got %v, want %v", version, got, want)
		}
	}
}

func TestStampDeferredSettings(t *testing.T) {
	lts := minimalCluster("c", "default")
	lts.Spec.Image.Tag = "5.26-enterprise"
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	created := &corev1.ConfigMap{}
	if !stampDeferredSettings(created, nil, lts, t0) || created.Annotations[deferredSettingsSinceAnnotation] != "2026-10-07T12:00:00Z" {
		t.Fatalf("a new ConfigMap records since now, got %v", created.Annotations)
	}
	again := &corev1.ConfigMap{}
	if stampDeferredSettings(again, created, lts, t0.Add(time.Hour)) || again.Annotations[deferredSettingsSinceAnnotation] != "2026-10-07T12:00:00Z" {
		t.Errorf("unchanged settings keep their since and need no write, got %v", again.Annotations)
	}
	calver := lts.DeepCopy()
	calver.Spec.Image.Tag = "2026.08.1-enterprise"
	cleared := &corev1.ConfigMap{}
	if !stampDeferredSettings(cleared, created, calver, t0) || len(cleared.Annotations) != 0 {
		t.Errorf("nothing deferred: the stamp is dropped and written, got %v", cleared.Annotations)
	}
}

func TestPendingRestartServers(t *testing.T) {
	since := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	deferred := clusterDeferredSettings(func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := minimalCluster("c", "default")
		c.Spec.Image.Tag = "5.26-enterprise"
		return c
	}())
	pending := pendingRestartServers(map[string]serverState{
		"c-server-0": {Version: "5.26.31", Started: since.Add(-time.Hour)},  // waiting
		"c-server-1": {Version: "5.26.31", Started: since.Add(time.Minute)}, // started with it
		"c-server-2": {Version: "5.26.28", Started: since.Add(-time.Hour)},  // too old to have it
	}, deferred, since)
	if len(pending) != 1 || len(pending["c-server-0"]) != 1 {
		t.Fatalf("only server-0 is waiting, got %v", pending)
	}
	status, reason, msg := restartPendingCondition(pending)
	if status != metav1.ConditionTrue || reason != ConditionReasonSettingsAwaitRestart ||
		!strings.Contains(msg, "c-server-0") || !strings.Contains(msg, resources.AsyncRaftChannelsSetting) {
		t.Errorf("unexpected condition %s/%s %q", status, reason, msg)
	}
	if status, reason, _ := restartPendingCondition(nil); status != metav1.ConditionFalse || reason != ConditionReasonNoSettingsAwaitRestart {
		t.Errorf("nothing pending must read False, got %s/%s", status, reason)
	}
}

// fakeVersionReader reports a fixed Neo4j version for one server.
type fakeVersionReader struct{ version string }

func (f *fakeVersionReader) GetLoadedComponents(context.Context) ([]neo4jclient.ComponentInfo, error) {
	return []neo4jclient.ComponentInfo{{Name: "Neo4j Kernel", Version: f.version, Edition: "enterprise"}}, nil
}
func (f *fakeVersionReader) Close() error { return nil }

// TestReconcileRestartPending pins #468's status: a Ready 5.26 cluster whose
// server started before the ConfigMap carried the setting is named in
// RestartPending=True; one started after is not; with nothing deferred the
// condition is removed.
func TestReconcileRestartPending(t *testing.T) {
	ctx := context.Background()
	since := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	cluster := minimalCluster("rp", "default")
	cluster.Spec.Image.Tag = "5.26-enterprise"
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rp-config", Namespace: "default"}}
	stampDeferredSettings(cm, nil, cluster, since)
	objs := []client.Object{cluster, cm}
	for name, started := range map[string]time.Time{"rp-server-0": since.Add(-time.Hour), "rp-server-1": since.Add(time.Minute)} {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: resources.ServerPodSelector("rp")},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name: "neo4j", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}},
			}}},
		})
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).WithStatusSubresource(cluster).Build()

	run := func() *metav1.Condition {
		t.Helper()
		r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: newTestScheme()}
		r.RestartPendingChecker = &restartPendingChecker{dial: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (versionReader, error) {
			return &fakeVersionReader{version: "5.26.31"}, nil
		}}
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "rp", Namespace: "default"}, latest); err != nil {
			t.Fatal(err)
		}
		r.reconcileRestartPending(ctx, latest)
		if err := fc.Get(ctx, types.NamespacedName{Name: "rp", Namespace: "default"}, latest); err != nil {
			t.Fatal(err)
		}
		return meta.FindStatusCondition(latest.Status.Conditions, ConditionTypeRestartPending)
	}

	c := run()
	if c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "rp-server-0 will apply") {
		t.Fatalf("server-0 started before the setting and must be named alone, got %+v", c)
	}

	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "rp", Namespace: "default"}, latest); err != nil {
		t.Fatal(err)
	}
	latest.Spec.Image.Tag = "2026.08.1-enterprise"
	if err := fc.Update(ctx, latest); err != nil {
		t.Fatal(err)
	}
	if c := run(); c != nil {
		t.Errorf("nothing deferred: the condition must be removed, got %+v", c)
	}
}
