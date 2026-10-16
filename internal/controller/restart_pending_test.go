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
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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
	if got := clusterDeferredSettings(lts, ""); len(got) != 1 || got[0].Name != resources.AsyncRaftChannelsSetting || got[0].Want != "true" {
		t.Errorf("5.26 must defer the async Raft channels setting, got %v", got)
	}
	calver := minimalCluster("c", "default")
	calver.Spec.Image.Tag = "2026.08.1-enterprise"
	if got := clusterDeferredSettings(calver, ""); len(got) != 0 {
		t.Errorf("CalVer has it on by default; nothing to defer, got %v", got)
	}
	own := lts.DeepCopy()
	own.Spec.Config = map[string]string{resources.AsyncRaftChannelsSetting: "false"}
	if got := clusterDeferredSettings(own, ""); len(got) != 0 {
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
	}(), "")
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

// TestClusterDeferredSettings_ConnectTimeout: the cluster connect timeout is
// applied live but taken up by Raft only at a restart, so the value the
// rendered neo4j.conf gives it is deferred on both lines — the operator's 5s,
// or the user's spec.config value — and needs no server version.
func TestClusterDeferredSettings_ConnectTimeout(t *testing.T) {
	find := func(deferred []deferredSetting) *deferredSetting {
		for i := range deferred {
			if deferred[i].Name == resources.ClusterConnectTimeoutSetting {
				return &deferred[i]
			}
		}
		return nil
	}
	for _, tag := range []string{"5.26.31-enterprise", "2026.08.1-enterprise"} {
		c := minimalCluster("c", "default")
		c.Spec.Image.Tag = tag
		conf := resources.BuildConfigMapForEnterprise(c).Data["neo4j.conf"]
		d := find(clusterDeferredSettings(c, conf))
		if d == nil || d.Want != "5s" || d.Supported != nil || !d.supports("") {
			t.Errorf("%s: the operator's 5s must be deferred for every version, got %+v", tag, d)
		}

		c.Spec.Config = map[string]string{resources.ClusterConnectTimeoutSetting: "10s"}
		conf = resources.BuildConfigMapForEnterprise(c).Data["neo4j.conf"]
		if d := find(clusterDeferredSettings(c, conf)); d == nil || d.Want != "10s" {
			t.Errorf("%s: a spec.config value is applied live too, so it is deferred, got %+v", tag, d)
		}
	}
	calver := minimalCluster("c", "default")
	calver.Spec.Image.Tag = "2026.08.1-enterprise"
	if got := clusterDeferredSettings(calver, "db.transaction.timeout=30s\n"); len(got) != 0 {
		t.Errorf("a conf without the setting defers nothing, got %v", got)
	}
}

// restartPendingFixture is a CalVer cluster with two server pods in a fake
// client, run through the real ConfigMap reconcile and the RestartPending
// pass. CalVer without TLS defers nothing else, so the connect timeout alone
// decides the condition.
type restartPendingFixture struct {
	t       *testing.T
	ctx     context.Context
	fc      client.Client
	name    string
	applier *fakeClusterApplier
}

func newRestartPendingFixture(t *testing.T, name string, existing *corev1.ConfigMap, started time.Time) *restartPendingFixture {
	t.Helper()
	cluster := minimalCluster(name, "default")
	cluster.Spec.Image.Tag = "2026.08.1-enterprise"
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	objs := []client.Object{cluster, serverSTS(name, "default")}
	if existing != nil {
		objs = append(objs, existing)
	}
	for i := range 2 {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-server-%d", name, i), Namespace: "default", Labels: resources.ServerPodSelector(name)},
			Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{neo4jRunningSince(started)}},
		})
	}
	return &restartPendingFixture{
		t: t, ctx: context.Background(), name: name, applier: &fakeClusterApplier{applied: true},
		fc: fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).WithStatusSubresource(cluster).Build(),
	}
}

func neo4jRunningSince(started time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: "neo4j", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}}}
}

func (f *restartPendingFixture) cluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	f.t.Helper()
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := f.fc.Get(f.ctx, types.NamespacedName{Name: f.name, Namespace: "default"}, c); err != nil {
		f.t.Fatal(err)
	}
	return c
}

// reconcileConfigMap runs the cluster's ConfigMap reconcile.
func (f *restartPendingFixture) reconcileConfigMap() {
	f.t.Helper()
	cm := NewConfigMapManager(f.fc)
	cm.LiveConfig = f.applier
	if err := cm.ReconcileConfigMap(f.ctx, f.cluster()); err != nil {
		f.t.Fatal(err)
	}
}

// restartPending runs the RestartPending pass and returns the condition. A
// setting no version gates needs no connection to any server.
func (f *restartPendingFixture) restartPending() *metav1.Condition {
	f.t.Helper()
	r := &Neo4jEnterpriseClusterReconciler{Client: f.fc, Scheme: newTestScheme()}
	r.RestartPendingChecker = &restartPendingChecker{dial: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (versionReader, error) {
		f.t.Error("no server's version is needed for the connect timeout; nothing may connect")
		return &fakeVersionReader{version: "2026.08.1"}, nil
	}}
	r.reconcileRestartPending(f.ctx, f.cluster())
	return meta.FindStatusCondition(f.cluster().Status.Conditions, ConditionTypeRestartPending)
}

// restartServer gives server i a neo4j container that started at the time given.
func (f *restartPendingFixture) restartServer(i int, at time.Time) {
	f.t.Helper()
	pod := &corev1.Pod{}
	if err := f.fc.Get(f.ctx, types.NamespacedName{Name: fmt.Sprintf("%s-server-%d", f.name, i), Namespace: "default"}, pod); err != nil {
		f.t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{neo4jRunningSince(at)}
	if err := f.fc.Status().Update(f.ctx, pod); err != nil {
		f.t.Fatal(err)
	}
}

func (f *restartPendingFixture) serverTemplate() map[string]string {
	f.t.Helper()
	sts := &appsv1.StatefulSet{}
	if err := f.fc.Get(f.ctx, types.NamespacedName{Name: f.name + "-server", Namespace: "default"}, sts); err != nil {
		f.t.Fatal(err)
	}
	return sts.Spec.Template.Annotations
}

// A cluster created with the setting already in its conf: its servers start
// after the ConfigMap carried it, so nothing awaits a restart.
func TestRestartPending_ConnectTimeout_FreshCluster(t *testing.T) {
	f := newRestartPendingFixture(t, "rpfresh", nil, time.Now().Add(time.Minute))
	f.reconcileConfigMap()
	c := f.restartPending()
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != ConditionReasonNoSettingsAwaitRestart {
		t.Fatalf("a fresh cluster must read RestartPending=False, got %+v", c)
	}
}

// A cluster whose conf already has the setting (v1.21.x) gets the record
// without a restart; RestartPending then names every server that started
// before it, and each drops out as it restarts.
func TestRestartPending_ConnectTimeout_UpgradeNamesTheServersUntilTheyRestart(t *testing.T) {
	before := minimalCluster("rpup", "default")
	before.Spec.Image.Tag = "2026.08.1-enterprise"
	existing := resources.BuildConfigMapForEnterprise(before) // as v1.21.x wrote it: no record
	f := newRestartPendingFixture(t, "rpup", existing, time.Now().Add(-time.Hour))

	f.reconcileConfigMap()
	if v, ok := f.serverTemplate()["neo4j.neo4j.com/config-restart"]; ok || f.applier.calls != 0 {
		t.Fatalf("recording the setting must neither restart nor re-apply anything; config-restart=%q, applies=%d", v, f.applier.calls)
	}
	c := f.restartPending()
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != ConditionReasonSettingsAwaitRestart ||
		!strings.Contains(c.Message, "rpup-server-0, rpup-server-1 will apply "+resources.ClusterConnectTimeoutSetting) {
		t.Fatalf("both servers started with the old timeout and must be named, got %+v", c)
	}

	f.restartServer(1, time.Now().Add(time.Minute))
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionTrue ||
		!strings.Contains(c.Message, "rpup-server-0 will apply") || strings.Contains(c.Message, "rpup-server-1") {
		t.Fatalf("server-1 restarted and must drop out, got %+v", c)
	}
	f.restartServer(0, time.Now().Add(time.Minute))
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("every server restarted: RestartPending must be False, got %+v", c)
	}
}

// A spec.config change of the setting is applied live, as before, and names
// the servers again: their Raft connections still use the value they started
// with.
func TestRestartPending_ConnectTimeout_SpecConfigChangeAppliedLive(t *testing.T) {
	before := minimalCluster("rpcfg", "default")
	before.Spec.Image.Tag = "2026.08.1-enterprise"
	existing := resources.BuildConfigMapForEnterprise(before)
	stampDeferredSettings(existing, nil, before, time.Now().Add(-2*time.Hour))
	f := newRestartPendingFixture(t, "rpcfg", existing, time.Now().Add(-time.Hour))
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("servers started after the record are not waiting, got %+v", c)
	}

	cluster := f.cluster()
	cluster.Spec.Config = map[string]string{resources.ClusterConnectTimeoutSetting: "10s"}
	if err := f.fc.Update(f.ctx, cluster); err != nil {
		t.Fatal(err)
	}
	f.reconcileConfigMap()
	if f.applier.got[resources.ClusterConnectTimeoutSetting] != "10s" {
		t.Fatalf("the change must still be applied live, applier got %v", f.applier.got)
	}
	if v, ok := f.serverTemplate()["neo4j.neo4j.com/config-restart"]; ok {
		t.Fatalf("a change applied live must not restart the servers; config-restart=%q", v)
	}
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "rpcfg-server-0, rpcfg-server-1") {
		t.Fatalf("both servers' Raft connections still use 5s and must be named, got %+v", c)
	}
}
