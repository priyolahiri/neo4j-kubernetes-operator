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

// TestStampRestartEffectiveSettings pins rule 137's record: only a change of
// the value on running servers is recorded, it keeps its since until the next
// change, and it never touches the startup-script record.
func TestStampRestartEffectiveSettings(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	conf := func(value string) *corev1.ConfigMap {
		data := "db.transaction.timeout=30s\n"
		if value != "" {
			data += resources.ClusterConnectTimeoutSetting + "=" + value + "\n"
		}
		return &corev1.ConfigMap{Data: map[string]string{"neo4j.conf": data}}
	}
	record := func(cm *corev1.ConfigMap) (string, string) {
		return cm.Annotations[restartEffectiveSettingsAnnotation], cm.Annotations[restartEffectiveSettingsSinceAnnotation]
	}

	if desired := conf("5s"); stampRestartEffectiveSettings(desired, nil, t0) || len(desired.Annotations) != 0 {
		t.Errorf("a new ConfigMap records nothing: its servers start with it, got %v", desired.Annotations)
	}
	if desired := conf("5s"); stampRestartEffectiveSettings(desired, conf("5s"), t0) || len(desired.Annotations) != 0 {
		t.Errorf("an unchanged value (the v1.21.x upgrade) records nothing, got %v", desired.Annotations)
	}

	added := conf("5s")
	if !stampRestartEffectiveSettings(added, conf(""), t0) {
		t.Fatal("an added line (the v1.20.x upgrade) must be recorded and written")
	}
	if names, since := record(added); names != resources.ClusterConnectTimeoutSetting+"=5s" || since != "2026-10-01T12:00:00Z" {
		t.Fatalf("got record %q since %q", names, since)
	}
	if got := restartEffectiveRecord(added.Annotations); len(got) != 1 || got[0].Name != resources.ClusterConnectTimeoutSetting || got[0].Supported != nil {
		t.Errorf("the record names the setting for every version, got %+v", got)
	}

	kept := conf("5s")
	if stampRestartEffectiveSettings(kept, added, t1) {
		t.Error("an unchanged value keeps the record and needs no write")
	}
	if _, since := record(kept); since != "2026-10-01T12:00:00Z" {
		t.Errorf("the record keeps its since, got %q", since)
	}

	edited := conf("10s")
	if !stampRestartEffectiveSettings(edited, kept, t1) {
		t.Fatal("a spec.config edit must be recorded")
	}
	if names, since := record(edited); names != resources.ClusterConnectTimeoutSetting+"=10s" || since != "2026-10-01T13:00:00Z" {
		t.Errorf("a later change moves the since, got record %q since %q", names, since)
	}

	removed := conf("")
	stampRestartEffectiveSettings(removed, edited, t1.Add(time.Hour))
	if names, _ := record(removed); names != resources.ClusterConnectTimeoutSetting+"=" {
		t.Errorf("a removed line resets Neo4j's default live: recorded with an empty value, got %q", names)
	}

	withStartup := func(value string) *corev1.ConfigMap {
		cm := conf(value)
		cm.Annotations = map[string]string{deferredSettingsAnnotation: "x=true", deferredSettingsSinceAnnotation: "2026-01-01T00:00:00Z"}
		return cm
	}
	desired := withStartup("10s")
	stampRestartEffectiveSettings(desired, withStartup("5s"), t1)
	if desired.Annotations[deferredSettingsAnnotation] != "x=true" || desired.Annotations[deferredSettingsSinceAnnotation] != "2026-01-01T00:00:00Z" {
		t.Errorf("the startup-script record is not this stamp's to change, got %v", desired.Annotations)
	}
}

const (
	ltsTag    = "5.26.31-enterprise"
	calverTag = "2026.08.1-enterprise"
)

// restartPendingFixture is a cluster with two server pods in a fake client,
// run through the real ConfigMap reconcile and the RestartPending pass.
type restartPendingFixture struct {
	t       *testing.T
	ctx     context.Context
	fc      client.Client
	name    string
	tag     string
	applier *fakeClusterApplier
	dials   int
}

func newRestartPendingFixture(t *testing.T, name, tag string, existing *corev1.ConfigMap, started time.Time) *restartPendingFixture {
	t.Helper()
	cluster := minimalCluster(name, "default")
	cluster.Spec.Image.Tag = tag
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
		t: t, ctx: context.Background(), name: name, tag: tag, applier: &fakeClusterApplier{applied: true},
		fc: fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).WithStatusSubresource(cluster).Build(),
	}
}

// configMapAsWrittenBy is the cluster's ConfigMap as an earlier release left
// it: rendered for the tag, with or without the connect-timeout line, and the
// startup-script record (async Raft channels on 5.26) stamped at since.
func configMapAsWrittenBy(t *testing.T, name, tag string, withConnectTimeout bool, since time.Time) *corev1.ConfigMap {
	t.Helper()
	c := minimalCluster(name, "default")
	c.Spec.Image.Tag = tag
	cm := resources.BuildConfigMapForEnterprise(c)
	if !withConnectTimeout {
		var kept []string
		for _, line := range strings.Split(cm.Data["neo4j.conf"], "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), resources.ClusterConnectTimeoutSetting+"=") {
				kept = append(kept, line)
			}
		}
		cm.Data["neo4j.conf"] = strings.Join(kept, "\n")
	}
	if tag == ltsTag {
		cm.Annotations = map[string]string{
			deferredSettingsAnnotation:      resources.AsyncRaftChannelsSetting + "=true",
			deferredSettingsSinceAnnotation: since.UTC().Format(time.RFC3339),
		}
	}
	return cm
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

func (f *restartPendingFixture) configMap() *corev1.ConfigMap {
	f.t.Helper()
	cm := &corev1.ConfigMap{}
	if err := f.fc.Get(f.ctx, types.NamespacedName{Name: f.name + "-config", Namespace: "default"}, cm); err != nil {
		f.t.Fatal(err)
	}
	return cm
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

// restartPending runs the RestartPending pass and returns the condition.
func (f *restartPendingFixture) restartPending() *metav1.Condition {
	f.t.Helper()
	r := &Neo4jEnterpriseClusterReconciler{Client: f.fc, Scheme: newTestScheme()}
	version := strings.TrimSuffix(f.tag, "-enterprise")
	r.RestartPendingChecker = &restartPendingChecker{dial: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (versionReader, error) {
		f.dials++
		return &fakeVersionReader{version: version}, nil
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

// Upgrading from v1.21.x, whose ConfigMap already has the 5s line: the value
// does not change, so nothing is recorded and nobody is named — the servers
// of a cluster created on v1.21.x started with 5s. A 5.26 cluster's record of
// the async Raft setting keeps its since.
func TestRestartPending_ConnectTimeout_UnchangedValueNamesNobody(t *testing.T) {
	recorded := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	for _, tag := range []string{calverTag, ltsTag} {
		t.Run(tag, func(t *testing.T) {
			f := newRestartPendingFixture(t, "rpsame", tag, configMapAsWrittenBy(t, "rpsame", tag, true, recorded), time.Now().Add(-time.Hour))
			f.reconcileConfigMap()
			if v, ok := f.serverTemplate()["neo4j.neo4j.com/config-restart"]; ok || f.applier.calls != 0 {
				t.Fatalf("nothing changed: nothing may restart or be applied; config-restart=%q, applies=%d", v, f.applier.calls)
			}
			cm := f.configMap()
			if _, ok := cm.Annotations[restartEffectiveSettingsAnnotation]; ok {
				t.Errorf("an unchanged value records nothing, got %v", cm.Annotations)
			}
			if tag == ltsTag && cm.Annotations[deferredSettingsSinceAnnotation] != recorded.Format(time.RFC3339) {
				t.Errorf("the async Raft record must keep its since, got %v", cm.Annotations)
			}
			c := f.restartPending()
			switch tag {
			case calverTag:
				if c != nil {
					t.Errorf("nothing is deferred on this CalVer cluster: no condition, as before, got %+v", c)
				}
				if f.dials != 0 {
					t.Errorf("no setting here is version-gated; nothing may connect, got %d dials", f.dials)
				}
			case ltsTag:
				if c == nil || c.Status != metav1.ConditionFalse {
					t.Errorf("both servers started after the async Raft record and with 5s: nobody waits, got %+v", c)
				}
			}
		})
	}
}

// A new cluster records nothing for the connect timeout: its servers start
// with it. On CalVer without TLS nothing is deferred, so there is no
// condition, as before.
func TestRestartPending_ConnectTimeout_FreshCluster(t *testing.T) {
	f := newRestartPendingFixture(t, "rpfresh", calverTag, nil, time.Now().Add(time.Minute))
	f.reconcileConfigMap()
	if _, ok := f.configMap().Annotations[restartEffectiveSettingsAnnotation]; ok {
		t.Errorf("a new ConfigMap records nothing, got %v", f.configMap().Annotations)
	}
	if c := f.restartPending(); c != nil {
		t.Errorf("nothing awaits a restart on a new cluster, got %+v", c)
	}
}

// Upgrading from v1.20.x, whose ConfigMap has no connect-timeout line: the
// line is applied live, recorded, and every server that started before it is
// named until it restarts. On 5.26, the async Raft record keeps its since and
// a server that started after it is named for the connect timeout only.
func TestRestartPending_ConnectTimeout_ChangedValueNamesTheServersUntilTheyRestart(t *testing.T) {
	recorded := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	for _, tag := range []string{calverTag, ltsTag} {
		t.Run(tag, func(t *testing.T) {
			f := newRestartPendingFixture(t, "rpadd", tag, configMapAsWrittenBy(t, "rpadd", tag, false, recorded), time.Now().Add(-time.Hour))
			f.reconcileConfigMap()
			if f.applier.got[resources.ClusterConnectTimeoutSetting] != "5s" {
				t.Fatalf("the added line is applied live, as before; applier got %v", f.applier.got)
			}
			if v, ok := f.serverTemplate()["neo4j.neo4j.com/config-restart"]; ok {
				t.Fatalf("a change applied live must not restart the servers; config-restart=%q", v)
			}
			cm := f.configMap()
			if cm.Annotations[restartEffectiveSettingsAnnotation] != resources.ClusterConnectTimeoutSetting+"=5s" {
				t.Fatalf("the change must be recorded, got %v", cm.Annotations)
			}
			if tag == ltsTag && cm.Annotations[deferredSettingsSinceAnnotation] != recorded.Format(time.RFC3339) {
				t.Errorf("the async Raft record must keep its since, got %v", cm.Annotations)
			}

			c := f.restartPending()
			if c == nil || c.Status != metav1.ConditionTrue ||
				c.Message != "rpadd-server-0, rpadd-server-1 will apply "+resources.ClusterConnectTimeoutSetting+" at their next restart; the operator does not restart them for it" {
				t.Fatalf("both servers' Raft connections still use 30s; only the connect timeout is named, got %+v", c)
			}
			f.restartServer(1, time.Now().Add(time.Minute))
			if c := f.restartPending(); c == nil || c.Status != metav1.ConditionTrue ||
				!strings.Contains(c.Message, "rpadd-server-0 will apply") || strings.Contains(c.Message, "rpadd-server-1") {
				t.Fatalf("server-1 restarted and must drop out, got %+v", c)
			}
			f.restartServer(0, time.Now().Add(time.Minute))
			if c := f.restartPending(); c == nil || c.Status != metav1.ConditionFalse {
				t.Fatalf("every server restarted: RestartPending must be False, got %+v", c)
			}
		})
	}
}

// A spec.config edit of the setting is applied live, as before, and recorded:
// each edit moves the since, so servers that restarted after the previous
// edit are named again, and only until they restart.
func TestRestartPending_ConnectTimeout_SpecConfigChange(t *testing.T) {
	f := newRestartPendingFixture(t, "rpcfg", calverTag, configMapAsWrittenBy(t, "rpcfg", calverTag, true, time.Time{}), time.Now().Add(-time.Hour))
	setTimeout := func(value string) {
		t.Helper()
		cluster := f.cluster()
		cluster.Spec.Config = map[string]string{resources.ClusterConnectTimeoutSetting: value}
		if err := f.fc.Update(f.ctx, cluster); err != nil {
			t.Fatal(err)
		}
		f.reconcileConfigMap()
		if f.applier.got[resources.ClusterConnectTimeoutSetting] != value {
			t.Fatalf("the change must still be applied live, applier got %v", f.applier.got)
		}
		if v, ok := f.serverTemplate()["neo4j.neo4j.com/config-restart"]; ok {
			t.Fatalf("a change applied live must not restart the servers; config-restart=%q", v)
		}
	}

	setTimeout("10s")
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "rpcfg-server-0, rpcfg-server-1") {
		t.Fatalf("both servers' Raft connections still use 5s and must be named, got %+v", c)
	}

	// Let the record age, as if the edit were an hour old, and restart both
	// servers since: nobody waits.
	cm := f.configMap()
	cm.Annotations[restartEffectiveSettingsSinceAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if err := f.fc.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	f.restartServer(0, time.Now().Add(-30*time.Minute))
	f.restartServer(1, time.Now().Add(-30*time.Minute))
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("both servers restarted after the edit, got %+v", c)
	}

	setTimeout("20s")
	if c := f.restartPending(); c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "rpcfg-server-0, rpcfg-server-1") {
		t.Fatalf("a later edit moves the since and names them again, got %+v", c)
	}
}

// The condition message names the waiting servers in a sentence; one server
// must not read "fc-server-0 will apply … at their next restart".
func TestRestartPendingCondition_AgreesInNumber(t *testing.T) {
	_, _, one := restartPendingCondition(map[string][]string{"c-server-0": {"a.setting"}})
	if one != "c-server-0 will apply a.setting at its next restart; the operator does not restart it for that" {
		t.Errorf("one server: %q", one)
	}
	_, _, two := restartPendingCondition(map[string][]string{"c-server-1": {"a.setting"}, "c-server-0": {"a.setting"}})
	if two != "c-server-0, c-server-1 will apply a.setting at their next restart; the operator does not restart them for it" {
		t.Errorf("two servers: %q", two)
	}
}
