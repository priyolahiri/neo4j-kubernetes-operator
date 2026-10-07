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
	"errors"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestNeo4jConfChanges(t *testing.T) {
	old := "# header\ndb.transaction.timeout=30s\ndb.logs.query.enabled=INFO\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=2\n"
	cases := []struct {
		name string
		new  string
		want map[string]string
	}{
		{"comments only", "# reworded\n\ndb.transaction.timeout=30s\ndb.logs.query.enabled=INFO\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=2\n", map[string]string{}},
		{"changed and added", "db.transaction.timeout=60s\ndb.logs.query.enabled=INFO\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=2\ndb.lock.acquisition.timeout=5s\n",
			map[string]string{"db.transaction.timeout": "60s", "db.lock.acquisition.timeout": "5s"}},
		{"removed resets to default", "db.transaction.timeout=30s\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=2\n",
			map[string]string{"db.logs.query.enabled": ""}},
		// A repeatable setting is compared as a whole, so a JVM flag change is
		// never missed (it is static and so always restarts).
		{"repeatable setting", "db.transaction.timeout=30s\ndb.logs.query.enabled=INFO\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=3\n",
			map[string]string{"server.jvm.additional": "-Da=1,-Db=3"}},
		{"line that is not key=value", "db.transaction.timeout=30s\ndb.logs.query.enabled=INFO\nserver.jvm.additional=-Da=1\nserver.jvm.additional=-Db=2\ngarbage\n",
			map[string]string{"garbage": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := neo4jConfChanges(old, tc.new); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLiveConfigBlocker(t *testing.T) {
	dynamic := map[string]bool{"db.transaction.timeout": true, "server.memory.heap.max_size": false}
	if r := liveConfigBlocker(map[string]string{"db.transaction.timeout": "60s"}, dynamic, nil); r != "" {
		t.Errorf("a dynamic setting must be appliable, got %q", r)
	}
	if r := liveConfigBlocker(map[string]string{"server.memory.heap.max_size": "2g"}, dynamic, nil); !strings.Contains(r, "only at startup") {
		t.Errorf("a static setting must block, got %q", r)
	}
	if r := liveConfigBlocker(map[string]string{"dbms.security.oidc.x.issuer": "y"}, dynamic, nil); !strings.Contains(r, "not a setting the server reports") {
		t.Errorf("a setting the server does not report must block, got %q", r)
	}
	env := []corev1.EnvVar{{Name: resources.Neo4jSettingEnvVarName("db.transaction.timeout"), Value: "10s"}}
	if r := liveConfigBlocker(map[string]string{"db.transaction.timeout": "60s"}, dynamic, env); !strings.Contains(r, "environment variable") {
		t.Errorf("a setting overridden by env must block, got %q", r)
	}
}

// fakeSetter records what a live apply did on one server.
type fakeSetter struct {
	dynamic map[string]bool
	refuse  bool
	set     []string
	closed  bool
}

func (f *fakeSetter) SettingsDynamic(_ context.Context, names []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, n := range names {
		if v, ok := f.dynamic[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}

func (f *fakeSetter) SetConfiguration(_ context.Context, key, value string) error {
	if f.refuse {
		return errors.New("refused")
	}
	f.set = append(f.set, key+"="+value)
	return nil
}

func (f *fakeSetter) Close() error { f.closed = true; return nil }

func readyClusterWithSettledServers(name string, servers int32) (*neo4jv1beta1.Neo4jEnterpriseCluster, *appsv1.StatefulSet) {
	cluster := minimalCluster(name, "default")
	cluster.Spec.Topology.Servers = servers
	cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{AdminSecret: "admin"}
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	sts := serverSTS(name, "default")
	sts.Spec.Replicas = int32PtrCM(servers)
	sts.Status = appsv1.StatefulSetStatus{
		ReadyReplicas: servers, UpdatedReplicas: servers,
		CurrentRevision: "r1", UpdateRevision: "r1",
	}
	return cluster, sts
}

func TestBoltLiveConfigApplier(t *testing.T) {
	ctx := context.Background()
	changes := map[string]string{"db.transaction.timeout": "60s", "db.logs.query.threshold": ""}
	dyn := map[string]bool{"db.transaction.timeout": true, "db.logs.query.threshold": true, "server.memory.heap.max_size": false}

	newApplier := func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, setters map[string]*fakeSetter) *boltLiveConfigApplier {
		fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster, sts).WithStatusSubresource(sts).Build()
		a := newBoltLiveConfigApplier(fc)
		a.dial = func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (confSetter, error) {
			s, ok := setters[pod]
			if !ok {
				return nil, errors.New("no such pod")
			}
			return s, nil
		}
		return a
	}
	threeSetters := func(refuseOn string) map[string]*fakeSetter {
		m := map[string]*fakeSetter{}
		for _, p := range []string{"c-server-0", "c-server-1", "c-server-2"} {
			m[p] = &fakeSetter{dynamic: dyn, refuse: p == refuseOn}
		}
		return m
	}

	t.Run("applies every change on every server", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		setters := threeSetters("")
		applied, reason := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, changes)
		if !applied {
			t.Fatalf("expected a live apply, got reason %q", reason)
		}
		want := []string{"db.logs.query.threshold=", "db.transaction.timeout=60s"}
		for pod, s := range setters {
			if !reflect.DeepEqual(s.set, want) {
				t.Errorf("%s: set %v, want %v", pod, s.set, want)
			}
			if !s.closed {
				t.Errorf("%s: connection not closed", pod)
			}
		}
	})

	t.Run("not Ready restarts", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		cluster.Status.Phase = neo4jv1beta1.PhaseForming
		setters := threeSetters("")
		if applied, reason := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, changes); applied || reason == "" {
			t.Errorf("a cluster that is not Ready must not be changed live (applied=%v, reason=%q)", applied, reason)
		}
	})

	t.Run("a rollout in flight restarts", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		sts.Status.UpdateRevision = "r2"
		setters := threeSetters("")
		if applied, _ := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, changes); applied {
			t.Error("servers that are rolling must not be changed live")
		}
	})

	t.Run("a static setting restarts and changes nothing", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		setters := threeSetters("")
		withStatic := map[string]string{"db.transaction.timeout": "60s", "server.memory.heap.max_size": "2g"}
		applied, reason := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, withStatic)
		if applied || !strings.Contains(reason, "server.memory.heap.max_size") {
			t.Errorf("a static setting must block the live apply (applied=%v, reason=%q)", applied, reason)
		}
		for pod, s := range setters {
			if len(s.set) != 0 {
				t.Errorf("%s: nothing may be set when the change restarts anyway, got %v", pod, s.set)
			}
		}
	})

	t.Run("a refusing server restarts", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		setters := threeSetters("c-server-1")
		applied, reason := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, changes)
		if applied || !strings.Contains(reason, "c-server-1") {
			t.Errorf("a refusal must fall back to a restart naming the server (applied=%v, reason=%q)", applied, reason)
		}
	})

	t.Run("an unreachable server restarts", func(t *testing.T) {
		cluster, sts := readyClusterWithSettledServers("c", 3)
		setters := threeSetters("")
		delete(setters, "c-server-2")
		if applied, _ := newApplier(cluster, sts, setters).ApplyLive(ctx, cluster, changes); applied {
			t.Error("a server that cannot be reached must fall back to a restart")
		}
	})
}

// fakeClusterApplier stands in for the Bolt applier in ConfigMapManager tests.
type fakeClusterApplier struct {
	applied bool
	reason  string
	got     map[string]string
	calls   int
}

func (f *fakeClusterApplier) ApplyLive(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, changes map[string]string) (bool, string) {
	f.calls++
	f.got = changes
	return f.applied, f.reason
}

func reconcileWithApplier(t *testing.T, name string, before, after *neo4jv1beta1.Neo4jEnterpriseCluster, applier *fakeClusterApplier) (*corev1.ConfigMap, *appsv1.StatefulSet, []string) {
	t.Helper()
	ctx := context.Background()
	existing := resources.BuildConfigMapForEnterprise(before)
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(after, serverSTS(name, "default"), existing).Build()
	rec := record.NewFakeRecorder(10)
	cm := NewConfigMapManager(fc).WithRecorder(rec)
	cm.LiveConfig = applier
	if err := cm.ReconcileConfigMap(ctx, after); err != nil {
		t.Fatalf("ReconcileConfigMap: %v", err)
	}
	got := &corev1.ConfigMap{}
	if err := fc.Get(ctx, types.NamespacedName{Name: name + "-config", Namespace: "default"}, got); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	sts := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: name + "-server", Namespace: "default"}, sts); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	close(rec.Events)
	var events []string
	for e := range rec.Events {
		events = append(events, e)
	}
	return got, sts, events
}

// TestReconcileConfigMap_DynamicOnlyChangeAppliedLive pins #466: a spec.config
// change of a dynamic setting is applied to the servers, written to the
// ConfigMap, and restarts nothing.
func TestReconcileConfigMap_DynamicOnlyChangeAppliedLive(t *testing.T) {
	before := minimalCluster("c466", "default")
	before.Spec.Config = map[string]string{"db.transaction.timeout": "30s"}
	after := before.DeepCopy()
	after.Spec.Config["db.transaction.timeout"] = "60s"
	applier := &fakeClusterApplier{applied: true}

	cm, sts, events := reconcileWithApplier(t, "c466", before, after, applier)
	if !reflect.DeepEqual(applier.got, map[string]string{"db.transaction.timeout": "60s"}) {
		t.Errorf("applier got %v", applier.got)
	}
	if !strings.Contains(cm.Data["neo4j.conf"], "db.transaction.timeout=60s") {
		t.Error("the new value must be written for the next restart")
	}
	if v, ok := sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"]; ok {
		t.Errorf("a change applied live must not restart the servers; config-restart=%q", v)
	}
	if len(events) != 1 || !strings.Contains(events[0], EventReasonConfigAppliedLive) {
		t.Errorf("expected one %s event, got %v", EventReasonConfigAppliedLive, events)
	}
}

// TestReconcileConfigMap_LiveApplyRefusedRestarts: when the applier refuses,
// the servers restart as before and the event says why.
func TestReconcileConfigMap_LiveApplyRefusedRestarts(t *testing.T) {
	before := minimalCluster("c466b", "default")
	before.Spec.Config = map[string]string{"db.transaction.timeout": "30s"}
	after := before.DeepCopy()
	after.Spec.Config["db.transaction.timeout"] = "60s"
	applier := &fakeClusterApplier{reason: "the cluster is not Ready with every server up"}

	_, sts, events := reconcileWithApplier(t, "c466b", before, after, applier)
	if sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"] == "" {
		t.Error("a change that cannot be applied live must restart the servers")
	}
	if len(events) != 1 || !strings.Contains(events[0], EventReasonConfigNeedsRestart) || !strings.Contains(events[0], "not Ready") {
		t.Errorf("expected one %s event naming the reason, got %v", EventReasonConfigNeedsRestart, events)
	}
}

// TestReconcileConfigMap_StartupScriptChangeIsNeverAppliedLive: a change that
// also touches startup.sh runs only at container start, so it restarts without
// asking the applier.
func TestReconcileConfigMap_StartupScriptChangeIsNeverAppliedLive(t *testing.T) {
	before := minimalCluster("c466c", "default")
	before.Spec.Config = map[string]string{"db.transaction.timeout": "30s"}
	after := before.DeepCopy()
	after.Spec.Config["db.transaction.timeout"] = "60s"
	after.Spec.Topology.Servers = 3 // changes the startup script's endpoint list
	applier := &fakeClusterApplier{applied: true}

	_, sts, _ := reconcileWithApplier(t, "c466c", before, after, applier)
	if applier.calls != 0 {
		t.Error("the applier must not be asked when startup.sh changed too")
	}
	if sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"] == "" {
		t.Error("a startup-script change must restart the servers")
	}
}

// fakeStandaloneApplier stands in for the standalone Bolt applier.
type fakeStandaloneApplier struct {
	applied bool
	got     map[string]string
}

func (f *fakeStandaloneApplier) ApplyLive(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseStandalone, changes map[string]string) (bool, string) {
	f.got = changes
	if f.applied {
		return true, ""
	}
	return false, "the standalone is not Ready"
}

// TestStandaloneDynamicConfChange pins #466 on the standalone: a dynamic
// change applied live keeps the pod's stamp (no restart); one that cannot be
// applied re-stamps it.
func TestStandaloneDynamicConfChange(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		applied     bool
		wantRestart bool
	}{
		{"applied live keeps the pod", true, false},
		{"refused restarts the pod", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := standaloneCMTestReconciler(t)
			applier := &fakeStandaloneApplier{applied: tc.applied}
			r.LiveConfig = applier

			sa := standaloneForSTS("2026.08.1-enterprise")
			sa.Spec.Config = map[string]string{"db.transaction.timeout": "30s"}
			if err := r.reconcileConfigMap(ctx, sa); err != nil {
				t.Fatalf("configmap: %v", err)
			}
			if err := r.reconcileStatefulSet(ctx, sa); err != nil {
				t.Fatalf("sts: %v", err)
			}
			stamp := getSTS(t, r).Spec.Template.Annotations[standaloneConfigHashAnnotation]

			sa.Spec.Config["db.transaction.timeout"] = "60s"
			if err := r.reconcileConfigMap(ctx, sa); err != nil {
				t.Fatalf("configmap 2: %v", err)
			}
			if err := r.reconcileStatefulSet(ctx, sa); err != nil {
				t.Fatalf("sts 2: %v", err)
			}
			if !reflect.DeepEqual(applier.got, map[string]string{"db.transaction.timeout": "60s"}) {
				t.Errorf("applier got %v", applier.got)
			}
			restamped := getSTS(t, r).Spec.Template.Annotations[standaloneConfigHashAnnotation] != stamp
			if restamped != tc.wantRestart {
				t.Errorf("restarted=%v, want %v", restamped, tc.wantRestart)
			}
		})
	}
}
