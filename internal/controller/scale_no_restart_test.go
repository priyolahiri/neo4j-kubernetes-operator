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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// reconcileScale renders the ConfigMap for `before`, then reconciles `after`
// against it, and reports whether the servers were restarted and the events.
func reconcileScale(t *testing.T, before, after *neo4jv1beta1.Neo4jEnterpriseCluster, existing *corev1.ConfigMap) (restarted bool, written *corev1.ConfigMap, events []string) {
	t.Helper()
	ctx := context.Background()
	if existing == nil {
		existing = resources.BuildConfigMapForEnterprise(before)
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(after, serverSTS(after.Name, "default"), existing).Build()
	rec := record.NewFakeRecorder(10)
	cm := NewConfigMapManager(fc).WithRecorder(rec)
	cm.LiveConfig = nil
	if err := cm.ReconcileConfigMap(ctx, after); err != nil {
		t.Fatalf("ReconcileConfigMap: %v", err)
	}
	sts := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: after.Name + "-server", Namespace: "default"}, sts); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	written = &corev1.ConfigMap{}
	if err := fc.Get(ctx, types.NamespacedName{Name: after.Name + "-config", Namespace: "default"}, written); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	close(rec.Events)
	for e := range rec.Events {
		events = append(events, e)
	}
	return sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"] != "", written, events
}

func scaledCluster(name, tag string, servers int32) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := minimalCluster(name, "default")
	c.Spec.Image.Tag = tag
	c.Spec.Topology.Servers = servers
	return c
}

// TestScale_RestartDecision pins #467 on both lines: a scale-up that adds only
// system secondaries, or a scale-down, writes the new endpoint list but
// restarts nothing; a scale-up that adds a system primary restarts, so Neo4j's
// discovery rule (every system primary in each server's list) holds.
func TestScale_RestartDecision(t *testing.T) {
	for _, tag := range []string{"5.26-enterprise", "2026.08.1-enterprise"} {
		for _, tc := range []struct {
			from, to    int32
			wantRestart bool
		}{
			{3, 4, false},
			{3, 6, false},
			{5, 3, false},
			{2, 3, true}, // server-2 is a system primary
			{2, 4, true},
		} {
			name := fmt.Sprintf("%s/%dto%d", tag, tc.from, tc.to)
			t.Run(name, func(t *testing.T) {
				before := scaledCluster("s467", tag, tc.from)
				after := scaledCluster("s467", tag, tc.to)
				restarted, written, events := reconcileScale(t, before, after, nil)
				if restarted != tc.wantRestart {
					t.Errorf("restarted=%v, want %v", restarted, tc.wantRestart)
				}
				lastServer := fmt.Sprintf("s467-server-%d.s467-headless", tc.to-1)
				if !strings.Contains(written.Data["startup.sh"], lastServer) {
					t.Errorf("the new endpoint list must be written; %s missing", lastServer)
				}
				scaledUpQuietly := !tc.wantRestart && tc.to > tc.from
				if got := len(events) == 1 && strings.Contains(events[0], EventReasonScaledWithoutRestart); got != scaledUpQuietly {
					t.Errorf("ScaledWithoutRestart event=%v, want %v (events %v)", got, scaledUpQuietly, events)
				}
			})
		}
	}
}

// TestScale_OtherChangeStillRestarts: a scale-up that also changes a static
// setting restarts — only the membership lines may change without one.
func TestScale_OtherChangeStillRestarts(t *testing.T) {
	before := scaledCluster("s467b", "2026.08.1-enterprise", 3)
	after := scaledCluster("s467b", "2026.08.1-enterprise", 4)
	after.Spec.Config = map[string]string{"db.tx_log.buffer.size": "2MiB"}
	if restarted, _, _ := reconcileScale(t, before, after, nil); !restarted {
		t.Error("a scale-up with another change must restart")
	}
}

// TestUpgrade_SystemDatabaseModeBlockRestartsNothing: an existing cluster's
// ConfigMap, rendered before the system database role block existed, takes the
// new script on operator upgrade without a restart — on both lines.
func TestUpgrade_SystemDatabaseModeBlockRestartsNothing(t *testing.T) {
	for _, tag := range []string{"5.26-enterprise", "2026.08.1-enterprise"} {
		t.Run(tag, func(t *testing.T) {
			cluster := scaledCluster("u467", tag, 5)
			old := resources.BuildConfigMapForEnterprise(cluster)
			old.Data["startup.sh"] = withoutRestartNeutralSections(old.Data["startup.sh"])
			if strings.Contains(old.Data["startup.sh"], "OPERATOR_SYSTEM_") {
				t.Fatal("test premise: the old script must not carry the block")
			}
			restarted, written, _ := reconcileScale(t, cluster, cluster, old)
			if restarted {
				t.Error("adding the system database role block must not restart the servers")
			}
			if !strings.Contains(written.Data["startup.sh"], "OPERATOR_SYSTEM_PRIMARIES=3") {
				t.Error("the new script must still be written for the next restart")
			}
		})
	}
}

// withoutRestartNeutralSections renders a startup script as an operator from
// before #467 did: without the restart-neutral sections.
func withoutRestartNeutralSections(script string) string {
	var kept []string
	in := false
	for _, line := range strings.Split(script, "\n") {
		switch strings.TrimSpace(line) {
		case resources.RestartNeutralBegin:
			in = true
			continue
		case resources.RestartNeutralEnd:
			in = false
			continue
		}
		if !in {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}
