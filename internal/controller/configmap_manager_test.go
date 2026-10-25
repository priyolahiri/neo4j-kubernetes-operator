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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = neo4jv1beta1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = networkingv1.AddToScheme(s)
	return s
}

func minimalCluster(name, ns string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 2},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
}

func configMapWithData(name, ns string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       data,
	}
}

func int32PtrCM(v int32) *int32 { return &v }

func serverSTS(clusterName, ns string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-server",
			Namespace: ns,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32PtrCM(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:5.26.0-enterprise"}},
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": clusterName},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// TestCalculateConfigMapHash
// ---------------------------------------------------------------------------

func TestCalculateConfigMapHash(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	cm1 := configMapWithData("a", "ns", map[string]string{
		"neo4j.conf": "server.bolt.enabled=true\n",
	})
	cm2 := configMapWithData("b", "ns", map[string]string{
		"neo4j.conf": "server.bolt.enabled=true\n",
	})
	cm3 := configMapWithData("c", "ns", map[string]string{
		"neo4j.conf": "server.bolt.enabled=false\n",
	})

	h1 := cm.calculateConfigMapHash(cm1)
	h2 := cm.calculateConfigMapHash(cm2)
	h3 := cm.calculateConfigMapHash(cm3)

	if h1 != h2 {
		t.Errorf("identical ConfigMaps must produce the same hash: %q vs %q", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("different ConfigMaps must produce different hashes")
	}
	// Hash is deterministic across two calls on the same object
	if cm.calculateConfigMapHash(cm1) != h1 {
		t.Errorf("hash must be deterministic")
	}
}

func TestCalculateConfigMapHash_MultipleKeys(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	// Key order in the map must not affect the hash (we hash over a fixed key order)
	a := configMapWithData("a", "ns", map[string]string{
		"neo4j.conf": "key=val\n",
		"startup.sh": "#!/bin/bash\n",
	})
	b := configMapWithData("b", "ns", map[string]string{
		"startup.sh": "#!/bin/bash\n",
		"neo4j.conf": "key=val\n",
	})
	if cm.calculateConfigMapHash(a) != cm.calculateConfigMapHash(b) {
		t.Error("hash should be order-independent because keys are iterated in fixed order")
	}
}

// ---------------------------------------------------------------------------
// TestNormalizeNeo4jConf
// ---------------------------------------------------------------------------

func TestNormalizeNeo4jConf_DeduplicatesKeys(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	input := "key=first\nkey=second\n"
	output := cm.normalizeNeo4jConf(input)

	if output == input {
		// Should have removed the second occurrence
		t.Error("normalizer should remove duplicate keys")
	}
	// Only the first occurrence should remain
	count := 0
	for _, line := range splitLines(output) {
		if line == "key=first" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 'key=first' line, got %d", count)
	}
}

// A comment or a blank line in neo4j.conf means nothing to Neo4j, so it must not
// reach the restart hash (#465).
func TestNormalizeNeo4jConf_IgnoresCommentsAndBlankLines(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	a := cm.normalizeNeo4jConf("# This is a comment\nkey=value\n")
	b := cm.normalizeNeo4jConf("# Reworded comment\n\n   # indented comment\nkey=value\n\n")
	if a != b {
		t.Errorf("comment-only difference changed the normalised conf: %q vs %q", a, b)
	}
	if containsLine(a, "# This is a comment") {
		t.Error("comments must not be part of the normalised conf")
	}
	if !containsLine(a, "key=value") {
		t.Error("settings must be kept")
	}
}

func TestNormalizeNeo4jConf_EmptyInput(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())
	out := cm.normalizeNeo4jConf("")
	if out != "" {
		t.Errorf("empty input should produce empty output, got %q", out)
	}
}

// ---------------------------------------------------------------------------
// TestNormalizeStartupScript
// ---------------------------------------------------------------------------

func TestNormalizeStartupScript_ReplacesRuntimeVars(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	input := "#!/bin/bash\nexport POD_ORDINAL=0\nexport HOSTNAME=pod-0\necho $(date)\nstatic line\n"
	output := cm.normalizeStartupScript(input)

	if containsLine(output, "export POD_ORDINAL=0") {
		t.Error("POD_ORDINAL line should be replaced")
	}
	if containsLine(output, "export HOSTNAME=pod-0") {
		t.Error("HOSTNAME line should be replaced")
	}
	if !containsLine(output, "static line") {
		t.Error("static lines should be preserved")
	}
	if !containsLine(output, runtimeVariablePlaceholder) {
		t.Error("replaced lines should contain the placeholder")
	}
	// The placeholder is not a comment, so adding a runtime line still counts.
	more := cm.normalizeStartupScript(input + "export HOSTNAME_FQDN=x\n")
	if more == output {
		t.Error("adding a runtime-variable line must still change the normalised script")
	}
}

// TestCalculateConfigMapHash_IgnoresCommentsAndBlankLines pins #465: rewording
// a comment, or adding blank lines, in any of the three files leaves the restart
// hash alone, while a trailing comment after a command or any real line still
// changes it.
func TestCalculateConfigMapHash_IgnoresCommentsAndBlankLines(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())
	base := map[string]string{
		"neo4j.conf": "# memory\nserver.memory.heap.max_size=1g\n",
		"startup.sh": "#!/bin/bash\n# start it\nset -e\nexec neo4j\n",
		"health.sh":  "#!/bin/bash\n# probe\ncurl -sf localhost:7474\n",
	}
	baseHash := cm.calculateConfigMapHash(configMapWithData("a", "ns", base))

	same := map[string]string{
		"neo4j.conf": "# heap and page cache\n\nserver.memory.heap.max_size=1g\n",
		"startup.sh": "#!/bin/bash\n\n# start Neo4j, reworded\n  # indented\nset -e\nexec neo4j\n",
		"health.sh":  "#!/bin/bash\n# probe the HTTP port\n\ncurl -sf localhost:7474\n",
	}
	if got := cm.calculateConfigMapHash(configMapWithData("b", "ns", same)); got != baseHash {
		t.Errorf("comment/blank-line changes must not change the restart hash")
	}

	for key, changed := range map[string]string{
		"neo4j.conf": "# memory\nserver.memory.heap.max_size=2g\n",
		"startup.sh": "#!/bin/bash\n# start it\nset -e\nexec neo4j # trailing note\n",
	} {
		data := map[string]string{}
		for k, v := range base {
			data[k] = v
		}
		data[key] = changed
		if cm.calculateConfigMapHash(configMapWithData("c", "ns", data)) == baseHash {
			t.Errorf("a real change to %s must change the restart hash", key)
		}
	}

	// health.sh reaches a server at its next restart, whatever changed in it
	// (operations.md id 181).
	data := map[string]string{}
	for k, v := range base {
		data[k] = v
	}
	data["health.sh"] = "#!/bin/bash\n# probe\ncurl -sf localhost:7475\n"
	if cm.calculateConfigMapHash(configMapWithData("d", "ns", data)) != baseHash {
		t.Error("a change to health.sh must not change the restart hash")
	}
}

// preReadinessHealthScript is the health.sh releases up to v1.22 rendered for
// a cluster server: the process, then anything answering on 7474, then a
// cluster-formation line in the log.
const preReadinessHealthScript = `#!/bin/bash
# Health check script for Neo4j clustering

# Check if Neo4j process is running
if ! (pgrep -f "EnterpriseEntryPoint" > /dev/null || pgrep -f "Neo4jEnterprise" > /dev/null); then
    echo "Neo4j process not running"
    exit 1
fi

# Try HTTP port check
if (echo > /dev/tcp/localhost/7474) >/dev/null 2>&1; then
    echo "Neo4j HTTP port responding - healthy"
    exit 0
fi

# If HTTP not responding, check if we're in cluster formation process
if grep -q "Resolved endpoints" /logs/neo4j.log 2>/dev/null || \
   grep -q "Starting.*cluster" /logs/neo4j.log 2>/dev/null || \
   grep -q "Waiting for.*servers" /logs/neo4j.log 2>/dev/null || \
   grep -q "minimum_initial_system_primaries_count" /logs/neo4j.log 2>/dev/null || \
   grep -q "cluster formation barrier" /logs/neo4j.log 2>/dev/null; then
    echo "Neo4j in cluster formation process - allowing more time"
    exit 0
fi

# If process is running but no clustering activity, fail
echo "Neo4j process running but HTTP port not responding and no cluster activity detected"
exit 1
`

// The operator upgrade that brings the readiness check of #564 writes the new
// health.sh to every cluster's ConfigMap and restarts no server: each picks it
// up at its next restart (operations.md id 181). Together with the probes, which
// are unchanged (TestServerProbes_Unchanged), no pod template moves.
func TestReconcileConfigMap_NewHealthScriptRestartsNothing(t *testing.T) {
	ctx := context.Background()
	cluster := minimalCluster("c564", "default")
	existing := resources.BuildConfigMapForEnterprise(cluster)
	if existing.Data["health.sh"] == preReadinessHealthScript {
		t.Fatal("test premise: the rendered health.sh is the new one")
	}
	existing.Data["health.sh"] = preReadinessHealthScript
	sts := serverSTS("c564", "default")
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster, sts, existing).Build()
	before := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c564-server", Namespace: "default"}, before); err != nil {
		t.Fatal(err)
	}

	if err := NewConfigMapManager(fc).ReconcileConfigMap(ctx, cluster); err != nil {
		t.Fatalf("ReconcileConfigMap: %v", err)
	}

	got := &corev1.ConfigMap{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c564-config", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Data["health.sh"] != resources.BuildConfigMapForEnterprise(cluster).Data["health.sh"] {
		t.Error("the new health.sh must be written, for each server's next restart")
	}
	after := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c564-server", Namespace: "default"}, after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("the StatefulSet was written (template annotations %v): the servers would restart",
			after.Spec.Template.Annotations)
	}
}

// rewordComments rewrites every full-line comment, as a new operator version
// that only edits comments in the rendered files would. The restart-neutral
// markers are left alone: they are structure, not prose (see their doc).
func rewordComments(content string) string {
	lines := splitLines(content)
	for i, l := range lines {
		if l == resources.RestartNeutralBegin || l == resources.RestartNeutralEnd {
			continue
		}
		if isCommentOrBlankLine(l) && len(l) > 0 && l[0] == '#' && (len(l) < 2 || l[1] != '!') {
			lines[i] = "# reworded: " + l
		}
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

// TestReconcileConfigMap_CommentOnlyChangeWritesWithoutRestart: the rendered
// startup script and neo4j.conf of a real cluster, with every comment reworded,
// are rewritten to the ConfigMap but the servers are not restarted (#465).
func TestReconcileConfigMap_CommentOnlyChangeWritesWithoutRestart(t *testing.T) {
	ctx := context.Background()
	cluster := minimalCluster("c465", "default")
	desired := resources.BuildConfigMapForEnterprise(cluster)
	if rewordComments(desired.Data["startup.sh"]) == desired.Data["startup.sh"] {
		t.Fatal("test premise: the rendered startup script has comments to reword")
	}
	existing := desired.DeepCopy()
	existing.Data["startup.sh"] = rewordComments(desired.Data["startup.sh"])
	existing.Data["neo4j.conf"] = rewordComments(desired.Data["neo4j.conf"])
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(cluster, serverSTS("c465", "default"), existing).Build()

	if err := NewConfigMapManager(fc).ReconcileConfigMap(ctx, cluster); err != nil {
		t.Fatalf("ReconcileConfigMap: %v", err)
	}

	got := &corev1.ConfigMap{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c465-config", Namespace: "default"}, got); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	if got.Data["startup.sh"] != desired.Data["startup.sh"] || got.Data["neo4j.conf"] != desired.Data["neo4j.conf"] {
		t.Error("the reworded files must still be written, so the next restart reads them")
	}
	sts := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c465-server", Namespace: "default"}, sts); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	if v, ok := sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"]; ok {
		t.Errorf("a comment-only change must not restart the servers; config-restart=%q", v)
	}
}

// TestReconcileConfigMap_SettingChangeRestarts: a changed setting still restarts.
func TestReconcileConfigMap_SettingChangeRestarts(t *testing.T) {
	ctx := context.Background()
	cluster := minimalCluster("c465b", "default")
	existing := resources.BuildConfigMapForEnterprise(cluster)
	existing.Data["neo4j.conf"] += "db.transaction.timeout=10s\n"
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(cluster, serverSTS("c465b", "default"), existing).Build()

	if err := NewConfigMapManager(fc).ReconcileConfigMap(ctx, cluster); err != nil {
		t.Fatalf("ReconcileConfigMap: %v", err)
	}
	sts := &appsv1.StatefulSet{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "c465b-server", Namespace: "default"}, sts); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	if sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"] == "" {
		t.Error("a changed setting must still restart the servers")
	}
}

// ---------------------------------------------------------------------------
// TestAnalyzeConfigChanges
// ---------------------------------------------------------------------------

func TestAnalyzeConfigChanges(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	old := configMapWithData("old", "ns", map[string]string{
		"neo4j.conf": "removed.key=yes\nshared.key=old\n",
	})
	newCM := configMapWithData("new", "ns", map[string]string{
		"neo4j.conf": "added.key=yes\nshared.key=new\n",
	})

	changes := cm.analyzeConfigChanges(old, newCM)

	found := func(substr string) bool {
		for _, c := range changes {
			if containsSubstr(c, substr) {
				return true
			}
		}
		return false
	}

	if !found("modified neo4j.conf") && !found("modified") {
		t.Errorf("expected 'modified neo4j.conf' in changes, got %v", changes)
	}

	// No changes scenario
	same := cm.analyzeConfigChanges(old, old)
	if len(same) != 1 || !containsSubstr(same[0], "no semantic differences") {
		t.Errorf("identical ConfigMaps should report 'no semantic differences', got %v", same)
	}
}

// ---------------------------------------------------------------------------
// TestRequiresRestart
// ---------------------------------------------------------------------------

func TestRequiresRestart(t *testing.T) {
	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	if !cm.requiresRestart([]string{"modified neo4j.conf"}) {
		t.Error("property change should require restart")
	}
	if cm.requiresRestart([]string{"hash changed but no semantic differences detected"}) {
		t.Error("no-semantic-diff should not require restart")
	}
	if cm.requiresRestart([]string{}) {
		t.Error("empty change list should not require restart")
	}
}

// ---------------------------------------------------------------------------
// TestTriggerRollingRestartForConfigChange — regression guard for Issue #19
// ---------------------------------------------------------------------------

func TestTriggerRollingRestartForConfigChange_StampsAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := minimalCluster("mycluster", "default")
	sts := serverSTS("mycluster", "default")

	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, sts).
		Build()

	cm := NewConfigMapManager(fc)
	err := cm.triggerRollingRestartForConfigChange(context.Background(), cluster, "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify annotation was stamped on the correct StatefulSet
	updated := &appsv1.StatefulSet{}
	if err := fc.Get(context.Background(), types.NamespacedName{Name: "mycluster-server", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to fetch updated STS: %v", err)
	}
	if ann := updated.Spec.Template.Annotations["neo4j.neo4j.com/config-hash"]; ann != "abc123" {
		t.Errorf("expected config-hash annotation 'abc123', got %q", ann)
	}
	if updated.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"] == "" {
		t.Error("expected config-restart timestamp annotation to be set")
	}
}

func TestTriggerRollingRestartForConfigChange_MissingSTS(t *testing.T) {
	// No StatefulSet in the fake store — should return nil gracefully
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	cluster := minimalCluster("missing-cluster", "default")
	cm := NewConfigMapManager(fc)
	if err := cm.triggerRollingRestartForConfigChange(context.Background(), cluster, "hash"); err != nil {
		t.Errorf("expected nil when STS not found, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestHasMemoryConfigChanged
// ---------------------------------------------------------------------------

func TestHasMemoryConfigChanged(t *testing.T) {
	base := minimalCluster("c", "ns")

	other := minimalCluster("c", "ns")
	other.Spec.Config = map[string]string{
		"server.memory.heap.max_size": "2g",
	}

	cm := NewConfigMapManager(fake.NewClientBuilder().WithScheme(newTestScheme()).Build())

	if !cm.HasMemoryConfigChanged(base, other) {
		t.Error("expected memory change detected when heap config differs")
	}
	if cm.HasMemoryConfigChanged(base, base) {
		t.Error("expected no memory change when clusters are identical")
	}

	// Resource limit change
	withLimit := minimalCluster("c", "ns")
	withLimit.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	if !cm.HasMemoryConfigChanged(base, withLimit) {
		t.Error("expected memory change detected when resource limits differ")
	}
}

// ---------------------------------------------------------------------------
// TestReconcileConfigMap_Creates
// ---------------------------------------------------------------------------

func TestReconcileConfigMap_Creates(t *testing.T) {
	scheme := newTestScheme()
	cluster := minimalCluster("newcluster", "default")
	sts := serverSTS("newcluster", "default")

	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, sts).
		Build()

	cm := NewConfigMapManager(fc)
	err := cm.ReconcileConfigMap(context.Background(), cluster)
	if err != nil {
		t.Fatalf("ReconcileConfigMap returned error: %v", err)
	}

	// ConfigMap should now exist
	created := &corev1.ConfigMap{}
	if err := fc.Get(context.Background(), types.NamespacedName{Name: "newcluster-config", Namespace: "default"}, created); err != nil {
		t.Errorf("expected ConfigMap to be created, got error: %v", err)
	}
}

// The ConfigMap's owner reference names the operator's own kind. It was built
// from the cluster's TypeMeta, which a read through an uncached reader leaves
// empty, and the API server refuses an owner reference without an apiVersion
// and a kind (#538).
func TestSetOwnerReference_NeedsNoTypeMeta(t *testing.T) {
	cluster := minimalCluster("c", "default")
	cluster.UID = "cluster-uid"
	cluster.TypeMeta = metav1.TypeMeta{} // as an uncached read leaves it
	cm := &corev1.ConfigMap{}

	if err := setOwnerReference(cluster, cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.OwnerReferences) != 1 {
		t.Fatalf("want one owner reference, got %v", cm.OwnerReferences)
	}
	ref := cm.OwnerReferences[0]
	if ref.APIVersion != neo4jv1beta1.GroupVersion.String() || ref.Kind != "Neo4jEnterpriseCluster" ||
		ref.Name != "c" || ref.UID != "cluster-uid" {
		t.Errorf("owner reference %+v does not name the cluster", ref)
	}
	if ref.Controller == nil || !*ref.Controller || ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Errorf("the cluster must be the controlling owner: %+v", ref)
	}
}

// ---------------------------------------------------------------------------
// Small string helpers (avoid importing strings in test without adding to prod)
// ---------------------------------------------------------------------------

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func containsLine(s, line string) bool {
	for _, l := range splitLines(s) {
		if l == line {
			return true
		}
	}
	return false
}

func containsSubstr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}
