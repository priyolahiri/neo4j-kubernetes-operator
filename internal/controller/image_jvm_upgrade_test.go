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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// withoutImageJVMBlock renders a cluster startup script as an operator before
// #594 did: without the restart-neutral section that copies the image's
// server.jvm.additional lines, the other sections as they are.
func withoutImageJVMBlock(script string) string {
	var kept, section []string
	in := false
	for _, line := range strings.Split(script, "\n") {
		switch {
		case strings.TrimSpace(line) == resources.RestartNeutralBegin:
			in, section = true, []string{line}
			continue
		case in && strings.TrimSpace(line) == resources.RestartNeutralEnd:
			in = false
			section = append(section, line)
			if !strings.Contains(strings.Join(section, "\n"), "OPERATOR_IMAGE_CONF_DIR") {
				kept = append(kept, section...)
			}
			continue
		case in:
			section = append(section, line)
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// The operator upgrade that adds the image's JVM arguments to an existing
// cluster's startup script restarts no server, on both lines: the block is
// restart-neutral, and each server runs the image's lines from its next start
// (operations.md id 212).
func TestUpgrade_ImageJVMBlockRestartsNothing(t *testing.T) {
	for _, tag := range []string{"5.26.32-enterprise", "2026.08.1-enterprise"} {
		t.Run(tag, func(t *testing.T) {
			cluster := scaledCluster("u594", tag, 3)
			old := resources.BuildConfigMapForEnterprise(cluster)
			old.Data["startup.sh"] = withoutImageJVMBlock(old.Data["startup.sh"])
			if strings.Contains(old.Data["startup.sh"], "OPERATOR_IMAGE_CONF_DIR") {
				t.Fatal("test premise: the old script must not carry the block")
			}
			restarted, written, _ := reconcileScale(t, cluster, cluster, old)
			if restarted {
				t.Error("adding the image's JVM arguments must not restart the servers")
			}
			if !strings.Contains(written.Data["startup.sh"], "OPERATOR_IMAGE_CONF_DIR") {
				t.Error("the new script must still be written for the next restart")
			}
		})
	}
}

// A cluster that sets spec.config server.jvm.additional: its ConfigMap from
// before v1.23.0 carries the line, the new one does not — the line now
// reaches the servers in NEO4J_server_jvm_additional, after the operator's
// list (operations.md id 208). The ConfigMap change restarts nothing; the
// variable's change on the pod template is the cluster's one roll.
func TestUpgrade_UserJVMLineLeavingTheConfRestartsNothingMore(t *testing.T) {
	cluster := scaledCluster("ujvm", "2026.08.1-enterprise", 3)
	cluster.Spec.Config = map[string]string{resources.JVMAdditionalSetting: "-XX:-UseStringDeduplication"}
	old := resources.BuildConfigMapForEnterprise(cluster)
	if strings.Contains(old.Data["neo4j.conf"], resources.JVMAdditionalSetting+"=") {
		t.Fatal("the new conf must not carry the line")
	}
	old.Data["neo4j.conf"] += "\n" + resources.JVMAdditionalSetting + "=-XX:-UseStringDeduplication\n"
	restarted, written, _ := reconcileScale(t, cluster, cluster, old)
	if restarted {
		t.Error("the line leaving neo4j.conf must not restart the servers: the variable's change rolls them once")
	}
	if strings.Contains(written.Data["neo4j.conf"], resources.JVMAdditionalSetting+"=") {
		t.Error("the new conf is written without the line")
	}
}

// A cluster whose spec.env takes NEO4J_server_jvm_additional from a Secret
// keeps spec.config's line in neo4j.conf (the operator cannot join the two),
// and a change of that line restarts the servers, as it always did.
func TestUpgrade_AJVMLineKeptInTheConfStillRestartsWhenItChanges(t *testing.T) {
	cluster := scaledCluster("ujvmsrc", "2026.08.1-enterprise", 3)
	cluster.Spec.Env = []corev1.EnvVar{{Name: resources.JVMAdditionalEnv, ValueFrom: &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "jvm"}, Key: "flags"}}}}
	cluster.Spec.Config = map[string]string{resources.JVMAdditionalSetting: "-XX:+HeapDumpOnOutOfMemoryError"}
	old := resources.BuildConfigMapForEnterprise(cluster)
	if !strings.Contains(old.Data["neo4j.conf"], resources.JVMAdditionalSetting+"=-XX:+HeapDumpOnOutOfMemoryError") {
		t.Fatal("test premise: the conf keeps the line")
	}
	if restarted, _, _ := reconcileScale(t, cluster, cluster, old.DeepCopy()); restarted {
		t.Error("the same line restarts nothing")
	}
	changed := cluster.DeepCopy()
	changed.Spec.Config[resources.JVMAdditionalSetting] = "-XX:+HeapDumpOnOutOfMemoryError -XX:MaxGCPauseMillis=100"
	if restarted, _, _ := reconcileScale(t, cluster, changed, old.DeepCopy()); !restarted {
		t.Error("a changed JVM line in neo4j.conf restarts the servers")
	}
}
