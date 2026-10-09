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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// The log configuration is read from the ConfigMap mount and reloaded by
// Log4j, so a changed list must restart nothing: the cluster's restart hash
// ignores it. Turning the feature on or off changes neo4j.conf's static
// server.logs.config line, which does restart.
func TestClusterRestartHash_IgnoresServerLogsXML(t *testing.T) {
	cm := &ConfigMapManager{}
	cluster := minimalCluster("c", "default")
	cluster.Spec.Monitoring = &neo4jv1beta1.MonitoringSpec{Logs: &neo4jv1beta1.MonitoringLogsSpec{Stdout: []string{"query"}}}
	queryOnly := resources.BuildConfigMapForEnterprise(cluster)
	cluster.Spec.Monitoring.Logs.Stdout = []string{"query", "security", "debug"}
	all := resources.BuildConfigMapForEnterprise(cluster)
	if queryOnly.Data[resources.ServerLogsConfigKey] == all.Data[resources.ServerLogsConfigKey] {
		t.Fatal("the two lists must render different files")
	}
	if cm.calculateConfigMapHash(queryOnly) != cm.calculateConfigMapHash(all) {
		t.Error("changing the list must not change the restart hash")
	}

	cluster.Spec.Monitoring = nil
	off := resources.BuildConfigMapForEnterprise(cluster)
	if cm.calculateConfigMapHash(off) == cm.calculateConfigMapHash(all) {
		t.Error("turning the feature on or off must change the restart hash (server.logs.config is static)")
	}
}

func standaloneLogsConfigMap(t *testing.T, r *Neo4jEnterpriseStandaloneReconciler) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "sa-config", Namespace: "default"}, cm); err != nil {
		t.Fatal(err)
	}
	return cm
}

// The standalone writes the file and the setting when a log is listed, and
// removes both when none is — a server restarting without the setting must
// not find the operator's file.
func TestStandaloneConfigMap_StdoutLogs(t *testing.T) {
	r, _ := standaloneCMTestReconciler(t)
	ctx := context.Background()
	sa := standaloneForConf(nil)
	sa.Spec.Monitoring = &neo4jv1beta1.MonitoringSpec{Logs: &neo4jv1beta1.MonitoringLogsSpec{Stdout: []string{"query"}}}
	if err := r.reconcileConfigMap(ctx, sa); err != nil {
		t.Fatal(err)
	}
	cm := standaloneLogsConfigMap(t, r)
	if !strings.Contains(cm.Data[resources.ServerLogsConfigKey], `<Console name="QueryStdout"`) {
		t.Errorf("the log configuration must route the query log to stdout:\n%s", cm.Data[resources.ServerLogsConfigKey])
	}
	if !strings.Contains(cm.Data["neo4j.conf"], "server.logs.config=/conf/server-logs.xml") {
		t.Errorf("neo4j.conf must read it from the standalone's mount:\n%s", cm.Data["neo4j.conf"])
	}

	sa.Spec.Monitoring.Logs.Stdout = []string{"query", "security"}
	if err := r.reconcileConfigMap(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(standaloneLogsConfigMap(t, r).Data[resources.ServerLogsConfigKey], `<Console name="SecurityStdout"`) {
		t.Error("a changed list must reach the ConfigMap")
	}

	sa.Spec.Monitoring = nil
	if err := r.reconcileConfigMap(ctx, sa); err != nil {
		t.Fatal(err)
	}
	cm = standaloneLogsConfigMap(t, r)
	if _, ok := cm.Data[resources.ServerLogsConfigKey]; ok {
		t.Error("turned off: the log configuration must be removed")
	}
	if strings.Contains(cm.Data["neo4j.conf"], "server.logs.config") {
		t.Errorf("turned off: the setting must be removed:\n%s", cm.Data["neo4j.conf"])
	}
}
