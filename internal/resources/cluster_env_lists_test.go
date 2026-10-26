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

package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// A cluster's spec.env lists are in its neo4j.conf, joined with its
// spec.config's and Aura Fleet Management's, and not on its StatefulSet
// (operations.md id 209): the variable is the plugins', and the servers'
// startup script joins the two. One from a Secret stays a variable.
func TestBuildClusterSpecEnvLists(t *testing.T) {
	fromSecret := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}
	cluster := systemModeCluster("5.26.32-enterprise", 3)
	cluster.Spec.Config = map[string]string{UnrestrictedSetting: "x.*"}
	cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	cluster.Spec.Env = []corev1.EnvVar{
		{Name: unrestrictedEnv, Value: "custom.*"},
		{Name: allowlistEnv, Value: "apoc.*, custom.*"},
		{Name: "NEO4J_dbms_security_procedures_denylist", Value: ""},
		{Name: "NEO4J_dbms_security_http__auth__allowlist", ValueFrom: fromSecret},
		{Name: "NEO4J_server_memory_pagecache_size", Value: "256m"},
	}
	conf := Neo4jConfSettings(BuildConfigMapForEnterprise(cluster).Data["neo4j.conf"])
	for key, want := range map[string]string{
		UnrestrictedSetting: "x.*,custom.*,fleetManagement.*",
		AllowlistSetting:    "apoc.*,custom.*,fleetManagement.*",
	} {
		if conf[key] != want {
			t.Errorf("%s=%q in neo4j.conf, want %q", key, conf[key], want)
		}
	}
	for _, key := range []string{"dbms.security.procedures.denylist", "dbms.security.http_auth_allowlist"} {
		if _, set := conf[key]; set {
			t.Errorf("%s in neo4j.conf: an empty variable adds nothing, and one from a Secret cannot be read", key)
		}
	}

	env := map[string]corev1.EnvVar{}
	for _, e := range BuildServerStatefulSetForEnterprise(cluster).Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e
	}
	for _, name := range []string{unrestrictedEnv, allowlistEnv, "NEO4J_dbms_security_procedures_denylist"} {
		if _, ok := env[name]; ok {
			t.Errorf("%s on the StatefulSet: the conf holds it, and the variable is the plugins'", name)
		}
	}
	if e, ok := env["NEO4J_dbms_security_http__auth__allowlist"]; !ok || e.ValueFrom == nil {
		t.Error("a list from a Secret stays a variable")
	}
	if e := env["NEO4J_server_memory_pagecache_size"]; e.Value != "256m" {
		t.Error("any other variable stays")
	}
	if !OwnsUnrestrictedList(cluster.Spec.Config, cluster.Spec.Env) {
		t.Error("the cluster sets the unrestricted list itself: no image entries")
	}
}

// Through the servers' start: the conf, the startup block, then the
// entrypoint with a plugin's variable. The spec.env list and the spec.config
// list end the same: the cluster's entries, Aura Fleet Management's and the
// plugin's.
func TestServerStart_AClustersSpecEnvListJoinsAPluginsList(t *testing.T) {
	r5 := entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}
	for name, inEnv := range map[string]bool{"spec.config": false, "spec.env": true} {
		t.Run(name, func(t *testing.T) {
			cluster := systemModeCluster("5.26.32-enterprise", 3)
			cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
			if inEnv {
				cluster.Spec.Env = []corev1.EnvVar{{Name: unrestrictedEnv, Value: "custom.*"}}
			} else {
				cluster.Spec.Config = map[string]string{UnrestrictedSetting: "custom.*"}
			}
			cm := BuildConfigMapForEnterprise(cluster)
			// The pod's variables: what the StatefulSet carries of the
			// cluster's, and GDS's list, which the plugin writes.
			vars := map[string]string{"NEO4J_PLUGINS": `["apoc","graph-data-science","fleet-management"]`,
				unrestrictedEnv: "gds.*,apoc.load.*"}
			for _, e := range BuildServerStatefulSetForEnterprise(cluster).Spec.Template.Spec.Containers[0].Env {
				if e.Name == unrestrictedEnv {
					vars[e.Name] = e.Value // the cluster's, in place of the plugin's
				}
			}
			started := entrypointsim.Start(t, r5, cm.Data["neo4j.conf"], buildPluginListMergeBlock(cluster.Spec.Config, cluster.Spec.Env), vars)
			got, lines := entrypointsim.Setting(started, UnrestrictedSetting)
			if lines != 1 || sortedTokens(got) != "apoc.load.*,custom.*,fleetManagement.*,gds.*" {
				t.Errorf("unrestricted %q (%d lines), want custom.*, fleetManagement.*, gds.* and apoc.load.*", got, lines)
			}
		})
	}
}
