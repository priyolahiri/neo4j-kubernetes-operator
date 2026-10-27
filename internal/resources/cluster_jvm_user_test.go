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

// A cluster user's own server.jvm.additional adds to the operator's JVM list,
// after it, instead of replacing it (docs/knowledge/operations.md id 208).
// Until v1.23.0 a spec.config line became the whole of
// NEO4J_server_jvm_additional — the operator's G1 pause target, string
// deduplication and ExitOnOutOfMemoryError gone, and the truststore flags with
// them — and it was also a conf line, so it was given twice.

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// operatorJVMList is buildJVMSettings for a cluster that sets nothing: what
// every cluster that sets no server.jvm.additional has run since before
// v1.23.0, so its pod template is unchanged.
const operatorJVMList = "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled " +
	"-XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions -XX:G1NewSizePercent=2 " +
	"-XX:G1MaxNewSizePercent=10 -XX:+G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=45 " +
	"-XX:+UseCompressedOops -XX:+UseCompressedClassPointers -XX:+UseStringDeduplication -XX:+ExitOnOutOfMemoryError"

const truststoreFlags = "-Djavax.net.ssl.trustStore=/truststore/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"

func jvmCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	c.Name, c.Namespace = "c", "default"
	c.Spec.Image = neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"}
	c.Spec.Topology.Servers = 3
	return c
}

// jvmEnv returns the NEO4J_server_jvm_additional entries of the server pod.
func jvmEnv(c *neo4jv1beta1.Neo4jEnterpriseCluster) []corev1.EnvVar {
	var out []corev1.EnvVar
	for _, e := range BuildPodSpecForEnterprise(c, "server", "neo4j-admin-secret").Containers[0].Env {
		if e.Name == JVMAdditionalEnv {
			out = append(out, e)
		}
	}
	return out
}

func TestBuildJVMSettings_UserFlagsComeAfterTheOperatorsList(t *testing.T) {
	t.Run("nothing set: the operator's list, as before", func(t *testing.T) {
		assert.Equal(t, operatorJVMList, buildJVMSettings(jvmCluster()))
	})

	t.Run("spec.config: after the operator's list, once", func(t *testing.T) {
		c := jvmCluster()
		c.Spec.Config = map[string]string{JVMAdditionalSetting: "-XX:-UseStringDeduplication -XX:MaxGCPauseMillis=100"}
		env := jvmEnv(c)
		require.Len(t, env, 1)
		assert.Equal(t, operatorJVMList+" -XX:-UseStringDeduplication -XX:MaxGCPauseMillis=100", env[0].Value)
		conf := BuildConfigMapForEnterprise(c).Data["neo4j.conf"]
		assert.NotContains(t, conf, JVMAdditionalSetting+"=", "not a conf line too: given once")
	})

	t.Run("spec.env: after the operator's list, one variable", func(t *testing.T) {
		c := jvmCluster()
		c.Spec.Env = []corev1.EnvVar{{Name: JVMAdditionalEnv, Value: "-XX:+HeapDumpOnOutOfMemoryError"}}
		env := jvmEnv(c)
		require.Len(t, env, 1, "the user's entry is joined, not repeated")
		assert.Equal(t, operatorJVMList+" -XX:+HeapDumpOnOutOfMemoryError", env[0].Value)
	})

	t.Run("both: spec.config's, then spec.env's", func(t *testing.T) {
		c := jvmCluster()
		c.Spec.Config = map[string]string{JVMAdditionalSetting: "-XX:+AlwaysPreTouch"}
		c.Spec.Env = []corev1.EnvVar{{Name: JVMAdditionalEnv, Value: "-XX:-AlwaysPreTouch"}}
		assert.Equal(t, operatorJVMList+" -XX:+AlwaysPreTouch -XX:-AlwaysPreTouch", buildJVMSettings(c))
	})

	t.Run("trusted CAs keep their flags beside a user's line", func(t *testing.T) {
		c := jvmCluster()
		c.Spec.TrustedCASecrets = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
		c.Spec.Config = map[string]string{JVMAdditionalSetting: "-XX:+HeapDumpOnOutOfMemoryError"}
		assert.Equal(t, operatorJVMList+" "+truststoreFlags+" -XX:+HeapDumpOnOutOfMemoryError", buildJVMSettings(c))
	})

	t.Run("spec.env from a Secret: the user's variable and spec.config's conf line, as before", func(t *testing.T) {
		c := jvmCluster()
		from := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "jvm"}, Key: "flags"}}
		c.Spec.Env = []corev1.EnvVar{{Name: JVMAdditionalEnv, ValueFrom: from}}
		c.Spec.Config = map[string]string{JVMAdditionalSetting: "-XX:+HeapDumpOnOutOfMemoryError"}
		env := jvmEnv(c)
		require.Len(t, env, 1)
		assert.Equal(t, from, env[0].ValueFrom)
		assert.Empty(t, env[0].Value)
		conf := BuildConfigMapForEnterprise(c).Data["neo4j.conf"]
		assert.Contains(t, conf, JVMAdditionalSetting+"=-XX:+HeapDumpOnOutOfMemoryError",
			"a value the operator cannot join keeps spec.config's flags in neo4j.conf")
	})
}

// Run as a pod runs it — the startup block over the image's neo4j.conf, the
// rendered neo4j.conf mounted at /conf, the entrypoint's /conf copy and its
// environment pass with the pod's variable — a user's flag comes after the
// operator's, once, and so wins the conflict with it.
func TestClusterJVMArguments_TheUsersFlagWinsAndIsGivenOnce(t *testing.T) {
	c := jvmCluster()
	c.Spec.Config = map[string]string{JVMAdditionalSetting: "-XX:-UseStringDeduplication"}
	cm := BuildConfigMapForEnterprise(c)
	imageConf := ""
	for _, v := range entrypointsim.ImageJVMArguments(t, entrypointsim.Release2026090) {
		imageConf += "server.jvm.additional=" + v + "\n"
	}
	env := jvmEnv(c)
	require.Len(t, env, 1)

	conf := entrypointsim.Start(t, entrypointsim.Image{Release: entrypointsim.Release2026090, Conf: imageConf},
		cm.Data["neo4j.conf"], buildImageJVMArgumentsBlock(), map[string]string{JVMAdditionalEnv: env[0].Value})

	var args []string
	for _, line := range strings.Split(conf, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), JVMAdditionalSetting+"="); ok {
			args = append(args, strings.Fields(v)...)
		}
	}
	on, off := -1, -1
	for i, a := range args {
		switch a {
		case "-XX:+UseStringDeduplication":
			on = i
		case "-XX:-UseStringDeduplication":
			assert.Equal(t, -1, off, "the user's flag is given once")
			off = i
		}
	}
	require.GreaterOrEqual(t, on, 0, "the operator's list is kept")
	assert.Greater(t, off, on, "the user's flag comes after the operator's and wins")
}
