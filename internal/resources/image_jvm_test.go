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

// The Neo4j image's own JVM arguments reach the servers (docs/knowledge/
// operations.md id 212). The image's neo4j.conf sets them, and the entrypoint
// replaces that file with the operator's: until #594 neither kind ran them.

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// The table is the images' own lists: each vendored release gets exactly what
// its image's neo4j.conf sets, in its order; a tag without a patch, a
// release between known ones, a newer CalVer and a digest-pinned tag get the
// list the table says; a tag that is no supported Neo4j gets none.
func TestImageJVMArguments_AreTheImagesOwn(t *testing.T) {
	for _, release := range entrypointsim.JVMReleases {
		assert.Equal(t, entrypointsim.ImageJVMArguments(t, release), ImageJVMArguments(release+"-enterprise"), release)
	}

	newest := entrypointsim.ImageJVMArguments(t, "2026.09.0")
	for tag, want := range map[string][]string{
		"5.26-enterprise":    entrypointsim.ImageJVMArguments(t, "5.26.32"),
		"2026.09-enterprise": newest,
		"2025.06.0":          entrypointsim.ImageJVMArguments(t, "2025.01.0"),
		"2026.07.1":          entrypointsim.ImageJVMArguments(t, "2026.06.0"),
		"2027.01.0":          newest, // newer than the newest known: best effort
		"2026.08.1-enterprise@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": newest,
	} {
		assert.Equal(t, want, ImageJVMArguments(tag), tag)
	}
	for _, tag := range []string{"4.4.30-enterprise", "enterprise", ""} {
		assert.Empty(t, ImageJVMArguments(tag), tag)
	}

	// Each list holds the one before it, which is what lets a release between
	// two known ones take the earlier list.
	for i := 1; i < len(imageJVMArgumentLists); i++ {
		newer, older := imageJVMArgumentLists[i-1].values, imageJVMArgumentLists[i].values
		for _, v := range older {
			assert.Contains(t, newer, v, "the list from %s keeps %s", imageJVMArgumentLists[i-1].from.String(), v)
		}
	}
}

// A standalone's block: the image's lines between the restart-neutral
// markers, which its restart decision leaves out.
func TestStandaloneImageJVMArgumentsBlock(t *testing.T) {
	block := StandaloneImageJVMArgumentsBlock("2026.08.1-enterprise")
	lines := strings.Split(block, "\n")
	require.GreaterOrEqual(t, len(lines), 3)
	assert.Equal(t, RestartNeutralBegin, lines[0])
	assert.Equal(t, RestartNeutralEnd, lines[len(lines)-1])
	var values []string
	for _, line := range lines {
		if v, ok := strings.CutPrefix(line, "server.jvm.additional="); ok {
			values = append(values, v)
		}
	}
	assert.Equal(t, entrypointsim.ImageJVMArguments(t, "2026.08.1"), values)
	assert.Contains(t, values, "--add-modules=jdk.incubator.vector")
	assert.Empty(t, StandaloneImageJVMArgumentsBlock("4.4.30-enterprise"))
}

// The cluster's startup block, run as a pod runs it: before the entrypoint,
// over the image's own neo4j.conf, then the entrypoint's copy of /conf over
// the image's conf directory and its environment pass. The server's
// neo4j.conf starts with the image's lines, then the user's own
// (spec.config), then the operator's list the entrypoint appends from
// NEO4J_server_jvm_additional (append-not-replace), so a later flag wins; the
// image's other settings do not come through. Without the block the image's
// lines were gone.
func TestImageJVMArgumentsBlock_PutsTheImagesLinesFirst(t *testing.T) {
	for _, release := range []string{entrypointsim.Release52632, entrypointsim.Release2026090} {
		t.Run(release, func(t *testing.T) {
			image := entrypointsim.ImageJVMArguments(t, release)
			imageConf := "# the image's neo4j.conf\nserver.directories.import=import\n"
			for _, v := range image {
				imageConf += "server.jvm.additional=" + v + "\n"
			}
			mounted := "server.jvm.additional=-XX:-AlwaysPreTouch\ndb.tx_log.rotation.size=256M\n"
			operator := buildJVMSettings(&neo4jv1beta1.Neo4jEnterpriseCluster{})

			conf := entrypointsim.Start(t, entrypointsim.Image{Release: release, Conf: imageConf}, mounted,
				buildImageJVMArgumentsBlock(), map[string]string{"NEO4J_server_jvm_additional": operator})

			var got []string
			for _, line := range strings.Split(conf, "\n") {
				if v, ok := strings.CutPrefix(strings.TrimSpace(line), "server.jvm.additional="); ok {
					got = append(got, v)
				}
			}
			want := append(append(append([]string{}, image...), "-XX:-AlwaysPreTouch"), operator)
			assert.Equal(t, want, got)
			assert.NotContains(t, conf, "server.directories.import", "only the image's JVM lines come through")
			value, _ := entrypointsim.Setting(conf, "db.tx_log.rotation.size")
			assert.Equal(t, "256M", value)
		})
	}

	t.Run("an image conf without JVM lines", func(t *testing.T) {
		conf := entrypointsim.Start(t, entrypointsim.Image{Conf: "server.directories.import=import\n"},
			"db.tx_log.rotation.size=256M\n", buildImageJVMArgumentsBlock(), nil)
		_, lines := entrypointsim.Setting(conf, "server.jvm.additional")
		assert.Zero(t, lines)
		value, _ := entrypointsim.Setting(conf, "db.tx_log.rotation.size")
		assert.Equal(t, "256M", value)
	})
}

// The block is in the startup script, after the ConfigMap's files are copied
// to /conf (an earlier prepend would be overwritten), between the
// restart-neutral markers.
func TestBuildStartupScript_CarriesTheImageJVMArgumentsBlock(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	cluster.Name, cluster.Namespace = "c", "default"
	cluster.Spec.Image.Tag = "2026.08.1-enterprise"
	cluster.Spec.Topology.Servers = 3
	cluster.Spec.Env = []corev1.EnvVar{}
	script := buildStartupScriptForEnterprise(cluster)
	block := buildImageJVMArgumentsBlock()
	at := strings.Index(script, block)
	require.Positive(t, at, "the script carries the block")
	assert.Greater(t, at, strings.Index(script, `cp -L "$f" /conf/`))
	assert.True(t, strings.HasPrefix(block, RestartNeutralBegin+"\n"))
	assert.True(t, strings.HasSuffix(strings.TrimSpace(block), RestartNeutralEnd))
}
