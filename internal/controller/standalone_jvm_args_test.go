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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A standalone has no JVM arguments of its own: its rendered neo4j.conf, which
// the image's entrypoint puts in place of the image's (and with it the image's
// own server.jvm.additional lines), carries none but the two truststore flags
// when trusted CAs are set, and its pod has no NEO4J_server_jvm_additional
// unless spec.env sets one (operations.md id 208). The live walk of round 8
// found the API reference saying a spec.env value was added to "the JVM
// arguments neo4j.conf already sets": on a standalone that set is empty. The
// reference now says so; this pins it to what is rendered.
func TestStandaloneRendersNoJVMArgumentsOfItsOwn(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
		},
	}
	jvmLines := func() []string {
		var out []string
		for _, line := range strings.Split(r.createConfigMap(standalone).Data["neo4j.conf"], "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "server.jvm.additional") {
				out = append(out, strings.TrimSpace(line))
			}
		}
		return out
	}

	assert.Empty(t, jvmLines(), "no JVM arguments of its own")
	for _, e := range r.buildEnvVars(standalone) {
		assert.NotEqual(t, "NEO4J_server_jvm_additional", e.Name)
	}

	standalone.Spec.TrustedCASecrets = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
	assert.Equal(t, []string{
		"server.jvm.additional=-Djavax.net.ssl.trustStore=/truststore/truststore.jks",
		"server.jvm.additional=-Djavax.net.ssl.trustStorePassword=changeit",
	}, jvmLines(), "the truststore flags only")

	data, err := os.ReadFile("../../docs/api_reference/neo4jenterprisestandalone.md")
	require.NoError(t, err)
	doc := strings.Join(strings.Fields(string(data)), " ")
	for _, says := range []string{
		"A standalone gets no extra JVM arguments unless you set them.",
		"Either way, list every flag you want.",
	} {
		assert.True(t, strings.Contains(doc, says), "the standalone API reference says: %s", says)
	}
	assert.False(t, strings.Contains(doc, "neo4j.conf already sets"),
		"the standalone API reference must not say neo4j.conf already sets JVM arguments: it sets none")
}
