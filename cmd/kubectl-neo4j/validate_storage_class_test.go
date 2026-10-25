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

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func dataPVC(name, class string, labels map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j", Labels: labels},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: ptr.To(class)},
	}
}

// The operator raises an advisory ValidationWarning while spec.storage.className
// names a class the existing data volumes do not use: a volume keeps the class
// it was created with (#560). Connected, validate says so before the apply,
// from the volumes the operator's storage pass finds (#566); offline there are
// no volumes to compare with.
func TestValidate_Connected_ClassNameAgainstTheVolumesThatExist(t *testing.T) {
	cluster := `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata: {name: prod, namespace: neo4j}
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "5.26-enterprise"}
  topology: {servers: 2}
  storage: {size: 10Gi, className: fast}
`
	labels := map[string]string{"neo4j.com/cluster": "prod", "neo4j.com/role": "server"}
	c := testClient(t,
		dataPVC("data-prod-server-0", "standard", labels),
		dataPVC("data-prod-server-1", "standard", labels))

	results, err := validateSource(writeManifest(t, cluster), c, "")
	require.NoError(t, err)
	text := findingsText(results[0])
	assert.Equal(t, 0, results[0].errorCount(), text)
	assert.True(t, strings.Contains(text, `spec.storage.className "fast" has no effect on the data volumes that already exist`), text)
	assert.Contains(t, text, `data-prod-server-0, data-prod-server-1 use StorageClass "standard"`)

	// The class the volumes use, and no connection, say nothing.
	same, err := validateSource(writeManifest(t, strings.Replace(cluster, "className: fast", "className: standard", 1)), c, "")
	require.NoError(t, err)
	assert.NotContains(t, findingsText(same[0]), "spec.storage.className")
	offline, err := validateSource(writeManifest(t, cluster), nil, "")
	require.NoError(t, err)
	assert.NotContains(t, findingsText(offline[0]), "spec.storage.className")

	// A standalone's volume, found by its name as the operator finds a volume
	// from before the labels.
	standalone := `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseStandalone
metadata: {name: dev, namespace: neo4j}
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "2026.08.1-enterprise"}
  storage: {size: 10Gi, className: fast}
`
	sa, err := validateSource(writeManifest(t, standalone), testClient(t, dataPVC("neo4j-data-dev-0", "standard", nil)), "")
	require.NoError(t, err)
	assert.Contains(t, findingsText(sa[0]), `neo4j-data-dev-0 uses StorageClass "standard"`)
}
