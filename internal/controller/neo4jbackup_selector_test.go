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

// Contract test for resources.BackupJobSelector, in the same spirit as
// internal/resources/cluster_selectors_test.go: a selector that consumers use
// to FIND a workload must be a subset of the labels the producer STAMPS on it.
//
// It lives in this package rather than next to the other selector tests
// because backupLabels is unexported here, and asserting against a copy of it
// would test the copy rather than the contract.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestBackupJobSelector_IsSubsetOfBackupLabels(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "prod"},
	}

	// Every component the controller stamps must remain selectable: the Job,
	// its pod template, and the CronJob for a scheduled backup.
	for _, component := range []string{"backup", "cronjob", "cleanup"} {
		labels := backupLabels(backup, component)
		for k, v := range resources.BackupJobSelector(backup.Name) {
			assert.Equal(t, v, labels[k],
				"BackupJobSelector key %q must match backupLabels(%q)", k, component)
		}
	}
}

// Guards the shape of the mistake that #68 made for server pods: a selector
// keyed on a derived name rather than the CR name matches nothing, and a
// consumer silently reports "no workload found" instead of the real failure.
func TestBackupJobSelector_UsesCRNameNotDerivedName(t *testing.T) {
	sel := resources.BackupJobSelector("nightly")
	assert.Equal(t, "nightly", sel["app.kubernetes.io/instance"])
	assert.Equal(t, "neo4j-backup", sel["app.kubernetes.io/name"])
}

// Contract test for resources.CloudCredentialKeys.
//
// The Job builder references Secret keys by name; a key missing from the
// Secret does not fail politely, the pod never starts and the kubelet reports
// CreateContainerConfigError with no mention of backups. kubectl-neo4j's
// `preflight` checks the Secret's shape BEFORE that happens, using the list in
// internal/resources. This test is what stops the two drifting: every key the
// builder actually wires must appear in that list.
func TestCloudCredentialKeys_CoverEveryKeyTheJobBuilderWires(t *testing.T) {
	r := &Neo4jBackupReconciler{}

	for _, provider := range []string{"aws", "azure"} {
		backup := &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "prod",
				Storage: neo4jv1beta1.StorageLocation{
					Type:   "s3",
					Bucket: "backups",
					Cloud: &neo4jv1beta1.CloudBlock{
						Provider:             provider,
						CredentialsSecretRef: "cloud-creds",
					},
				},
			},
		}

		declared := map[string]bool{}
		for _, k := range resources.CloudCredentialKeys(provider) {
			declared[k] = true
		}

		wired := 0
		for _, env := range r.buildCloudEnvVars(backup) {
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				continue // a literal such as AWS_ENDPOINT_URL_S3, not a Secret key
			}
			wired++
			assert.True(t, declared[env.ValueFrom.SecretKeyRef.Key],
				"provider %q wires Secret key %q, which CloudCredentialKeys does not declare",
				provider, env.ValueFrom.SecretKeyRef.Key)
		}
		assert.NotZero(t, wired, "provider %q should wire at least one Secret key", provider)
	}
}

// GCP is the odd one out: its credential is projected as a volume item rather
// than an env var, so the key has to be checked on the other builder.
func TestCloudCredentialKeys_CoverTheGCPVolumeProjection(t *testing.T) {
	r := &Neo4jBackupReconciler{}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "prod",
			Storage: neo4jv1beta1.StorageLocation{
				Type:   "gcs",
				Bucket: "backups",
				Cloud: &neo4jv1beta1.CloudBlock{
					Provider:             "gcp",
					CredentialsSecretRef: "cloud-creds",
				},
			},
		},
	}

	declared := map[string]bool{}
	for _, k := range resources.CloudCredentialKeys("gcp") {
		declared[k] = true
	}

	projected := 0
	for _, v := range r.buildVolumes(backup) {
		if v.Secret == nil {
			continue
		}
		for _, item := range v.Secret.Items {
			projected++
			assert.True(t, declared[item.Key],
				"the GCP credentials volume projects Secret key %q, which CloudCredentialKeys does not declare", item.Key)
		}
	}
	assert.NotZero(t, projected, "the GCP path should project a Secret key")
}
