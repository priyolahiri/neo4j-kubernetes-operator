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
