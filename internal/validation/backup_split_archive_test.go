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

package validation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func splitBackup(size string, extra ...string) *neo4jv1beta1.Neo4jBackup {
	return &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "c",
			Database:    "neo4j",
			Storage: neo4jv1beta1.StorageLocation{
				Type: "pvc",
				PVC:  &neo4jv1beta1.PVCSpec{Name: "backups"},
			},
			Options: &neo4jv1beta1.BackupOptions{SplitArchivePartSize: size, AdditionalArgs: extra},
		},
	}
}

// spec.options.splitArchivePartSize is a Kubernetes quantity of at least 1Gi,
// Neo4j's minimum part size. Refusing a smaller one here beats a backup Job
// that fails on it — and "1G" is the trap: 10^9 bytes, under the minimum.
func TestBackupValidator_SplitArchivePartSize(t *testing.T) {
	cases := []struct {
		size    string
		wantErr string
	}{
		{"500Gi", ""},
		{"1Gi", ""},
		{"1Ti", ""},
		{"2T", ""},
		{"0", ""},
		{"1G", "at least 1Gi"},
		{"512Mi", "at least 1Gi"},
		{"-1Gi", "negative"},
		{"500GB", "Kubernetes quantity"},
		{"lots", "Kubernetes quantity"},
	}
	for _, tc := range cases {
		t.Run(tc.size, func(t *testing.T) {
			errs := NewBackupValidator().Validate(splitBackup(tc.size))
			var msgs []string
			for _, e := range errs {
				if strings.Contains(e.Field, "splitArchivePartSize") {
					msgs = append(msgs, e.Error())
				}
			}
			if tc.wantErr == "" {
				assert.Empty(t, msgs)
				return
			}
			if assert.Len(t, msgs, 1) {
				assert.Contains(t, msgs[0], tc.wantErr)
				assert.Contains(t, msgs[0], "spec.options.splitArchivePartSize")
			}
		})
	}
}

// The same flag twice is ambiguous; neo4j-admin would get both.
func TestBackupValidator_SplitArchivePartSizeNotAlsoInAdditionalArgs(t *testing.T) {
	errs := NewBackupValidator().Validate(splitBackup("500Gi", "--split-archive-part-size=100G"))
	if assert.Len(t, errs, 1) {
		assert.Equal(t, "spec.options.additionalArgs[0]", errs[0].Field)
		assert.Contains(t, errs[0].Error(), "one place")
	}

	// Without the typed field, the flag in additionalArgs stays the user's call.
	assert.Empty(t, NewBackupValidator().Validate(splitBackup("", "--split-archive-part-size=100G")))
}
