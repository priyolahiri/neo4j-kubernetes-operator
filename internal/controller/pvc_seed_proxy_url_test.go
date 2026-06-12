/*
Copyright 2025.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
)

// Pins the #227 URL-escaping fix: type=storage restores feed user-supplied
// spec.source.backupPath into the proxy URL — reserved URL characters must
// be percent-escaped per path segment, with '/' separators preserved.
func TestPvcSeedProxyURL_EscapesUserControlledSegments(t *testing.T) {
	cases := []struct {
		name        string
		backupsPath string
		filename    string
		want        string
	}{
		{
			name:        "plain path untouched",
			backupsPath: "inventory-backup",
			filename:    "inventory-2026-06-08T01-18-06.backup",
			want:        "http://backup-seed-proxy-r.ns.svc.cluster.local:8080/inventory-backup/inventory-2026-06-08T01-18-06.backup",
		},
		{
			name:        "space, percent and hash escaped; slashes preserved",
			backupsPath: "my backups/100%",
			filename:    "db#1.backup",
			want:        "http://backup-seed-proxy-r.ns.svc.cluster.local:8080/my%20backups/100%25/db%231.backup",
		},
		{
			name:        "question mark cannot start a query string",
			backupsPath: "dir",
			filename:    "a?b.backup",
			want:        "http://backup-seed-proxy-r.ns.svc.cluster.local:8080/dir/a%3Fb.backup",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, pvcSeedProxyURL("r", "ns", tc.backupsPath, tc.filename))
		})
	}
}

// Pins the #227 seed-proxy wait anchor: the first stamp wins (the deadline
// must not slide on requeues), clearing is idempotent, and a fresh attempt
// can't inherit a stale anchor.
func TestSeedProxyWaitStarted_AnchorLifecycle(t *testing.T) {
	restore := restoreWithBackupRef("r1", "default", "nightly")
	r := newResolvedSourceReconciler(t, restore)
	ctx := context.Background()

	first, err := r.markSeedProxyWaitStarted(ctx, restore)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), first, time.Minute)

	// Second call preserves the original anchor.
	again, err := r.markSeedProxyWaitStarted(ctx, restore)
	require.NoError(t, err)
	assert.True(t, again.Equal(first), "anchor must not slide on requeue")

	persisted := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(restore), persisted))
	assert.Contains(t, persisted.Annotations, AnnotationSeedProxyWaitStarted)

	require.NoError(t, r.clearSeedProxyWaitStarted(ctx, restore))
	require.NoError(t, r.clearSeedProxyWaitStarted(ctx, restore)) // idempotent

	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(restore), persisted))
	assert.NotContains(t, persisted.Annotations, AnnotationSeedProxyWaitStarted)
	assert.NotContains(t, restore.Annotations, AnnotationSeedProxyWaitStarted)
}
