/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/neo4j-partners/neo4j-kubernetes-operator/internal/neo4j"
)

func ptrBool(b bool) *bool { return &b }

func newShardedTestReconciler(t *testing.T, objs ...runtime.Object) *Neo4jBackupReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add neo4j scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jBackup{}).
		Build()
	return &Neo4jBackupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(16),
	}
}

// TestEffectiveRemoteAddressResolution pins the defaulting matrix for the
// --remote-address-resolution flag. Explicit user values always win; the
// default-true behavior only fires for kind=ShardedDatabase on a Neo4j version
// that supports the flag (2025.09+).
func TestEffectiveRemoteAddressResolution(t *testing.T) {
	v202509, _ := neo4j.ParseVersion("2025.09.0-enterprise")
	v202508, _ := neo4j.ParseVersion("2025.08.0-enterprise")
	v526, _ := neo4j.ParseVersion("5.26.0-enterprise")

	cases := []struct {
		name    string
		kind    string
		options *neo4jv1beta1.BackupOptions
		version *neo4j.Version
		want    bool
	}{
		{
			name:    "ShardedDatabase + 2025.09 + nil opts → defaults true",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: nil,
			version: v202509,
			want:    true,
		},
		{
			name:    "ShardedDatabase + 2025.09 + field unset → defaults true",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: &neo4jv1beta1.BackupOptions{},
			version: v202509,
			want:    true,
		},
		{
			name:    "ShardedDatabase + 2025.08 → false (version too old)",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: nil,
			version: v202508,
			want:    false,
		},
		{
			name:    "ShardedDatabase + 5.26 (semver) → false",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: nil,
			version: v526,
			want:    false,
		},
		{
			name:    "ShardedDatabase + explicit false → false (user wins)",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: &neo4jv1beta1.BackupOptions{RemoteAddressResolution: ptrBool(false)},
			version: v202509,
			want:    false,
		},
		{
			name:    "ShardedDatabase + explicit true → true",
			kind:    neo4jv1beta1.BackupTargetKindShardedDatabase,
			options: &neo4jv1beta1.BackupOptions{RemoteAddressResolution: ptrBool(true)},
			version: v202509,
			want:    true,
		},
		{
			name:    "Cluster kind + nil opts → false (no defaulting for non-sharded)",
			kind:    neo4jv1beta1.BackupTargetKindCluster,
			options: nil,
			version: v202509,
			want:    false,
		},
		{
			name:    "Cluster kind + explicit true → true (user opt-in honored)",
			kind:    neo4jv1beta1.BackupTargetKindCluster,
			options: &neo4jv1beta1.BackupOptions{RemoteAddressResolution: ptrBool(true)},
			version: v202509,
			want:    true,
		},
		{
			name:    "Database kind + nil opts → false (no defaulting for non-sharded)",
			kind:    neo4jv1beta1.BackupTargetKindDatabase,
			options: nil,
			version: v202509,
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backup := &neo4jv1beta1.Neo4jBackup{
				Spec: neo4jv1beta1.Neo4jBackupSpec{
					Target:  neo4jv1beta1.BackupTarget{Kind: tc.kind, Name: "x"},
					Options: tc.options,
				},
			}
			got := effectiveRemoteAddressResolution(backup, tc.version)
			if got != tc.want {
				t.Errorf("effectiveRemoteAddressResolution() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBuildBackupCommand_ShardedDatabase_EmitsFlagsWithNilOptions guards
// against the regression where the --remote-address-resolution emission was
// gated on Spec.Options != nil. A sharded backup CR with only target + storage
// set MUST still get the flag injected because the ShardedDatabase + 2025.09+
// default fires from effectiveRemoteAddressResolution regardless of Options.
// Caught by the integration test in property_sharding_backup_test.go on first
// run.
func TestBuildBackupCommand_ShardedDatabase_EmitsFlagsWithNilOptions(t *testing.T) {
	r := newShardedTestReconciler(t)
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2025.12-enterprise"},
			Topology: neo4jv1beta1.TopologyConfiguration{
				Servers: 3,
			},
		},
	}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			Target: neo4jv1beta1.BackupTarget{
				Kind:       neo4jv1beta1.BackupTargetKindShardedDatabase,
				Name:       "products",
				ClusterRef: "ec",
			},
			Storage: neo4jv1beta1.StorageLocation{
				Type: "pvc",
				PVC:  &neo4jv1beta1.PVCSpec{Name: "backup-pvc"},
			},
			// Options DELIBERATELY left nil — this is what catches the bug.
		},
	}

	cmd, err := r.buildBackupCommand(context.Background(), backup, cluster)
	if err != nil {
		t.Fatalf("buildBackupCommand: %v", err)
	}
	if !strings.Contains(cmd, "--remote-address-resolution=true") {
		t.Errorf("expected --remote-address-resolution=true in cmd, got: %q", cmd)
	}
	if !strings.Contains(cmd, `"products*"`) {
		t.Errorf("expected quoted glob \"products*\" in cmd, got: %q", cmd)
	}
}

// TestShardedShardNamePattern pins the per-shard regex used to filter
// SHOW DATABASES output during glob-safety checks.
func TestShardedShardNamePattern(t *testing.T) {
	pat := shardedShardNamePattern("products")
	matches := []string{
		"products-g000",
		"products-p000",
		"products-p001",
		"products-p999",
	}
	for _, m := range matches {
		if !pat.MatchString(m) {
			t.Errorf("expected pattern to match %q", m)
		}
	}
	rejects := []string{
		"products",           // virtual DB (handled separately by the caller)
		"products-g00",       // too few digits
		"products-g0000",     // too many digits
		"products-g000-",     // trailing dash
		"products-x000",      // wrong shard prefix
		"productsales",       // unrelated DB starting with same prefix — the POISONING case
		"productsales-g000",  // same prefix, different family
		"PRODUCTS-G000",      // case-sensitive: Neo4j DB names are case-preserving
		"products-G000",      // mixed case in shard prefix
		"products-g000.test", // extra suffix
	}
	for _, r := range rejects {
		if pat.MatchString(r) {
			t.Errorf("expected pattern NOT to match %q", r)
		}
	}

	// Regex-special char in logical name must be escaped (QuoteMeta) so a
	// name like "v1.2" doesn't widen the match. Without QuoteMeta the dot
	// would match any char.
	patDot := shardedShardNamePattern("v1.2")
	if patDot.MatchString("v1X2-g000") {
		t.Errorf("regex-special char in logical name was not escaped: %q matched %q", "v1.2", "v1X2-g000")
	}
	if !patDot.MatchString("v1.2-g000") {
		t.Errorf("expected literal pattern to match its own name")
	}
}

func TestShardedPreflightStatic(t *testing.T) {
	clusterReady := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:            neo4jv1beta1.ImageSpec{Tag: "2025.12.0-enterprise"},
			PropertySharding: &neo4jv1beta1.PropertyShardingSpec{Enabled: true},
		},
	}
	clusterNoSharding := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{Tag: "2025.12.0-enterprise"},
		},
	}
	clusterOldVersion := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:            neo4jv1beta1.ImageSpec{Tag: "2025.11.0-enterprise"},
			PropertySharding: &neo4jv1beta1.PropertyShardingSpec{Enabled: true},
		},
	}

	shardedDBReady := &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jShardedDatabaseSpec{Name: "products", ClusterRef: "ec"},
		Status:     neo4jv1beta1.Neo4jShardedDatabaseStatus{ShardingReady: ptrBool(true)},
	}
	shardedDBNotReady := &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jShardedDatabaseSpec{Name: "products", ClusterRef: "ec"},
	}
	shardedDBOtherCluster := &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jShardedDatabaseSpec{Name: "products", ClusterRef: "different-cluster"},
		Status:     neo4jv1beta1.Neo4jShardedDatabaseStatus{ShardingReady: ptrBool(true)},
	}

	backup := func() *neo4jv1beta1.Neo4jBackup {
		return &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				Target: neo4jv1beta1.BackupTarget{
					Kind:       neo4jv1beta1.BackupTargetKindShardedDatabase,
					Name:       "products",
					ClusterRef: "ec",
				},
			},
		}
	}

	type tc struct {
		name        string
		objs        []runtime.Object
		cluster     *neo4jv1beta1.Neo4jEnterpriseCluster
		mutate      func(*neo4jv1beta1.Neo4jBackup)
		wantAction  preflightAction
		wantErrSub  string
		wantWaitSub string
	}
	cases := []tc{
		{
			name:       "non-sharded kind short-circuits",
			cluster:    clusterReady,
			mutate:     func(b *neo4jv1beta1.Neo4jBackup) { b.Spec.Target.Kind = neo4jv1beta1.BackupTargetKindCluster },
			wantAction: preflightContinue,
		},
		{
			name:       "happy path",
			objs:       []runtime.Object{shardedDBReady},
			cluster:    clusterReady,
			wantAction: preflightContinue,
		},
		{
			name:       "cluster sharding disabled → Fail",
			objs:       []runtime.Object{shardedDBReady},
			cluster:    clusterNoSharding,
			wantAction: preflightFail,
			wantErrSub: "property sharding enabled",
		},
		{
			name:       "cluster version too old → Fail",
			objs:       []runtime.Object{shardedDBReady},
			cluster:    clusterOldVersion,
			wantAction: preflightFail,
			wantErrSub: "below the 2025.12 minimum",
		},
		{
			name:       "sharded DB CR missing → Fail",
			objs:       nil,
			cluster:    clusterReady,
			wantAction: preflightFail,
			wantErrSub: `Neo4jShardedDatabase "products" not found`,
		},
		{
			name:        "sharded DB CR not Ready → Wait",
			objs:        []runtime.Object{shardedDBNotReady},
			cluster:     clusterReady,
			wantAction:  preflightWait,
			wantWaitSub: "not yet Ready",
		},
		{
			name:       "sharded DB CR cluster mismatch → Fail",
			objs:       []runtime.Object{shardedDBOtherCluster},
			cluster:    clusterReady,
			wantAction: preflightFail,
			wantErrSub: `references cluster "different-cluster"`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newShardedTestReconciler(t, c.objs...)
			b := backup()
			if c.mutate != nil {
				c.mutate(b)
			}
			action, waitMsg, err := r.shardedPreflightStatic(context.Background(), b, c.cluster)
			if action != c.wantAction {
				t.Fatalf("action=%v, want %v (err=%v, waitMsg=%q)", action, c.wantAction, err, waitMsg)
			}
			if c.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErrSub) {
					t.Errorf("err %v does not contain %q", err, c.wantErrSub)
				}
			}
			if c.wantWaitSub != "" {
				if !strings.Contains(waitMsg, c.wantWaitSub) {
					t.Errorf("waitMsg %q does not contain %q", waitMsg, c.wantWaitSub)
				}
			}
		})
	}
}
