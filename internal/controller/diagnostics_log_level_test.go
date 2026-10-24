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
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// #549: deleting a server pod logged one "ERROR Failed to collect SHOW
// SERVERS … ConnectivityError: EOF" (journey Phase 2), although the restart is
// routine and the cluster's status already reports it. A server that cannot be
// reached is logged at info; any other failure stays an error.

// logEntry is one call a captureSink received.
type logEntry struct {
	isError bool
	msg     string
}

// captureSink records every log call, at whatever verbosity.
type captureSink struct {
	mu      *sync.Mutex
	entries *[]logEntry
}

func newCaptureLogger() (logr.Logger, func() []logEntry) {
	var mu sync.Mutex
	var entries []logEntry
	sink := captureSink{mu: &mu, entries: &entries}
	return logr.New(sink), func() []logEntry {
		mu.Lock()
		defer mu.Unlock()
		return append([]logEntry(nil), entries...)
	}
}

func (s captureSink) Init(logr.RuntimeInfo) {}
func (s captureSink) Enabled(int) bool      { return true }
func (s captureSink) Info(_ int, msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.entries = append(*s.entries, logEntry{msg: msg})
}
func (s captureSink) Error(_ error, msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.entries = append(*s.entries, logEntry{isError: true, msg: msg})
}
func (s captureSink) WithValues(...any) logr.LogSink { return s }
func (s captureSink) WithName(string) logr.LogSink   { return s }

func errorEntries(entries []logEntry) []logEntry {
	var out []logEntry
	for _, e := range entries {
		if e.isError {
			out = append(out, e)
		}
	}
	return out
}

// hangUpListener accepts Bolt connections and closes each at once, as a
// server pod that is going away does: the driver reads EOF.
func hangUpListener(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().String()
}

// The whole diagnostics pass against a server that hangs up: every query
// fails, nothing is logged as an error, and the failure still reaches
// status.diagnostics.collectionError.
func TestCollectDiagnostics_AServerThatHangsUpIsNotAnError(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("c", "default")
	cluster.Spec.Topology.Servers = 3
	admin := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultAdminSecretName, Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("not-used-here")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, admin).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()

	nc, err := neo4jclient.NewClientForPod(cluster, c, DefaultAdminSecretName, "bolt://"+hangUpListener(t))
	require.NoError(t, err)
	defer func() { _ = nc.Close() }()

	logger, entries := newCaptureLogger()
	qm := NewQueryMonitor(c, scheme)
	require.NoError(t, qm.CollectDiagnostics(log.IntoContext(ctx, logger), cluster, nc),
		"diagnostics are non-fatal")

	assert.Empty(t, errorEntries(entries()), "a server that cannot be reached is not an error: %+v", entries())
	var sawServers bool
	for _, e := range entries() {
		if strings.Contains(e.msg, "SHOW SERVERS") {
			sawServers = true
		}
	}
	assert.True(t, sawServers, "the failed SHOW SERVERS is still logged, at info: %+v", entries())

	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), got))
	require.NotNil(t, got.Status.Diagnostics)
	assert.Contains(t, got.Status.Diagnostics.CollectionError, "SHOW SERVERS failed")
}

func TestLogDiagnosticsQueryFailure(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantError bool
	}{
		{"connection refused", errors.New("failed to execute SHOW SERVERS: dial tcp 10.0.0.7:7687: connect: connection refused"), false},
		{"connection reset", errors.New("failed to execute SHOW DATABASES: read: connection reset by peer"), false},
		{"a refusal by Neo4j", errors.New("Neo.ClientError.Security.Forbidden: Permission denied"), true},
		{"a query error", errors.New("Neo.ClientError.Statement.SyntaxError: Invalid input"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, entries := newCaptureLogger()
			logDiagnosticsQueryFailure(logger, "SHOW SERVERS", tc.err)
			got := entries()
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantError, got[0].isError, "%+v", got[0])
		})
	}
}
