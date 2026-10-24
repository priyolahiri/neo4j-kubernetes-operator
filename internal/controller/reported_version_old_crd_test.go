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

// With the CRDs of an earlier release installed, the API server drops
// status.runningVersion, so a cluster's Ready pass sends it again every time
// (the first pass after the CRDs are applied records it). That write changes
// nothing; it was logged at Info on every pass. The standalone logs it at
// V(1) since #555, and the cluster now does the same (#550).

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// capturedLine is one log line: its V level and message.
type capturedLine struct {
	level int
	msg   string
}

// levelCaptureSink is a logr.LogSink that keeps every line, at every level.
type levelCaptureSink struct {
	mu    *sync.Mutex
	lines *[]capturedLine
}

func newLevelCaptureSink() levelCaptureSink {
	return levelCaptureSink{mu: &sync.Mutex{}, lines: &[]capturedLine{}}
}

func (s levelCaptureSink) Init(logr.RuntimeInfo)          {}
func (s levelCaptureSink) Enabled(int) bool               { return true }
func (s levelCaptureSink) Error(error, string, ...any)    {}
func (s levelCaptureSink) WithValues(...any) logr.LogSink { return s }
func (s levelCaptureSink) WithName(string) logr.LogSink   { return s }
func (s levelCaptureSink) Info(level int, msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.lines = append(*s.lines, capturedLine{level, msg})
}

// about returns the lines at level whose message mentions what the servers
// report.
func (s levelCaptureSink) about(level int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, l := range *s.lines {
		if l.level == level && strings.Contains(l.msg, "servers report") {
			out = append(out, l.msg)
		}
	}
	return out
}

func TestRecordClusterReady_ADroppedReportedVersionIsNotAnInfoLineOnEveryPass(t *testing.T) {
	sink := newLevelCaptureSink()
	ctx := log.IntoContext(context.Background(), logr.New(sink))
	key := types.NamespacedName{Name: "c", Namespace: "default"}

	sts := gateTestSTS("neo4j:"+floating, 3, "rev1", "rev1", 3, 3)
	cluster := gateTestCluster(floating, "Ready")
	cluster.Status.Version = floating
	oldCRD := true
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(sts, cluster).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if c, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok && oldCRD {
					c.Status.RunningVersion = "" // pruned: the installed CRD has no such field
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: newTestScheme(), Recorder: record.NewFakeRecorder(16)}
	latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, r.Get(ctx, key, got))
		return got
	}

	for pass := 0; pass < 3; pass++ {
		r.recordClusterReady(ctx, latest(), showServers(resolved, resolved, resolved))
	}
	assert.Empty(t, latest().Status.RunningVersion, "the old CRD drops the field")
	assert.Empty(t, sink.about(0), "a write the API server drops is not an Info line on every pass")
	assert.Len(t, sink.about(1), 3, "it is said at V(1), where it is resent")

	oldCRD = false // the CRDs are applied: the next pass records it, and says so once
	r.recordClusterReady(ctx, latest(), showServers(resolved, resolved, resolved))
	r.recordClusterReady(ctx, latest(), showServers(resolved, resolved, resolved))
	assert.Equal(t, resolved, latest().Status.RunningVersion)
	assert.Len(t, sink.about(0), 1)
}
