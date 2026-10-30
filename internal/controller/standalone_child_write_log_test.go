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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// levelLine is one log line with the verbosity it was written at (0 = info).
type levelLine struct {
	level int
	msg   string
}

// levelSink records every Info line with its level.
type levelSink struct {
	mu    *sync.Mutex
	lines *[]levelLine
}

func newLevelLogger() (logr.Logger, func() []levelLine) {
	var mu sync.Mutex
	var lines []levelLine
	return logr.New(levelSink{mu: &mu, lines: &lines}), func() []levelLine {
		mu.Lock()
		defer mu.Unlock()
		return append([]levelLine(nil), lines...)
	}
}

func (s levelSink) Init(logr.RuntimeInfo) {}
func (s levelSink) Enabled(int) bool      { return true }
func (s levelSink) Info(level int, msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.lines = append(*s.lines, levelLine{level: level, msg: msg})
}
func (s levelSink) Error(error, string, ...any)    {}
func (s levelSink) WithValues(...any) logr.LogSink { return s }
func (s levelSink) WithName(string) logr.LogSink   { return s }

// childWriteLines returns the lines logChildWrite wrote, by level.
func childWriteLines(lines []levelLine) (info, debug []string) {
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l.msg, "Successfully created or updated"):
			info = append(info, "OLD: "+l.msg) // the line before #624
		case strings.HasPrefix(l.msg, "Successfully ") && !strings.HasPrefix(l.msg, "Successfully reconciled"):
			if l.level == 0 {
				info = append(info, l.msg)
			} else {
				debug = append(debug, l.msg)
			}
		case strings.HasSuffix(l.msg, "already as rendered; nothing written"):
			if l.level == 0 {
				info = append(info, l.msg)
			} else {
				debug = append(debug, l.msg)
			}
		}
	}
	return info, debug
}

// An idle standalone logged five "Successfully created or updated …" lines and
// "Successfully reconciled Neo4jEnterpriseStandalone" at info on every pass,
// about every six seconds, although a pass that changes nothing writes nothing
// (#624, operations.md id 235). The pass that creates its objects says so at
// info; an idle pass after it logs nothing at info.
func TestStandaloneReconcile_AnIdlePassLogsNoWritesAtInfo(t *testing.T) {
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "quiet", Namespace: "q", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
	c, _ := defaultingClient(t, false, standalone, standardStorageClass())
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(1000),
		RequeueAfter: time.Minute, Validator: validation.NewStandaloneValidator()}
	key := client.ObjectKeyFromObject(standalone)
	pass := func() []levelLine {
		t.Helper()
		logger, lines := newLevelLogger()
		_, err := r.Reconcile(log.IntoContext(context.Background(), logger), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		return lines()
	}

	first, _ := childWriteLines(pass())
	assert.Contains(t, first, "Successfully created ConfigMap", "the creating pass says so at info")
	assert.Contains(t, first, "Successfully created StatefulSet")

	// Settle: the StatefulSet controller's status write, and the passes that
	// record what the first one created.
	kubeControllerManagerWrites(t, c, key, false)
	pass()
	pass()

	for i := range 3 {
		kubeControllerManagerWrites(t, c, key, false)
		all := pass()
		var infoLines []string
		for _, l := range all {
			if l.level == 0 {
				infoLines = append(infoLines, l.msg)
			}
		}
		assert.Empty(t, infoLines, "idle pass %d wrote nothing, so it logs nothing at info", i)
		_, debug := childWriteLines(all)
		assert.Len(t, debug, 5, "pass %d: the five child objects, each at V(1): %v", i, debug)
	}
}

// logChildWrite: a create and an update are info lines naming what happened;
// unchanged is V(1).
func TestLogChildWrite(t *testing.T) {
	logger, lines := newLevelLogger()
	logChildWrite(logger, controllerutil.OperationResultCreated, "ConfigMap", "a")
	logChildWrite(logger, controllerutil.OperationResultUpdated, "client Service", "a")
	logChildWrite(logger, controllerutil.OperationResultNone, "StatefulSet", "a")
	got := lines()
	require.Len(t, got, 3)
	assert.Equal(t, levelLine{0, "Successfully created ConfigMap"}, got[0])
	assert.Equal(t, levelLine{0, "Successfully updated client Service"}, got[1])
	assert.Equal(t, levelLine{1, "StatefulSet already as rendered; nothing written"}, got[2])
}
