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

package resources

import (
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// #564 (operations.md id 181): a server pod is Ready when its Neo4j server is
// available in the cluster, not when anything answers on 7474. These run the
// rendered /conf/health.sh under bash, with pgrep and ps faked on PATH and the
// server's HTTP endpoints served by a local test server.

// fakeNeo4j serves the two cluster endpoints the script reads.
type fakeNeo4j struct {
	mu            sync.Mutex
	available     int    // status of /db/system/cluster/available
	status        int    // status of /dbms/cluster/status
	statusBody    string // its JSON
	wantAuth      string // "user:password" the endpoints require; "" for none
	authorization []string
}

func (f *fakeNeo4j) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authorization = append(f.authorization, r.Header.Get("Authorization"))
	if f.wantAuth != "" && r.Header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte(f.wantAuth)) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"Neo.ClientError.Security.Unauthorized"}]}`))
		return
	}
	switch r.URL.Path {
	case "/db/system/cluster/available":
		w.WriteHeader(f.available)
		_, _ = w.Write([]byte(strconv.FormatBool(f.available == http.StatusOK)))
	case "/dbms/cluster/status":
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.statusBody))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// databaseStatus is one element of /dbms/cluster/status, shaped as the
// Operations Manual's example ("Combined status endpoints", /5/ and /current/).
func databaseStatus(name string, core, participating, healthy bool) string {
	return `{
    "databaseName": "` + name + `",
    "databaseUuid": "f4dacc01-f88a-4512-b3bf-68f7539c941e",
    "databaseStatus": {
      "lastAppliedRaftIndex": 7,
      "votingMembers": ["0cff51ad-7cee-44cc-9102-538fc4544b95", "90ff5df1-f5f8-4b4c-8289-a0e3deb2235c"],
      "memberId": "90ff5df1-f5f8-4b4c-8289-a0e3deb2235c",
      "leader": "0cff51ad-7cee-44cc-9102-538fc4544b95",
      "millisSinceLastLeaderMessage": 12,
      "raftCommandsPerSecond": 0.0,
      "core": ` + strconv.FormatBool(core) + `,
      "participatingInRaftGroup": ` + strconv.FormatBool(participating) + `,
      "healthy": ` + strconv.FormatBool(healthy) + `
    }
  }`
}

func statusArray(elements ...string) string {
	return "[\n  " + strings.Join(elements, ",\n  ") + "\n]\n"
}

// healthRun is one run of the script.
type healthRun struct {
	processUp bool
	age       string // what `ps -o etimes=` prints; "" for nothing
	started   string // what `ps -o lstart=` prints
	record    string // the available record's content before the run; "" for none
	password  string // DB_PASSWORD in the probe's environment
}

type healthResult struct {
	exitCode int
	output   string
	record   string
}

// runHealthScript runs script against port (0: nothing listens).
func runHealthScript(t *testing.T, script string, port int, run healthRun) healthResult {
	t.Helper()
	bin, state := t.TempDir(), t.TempDir()
	pgrepExit := "1"
	if run.processUp {
		pgrepExit = "0"
	}
	writeExecutable(t, filepath.Join(bin, "pgrep"), "#!/bin/sh\nexit "+pgrepExit+"\n")
	writeExecutable(t, filepath.Join(bin, "ps"), `#!/bin/sh
case "$*" in
  *lstart=*) printf '%s\n' "$FAKE_STARTED" ;;
  *etimes=*) [ -n "$FAKE_AGE" ] && printf '%7s\n' "$FAKE_AGE" ;;
esac
`)
	record := filepath.Join(state, ".neo4j-operator-available")
	if run.record != "" {
		if err := os.WriteFile(record, []byte(run.record), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if port == 0 {
		port = closedPort(t)
	}
	if !strings.Contains(script, healthScriptHTTPPortLine) || !strings.Contains(script, healthScriptStateDirLine) {
		t.Fatal("test premise: the script sets the HTTP port and the state directory on lines of their own")
	}
	script = strings.Replace(script, healthScriptHTTPPortLine, "HTTP_PORT="+strconv.Itoa(port), 1)
	script = strings.Replace(script, healthScriptStateDirLine, "STATE_DIR="+state, 1)

	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"FAKE_AGE=" + run.age,
		"FAKE_STARTED=" + run.started,
		"DB_USERNAME=neo4j",
		"DB_PASSWORD=" + run.password,
	}
	out, err := cmd.CombinedOutput()
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("running the script: %v", err)
	}
	recorded, _ := os.ReadFile(record)
	return healthResult{exitCode: code, output: string(out), record: string(recorded)}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

// closedPort is a local port nothing listens on.
func closedPort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("not a TCP address: %v", ln.Addr())
	}
	_ = ln.Close()
	return addr.Port
}

func serve(t *testing.T, f *fakeNeo4j) int {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	addr, ok := srv.Listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("not a TCP address: %v", srv.Listener.Addr())
	}
	return addr.Port
}

func TestHealthScript_ReadyMeansAvailableInTheCluster(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := buildHealthScript(nil)
	const started = "Sat Oct 25 10:00:00 2026"
	healthy := statusArray(databaseStatus("neo4j", true, true, true), databaseStatus("system", true, true, true))
	young := healthRun{processUp: true, age: "30", started: started, password: "secret"}

	cases := []struct {
		name       string
		server     *fakeNeo4j // nil: nothing listens
		run        healthRun
		wantExit   int
		wantOutput string
		wantRecord string
	}{
		{
			name:     "the Neo4j process is not running",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: healthy},
			run:      healthRun{age: "30", started: started, password: "secret"},
			wantExit: 1, wantOutput: "Neo4j process not running",
		},
		{
			name:     "available, every database healthy and in its Raft group",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: healthy},
			run:      young,
			wantExit: 0, wantOutput: "available in the cluster", wantRecord: started,
		},
		{
			name:     "the HTTP port answers but system is not available on this server",
			server:   &fakeNeo4j{available: 404, status: 200, statusBody: healthy},
			run:      young,
			wantExit: 1, wantOutput: "Waiting for this server to be available in the cluster (30s of 300s): the system database is not available on this server (HTTP 404)",
		},
		{
			name:     "a database on this server is failing",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: statusArray(databaseStatus("orders", true, true, false), databaseStatus("system", true, true, true))},
			run:      young,
			wantExit: 1, wantOutput: "orders (not healthy)",
		},
		{
			name:     "a primary has not rejoined its Raft group",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: statusArray(databaseStatus("neo4j", true, false, true), databaseStatus("system", true, true, true))},
			run:      young,
			wantExit: 1, wantOutput: "neo4j (a primary not yet in its Raft group)",
		},
		{
			name:     "a secondary takes part in no Raft group, and is ready",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: statusArray(databaseStatus("neo4j", false, false, true), databaseStatus("system", false, false, true))},
			run:      young,
			wantExit: 0, wantRecord: started,
		},
		{
			name:     "a version without the combined status endpoint",
			server:   &fakeNeo4j{available: 200, status: 404},
			run:      young,
			wantExit: 0, wantRecord: started,
		},
		{
			name:     "the HTTP port does not answer yet",
			run:      young,
			wantExit: 1, wantOutput: "the HTTP port does not answer",
		},
		{
			name:     "the admin credentials are sent",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: healthy, wantAuth: "neo4j:secret"},
			run:      young,
			wantExit: 0, wantRecord: started,
		},
		{
			name:     "credentials refused (the system database still starting) is a wait",
			server:   &fakeNeo4j{available: 200, status: 200, statusBody: healthy, wantAuth: "neo4j:other"},
			run:      young,
			wantExit: 1, wantOutput: "(HTTP 401)",
		},
		{
			name:     "the window is over: the check before, and no more waiting",
			server:   &fakeNeo4j{available: 404, status: 200, statusBody: healthy},
			run:      healthRun{processUp: true, age: "301", started: started, password: "secret"},
			wantExit: 0, wantOutput: "not waiting past 300s for the server to be available in the cluster", wantRecord: started,
		},
		{
			name:     "available once in this container: the check before",
			server:   &fakeNeo4j{available: 404, status: 200, statusBody: healthy},
			run:      healthRun{processUp: true, age: "30", started: started, record: started, password: "secret"},
			wantExit: 0, wantOutput: "HTTP port responding", wantRecord: started,
		},
		{
			name:     "a record from an earlier start of the container counts for nothing",
			server:   &fakeNeo4j{available: 404, status: 200, statusBody: healthy},
			run:      healthRun{processUp: true, age: "30", started: started, record: "Fri Oct 24 09:00:00 2026", password: "secret"},
			wantExit: 1, wantOutput: "Waiting for this server", wantRecord: "Fri Oct 24 09:00:00 2026",
		},
		{
			name:     "the container's age cannot be read: never held for it",
			server:   &fakeNeo4j{available: 404, status: 200, statusBody: healthy},
			run:      healthRun{processUp: true, started: started, password: "secret"},
			wantExit: 0, wantOutput: "cannot tell how long this container has run",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := 0
			if tc.server != nil {
				port = serve(t, tc.server)
			}
			got := runHealthScript(t, script, port, tc.run)
			if got.exitCode != tc.wantExit {
				t.Errorf("exit %d, want %d; output:\n%s", got.exitCode, tc.wantExit, got.output)
			}
			if !strings.Contains(got.output, tc.wantOutput) {
				t.Errorf("output %q does not say %q", got.output, tc.wantOutput)
			}
			if got.record != tc.wantRecord {
				t.Errorf("available record %q, want %q", got.record, tc.wantRecord)
			}
		})
	}
}

// The password reaches the server in the request, never on a command line.
func TestHealthScript_CredentialsStayOffTheCommandLine(t *testing.T) {
	script := buildHealthScript(nil)
	for _, tool := range []string{"curl ", "wget ", "cypher-shell"} {
		if strings.Contains(script, tool) {
			t.Errorf("the script runs %q: the Neo4j images ship no curl, and a command line shows its arguments", tool)
		}
	}
	if !strings.Contains(script, `"/dev/tcp/127.0.0.1/${HTTP_PORT}"`) {
		t.Error("the status endpoints are read through bash's /dev/tcp")
	}
}

// The bound sits inside the startup probe's budget: a server that never
// becomes available is not restarted for it.
func TestReadyWindowFitsTheStartupProbe(t *testing.T) {
	p := buildStartupProbe(nil)
	budget := p.InitialDelaySeconds + p.PeriodSeconds*p.FailureThreshold
	if ReadyWindowSeconds+2*p.PeriodSeconds*3 > budget {
		t.Errorf("ReadyWindowSeconds %d leaves too little of the startup probe's %ds budget", ReadyWindowSeconds, budget)
	}
}

// The probes are what they were: the new check reaches the servers through the
// ConfigMap, at each server's next restart, and an operator upgrade changes no
// pod template (operations.md id 181).
func TestServerProbes_Unchanged(t *testing.T) {
	cmd := []string{"/bin/bash", "-c", "/conf/health.sh"}
	want := map[string]corev1.Probe{
		"readiness": {ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: cmd}},
			InitialDelaySeconds: 45, PeriodSeconds: 15, TimeoutSeconds: 5, FailureThreshold: 8},
		"liveness": {ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: cmd}},
			InitialDelaySeconds: 120, PeriodSeconds: 60, TimeoutSeconds: 10, FailureThreshold: 3},
		"startup": {ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: cmd}},
			InitialDelaySeconds: 30, PeriodSeconds: 10, TimeoutSeconds: 5, FailureThreshold: 60, SuccessThreshold: 1},
	}
	got := map[string]*corev1.Probe{
		"readiness": buildReadinessProbe(nil), "liveness": buildLivenessProbe(nil), "startup": buildStartupProbe(nil),
	}
	for name, w := range want {
		g := got[name]
		if g == nil || g.Exec == nil || strings.Join(g.Exec.Command, " ") != strings.Join(cmd, " ") ||
			g.InitialDelaySeconds != w.InitialDelaySeconds || g.PeriodSeconds != w.PeriodSeconds ||
			g.TimeoutSeconds != w.TimeoutSeconds || g.FailureThreshold != w.FailureThreshold ||
			g.SuccessThreshold != w.SuccessThreshold {
			t.Errorf("%s probe changed: %+v", name, g)
		}
	}
}
