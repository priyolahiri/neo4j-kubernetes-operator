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
	"strconv"
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ReadyWindowSeconds bounds how long a server's readiness waits for the server
// to be available in the cluster after its container starts (operations.md id
// 181). It sits well inside the startup probe's budget (buildStartupProbe:
// 30s + 60 × 10s), so a server that never becomes available is never restarted
// for it: after the window the check is what it was before.
const ReadyWindowSeconds = 300

// The values the health script reads at the top, named so a test can point
// them elsewhere.
const (
	healthScriptHTTPPortLine = "HTTP_PORT=7474"
	healthScriptStateDirLine = "STATE_DIR=/tmp"
)

// buildHealthScript renders /conf/health.sh for a cluster server. The
// readiness, liveness and startup probes all run it (they share one command,
// so the pod template stays as it was): the file is a copy startup.sh makes in
// /conf when the container starts, so a new version reaches each server at its
// next restart, and the ConfigMap manager never restarts a server for it.
//
// What it checks, in order:
//
//  1. The Neo4j process runs; if not, unhealthy.
//  2. Once this container has seen its server available (a record in /tmp,
//     keyed by when PID 1 started, so a container restart starts afresh), the
//     check before #564: the HTTP port answers, or the server is forming.
//  3. Otherwise, available in the cluster: GET /db/system/cluster/available on
//     the local HTTP port answers 200 (the server hosts `system` and it is
//     available on it), and GET /dbms/cluster/status reports no database
//     `healthy: false` and none this server hosts as a primary
//     (`core: true`) with `participatingInRaftGroup: false`. Both endpoints
//     are documented on 5.26 and on CalVer (Operations Manual, "Monitor
//     cluster endpoints for status information", /5/ and /current/); they
//     need credentials while dbms.security.cluster_status_auth_enabled is
//     true (its default), so the script sends the admin credentials the
//     container already has (DB_USERNAME, DB_PASSWORD) to localhost.
//  4. Not available yet: not ready while the container has run less than
//     ReadyWindowSeconds; after that, or when its age cannot be read, the
//     check before #564 again, so a cluster that cannot form does not hold
//     its servers forever and the liveness probe never restarts one for it.
//
// HTTP goes through bash's /dev/tcp: the Neo4j images ship no curl (the
// Debian ones purge it after the build), and a request built in the shell
// keeps the password out of any command line.
func buildHealthScript(_ *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	return strings.NewReplacer(
		"@HTTP_PORT_LINE@", healthScriptHTTPPortLine,
		"@STATE_DIR_LINE@", healthScriptStateDirLine,
		"@READY_WINDOW@", strconv.Itoa(ReadyWindowSeconds),
	).Replace(clusterHealthScriptTemplate)
}

const clusterHealthScriptTemplate = `#!/bin/bash
# Health check for a Neo4j cluster server, run by the readiness, liveness and
# startup probes. Ready means the server is available in the cluster: a
# StatefulSet roll (a plugin, a config change, TLS) waits for it before it
# restarts the next server, so the cluster keeps its majority. The wait is
# bounded, and once this container has seen its server available the check is
# the process and the HTTP port only, so the liveness probe never restarts a
# server for being out of the cluster.

READY_WINDOW_SECONDS=@READY_WINDOW@
@HTTP_PORT_LINE@
@STATE_DIR_LINE@
AVAILABLE_RECORD="${STATE_DIR}/.neo4j-operator-available"

if ! (pgrep -f "EnterpriseEntryPoint" > /dev/null || pgrep -f "Neo4jEnterprise" > /dev/null); then
    echo "Neo4j process not running"
    exit 1
fi

# The check before the server's availability counted: the HTTP port answers,
# or the server is in cluster formation.
process_and_port() {
    if (echo > "/dev/tcp/localhost/${HTTP_PORT}") >/dev/null 2>&1; then
        echo "Neo4j HTTP port responding - healthy${1:+ ($1)}"
        exit 0
    fi
    if grep -q "Resolved endpoints" /logs/neo4j.log 2>/dev/null || \
       grep -q "Starting.*cluster" /logs/neo4j.log 2>/dev/null || \
       grep -q "Waiting for.*servers" /logs/neo4j.log 2>/dev/null || \
       grep -q "minimum_initial_system_primaries_count" /logs/neo4j.log 2>/dev/null || \
       grep -q "cluster formation barrier" /logs/neo4j.log 2>/dev/null; then
        echo "Neo4j in cluster formation process - allowing more time"
        exit 0
    fi
    echo "Neo4j process running but HTTP port not responding and no cluster activity detected"
    exit 1
}

# http_get PATH prints "<status> <body>" for a GET of PATH on the local HTTP
# port, sent with the container's admin credentials; it fails when nothing
# answers.
http_get() {
    local auth="" line="" status="" body="" in_body=0 deadline
    if [ -n "${DB_PASSWORD:-}" ]; then
        auth=$(printf '%s:%s' "${DB_USERNAME:-neo4j}" "${DB_PASSWORD}" | base64 | tr -d '\r\n')
    fi
    { exec 3<>"/dev/tcp/127.0.0.1/${HTTP_PORT}"; } 2>/dev/null || return 1
    {
        printf 'GET %s HTTP/1.0\r\nHost: localhost\r\nAccept: */*\r\n' "$1"
        if [ -n "$auth" ]; then
            printf 'Authorization: Basic %s\r\n' "$auth"
        fi
        printf '\r\n'
    } >&3 2>/dev/null
    # Two seconds for the whole answer: the probe that runs this allows five
    # for both requests.
    deadline=$((SECONDS + 2))
    if ! IFS= read -r -t 2 line <&3; then
        exec 3<&-
        return 1
    fi
    status=${line#* }
    status=${status%% *}
    while [ "$SECONDS" -le "$deadline" ] && { IFS= read -r -t 1 line <&3 || [ -n "$line" ]; }; do
        line=${line%$'\r'}
        if [ "$in_body" = 1 ]; then
            body="${body}${line}"
        elif [ -z "$line" ]; then
            in_body=1
        fi
        line=""
    done
    exec 3<&-
    printf '%s %s' "$status" "$body"
}

# not_available prints why the server is not available in the cluster, and
# nothing when it is.
not_available() {
    local response status body name="" part bad=""
    if ! response=$(http_get /db/system/cluster/available); then
        echo "the HTTP port does not answer"
        return
    fi
    status=${response%% *}
    if [ "$status" != "200" ]; then
        echo "the system database is not available on this server (HTTP ${status})"
        return
    fi
    if ! response=$(http_get /dbms/cluster/status); then
        echo "the cluster status does not answer"
        return
    fi
    status=${response%% *}
    body=${response#* }
    case "$status" in
        200) ;;
        404) return ;; # no per-database status to read
        *) echo "the cluster status answered HTTP ${status}"; return ;;
    esac
    # One line per JSON object: a database's name, then its status fields.
    while IFS= read -r part; do
        case "$part" in
            *'"databaseName":"'*)
                name=${part#*\"databaseName\":\"}
                name=${name%%\"*}
                ;;
        esac
        case "$part" in
            *'"healthy":false'* | *'"isHealthy":false'*)
                bad="${bad} ${name} (not healthy)"
                ;;
            *'"core":true'*'"participatingInRaftGroup":false'* | *'"participatingInRaftGroup":false'*'"core":true'*)
                bad="${bad} ${name} (a primary not yet in its Raft group)"
                ;;
        esac
    done <<EOF
$(printf '%s' "$body" | tr -d ' \t\r\n' | tr '{' '\n')
EOF
    if [ -n "$bad" ]; then
        echo "databases on this server:${bad}"
    fi
}

# record_started: from here on this container gets the check before.
record_started() {
    if [ -n "$STARTED" ]; then
        printf '%s' "$STARTED" > "$AVAILABLE_RECORD" 2>/dev/null
    fi
}

# The script cannot tell which probe runs it — readiness, liveness or startup —
# so it waits only where waiting cannot restart a server: before this
# container's server has been seen available, for at most READY_WINDOW_SECONDS
# (only the startup probe runs then, and its budget is longer).
STARTED=$(ps -o lstart= -p 1 2>/dev/null)
if [ -n "$STARTED" ] && [ "$(cat "$AVAILABLE_RECORD" 2>/dev/null)" = "$STARTED" ]; then
    process_and_port
fi
AGE=$(ps -o etimes= -p 1 2>/dev/null | tr -d ' ')
case "$AGE" in
    '' | *[!0-9]*)
        process_and_port "cannot tell how long this container has run"
        ;;
esac
if [ "$AGE" -ge "$READY_WINDOW_SECONDS" ]; then
    record_started
    process_and_port "not waiting past ${READY_WINDOW_SECONDS}s for the server to be available in the cluster"
fi

WHY=$(not_available)
if [ -z "$WHY" ]; then
    record_started
    echo "Neo4j server available in the cluster"
    exit 0
fi
echo "Waiting for this server to be available in the cluster (${AGE}s of ${READY_WINDOW_SECONDS}s): ${WHY}"
exit 1
`
