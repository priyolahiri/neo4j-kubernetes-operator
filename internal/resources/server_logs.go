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
	"fmt"
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Neo4j's own logs on standard output (spec.monitoring.logs.stdout).
//
// Neo4j writes query.log, security.log and debug.log to files under /logs,
// where a node-level log collector cannot see them; only neo4j.log reaches the
// container's standard output. Its logging is a Log4j 2 file, server-logs.xml,
// on 5.26 and CalVer alike (Operations Manual, monitoring/logging): a Console
// appender writes to standard output, which Neo4j honours in console mode —
// how the container runs it — and Neo4j's JSON layouts give one record per
// event. The operator renders that file into the deployment's ConfigMap and
// points server.logs.config at the mounted copy:
//
//   - server.logs.config is static, so turning the feature on or off restarts
//     each server once;
//   - the file is read from the ConfigMap mount, which the kubelet updates in
//     place, and Log4j re-reads it (monitorInterval="30"), so changing the
//     list afterwards restarts nothing. The restart decision never hashes it.
//
// The file is the operator's own, not a copy of Neo4j's: the same appenders,
// loggers, layouts and rollover as the default each line ships (debug.log is
// JSON on CalVer, Neo4j's debug-log text layout on 5.26), plus one Console
// appender per listed log. The files keep their formats, so Neo4j support
// tooling reading debug.log is unaffected.

const (
	// ServerLogsConfigKey is the ConfigMap key, and file name, of the
	// rendered log configuration.
	ServerLogsConfigKey = "server-logs.xml"
	// ClusterServerLogsConfigPath is where a cluster server reads it: the
	// ConfigMap mount, not startup.sh's copy in /conf, which never changes.
	ClusterServerLogsConfigPath = OperatorConfStagingPath + "/" + ServerLogsConfigKey
	// StandaloneServerLogsConfigPath is the standalone's ConfigMap mount.
	StandaloneServerLogsConfigPath = "/conf/" + ServerLogsConfigKey

	StdoutLogQuery    = "query"
	StdoutLogSecurity = "security"
	StdoutLogDebug    = "debug"
)

// StdoutLogs returns the logs spec.monitoring.logs.stdout lists, in a fixed
// order and without duplicates; nil when there are none.
func StdoutLogs(mon *neo4jv1beta1.MonitoringSpec) []string {
	if mon == nil || mon.Logs == nil {
		return nil
	}
	listed := map[string]bool{}
	for _, l := range mon.Logs.Stdout {
		listed[l] = true
	}
	var out []string
	for _, l := range []string{StdoutLogQuery, StdoutLogSecurity, StdoutLogDebug} {
		if listed[l] {
			out = append(out, l)
		}
	}
	return out
}

// ServerLogsConfigLine is the neo4j.conf line that points Neo4j at the
// rendered file.
func ServerLogsConfigLine(path string) string {
	return "server.logs.config=" + path
}

// debugLogLayout is the layout each line ships for debug.log: JSON on CalVer
// (the default since 2025.01), Neo4j's debug-log text layout on 5.26. A tag
// that does not parse gets the text layout, which both lines provide.
func debugLogLayout(tag string) string {
	if v, err := neo4j.ParseVersion(tag); err == nil && v.IsCalver {
		return `<JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredLayoutWithMessage.json"/>`
	}
	return `<Neo4jDebugLogLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p [%c{1.}] %m%n"/>`
}

// stdoutAppenders maps each log to its Console appender's name and JSON
// template, both documented on both lines.
var stdoutAppenders = map[string]struct{ name, template string }{
	StdoutLogQuery:    {"QueryStdout", "classpath:org/neo4j/logging/QueryLogJsonLayout.json"},
	StdoutLogSecurity: {"SecurityStdout", "classpath:org/neo4j/logging/StructuredJsonLayout.json"},
	StdoutLogDebug:    {"DebugStdout", "classpath:org/neo4j/logging/StructuredLayoutWithMessage.json"},
}

// BuildServerLogsXML renders server-logs.xml for an image tag, writing the
// listed logs to standard output as well as to their files.
func BuildServerLogsXML(tag string, stdout []string) string {
	rolling := func(name, file, layout string, keep int) string {
		return fmt.Sprintf(`        <RollingRandomAccessFile name="%s" fileName="${config:server.directories.logs}/%s"
                                 filePattern="$${config:server.directories.logs}/%s.%%02i">
            %s
            <Policies>
                <SizeBasedTriggeringPolicy size="20 MB"/>
            </Policies>
            <DefaultRolloverStrategy fileIndex="min" max="%d"/>
        </RollingRandomAccessFile>
`, name, file, file, layout, keep)
	}
	pattern := `<PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>`

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!--
    Rendered by the Neo4j Kubernetes operator from spec.monitoring.logs; edits
    are overwritten. The file appenders match Neo4j's defaults for this version.
    Each Console appender writes one log to standard output as JSON.
-->
<Configuration status="ERROR" monitorInterval="30">
    <Appenders>
`)
	b.WriteString(rolling("DebugLog", "debug.log", debugLogLayout(tag), 7))
	b.WriteString(rolling("HttpLog", "http.log", pattern, 5))
	b.WriteString(rolling("QueryLog", "query.log", pattern, 7))
	b.WriteString(rolling("SecurityLog", "security.log", pattern, 7))
	refs := map[string]string{}
	for _, l := range stdout {
		a, ok := stdoutAppenders[l]
		if !ok {
			continue
		}
		fmt.Fprintf(&b, `        <Console name="%s" target="SYSTEM_OUT">
            <JsonTemplateLayout eventTemplateUri="%s"/>
        </Console>
`, a.name, a.template)
		refs[l] = fmt.Sprintf("\n            <AppenderRef ref=\"%s\"/>", a.name)
	}
	fmt.Fprintf(&b, `    </Appenders>
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="DebugLog"/>%s
        </Root>
        <Logger name="QueryLogger" level="INFO" additivity="false">
            <AppenderRef ref="QueryLog"/>%s
        </Logger>
        <Logger name="HttpLogger" level="INFO" additivity="false">
            <AppenderRef ref="HttpLog"/>
        </Logger>
        <Logger name="SecurityLogger" level="INFO" additivity="false">
            <AppenderRef ref="SecurityLog"/>%s
        </Logger>
    </Loggers>
</Configuration>
`, refs[StdoutLogDebug], refs[StdoutLogQuery], refs[StdoutLogSecurity])
	return b.String()
}
