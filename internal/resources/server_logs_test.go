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

package resources_test

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

type xmlTemplate struct {
	URI string `xml:"eventTemplateUri,attr"`
}

type xmlPattern struct {
	Pattern string `xml:"pattern,attr"`
}

type xmlRolling struct {
	Name        string       `xml:"name,attr"`
	FileName    string       `xml:"fileName,attr"`
	FilePattern string       `xml:"filePattern,attr"`
	JSON        *xmlTemplate `xml:"JsonTemplateLayout"`
	DebugText   *xmlPattern  `xml:"Neo4jDebugLogLayout"`
	Text        *xmlPattern  `xml:"PatternLayout"`
	Size        struct {
		Size string `xml:"size,attr"`
	} `xml:"Policies>SizeBasedTriggeringPolicy"`
	Rollover struct {
		FileIndex string `xml:"fileIndex,attr"`
		Max       string `xml:"max,attr"`
	} `xml:"DefaultRolloverStrategy"`
}

type xmlConsole struct {
	Name   string      `xml:"name,attr"`
	Target string      `xml:"target,attr"`
	JSON   xmlTemplate `xml:"JsonTemplateLayout"`
}

type xmlLogger struct {
	Name       string `xml:"name,attr"`
	Level      string `xml:"level,attr"`
	Additivity string `xml:"additivity,attr"`
	Refs       []struct {
		Ref string `xml:"ref,attr"`
	} `xml:"AppenderRef"`
}

type xmlLog4j struct {
	MonitorInterval string       `xml:"monitorInterval,attr"`
	Rolling         []xmlRolling `xml:"Appenders>RollingRandomAccessFile"`
	Console         []xmlConsole `xml:"Appenders>Console"`
	Root            xmlLogger    `xml:"Loggers>Root"`
	Loggers         []xmlLogger  `xml:"Loggers>Logger"`
}

func parseServerLogs(t *testing.T, s string) xmlLog4j {
	t.Helper()
	var c xmlLog4j
	require.NoError(t, xml.Unmarshal([]byte(s), &c), "server-logs.xml must be well-formed:\n%s", s)
	return c
}

func refs(l xmlLogger) []string {
	var out []string
	for _, r := range l.Refs {
		out = append(out, r.Ref)
	}
	return out
}

func TestStdoutLogs(t *testing.T) {
	assert.Nil(t, resources.StdoutLogs(nil))
	assert.Nil(t, resources.StdoutLogs(&neo4jv1beta1.MonitoringSpec{}))
	assert.Nil(t, resources.StdoutLogs(&neo4jv1beta1.MonitoringSpec{Logs: &neo4jv1beta1.MonitoringLogsSpec{}}))
	assert.Equal(t, []string{"query", "debug"}, resources.StdoutLogs(&neo4jv1beta1.MonitoringSpec{
		Logs: &neo4jv1beta1.MonitoringLogsSpec{Stdout: []string{"debug", "query", "query"}}}),
		"a fixed order, without duplicates")
}

// Every combination of listed logs, on both lines: one Console appender per
// listed log, referenced by exactly that log's logger, and nothing else moves.
func TestBuildServerLogsXML_RoutesEachListedLogToStdout(t *testing.T) {
	appender := map[string]struct{ name, template, logger string }{
		"query":    {"QueryStdout", "classpath:org/neo4j/logging/QueryLogJsonLayout.json", "QueryLogger"},
		"security": {"SecurityStdout", "classpath:org/neo4j/logging/StructuredJsonLayout.json", "SecurityLogger"},
		"debug":    {"DebugStdout", "classpath:org/neo4j/logging/StructuredLayoutWithMessage.json", "Root"},
	}
	all := []string{"query", "security", "debug"}
	for _, tag := range []string{"5.26-enterprise", "2026.08.1-enterprise"} {
		for mask := 0; mask < 8; mask++ {
			var listed []string
			for i, l := range all {
				if mask&(1<<i) != 0 {
					listed = append(listed, l)
				}
			}
			c := parseServerLogs(t, resources.BuildServerLogsXML(tag, listed))
			assert.Equal(t, "30", c.MonitorInterval, "Log4j must keep re-reading the file")
			require.Len(t, c.Console, len(listed), "%s %v", tag, listed)

			want := map[string][]string{
				"Root": {"DebugLog"}, "QueryLogger": {"QueryLog"},
				"HttpLogger": {"HttpLog"}, "SecurityLogger": {"SecurityLog"},
			}
			for i, l := range listed {
				a := appender[l]
				assert.Equal(t, a.name, c.Console[i].Name)
				assert.Equal(t, "SYSTEM_OUT", c.Console[i].Target)
				assert.Equal(t, a.template, c.Console[i].JSON.URI)
				want[a.logger] = append(want[a.logger], a.name)
			}
			assert.Equal(t, want["Root"], refs(c.Root), "%s %v", tag, listed)
			for _, lg := range c.Loggers {
				assert.Equal(t, want[lg.Name], refs(lg), "%s %v: %s", tag, listed, lg.Name)
			}
		}
	}
}

// The file appenders and loggers are Neo4j's defaults for the line, whatever
// is listed: debug.log JSON on CalVer, Neo4j's debug text layout on 5.26 (and
// on a tag that does not parse), every other file in the default pattern.
func TestBuildServerLogsXML_FilesKeepNeo4jDefaults(t *testing.T) {
	const text = "%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"
	for _, tc := range []struct {
		tag       string
		debugJSON bool
	}{
		{"5.26-enterprise", false},
		{"5.26.31-enterprise", false},
		{"2025.01.0-enterprise", true},
		{"2026.08.1-enterprise", true},
		{"my-registry-build", false},
	} {
		for _, listed := range [][]string{nil, {"query", "security", "debug"}} {
			c := parseServerLogs(t, resources.BuildServerLogsXML(tc.tag, listed))
			require.Len(t, c.Rolling, 4)
			for i, f := range []struct {
				name, file, max string
			}{{"DebugLog", "debug.log", "7"}, {"HttpLog", "http.log", "5"}, {"QueryLog", "query.log", "7"}, {"SecurityLog", "security.log", "7"}} {
				r := c.Rolling[i]
				assert.Equal(t, f.name, r.Name)
				assert.Equal(t, "${config:server.directories.logs}/"+f.file, r.FileName)
				assert.Equal(t, "$${config:server.directories.logs}/"+f.file+".%02i", r.FilePattern)
				assert.Equal(t, "20 MB", r.Size.Size)
				assert.Equal(t, "min", r.Rollover.FileIndex)
				assert.Equal(t, f.max, r.Rollover.Max, f.name)
				if f.name != "DebugLog" {
					require.NotNil(t, r.Text, f.name)
					assert.Equal(t, text, r.Text.Pattern)
					continue
				}
				if tc.debugJSON {
					require.NotNil(t, r.JSON, tc.tag)
					assert.Equal(t, "classpath:org/neo4j/logging/StructuredLayoutWithMessage.json", r.JSON.URI)
					assert.Nil(t, r.DebugText)
				} else {
					require.NotNil(t, r.DebugText, tc.tag)
					assert.Equal(t, "%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p [%c{1.}] %m%n", r.DebugText.Pattern)
					assert.Nil(t, r.JSON)
				}
			}
			assert.Equal(t, "INFO", c.Root.Level)
			for _, lg := range c.Loggers {
				assert.Equal(t, "INFO", lg.Level, lg.Name)
				assert.Equal(t, "false", lg.Additivity, lg.Name)
			}
		}
	}
}

func TestClusterConfigMap_StdoutLogs(t *testing.T) {
	cluster := newTestCluster()
	cm := resources.BuildConfigMapForEnterprise(cluster)
	assert.NotContains(t, cm.Data, resources.ServerLogsConfigKey, "nothing listed: no log configuration")
	assert.NotContains(t, cm.Data["neo4j.conf"], "server.logs.config")

	cluster.Spec.Monitoring = &neo4jv1beta1.MonitoringSpec{Logs: &neo4jv1beta1.MonitoringLogsSpec{}}
	cm = resources.BuildConfigMapForEnterprise(cluster)
	assert.NotContains(t, cm.Data, resources.ServerLogsConfigKey, "an empty list renders nothing")

	cluster.Spec.Monitoring.Logs.Stdout = []string{"security"}
	cm = resources.BuildConfigMapForEnterprise(cluster)
	require.Contains(t, cm.Data, resources.ServerLogsConfigKey)
	assert.Contains(t, cm.Data[resources.ServerLogsConfigKey], `<Console name="SecurityStdout"`)
	assert.Equal(t, 1, strings.Count(cm.Data["neo4j.conf"], "server.logs.config=/operator-conf/server-logs.xml"),
		"read from the ConfigMap mount, which the kubelet keeps current")
}
