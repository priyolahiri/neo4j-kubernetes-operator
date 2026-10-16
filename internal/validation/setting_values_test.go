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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// The parsers mirror Neo4j's own, read from its source on release/5.26.0,
// release/5.26.31 and the CalVer releases listed on typedSettings (identical on
// all of them): ByteUnit.parse, TimeUtil.parseTimeMillis, and
// SettingValueParsers.INT/LONG/BOOL/ofEnum. Each "accepted" row is a value
// Neo4j starts with, each "refused" row one it refuses, and each "unknown" row
// a spelling the operator deliberately does not judge (it must never refuse it).

func TestReadNeo4jByteSize(t *testing.T) {
	const (
		ki = int64(1) << 10
		mi = int64(1) << 20
		gi = int64(1) << 30
	)
	// Every one of ByteUnit's 25 names, as Neo4j documents them on both lines.
	units := map[string]int64{
		"B":   1,
		"KiB": ki, "KB": ki, "K": ki, "kB": ki, "kb": ki, "k": ki,
		"MiB": mi, "MB": mi, "M": mi, "mB": mi, "mb": mi, "m": mi,
		"GiB": gi, "GB": gi, "G": gi, "gB": gi, "gb": gi, "g": gi,
		"TiB": 1 << 40, "TB": 1 << 40,
		"PiB": 1 << 50, "PB": 1 << 50,
		"EiB": 1 << 60, "EB": 1 << 60,
	}
	require.Len(t, units, 25)
	for unit, factor := range units {
		for _, spelling := range []string{"2" + unit, "2 " + unit, " 2" + unit + " "} {
			r := readNeo4jByteSize(javaTrim(spelling))
			if assert.Equal(t, valueAccepted, r.verdict, "%q", spelling) {
				assert.Equal(t, 2*factor, r.n, "%q", spelling)
			}
		}
	}

	accepted := map[string]int64{
		"0":           0,
		"0B":          0,
		"-0":          0, // NumberFormat reads -0.0, which is not below zero
		"1048576":     mi,
		"1.5G":        gi + gi/2,
		"0.5MiB":      mi / 2,
		".5k":         512,
		"1.":          1,
		"1.4g":        1503238553, // 1.4 × 2^30, truncated; what the operator itself renders
		"1.9":         1,          // a fraction of a byte is truncated
		"10GiB extra": 10 * gi,    // Neo4j ignores what follows the unit
		"512\tM":      512 * mi,
		// Neo4j's range check is "> Long.MAX_VALUE" in double arithmetic, so
		// exactly 2^63 bytes passes and is saturated.
		"8388608.00TiB":       math.MaxInt64,
		"8EiB":                math.MaxInt64,
		"9223372036854775808": math.MaxInt64,
	}
	for value, want := range accepted {
		r := readNeo4jByteSize(javaTrim(value))
		if assert.Equal(t, valueAccepted, r.verdict, "%q", value) && assert.True(t, r.exact, "%q", value) {
			assert.Equal(t, want, r.n, "%q", value)
		}
	}

	for _, value := range []string{
		"", "   ", "-", ".", "G", "MiB",
		"2Gi", "512Mi", "100Ki", "1Ti", "4Ei", // Kubernetes suffixes
		"1gib", "1GIB", "1Kib", "1kiB", "1Gb", "1Mb", "1tb", "1T", "1P", "1E",
		"1 Gi", "10GiBs", "5KK", "1.5XB", "1e3", "0x10", "abc", "+5G",
		"-1", "-1G", "-0.5K",
		"9EiB", "10000000000000000000", // above 2^63 bytes
	} {
		assert.Equal(t, valueRefused, readNeo4jByteSize(javaTrim(value)).verdict, "%q", value)
	}

	for _, value := range []string{
		"1,024K", "1,5G", // NumberFormat's grouping
		"1E3", "1E-3", // a possible exponent
		"NaN", "NaNG",
		"１G", "2 Ｇ", "∞", // non-ASCII
		"1.2.3G",
	} {
		assert.Equal(t, valueUnknown, readNeo4jByteSize(javaTrim(value)).verdict, "%q", value)
	}
}

func TestReadNeo4jDuration(t *testing.T) {
	accepted := map[string]time.Duration{
		"0":           0,
		"30":          30 * time.Second, // a bare number is seconds
		"007":         7 * time.Second,
		"2147483647":  2147483647 * time.Second,
		"30s":         30 * time.Second,
		"30S":         30 * time.Second,
		"500ms":       500 * time.Millisecond,
		"500MS":       500 * time.Millisecond,
		"1m":          time.Minute,
		"1M":          time.Minute, // units are lower-cased: M is minutes
		"2h":          2 * time.Hour,
		"2H":          2 * time.Hour,
		"1d":          24 * time.Hour,
		"1D":          24 * time.Hour,
		"1500us":      time.Millisecond, // truncated to milliseconds, as Neo4j does
		"1500US":      time.Millisecond,
		"1500μs":      time.Millisecond,
		"1500Μs":      time.Millisecond, // capital mu lower-cases to μ
		"5000000ns":   5 * time.Millisecond,
		"999us":       0,
		"1h30m":       90 * time.Minute,
		"1d2h3m4s5ms": 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second + 5*time.Millisecond,
		"0s":          0,
	}
	for value, want := range accepted {
		r := readNeo4jDuration(javaTrim(value))
		if assert.Equal(t, valueAccepted, r.verdict, "%q", value) && assert.True(t, r.exact, "%q", value) {
			assert.Equal(t, int64(want), r.n, "%q", value)
		}
	}
	// Surrounding whitespace and control characters go, as String.trim does.
	for _, value := range []string{" 5s ", "\t5s\x0b", "\x005s\x1f"} {
		assert.Equal(t, exactly(int64(5*time.Second)), readNeo4jDuration(javaTrim(value)), "%q", value)
	}

	for _, value := range []string{
		"", "s", "ms", "-1s", "+5s", "-5",
		"1.5s", "0.5h", "5 s", "5s 3m",
		"5sec", "5min", "5mins", "1hr", "2days", "1w", "1y",
		"PT30S", "P1D", "1h30", "5s5",
		"2147483648", "2147483648s", "99999999999ms",
	} {
		assert.Equal(t, valueRefused, readNeo4jDuration(javaTrim(value)).verdict, "%q", value)
	}

	// Non-ASCII other than mu is not judged: "5µs" (the micro sign, which Neo4j
	// refuses) and full-width digits (which Java's isDigit accepts).
	for _, value := range []string{"5µs", "５s", "5ｓ"} {
		assert.Equal(t, valueUnknown, readNeo4jDuration(javaTrim(value)).verdict, "%q", value)
	}

	// Saturating sums are accepted without a value: Neo4j wraps, so bounds
	// are not judged.
	r := readNeo4jDuration("2147483647d2147483647d")
	assert.Equal(t, valueAccepted, r.verdict)
	assert.False(t, r.exact)
}

func TestReadJavaInteger(t *testing.T) {
	for value, want := range map[string]int64{
		"0": 0, "-1": -1, "+5": 5, "007": 7, "42": 42,
		"2147483647": 2147483647, "-2147483648": -2147483648,
	} {
		r := readJavaInteger(javaTrim(value), 32)
		if assert.Equal(t, valueAccepted, r.verdict, "%q", value) {
			assert.Equal(t, want, r.n, "%q", value)
		}
	}
	for _, value := range []string{
		"", "+", "-", "1.0", "1e3", "1k", "1K", "ten", "1_000", "1,000", "0x10", "1 0",
		"2147483648", "-2147483649",
	} {
		assert.Equal(t, valueRefused, readJavaInteger(javaTrim(value), 32).verdict, "%q", value)
	}
	assert.Equal(t, valueUnknown, readJavaInteger("١٢", 32).verdict, "Java reads non-ASCII digits")

	assert.Equal(t, valueAccepted, readJavaInteger("9223372036854775807", 64).verdict)
	assert.Equal(t, valueAccepted, readJavaInteger("2147483648", 64).verdict)
	for _, value := range []string{"9223372036854775808", "1MiB", "1048576B", "128K"} {
		assert.Equal(t, valueRefused, readJavaInteger(value, 64).verdict, "%q", value)
	}
}

func TestReadOneOf(t *testing.T) {
	booleans := []string{"true", "false"}
	for _, value := range []string{"true", "TRUE", "True", "false", "FALSE", "fAlSe"} {
		assert.Equal(t, valueAccepted, readOneOf(value, booleans).verdict, "%q", value)
	}
	for _, value := range []string{"", "yes", "no", "on", "off", "1", "0", "enabled", "t", "truee"} {
		assert.Equal(t, valueRefused, readOneOf(value, booleans).verdict, "%q", value)
	}
	assert.Equal(t, valueUnknown, readOneOf("falſe", booleans).verdict,
		"Java's equalsIgnoreCase matches ſ with s; non-ASCII is not judged")

	levels := []string{"OFF", "INFO", "VERBOSE"}
	for _, value := range []string{"OFF", "off", "Info", "verbose"} {
		assert.Equal(t, valueAccepted, readOneOf(value, levels).verdict, "%q", value)
	}
	for _, value := range []string{"", "true", "false", "DEBUG", "ON", "INFO,VERBOSE"} {
		assert.Equal(t, valueRefused, readOneOf(value, levels).verdict, "%q", value)
	}
}

// Every table type, through the entry point, including the documented minimums.
func TestValidateSettingValues(t *testing.T) {
	path := field.NewPath("spec", "config")
	cases := []struct {
		key      string
		accepted []string
		refused  []string
	}{
		{"db.tx_log.buffer.size", []string{"131072", "1048576", " 4194304 "}, []string{"1MiB", "1M", "128K", "131071", "0", "-1", "1.5"}},
		{"server.memory.heap.max_size", []string{"4G", "4g", "4GiB", "4096m", "512MB", "1.5G", "4294967296"}, []string{"4Gi", "4 Gi", "4GIB", "four", ""}},
		{"server.memory.pagecache.size", []string{"512m", "2G", "1g"}, []string{"512Mi", "2Gi"}},
		{"db.memory.transaction.max", []string{"0", "0B", "1M", "1MiB", "256m"}, []string{"512K", "1048575", "1Mi"}},
		{"dbms.memory.transaction.total.max", []string{"0", "10M", "10MiB", "3g"}, []string{"5M", "9MiB", "3Gi"}},
		{"db.tx_log.rotation.size", []string{"128K", "256M"}, []string{"64K", "127KiB"}},
		{"db.checkpoint.interval.volume", []string{"1K", "250M"}, []string{"1000", "0"}},
		{"db.transaction.timeout", []string{"0", "60s", "5m", "1h30m", "300"}, []string{"60 s", "1.5m", "60sec", "PT60S", "-1s"}},
		{"db.transaction.bookmark_ready_timeout", []string{"1s", "1000ms", "30s"}, []string{"500ms", "0", "999ms"}},
		{"server.bolt.connection_keep_alive", []string{"1ms", "1m"}, []string{"999us", "0"}},
		{"dbms.routing_ttl", []string{"1s", "300s"}, []string{"0", "500ms"}},
		{"db.checkpoint.interval.tx", []string{"1", "100000"}, []string{"0", "-5", "1e5", "100k"}},
		{"dbms.security.auth_max_failed_attempts", []string{"0", "3"}, []string{"-1", "three"}},
		{"server.bolt.thread_pool_max_size", []string{"400", "+400"}, []string{"400.0", "4e2", "2147483648"}},
		{"db.tx_log.preallocate", []string{"true", "False"}, []string{"yes", "1", ""}},
		{"db.logs.query.enabled", []string{"INFO", "off", "Verbose"}, []string{"true", "false", "DEBUG"}},
		{"db.checkpoint", []string{"PERIODIC", "volumetric"}, []string{"periodical", "FAST"}},
		{"dbms.routing.default_router", []string{"SERVER", "client"}, []string{"DRIVER"}},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			for _, v := range tc.accepted {
				assert.Empty(t, ValidateSettingValues(path, map[string]string{tc.key: v}), "%s=%q", tc.key, v)
			}
			for _, v := range tc.refused {
				errs := ValidateSettingValues(path, map[string]string{tc.key: v})
				if assert.Len(t, errs, 1, "%s=%q", tc.key, v) {
					assert.Equal(t, "spec.config["+tc.key+"]", errs[0].Field)
					assert.Equal(t, field.ErrorTypeInvalid, errs[0].Type)
					assert.Equal(t, v, errs[0].BadValue)
				}
			}
		})
	}
}

// Values Neo4j does not parse as written are never judged.
func TestValidateSettingValues_TransformedValuesAreNotJudged(t *testing.T) {
	path := field.NewPath("spec", "config")
	for _, v := range []string{
		"$(cat /run/secrets/heap)", " $(echo 4G) ", // --expand-commands
		"4\\" + "u0047", `1\M`, // java.util.Properties decodes escapes ("4G", "1M")
	} {
		assert.Empty(t, ValidateSettingValues(path, map[string]string{"server.memory.heap.max_size": v}), "%q", v)
		assert.Empty(t, ValidateSettingValues(path, map[string]string{"db.tx_log.buffer.size": v}), "%q", v)
	}
	// Too short to be a command (Config.isCommand needs more than "$()").
	assert.Len(t, ValidateSettingValues(path, map[string]string{"db.tx_log.buffer.size": "$()"}), 1)
	// A line break is refused by ConfigValidator itself, once.
	errs := NewConfigValidator().ValidateConfigMap(map[string]string{"db.tx_log.buffer.size": "1MiB\nfoo=bar"})
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Detail, "newline")
}

func TestValidateSettingValues_UnlistedSettingsPassUntouched(t *testing.T) {
	config := map[string]string{
		"db.tx_log.rotation.retention_policy":               "whenever",
		"dbms.cluster.raft.leader_failure_detection_window": "nonsense",
		"server.metrics.enabled":                            "maybe",
		"apoc.export.file.enabled":                          "yes please",
		"my.custom.plugin.setting":                          "1MiB",
		"dbms.tx_log.buffer.size":                           "1MiB", // the 4.x name: Neo4j migrates it, not ours to judge
	}
	assert.Empty(t, ValidateSettingValues(field.NewPath("spec", "config"), config))
	assert.Empty(t, ValidateSettingValues(field.NewPath("spec", "config"), nil))
}

func TestValidateSettingValues_ReportsEveryBadValueInKeyOrder(t *testing.T) {
	errs := ValidateSettingValues(field.NewPath("spec", "config"), map[string]string{
		"server.memory.pagecache.size": "1Gi",
		"db.tx_log.buffer.size":        "1MiB",
		"db.transaction.timeout":       "60 seconds",
		"db.logs.query.enabled":        "true",
	})
	var fields []string
	for _, e := range errs {
		fields = append(fields, e.Field)
	}
	assert.Equal(t, []string{
		"spec.config[db.logs.query.enabled]",
		"spec.config[db.transaction.timeout]",
		"spec.config[db.tx_log.buffer.size]",
		"spec.config[server.memory.pagecache.size]",
	}, fields)
}

func TestSettingTypeExpectation(t *testing.T) {
	cases := map[string]string{
		"db.tx_log.buffer.size": "must be a whole number of bytes, at least 131072 (Neo4j reads it as a long, " +
			"so a unit such as MiB is refused), for example 1048576",
		"db.checkpoint.interval.tx":        "must be a whole number, at least 1 (Neo4j reads it as an integer), for example 100000",
		"server.bolt.thread_pool_max_size": "must be a whole number (Neo4j reads it as an integer), for example 400",
		"db.tx_log.preallocate":            "must be true or false (Neo4j reads it as a boolean), for example true",
		"db.logs.query.enabled":            "must be one of OFF, INFO, VERBOSE, in any case, for example INFO",
	}
	for key, want := range cases {
		assert.Equal(t, want, typedSettings[key].expectation(), key)
	}

	d := typedSettings["db.transaction.bookmark_ready_timeout"].expectation()
	assert.True(t, strings.HasPrefix(d, "must be a duration of at least 1s: "), d)
	assert.Contains(t, d, "ns, us, μs, ms, s, m, h or d")
	assert.Contains(t, d, "a number without a unit is seconds")
	assert.True(t, strings.HasSuffix(d, ", for example 30s"), d)

	b := typedSettings["db.memory.transaction.max"].expectation()
	assert.True(t, strings.HasPrefix(b, "must be a byte size of at least 1MiB, or 0: "), b)
	assert.Contains(t, b, "Kubernetes suffixes such as Gi or Mi are not Neo4j units")
	assert.True(t, strings.HasSuffix(b, ", for example 256M"), b)
	assert.True(t, strings.HasPrefix(typedSettings["db.tx_log.rotation.size"].expectation(),
		"must be a byte size of at least 128KiB: "))
	assert.True(t, strings.HasPrefix(typedSettings["server.memory.heap.max_size"].expectation(),
		"must be a byte size: "))
}

// The table must be usable as documentation: every entry's example is a value
// it accepts, and every enum example is one of its constants.
func TestTypedSettingsExamplesAreValid(t *testing.T) {
	require.NotEmpty(t, typedSettings)
	for key, st := range typedSettings {
		require.NotEmpty(t, st.example, key)
		assert.True(t, st.accepts(st.example), "%s: its own example %q must be accepted", key, st.example)
		if st.kind == enumValue {
			assert.Contains(t, st.values, st.example, key)
		}
	}
}

// Keys the operator manages or forbids are not typed: the user's value either
// never reaches neo4j.conf or is already refused for its key.
func TestTypedSettingsAreUserSettings(t *testing.T) {
	forbidden := []string{
		"db.format", "server.bolt.tls_level", "server.directories.certificates",
		"server.default_advertised_address", "initial.server.mode_constraint",
		"dbms.cluster.minimum_initial_system_primaries_count", "db.query.default_language",
		"server.config.strict_validation.enabled", "server.logs.config",
	}
	for _, key := range forbidden {
		_, typed := typedSettings[key]
		assert.False(t, typed, key)
	}
	for key := range typedSettings {
		assert.False(t, strings.HasPrefix(key, "dbms.ssl.policy."), key)
		assert.False(t, strings.HasPrefix(key, "internal."), key)
	}
}

// The operator's own neo4j.conf values for typed settings must pass the same
// check: if they did not, the parser — not the operator — would be wrong.
func TestOperatorRenderedConfPassesTheValueCheck(t *testing.T) {
	cluster := validClusterForWarnings()
	cluster.Spec.Config = map[string]string{"server.memory.heap.max_size": "3G"} // exercises the derived limits ("2.1g")
	cm := resources.BuildConfigMapForEnterprise(cluster)
	conf := cm.Data["neo4j.conf"]
	require.NotEmpty(t, conf)

	rendered := map[string]string{}
	for _, line := range strings.Split(conf, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			rendered[strings.TrimSpace(k)] = v
		}
	}
	var typed int
	for k := range rendered {
		if _, ok := typedSettings[k]; ok {
			typed++
		}
	}
	require.GreaterOrEqual(t, typed, 5, "expected the rendered conf to carry typed settings, got:\n%s", conf)
	assert.Empty(t, ValidateSettingValues(field.NewPath("neo4j.conf"), rendered), conf)
}

// The memory rules read sizes with the same parser and leave format errors to
// the value check, so a bad heap is reported once, saying what Neo4j expects.
func TestMemoryRulesLeaveFormatErrorsToTheValueCheck(t *testing.T) {
	standalone := validStandalone()
	standalone.Spec.Resources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("4Gi"),
	}}
	for _, heap := range []string{"2Gi", "two gigs", ""} {
		standalone.Spec.Config = map[string]string{"server.memory.heap.max_size": heap}
		var onHeap []*field.Error
		for _, e := range NewStandaloneValidator().ValidateCreate(standalone) {
			if strings.Contains(e.Field, "server.memory.heap.max_size") {
				onHeap = append(onHeap, e)
			}
		}
		if heap == "" {
			// The memory rules treat an empty heap as unset; the value check
			// refuses it, because the operator would render it empty.
			require.Len(t, onHeap, 1, "%q", heap)
			continue
		}
		require.Len(t, onHeap, 1, "%q: one error, not one per validator", heap)
		assert.Equal(t, "spec.config[server.memory.heap.max_size]", onHeap[0].Field)
		assert.Contains(t, onHeap[0].Detail, "must be a byte size")
	}
}

// propertySharding.config lands in neo4j.conf once sharding is enabled.
func TestClusterValidator_TypedValuesInPropertyShardingConfig(t *testing.T) {
	find := func(errs field.ErrorList) bool {
		for _, e := range errs {
			if e.Field == "spec.propertySharding.config[db.tx_log.buffer.size]" {
				return true
			}
		}
		return false
	}
	cluster := validClusterForWarnings()
	cluster.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{
		Enabled: true,
		Config:  map[string]string{"db.tx_log.buffer.size": "1MiB"},
	}
	assert.True(t, find(NewClusterValidator(nil).ValidateCreateWithWarnings(t.Context(), cluster).Errors))

	cluster.Spec.PropertySharding.Enabled = false
	assert.False(t, find(NewClusterValidator(nil).ValidateCreateWithWarnings(t.Context(), cluster).Errors),
		"a disabled block is not rendered, so its values are not judged")
}
