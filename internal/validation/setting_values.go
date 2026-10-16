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
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Typed spec.config values (docs/knowledge/operations.md id 140).
//
// Neo4j parses every neo4j.conf value when the server starts, and a value it
// cannot parse stops the server: "Error evaluating value for setting", whatever
// server.config.strict_validation says (that setting is about unknown names and
// duplicates, not values). A static setting is read only at startup, so the pod
// crash-loops and nothing before it notices — the v1.21.0 journey watched a
// 5.26.31 standalone do exactly that with db.tx_log.buffer.size: "1MiB", a
// setting Neo4j reads as a plain long.
//
// typedSettings is deliberately narrow. A setting is listed only when ALL of
// these hold, so that the check never refuses a value Neo4j accepts:
//
//   - configuration-settings.adoc gives it the same "Valid values" on branch 5.x
//     (5.26 LTS) and branch dev (CalVer) of github.com/neo4j/docs-operations;
//   - Neo4j's public source declares it with the same standard parser and the
//     same constraints on release/5.26.0, release/5.26.31 and every CalVer
//     checked (2025.01, .04, .07, .10, 2026.01, .04, .06, .08, .09). So no entry
//     needs a version gate; one that would needs one first (ValidateConfigMap
//     does not know the image tag today);
//   - no setting migrator rewrites its value before parsing (SettingMigrators,
//     ServerSettingsMigrator, BoltSettingsMigrator, read on release/5.26.31,
//     2025.01 and 2026.09);
//   - the operator writes the user's value into neo4j.conf unchanged (it is not
//     one of the keys the renderer drops, such as auth-generated keys).
//
// Unlisted settings are not checked; Neo4j reports a malformed value at
// startup. To add a setting, see id 140.
var typedSettings = map[string]settingType{
	// Memory
	"server.memory.heap.initial_size":   byteSize("4G"),
	"server.memory.heap.max_size":       byteSize("4G"),
	"server.memory.pagecache.size":      byteSize("2G"),
	"dbms.memory.transaction.total.max": byteSize("2G", zeroOrAtLeast(10*mebibyte)),
	"db.memory.transaction.total.max":   byteSize("1G", zeroOrAtLeast(10*mebibyte)),
	"db.memory.transaction.max":         byteSize("256M", zeroOrAtLeast(mebibyte)),

	// Transaction log
	"db.tx_log.buffer.size":   long("bytes", "1048576", atLeast(128*kibibyte)),
	"db.tx_log.rotation.size": byteSize("256M", atLeast(128*kibibyte)),
	"db.tx_log.preallocate":   boolean(),

	// Checkpoints
	"db.checkpoint":                 enum("PERIODIC", "PERIODIC", "CONTINUOUS", "VOLUME", "VOLUMETRIC"),
	"db.checkpoint.interval.time":   duration("15m"),
	"db.checkpoint.interval.tx":     integer("100000", atLeast(1)),
	"db.checkpoint.interval.volume": byteSize("250M", atLeast(kibibyte)),
	"db.checkpoint.iops.limit":      integer("600"),

	// Transactions and locks
	"db.transaction.timeout":                duration("60s"),
	"db.transaction.concurrent.maximum":     integer("1000"),
	"db.transaction.bookmark_ready_timeout": duration("30s", atLeastDuration(time.Second)),
	"db.lock.acquisition.timeout":           duration("2m"),
	"db.shutdown_transaction_end_timeout":   duration("10s"),

	// Query log
	"db.logs.query.enabled":                   enum("INFO", "OFF", "INFO", "VERBOSE"),
	"db.logs.query.threshold":                 duration("1s"),
	"db.logs.query.transaction.enabled":       enum("INFO", "OFF", "INFO", "VERBOSE"),
	"db.logs.query.transaction.threshold":     duration("1s"),
	"db.logs.query.parameter_logging_enabled": boolean(),
	"db.logs.query.obfuscate_literals":        boolean(),
	"db.logs.query.plan_description_enabled":  boolean(),
	"db.logs.query.early_raw_logging_enabled": boolean(),
	"db.logs.query.max_parameter_length":      integer("1000"),

	// Bolt
	"server.bolt.thread_pool_min_size":   integer("5"),
	"server.bolt.thread_pool_max_size":   integer("400"),
	"server.bolt.thread_pool_keep_alive": duration("5m"),
	"server.bolt.connection_keep_alive":  duration("1m", atLeastDuration(time.Millisecond)),

	// Routing
	"dbms.routing.enabled":        boolean(),
	"dbms.routing.default_router": enum("SERVER", "SERVER", "CLIENT"),
	"dbms.routing_ttl":            duration("300s", atLeastDuration(time.Second)),

	// Security
	"dbms.security.auth_enabled":                    boolean(),
	"dbms.security.auth_minimum_password_length":    integer("12", atLeast(1)),
	"dbms.security.auth_max_failed_attempts":        integer("3", atLeast(0)),
	"dbms.security.auth_lock_time":                  duration("5s"),
	"dbms.security.allow_csv_import_from_file_urls": boolean(),

	// Other
	"db.store.files.preallocate":                         boolean(),
	"db.memory.pagecache.warmup.enable":                  boolean(),
	"dbms.usage_report.enabled":                          boolean(),
	"server.logs.debug.enabled":                          boolean(),
	"server.threads.worker_count":                        integer("8", atLeast(1)),
	"server.memory.query_cache.per_db_cache_num_entries": integer("1000", atLeast(0)),
}

// ValidateSettingValues refuses a value Neo4j would refuse to start with, for
// the settings listed in typedSettings. path is where the map is reported
// (spec.config, or a cluster's spec.propertySharding.config). It is called by
// ConfigValidator.ValidateConfigMap, which both the cluster and the standalone
// validator run.
func ValidateSettingValues(path *field.Path, config map[string]string) field.ErrorList {
	keys := make([]string, 0, len(config))
	for key := range config {
		if _, typed := typedSettings[key]; typed {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var errs field.ErrorList
	for _, key := range keys {
		t := typedSettings[key]
		if value := config[key]; !t.accepts(value) {
			errs = append(errs, field.Invalid(path.Key(key), value, t.expectation()))
		}
	}
	return errs
}

const (
	kibibyte = int64(1) << 10
	mebibyte = int64(1) << 20
)

// valueKind is the parser Neo4j declares the setting with
// (org.neo4j.configuration.SettingValueParsers).
type valueKind int

const (
	booleanValue  valueKind = iota // BOOL: true or false, any case
	integerValue                   // INT: Integer.parseInt
	longValue                      // LONG: Long.parseLong
	durationValue                  // DURATION: TimeUtil.parseTimeMillis
	byteSizeValue                  // BYTES: ByteUnit.parse
	enumValue                      // ofEnum: the constant's name, any case
)

type settingType struct {
	kind valueKind
	// min is the smallest value Neo4j accepts (SettingConstraints.min): a
	// count, bytes for a byte size, nanoseconds for a duration. Maximums are
	// not encoded; a value above one is left to Neo4j.
	min    int64
	hasMin bool
	// zeroAllowed is Neo4j's any(min(n), is(0)): 0 is accepted below min.
	zeroAllowed bool
	values      []string // enum constants
	unit        string   // what a long or integer counts, for the message
	example     string
}

type bound func(*settingType)

func atLeast(n int64) bound {
	return func(t *settingType) { t.min, t.hasMin = n, true }
}

func atLeastDuration(d time.Duration) bound { return atLeast(int64(d)) }

func zeroOrAtLeast(n int64) bound {
	return func(t *settingType) { t.min, t.hasMin, t.zeroAllowed = n, true, true }
}

func newType(kind valueKind, example string, bounds []bound) settingType {
	t := settingType{kind: kind, example: example}
	for _, b := range bounds {
		b(&t)
	}
	return t
}

func boolean() settingType { return settingType{kind: booleanValue, example: "true"} }

func integer(example string, bounds ...bound) settingType {
	return newType(integerValue, example, bounds)
}

func long(unit, example string, bounds ...bound) settingType {
	t := newType(longValue, example, bounds)
	t.unit = unit
	return t
}

func duration(example string, bounds ...bound) settingType {
	return newType(durationValue, example, bounds)
}

func byteSize(example string, bounds ...bound) settingType {
	return newType(byteSizeValue, example, bounds)
}

func enum(example string, values ...string) settingType {
	return settingType{kind: enumValue, example: example, values: values}
}

// verdict is what the operator can say about how Neo4j reads a value.
type verdict int

const (
	valueAccepted verdict = iota
	valueRefused
	// valueUnknown: the operator does not model Neo4j's reading of this
	// spelling exactly (non-ASCII text, number grouping, NaN…). Never refused.
	valueUnknown
)

// reading is the outcome of parsing a value the way Neo4j does.
type reading struct {
	verdict verdict
	n       int64 // the parsed value, in the units of settingType.min
	exact   bool  // n is exactly what Neo4j computes, so bounds can be checked
}

func refused() reading { return reading{verdict: valueRefused} }
func unknown() reading { return reading{verdict: valueUnknown} }
func exactly(n int64) reading {
	return reading{verdict: valueAccepted, n: n, exact: true}
}

// accepts reports whether Neo4j would start with value. It answers true
// whenever the operator cannot be certain.
func (t settingType) accepts(value string) bool {
	if neo4jTransformsValue(value) {
		return true
	}
	r := t.read(javaTrim(value))
	switch {
	case r.verdict == valueUnknown:
		return true
	case r.verdict == valueRefused:
		return false
	case !r.exact:
		return true
	case t.zeroAllowed && r.n == 0:
		return true
	case t.hasMin && r.n < t.min:
		return false
	}
	return true
}

// neo4jTransformsValue reports values Neo4j does not parse as written:
//   - $(command), which Neo4j replaces with the command's output when started
//     with --expand-commands (Config.isCommand);
//   - anything with a backslash: neo4j.conf is read by java.util.Properties,
//     which decodes escapes first;
//   - line breaks, which ConfigValidator already refuses and the renderer
//     strips.
func neo4jTransformsValue(value string) bool {
	if strings.ContainsAny(value, "\\\n\r") {
		return true
	}
	s := javaTrim(value)
	return len(s) > 3 && strings.HasPrefix(s, "$(") && strings.HasSuffix(s, ")")
}

func (t settingType) read(s string) reading {
	switch t.kind {
	case booleanValue:
		return readOneOf(s, []string{"true", "false"})
	case enumValue:
		return readOneOf(s, t.values)
	case integerValue:
		return readJavaInteger(s, 32)
	case longValue:
		return readJavaInteger(s, 64)
	case durationValue:
		return readNeo4jDuration(s)
	case byteSizeValue:
		return readNeo4jByteSize(s)
	}
	return unknown()
}

// javaTrim is java.lang.String.trim, which every Neo4j parser applies first:
// it strips each character up to U+0020 from both ends.
func javaTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// readOneOf is BOOL and ofEnum: equalsIgnoreCase against each constant. Java's
// case folding differs from ASCII outside it ("ſ" matches "s"), so non-ASCII
// text is not judged.
func readOneOf(s string, values []string) reading {
	if !isASCII(s) {
		return unknown()
	}
	for _, v := range values {
		if strings.EqualFold(s, v) {
			return reading{verdict: valueAccepted}
		}
	}
	return refused()
}

// readJavaInteger is Integer.parseInt (bitSize 32) and Long.parseLong (64): an
// optional sign and decimal digits, in range. Java also accepts non-ASCII
// digits, which the operator does not judge.
func readJavaInteger(s string, bitSize int) reading {
	if !isASCII(s) {
		return unknown()
	}
	n, err := strconv.ParseInt(s, 10, bitSize)
	if err != nil {
		return refused()
	}
	return exactly(n)
}

// durationUnits are TimeUtil's units, matched after lower-casing.
var durationUnits = map[string]int64{
	"ns": int64(time.Nanosecond),
	"us": int64(time.Microsecond),
	"μs": int64(time.Microsecond),
	"ms": int64(time.Millisecond),
	"s":  int64(time.Second),
	"m":  int64(time.Minute),
	"h":  int64(time.Hour),
	"d":  int64(24 * time.Hour),
}

// readNeo4jDuration is TimeUtil.parseTimeMillis, identical on 5.26 and CalVer:
// a bare whole number is seconds; otherwise one or more <digits><unit> pairs
// ("1h30m"), each amount an int, no sign, no decimals, no spaces. The result
// is truncated to milliseconds, as Neo4j does.
func readNeo4jDuration(s string) reading {
	for _, r := range s {
		if r > 0x7f && r != 'μ' && r != 'Μ' {
			return unknown() // Java's isDigit also takes non-ASCII digits
		}
	}
	if s == "" || !isDigit(s[0]) {
		return refused() // "Missing numeric value"
	}

	var total int64
	exact := true
	for i, first := 0, true; i < len(s); first = false {
		start := i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		amount, err := strconv.ParseInt(s[start:i], 10, 32)
		if err != nil {
			return refused()
		}
		if i == len(s) {
			if !first {
				return refused() // a number after the last unit, as in "1h30"
			}
			return exactly(amount * int64(time.Second))
		}
		unitStart := i
		for i < len(s) && !isDigit(s[i]) {
			i++
		}
		factor, ok := durationUnits[strings.ToLower(s[unitStart:i])]
		if !ok {
			return refused()
		}
		if amount > (math.MaxInt64-total)/factor {
			exact = false // Neo4j saturates and wraps; do not judge bounds
			continue
		}
		total += amount * factor
	}
	if !exact {
		return reading{verdict: valueAccepted}
	}
	return exactly(total / int64(time.Millisecond) * int64(time.Millisecond))
}

// byteUnits are ByteUnit's names, all powers of 1024 and matched exactly
// (case-sensitive), identical on 5.26 and CalVer.
var byteUnits = map[string]int64{
	"B":   1,
	"KiB": 1 << 10, "KB": 1 << 10, "K": 1 << 10, "kB": 1 << 10, "kb": 1 << 10, "k": 1 << 10,
	"MiB": 1 << 20, "MB": 1 << 20, "M": 1 << 20, "mB": 1 << 20, "mb": 1 << 20, "m": 1 << 20,
	"GiB": 1 << 30, "GB": 1 << 30, "G": 1 << 30, "gB": 1 << 30, "gb": 1 << 30, "g": 1 << 30,
	"TiB": 1 << 40, "TB": 1 << 40,
	"PiB": 1 << 50, "PB": 1 << 50,
	"EiB": 1 << 60, "EB": 1 << 60,
}

// readNeo4jByteSize is ByteUnit.parse: a number read by Java's English
// NumberFormat, optional whitespace, then an optional unit that must be one of
// byteUnits exactly; anything after whitespace following the unit is ignored.
// NumberFormat is lenient (grouping commas, NaN, exponents), so only plain
// decimal numbers are judged exactly; other number spellings are judged only on
// whether there is a number at all and whether the unit exists.
func readNeo4jByteSize(s string) reading {
	if !isASCII(s) || strings.HasPrefix(s, "NaN") {
		return unknown()
	}
	i := 0
	negative := false
	if i < len(s) && s[i] == '-' {
		negative = true
		i++
	}
	numStart := i
	for i < len(s) && (isDigit(s[i]) || s[i] == '.' || s[i] == ',') {
		i++
	}
	number := s[numStart:i]
	if !strings.ContainsAny(number, "0123456789") {
		return refused() // "is not a valid size"
	}
	for i < len(s) && javaWhitespace(s[i]) {
		i++
	}
	unitStart := i
	for i < len(s) && !javaWhitespace(s[i]) {
		i++
	}
	unit := s[unitStart:i]

	factor := int64(1)
	if unit != "" {
		if unit[0] == 'E' && len(unit) > 1 && (isDigit(unit[1]) || unit[1] == '-') {
			return unknown() // NumberFormat may read this as an exponent
		}
		f, ok := byteUnits[unit]
		if !ok {
			return refused()
		}
		factor = f
	}

	if strings.Contains(number, ",") || strings.Count(number, ".") > 1 {
		return unknown() // grouping or a second decimal point: not judged
	}
	x, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return unknown()
	}
	if negative {
		if x != 0 {
			return refused() // "Value should be between 0 and ..."
		}
		return exactly(0)
	}
	bytes := x * float64(factor)
	if bytes > math.MaxInt64 {
		return refused()
	}
	if bytes >= math.MaxInt64 {
		return exactly(math.MaxInt64)
	}
	return exactly(int64(bytes))
}

// javaWhitespace is Character.isWhitespace for ASCII.
func javaWhitespace(c byte) bool {
	return c == ' ' || (c >= '\t' && c <= '\r') || (c >= 0x1c && c <= 0x1f)
}

// expectation is the error detail: the expected format, its bounds, and an
// example of a valid value.
func (t settingType) expectation() string {
	var b strings.Builder
	switch t.kind {
	case booleanValue:
		b.WriteString("must be true or false (Neo4j reads it as a boolean)")
	case enumValue:
		fmt.Fprintf(&b, "must be one of %s, in any case", strings.Join(t.values, ", "))
	case integerValue, longValue:
		b.WriteString("must be a whole number")
		if t.unit != "" {
			b.WriteString(" of " + t.unit)
		}
		if t.hasMin {
			fmt.Fprintf(&b, ", at least %d", t.min)
		}
		if t.kind == longValue {
			b.WriteString(" (Neo4j reads it as a long, so a unit such as MiB is refused)")
		} else {
			b.WriteString(" (Neo4j reads it as an integer)")
		}
	case durationValue:
		b.WriteString("must be a duration")
		if t.hasMin {
			b.WriteString(" of at least " + formatNeo4jDuration(t.min))
		}
		b.WriteString(": a whole number with a unit — ns, us, μs, ms, s, m, h or d — or several such as 1h30m, " +
			"and a number without a unit is seconds (no decimals, spaces or ISO-8601)")
	case byteSizeValue:
		b.WriteString("must be a byte size")
		if t.hasMin {
			b.WriteString(" of at least " + formatNeo4jBytes(t.min))
			if t.zeroAllowed {
				b.WriteString(", or 0")
			}
		}
		b.WriteString(": a number with an optional unit — B, K, KB, KiB, kB, kb, k, M, MB, MiB, mB, mb, m, " +
			"G, GB, GiB, gB, gb, g, TB, TiB, PB, PiB, EB or EiB, each a power of 1024 " +
			"(Kubernetes suffixes such as Gi or Mi are not Neo4j units)")
	}
	b.WriteString(", for example " + t.example)
	return b.String()
}

func formatNeo4jBytes(n int64) string {
	for _, u := range []struct {
		name string
		size int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if n >= u.size && n%u.size == 0 {
			return fmt.Sprintf("%d%s", n/u.size, u.name)
		}
	}
	return fmt.Sprintf("%dB", n)
}

func formatNeo4jDuration(nanos int64) string {
	for _, u := range []struct {
		name string
		size time.Duration
	}{{"h", time.Hour}, {"m", time.Minute}, {"s", time.Second}, {"ms", time.Millisecond}} {
		if nanos >= int64(u.size) && nanos%int64(u.size) == 0 {
			return fmt.Sprintf("%d%s", nanos/int64(u.size), u.name)
		}
	}
	return fmt.Sprintf("%dns", nanos)
}
