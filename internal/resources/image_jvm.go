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

// The Neo4j image's own JVM arguments (docs/knowledge/operations.md ids 208,
// 212).
//
// The image's neo4j.conf sets server.jvm.additional on 20 to 24 lines: the
// --add-opens and --enable-native-access flags Neo4j's runtime needs, and on
// CalVer the vector incubator module its vector search uses, beside GC and
// diagnostics flags. A pod never reads that file: given a /conf directory, the
// entrypoint empties ${NEO4J_HOME}/conf and copies /conf into it
// (docker-entrypoint.sh lines 479-483 on 5.26.32 and 2026.09.0). So the
// operator puts the image's lines back, first in the server's neo4j.conf:
//
//   - a cluster's startup script copies them from the image's own neo4j.conf,
//     which it can still read when it runs (buildImageJVMArgumentsBlock);
//   - a standalone has no startup script, so its neo4j.conf carries them from
//     the table below (StandaloneImageJVMArgumentsBlock).
//
// Both are restart-neutral: a running server gets them at its next start,
// and the operator upgrade that adds them restarts nothing. What follows them
// — the user's spec.config and spec.env, and on a cluster the operator's own
// list — wins a conflicting flag, as the JVM takes the last one given; a flag
// is turned off with its -XX:- form, a property overridden with its own -D.
//
// One of the image's lines is left out on both kinds: -XX:+AlwaysPreTouch
// (leftOutImageJVMArgument).

import (
	"strings"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// imageJVMArgumentLists are the server.jvm.additional values of the images'
// own neo4j.conf, by the first release known to ship each list, newest first.
// Read from the images (internal/entrypointsim/testdata, pinned by
// TestImageJVMArguments_AreTheImagesOwn): 5.26.0 to 5.26.32 and 2025.01.0
// ship the first; 2026.03.1, 2026.04.0 and 2026.06.0 the second, which adds
// the netty leak-detection property, java.util.concurrent to the --add-opens
// and the Lucene vectorization property; 2026.08.1 and 2026.09.0 the third,
// which adds the vector incubator module. A release between two known ones
// gets the earlier list (2025.02 to 2026.02 the first; 2026.07 the second):
// each list holds the one before it, so that leaves out at worst a flag its
// image would set. A CalVer newer than the newest known gets the newest list,
// on a best-effort basis: it is the list the next image is most likely to
// carry, and the JVM of every image so far accepts all of it.
var imageJVMArgumentLists = []struct {
	from   neo4j.Version
	values []string
}{
	{neo4j.Version{Major: 2026, Minor: 8}, []string{
		"-XX:+UseG1GC",
		"-XX:-OmitStackTraceInFastThrow",
		"-XX:+AlwaysPreTouch",
		"-XX:+UnlockExperimentalVMOptions",
		"-XX:+TrustFinalNonStaticFields",
		"-XX:+DisableExplicitGC",
		"-XX:-RestrictContended",
		"-Djdk.nio.maxCachedBufferSize=1024",
		"-Dio.netty.tryReflectionSetAccessible=true",
		"-Dio.netty.leakDetection.level=DISABLED",
		"-Djdk.tls.ephemeralDHKeySize=2048",
		"-Djdk.tls.rejectClientInitiatedRenegotiation=true",
		"-XX:FlightRecorderOptions=stackdepth=256",
		"-XX:+UnlockDiagnosticVMOptions",
		"-XX:+DebugNonSafepoints",
		"--add-opens=java.base/java.nio=ALL-UNNAMED",
		"--add-opens=java.base/java.io=ALL-UNNAMED",
		"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
		"--enable-native-access=ALL-UNNAMED",
		"--add-modules=jdk.incubator.vector",
		"-Dlog4j2.disable.jmx=true",
		"-Dlog4j.layout.jsonTemplate.maxStringLength=32768",
		"-Dorg.neo4j.shaded.lucene9.vectorization.upperJavaFeatureVersion=25",
	}},
	{neo4j.Version{Major: 2026, Minor: 3}, []string{
		"-XX:+UseG1GC",
		"-XX:-OmitStackTraceInFastThrow",
		"-XX:+AlwaysPreTouch",
		"-XX:+UnlockExperimentalVMOptions",
		"-XX:+TrustFinalNonStaticFields",
		"-XX:+DisableExplicitGC",
		"-XX:-RestrictContended",
		"-Djdk.nio.maxCachedBufferSize=1024",
		"-Dio.netty.tryReflectionSetAccessible=true",
		"-Dio.netty.leakDetection.level=DISABLED",
		"-Djdk.tls.ephemeralDHKeySize=2048",
		"-Djdk.tls.rejectClientInitiatedRenegotiation=true",
		"-XX:FlightRecorderOptions=stackdepth=256",
		"-XX:+UnlockDiagnosticVMOptions",
		"-XX:+DebugNonSafepoints",
		"--add-opens=java.base/java.nio=ALL-UNNAMED",
		"--add-opens=java.base/java.io=ALL-UNNAMED",
		"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
		"--enable-native-access=ALL-UNNAMED",
		"-Dlog4j2.disable.jmx=true",
		"-Dlog4j.layout.jsonTemplate.maxStringLength=32768",
		"-Dorg.neo4j.shaded.lucene9.vectorization.upperJavaFeatureVersion=25",
	}},
	{neo4j.Version{Major: 5, Minor: 26}, []string{
		"-XX:+UseG1GC",
		"-XX:-OmitStackTraceInFastThrow",
		"-XX:+AlwaysPreTouch",
		"-XX:+UnlockExperimentalVMOptions",
		"-XX:+TrustFinalNonStaticFields",
		"-XX:+DisableExplicitGC",
		"-XX:-RestrictContended",
		"-Djdk.nio.maxCachedBufferSize=1024",
		"-Dio.netty.tryReflectionSetAccessible=true",
		"-Djdk.tls.ephemeralDHKeySize=2048",
		"-Djdk.tls.rejectClientInitiatedRenegotiation=true",
		"-XX:FlightRecorderOptions=stackdepth=256",
		"-XX:+UnlockDiagnosticVMOptions",
		"-XX:+DebugNonSafepoints",
		"--add-opens=java.base/java.nio=ALL-UNNAMED",
		"--add-opens=java.base/java.io=ALL-UNNAMED",
		"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"--enable-native-access=ALL-UNNAMED",
		"-Dlog4j2.disable.jmx=true",
		"-Dlog4j.layout.jsonTemplate.maxStringLength=32768",
	}},
}

// leftOutImageJVMArgument is the one image line the operator does not render
// on either kind. -XX:+AlwaysPreTouch writes every page of the heap at the
// JVM's start, and the operator sets the heap's initial size to its maximum,
// so a server holds its whole heap from the moment it starts. On a 3-server
// 2026.08.1 cluster at the documented minimum (1536Mi, heap 768M, page cache
// 384M) a server's idle memory went from 529-589Mi to 1136-1167Mi, and under a
// write load the writer was OOMKilled and crash-looped; without the flag the
// same load passed, and startup time was the same either way (round 9 live
// walk of #598). A user who wants it adds it in spec.config.
const leftOutImageJVMArgument = "-XX:+AlwaysPreTouch"

// ImageJVMArguments returns the server.jvm.additional values the Neo4j image
// tagged tag sets in its own neo4j.conf (imageJVMArgumentLists), less
// leftOutImageJVMArgument: the list of the newest known release at or below
// its version. A tag without a patch (5.26-enterprise, 2026.09-enterprise)
// reads as the line's first release. A tag that is not a supported Neo4j
// version gets none.
func ImageJVMArguments(tag string) []string {
	v, err := neo4j.ParseVersion(tag)
	if err != nil || !v.IsSupported() {
		return nil
	}
	for _, list := range imageJVMArgumentLists {
		if v.Compare(&list.from) >= 0 {
			out := make([]string, 0, len(list.values))
			for _, value := range list.values {
				if value != leftOutImageJVMArgument {
					out = append(out, value)
				}
			}
			return out
		}
	}
	return nil
}

// imageJVMArgumentsComment opens the image's lines in a server's neo4j.conf.
const imageJVMArgumentsComment = "# The Neo4j image's own JVM arguments (server.jvm.additional), which its neo4j.conf\n" +
	"# sets and this file replaces, less " + leftOutImageJVMArgument + ". Later server.jvm.additional lines win a conflicting flag."

// StandaloneImageJVMArgumentsBlock renders, for a standalone's neo4j.conf, the
// image's own server.jvm.additional lines (ImageJVMArguments) between the
// restart-neutral markers, which the standalone's restart decision leaves out:
// a running pod gets them at its next start (operations.md id 212). "" for a
// tag that has none.
func StandaloneImageJVMArgumentsBlock(tag string) string {
	values := ImageJVMArguments(tag)
	if len(values) == 0 {
		return ""
	}
	lines := []string{RestartNeutralBegin, imageJVMArgumentsComment}
	for _, v := range values {
		lines = append(lines, "server.jvm.additional="+v)
	}
	lines = append(lines, RestartNeutralEnd)
	return strings.Join(lines, "\n")
}

// buildImageJVMArgumentsBlock renders the cluster startup script's
// restart-neutral block that puts the image's own server.jvm.additional
// lines, less leftOutImageJVMArgument (matched as a whole line), first in
// /conf/neo4j.conf. The script runs before the entrypoint,
// while ${NEO4J_HOME}/conf/neo4j.conf is still the image's file, so the lines
// are the image's own on every release, with no table. They go before the
// operator's rendered conf, so the user's spec.config lines, and the
// operator's NEO4J_server_jvm_additional the entrypoint appends at the end
// (server.jvm.additional is the one setting it appends rather than
// replaces), come after them and win a conflicting flag (operations.md id
// 212).
func buildImageJVMArgumentsBlock() string {
	return RestartNeutralBegin + `
` + strings.ReplaceAll(imageJVMArgumentsComment, "this file replaces", "the entrypoint replaces with /conf") + `
OPERATOR_IMAGE_CONF_DIR="${NEO4J_HOME:-/var/lib/neo4j}/conf"
if [ "$OPERATOR_IMAGE_CONF_DIR" != /conf ] && [ -f "$OPERATOR_IMAGE_CONF_DIR/neo4j.conf" ] && [ -f /conf/neo4j.conf ]; then
    { grep '^server.jvm.additional=' "$OPERATOR_IMAGE_CONF_DIR/neo4j.conf" | grep -v -x -F 'server.jvm.additional=` + leftOutImageJVMArgument + `' || true; cat /conf/neo4j.conf; } > /conf/neo4j.conf.image-jvm
    mv /conf/neo4j.conf.image-jvm /conf/neo4j.conf
fi
` + RestartNeutralEnd + `
`
}
