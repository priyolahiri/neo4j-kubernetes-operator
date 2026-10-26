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
	"strings"
	"testing"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// withoutImageJVMBlock renders a cluster startup script as an operator before
// #594 did: without the restart-neutral section that copies the image's
// server.jvm.additional lines, the other sections as they are.
func withoutImageJVMBlock(script string) string {
	var kept, section []string
	in := false
	for _, line := range strings.Split(script, "\n") {
		switch {
		case strings.TrimSpace(line) == resources.RestartNeutralBegin:
			in, section = true, []string{line}
			continue
		case in && strings.TrimSpace(line) == resources.RestartNeutralEnd:
			in = false
			section = append(section, line)
			if !strings.Contains(strings.Join(section, "\n"), "OPERATOR_IMAGE_CONF_DIR") {
				kept = append(kept, section...)
			}
			continue
		case in:
			section = append(section, line)
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// The operator upgrade that adds the image's JVM arguments to an existing
// cluster's startup script restarts no server, on both lines: the block is
// restart-neutral, and each server runs the image's lines from its next start
// (operations.md id 212).
func TestUpgrade_ImageJVMBlockRestartsNothing(t *testing.T) {
	for _, tag := range []string{"5.26.32-enterprise", "2026.08.1-enterprise"} {
		t.Run(tag, func(t *testing.T) {
			cluster := scaledCluster("u594", tag, 3)
			old := resources.BuildConfigMapForEnterprise(cluster)
			old.Data["startup.sh"] = withoutImageJVMBlock(old.Data["startup.sh"])
			if strings.Contains(old.Data["startup.sh"], "OPERATOR_IMAGE_CONF_DIR") {
				t.Fatal("test premise: the old script must not carry the block")
			}
			restarted, written, _ := reconcileScale(t, cluster, cluster, old)
			if restarted {
				t.Error("adding the image's JVM arguments must not restart the servers")
			}
			if !strings.Contains(written.Data["startup.sh"], "OPERATOR_IMAGE_CONF_DIR") {
				t.Error("the new script must still be written for the next restart")
			}
		})
	}
}
