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
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// backupContainerName is the backup Job's one container, in a one-time
// backup's Job and a scheduled backup's CronJob template alike.
const backupContainerName = "backup"

// jobFailureTailLines and jobFailureMaxLen bound what a failed container's
// termination message adds to a status message, an event or a run's history
// entry: its last lines, where neo4j-admin and the shell say what went wrong.
const (
	jobFailureTailLines = 5
	jobFailureMaxLen    = 600
)

// jobFailureMessage says why a restore or backup Job failed (what is "Restore
// job" or "Backup job"): "<what> <name> failed (<the Job's Failed reason>:
// <message>): <container> exited with code <n>: <tail>" — the Job's Failed
// condition, and how container ended in the Job's most recently failed pod
// (jobPodFailure). Either part is left out when there is nothing to say
// (knowledge operations.md id 215).
func jobFailureMessage(ctx context.Context, c client.Reader, what string, job *batchv1.Job, container string) string {
	msg := fmt.Sprintf("%s %s failed", what, job.Name)
	if cond := jobCondition(job, batchv1.JobFailed); cond != nil {
		switch {
		case cond.Reason != "" && cond.Message != "":
			msg += fmt.Sprintf(" (%s: %s)", cond.Reason, cond.Message)
		case cond.Reason != "" || cond.Message != "":
			msg += fmt.Sprintf(" (%s%s)", cond.Reason, cond.Message)
		}
	}
	if why := jobPodFailure(ctx, c, job, container); why != "" {
		msg += ": " + why
	}
	return msg
}

// jobCondition returns the Job's condition of type t that is True, or nil.
func jobCondition(job *batchv1.Job, t batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		if cond := &job.Status.Conditions[i]; cond.Type == t && cond.Status == corev1.ConditionTrue {
			return cond
		}
	}
	return nil
}

// jobPodFailure is how container ended in the Job's most recently failed pod —
// a pod the Job controls, so a pod of an earlier Job of the same name is never
// read: its exit code and its termination message, which is the tail of its
// log when it wrote none (the container's FallbackToLogsOnError policy) — or
// "" when no pod of the Job is left to say.
func jobPodFailure(ctx context.Context, c client.Reader, job *batchv1.Job, container string) string {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		log.FromContext(ctx).V(1).Info("Could not list the Job's pods for its failure reason", "job", job.Name, "error", err.Error())
		return ""
	}
	var last *corev1.ContainerStateTerminated
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != container {
				continue
			}
			for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
				if t != nil && t.ExitCode != 0 && (last == nil || last.FinishedAt.Before(&t.FinishedAt)) {
					last = t
				}
			}
		}
	}
	if last == nil {
		return ""
	}
	why := fmt.Sprintf("%s exited with code %d", container, last.ExitCode)
	if last.Reason != "" && last.Reason != "Error" {
		why += fmt.Sprintf(" (%s)", last.Reason)
	}
	if tail := messageTail(last.Message); tail != "" {
		why += ": " + tail
	}
	return why
}

// messageTail is the last jobFailureTailLines non-empty lines of msg, one
// line, at most jobFailureMaxLen characters from its end.
func messageTail(msg string) string {
	var lines []string
	for _, l := range strings.Split(msg, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > jobFailureTailLines {
		lines = lines[len(lines)-jobFailureTailLines:]
	}
	tail := strings.Join(lines, " | ")
	if r := []rune(tail); len(r) > jobFailureMaxLen {
		tail = "…" + string(r[len(r)-jobFailureMaxLen:])
	}
	return tail
}
