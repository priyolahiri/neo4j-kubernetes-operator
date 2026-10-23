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

// A standalone's status.runningVersion (operations.md id 153): the version the
// server reports when status.version is a tag without a patch. The cluster
// reads it from the SHOW SERVERS its formation check runs on every pass; a
// standalone has no such query, so it asks once Ready, and again only when its
// container runs a different image (another pod, or the same pod's container
// started on another image digest, which a floating tag with pullPolicy Always
// can do at any restart).

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// standaloneVersionReadTimeout bounds the one query that reads the version.
const standaloneVersionReadTimeout = 15 * time.Second

// standaloneRunningImage identifies the image the standalone's Neo4j
// container runs: its pod's UID and the container's image ID (the digest the
// kubelet started). "" when the pod or the container is not running yet.
func (r *Neo4jEnterpriseStandaloneReconciler) standaloneRunningImage(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) string {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: standalone.Name + "-0", Namespace: standalone.Namespace}, pod); err != nil {
		return ""
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "neo4j" && cs.ImageID != "" {
			return fmt.Sprintf("%s/%s", pod.UID, cs.ImageID)
		}
	}
	return ""
}

// readStandaloneServerVersion asks the standalone for the version it runs:
// SHOW SERVERS `version`, the source a cluster's verification trusts.
func (r *Neo4jEnterpriseStandaloneReconciler) readStandaloneServerVersion(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) (string, error) {
	if r.ServerVersionReader != nil {
		return r.ServerVersionReader(ctx, standalone)
	}
	ctx, cancel := context.WithTimeout(ctx, standaloneVersionReadTimeout)
	defer cancel()
	c, err := neo4jclient.NewClientForEnterpriseStandalone(standalone, r.Client, getStandaloneAdminSecretName(standalone))
	if err != nil {
		return "", err
	}
	defer func() { _ = c.Close() }()
	servers, err := c.GetServerList(ctx)
	if err != nil {
		return "", err
	}
	return highestReportedVersion(servers, nil), nil
}

// recordStandaloneRunningVersion keeps status.runningVersion current for a
// Ready standalone. For a status.version that names a patch it is cleared (the
// tag says the version). For one that does not, the server is asked once, and
// asked again only when its container runs another image than at the last
// read: no query on every pass. Non-fatal: a read that fails leaves the field
// as it is, and the next pass asks again.
func (r *Neo4jEnterpriseStandaloneReconciler) recordStandaloneRunningVersion(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) {
	logger := log.FromContext(ctx)
	latest := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(standalone), latest); err != nil || latest.Status.Phase != "Ready" {
		return
	}
	key := standalone.Namespace + "/" + standalone.Name
	want, image := "", ""
	if tagFloats(latest.Status.Version) {
		// Any rollout replaces the pod, so a new status.version always comes
		// with a new image here.
		if image = r.standaloneRunningImage(ctx, standalone); image == "" {
			return
		}
		if read, ok := r.runningVersionReads.Load(key); ok && read == image {
			return
		}
		version, err := r.readStandaloneServerVersion(ctx, standalone)
		if err != nil || version == "" {
			logger.V(1).Info("Could not read the version the standalone runs; trying again on the next pass",
				"error", fmt.Sprint(err))
			return
		}
		want = version
	}
	// The read is remembered once the status says it, so a write that fails
	// is read again on the next pass. A standalone whose CRD lacks the field
	// (CRDs not applied after an operator upgrade) has it dropped by the API
	// server; that is remembered too, rather than asked on every pass.
	remember := func() {
		if image != "" {
			r.runningVersionReads.Store(key, image)
		}
	}
	if latest.Status.RunningVersion == want {
		remember()
		return
	}
	moved := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(standalone), current); err != nil {
			return err
		}
		if moved = current.Status.Version != latest.Status.Version; moved {
			// status.version moved meanwhile: the next pass reads again.
			return nil
		}
		if current.Status.RunningVersion == want {
			return nil
		}
		current.Status.RunningVersion = want
		return r.Status().Update(ctx, current)
	})
	if err != nil {
		logger.Error(err, "Failed to record the version the standalone runs (non-fatal)")
		return
	}
	if moved {
		return
	}
	remember()
	logger.Info("Recorded the version the standalone reports", "version", latest.Status.Version, "reported", want)
}
