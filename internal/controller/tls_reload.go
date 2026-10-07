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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Reloading a renewed TLS certificate without a restart (#469).
//
// cert-manager renews {name}-tls-secret and the kubelet updates the files
// mounted at /ssl in place, but Neo4j keeps serving the certificate it loaded
// at startup. On CalVer 2025.03+ with dbms.security.tls_reload_enabled=true,
// dbms.security.reloadTLS() makes one server re-read them ("Connect to each
// cluster member in turn … and run the reload procedure", security/
// ssl-framework). So when the Secret's certificate is not the one the servers
// present, the operator runs it on each server and checks the certificate a
// new TLS connection gets. The procedure succeeds — doing nothing — on a
// server without reload enabled (2026.08.1), so that is checked first: such a
// server (it gets the setting at its next restart) keeps the old certificate
// until it restarts, and a Warning says so. The kubelet projects a changed
// Secret only after its sync period, so a server still presenting the old
// certificate after a reload is retried, until tlsReloadGiveUp after the
// certificate was issued.

// tlsCertificateAnnotation records, on the StatefulSet, the fingerprint of the
// certificate every server was last seen presenting; while it matches the
// Secret there is nothing to do and nothing is dialled.
const tlsCertificateAnnotation = "neo4j.com/tls-certificate"

// tlsReloadRetry is how soon a reload is checked again while the kubelet is
// still projecting the renewed files; no deployment is tried more often.
const tlsReloadRetry = 30 * time.Second

// tlsReloadGiveUp is how long after a certificate's NotBefore a server may
// still present the previous one before the operator stops retrying and
// reports it (the kubelet projects a changed Secret within a minute or two).
const tlsReloadGiveUp = 10 * time.Minute

// tlsReloadAttempts throttles reload passes per deployment: the reconcile can
// run far more often than tlsReloadRetry.
var tlsReloadAttempts sync.Map // key string -> time.Time

// certFingerprint returns the SHA-256 of the first certificate in PEM data.
func certFingerprint(pemData []byte) (string, error) {
	block, _ := pem.Decode(pemData)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("no certificate in the PEM data")
	}
	sum := sha256.Sum256(block.Bytes)
	return hex.EncodeToString(sum[:]), nil
}

// certNotBefore returns when the first certificate in PEM data became valid.
func certNotBefore(pemData []byte) (time.Time, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return time.Time{}, fmt.Errorf("no certificate in the PEM data")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, err
	}
	return cert.NotBefore, nil
}

// presentedCertFingerprint opens a TLS connection to hostPort and returns the
// fingerprint of the certificate the server presents. Only the certificate is
// wanted, so it is not verified.
func presentedCertFingerprint(ctx context.Context, hostPort string) (string, error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		Config:    &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // reading the presented certificate, not trusting the peer
	}
	conn, err := dialer.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return "", fmt.Errorf("%s: not a TLS connection", hostPort)
	}
	certs := tlsConn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return "", fmt.Errorf("%s presented no certificate", hostPort)
	}
	sum := sha256.Sum256(certs[0].Raw)
	return hex.EncodeToString(sum[:]), nil
}

// tlsReloadConn is what a reload needs from a connection to one server.
type tlsReloadConn interface {
	SettingValue(ctx context.Context, name string) (string, bool, error)
	ReloadTLS(ctx context.Context) error
	Close() error
}

// tlsTarget is one server to bring onto the renewed certificate.
type tlsTarget struct {
	Pod, HostPort string
}

// tlsReloadOutcome is what a reload pass did.
type tlsReloadOutcome struct {
	Reloaded    []string          // servers now presenting the renewed certificate after a reload
	NeedRestart map[string]string // servers that cannot reload it, with why
	Waiting     []string          // servers still presenting the old certificate: retry
}

// reloadRenewedCertificate brings every target onto the certificate with
// fingerprint want: a server already presenting it is left alone; any other is
// asked to reload and checked again.
func reloadRenewedCertificate(ctx context.Context, want string, targets []tlsTarget,
	presented func(ctx context.Context, hostPort string) (string, error),
	dial func(ctx context.Context, pod string) (tlsReloadConn, error)) tlsReloadOutcome {
	out := tlsReloadOutcome{NeedRestart: map[string]string{}}
	for _, t := range targets {
		if fp, err := presented(ctx, t.HostPort); err == nil && fp == want {
			continue
		}
		conn, err := dial(ctx, t.Pod)
		if err != nil {
			out.Waiting = append(out.Waiting, t.Pod) // not reachable over Bolt right now
			continue
		}
		reason := reloadOne(ctx, conn)
		_ = conn.Close()
		if reason != "" {
			out.NeedRestart[t.Pod] = reason
			continue
		}
		if fp, err := presented(ctx, t.HostPort); err == nil && fp == want {
			out.Reloaded = append(out.Reloaded, t.Pod)
		} else {
			out.Waiting = append(out.Waiting, t.Pod) // the kubelet has not projected the renewed files yet
		}
	}
	sort.Strings(out.Reloaded)
	sort.Strings(out.Waiting)
	return out
}

// reloadOne reloads TLS on one server, or returns why it cannot.
func reloadOne(ctx context.Context, conn tlsReloadConn) string {
	enabled, known, err := conn.SettingValue(ctx, resources.TLSReloadSetting)
	switch {
	case err != nil:
		return fmt.Sprintf("could not read %s: %v", resources.TLSReloadSetting, err)
	case !known || !strings.EqualFold(enabled, "true"):
		return "TLS reload is not enabled on it yet; it takes effect at its next restart"
	}
	if err := conn.ReloadTLS(ctx); err != nil {
		return err.Error()
	}
	return ""
}

// tlsReloadDeps lets tests replace the network.
type tlsReloadDeps struct {
	presented func(ctx context.Context, hostPort string) (string, error)
}

func (d *tlsReloadDeps) presentedFn() func(ctx context.Context, hostPort string) (string, error) {
	if d != nil && d.presented != nil {
		return d.presented
	}
	return presentedCertFingerprint
}

// reconcileTLSReload is the shared body of the cluster and standalone passes.
// It returns how soon to look again (0: no need).
func reconcileTLSReload(ctx context.Context, c client.Client, rec record.EventRecorder, owner client.Object,
	secretName, stsName string, deps *tlsReloadDeps,
	dial func(ctx context.Context, pod string) (tlsReloadConn, error)) time.Duration {
	logger := log.FromContext(ctx)
	ns := owner.GetNamespace()

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: ns}, secret); err != nil {
		return 0
	}
	want, err := certFingerprint(secret.Data["tls.crt"])
	if err != nil {
		return 0
	}
	issued, err := certNotBefore(secret.Data["tls.crt"])
	if err != nil {
		return 0
	}
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: stsName, Namespace: ns}, sts); err != nil {
		return 0
	}
	if sts.Annotations[tlsCertificateAnnotation] == want {
		return 0
	}
	throttleKey := fmt.Sprintf("%T/%s/%s", owner, ns, owner.GetName())
	if v, ok := tlsReloadAttempts.Load(throttleKey); ok {
		if last, ok := v.(time.Time); ok && time.Since(last) < tlsReloadRetry {
			return tlsReloadRetry - time.Since(last)
		}
	}
	tlsReloadAttempts.Store(throttleKey, time.Now())

	// Only ready pods: one that is starting reads the renewed files anyway.
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		return 0
	}
	var targets []tlsTarget
	for _, p := range pods.Items {
		if p.Status.PodIP != "" && isPodReady(&p) {
			targets = append(targets, tlsTarget{Pod: p.Name, HostPort: net.JoinHostPort(p.Status.PodIP, "7687")})
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Pod < targets[j].Pod })

	out := reloadRenewedCertificate(ctx, want, targets, deps.presentedFn(), dial)
	// Past the give-up time a server still on the old certificate is reported,
	// not retried for ever.
	if len(out.Waiting) > 0 && time.Since(issued) > tlsReloadGiveUp {
		for _, p := range out.Waiting {
			out.NeedRestart[p] = fmt.Sprintf("it still presents the previous certificate %s after the renewed one was issued", tlsReloadGiveUp)
		}
		out.Waiting = nil
	}
	if len(out.Reloaded) > 0 && rec != nil {
		rec.Eventf(owner, corev1.EventTypeNormal, EventReasonTLSCertificateReloaded,
			"Reloaded the renewed TLS certificate on %s without a restart", strings.Join(out.Reloaded, ", "))
	}
	if len(out.NeedRestart) > 0 && rec != nil {
		var pods []string
		for p := range out.NeedRestart {
			pods = append(pods, p)
		}
		sort.Strings(pods)
		rec.Eventf(owner, corev1.EventTypeWarning, EventReasonTLSCertificateNeedsRestart,
			"%s keep presenting the previous TLS certificate until they restart: %s",
			strings.Join(pods, ", "), out.NeedRestart[pods[0]])
	}
	if len(out.Waiting) > 0 {
		logger.V(1).Info("TLS reload: servers still present the previous certificate; retrying", "statefulSet", stsName, "servers", out.Waiting)
		return tlsReloadRetry
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Name: stsName, Namespace: ns}, latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[tlsCertificateAnnotation] = want
		return c.Update(ctx, latest)
	})
	if err != nil {
		return tlsReloadRetry
	}
	return 0
}

// tlsSecretOwnerRequest maps {name}-tls-secret to a request for {name}; the
// reconciler it reaches ignores names that are not its own Kind.
func tlsSecretOwnerRequest(_ context.Context, obj client.Object) []reconcile.Request {
	name, ok := strings.CutSuffix(obj.GetName(), "-tls-secret")
	if !ok || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}
