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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// testCertPEM returns a fresh self-signed certificate in PEM.
func testCertPEM(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestCertFingerprint(t *testing.T) {
	a, b := testCertPEM(t, "a"), testCertPEM(t, "b")
	fa, err := certFingerprint(a)
	if err != nil || len(fa) != 64 {
		t.Fatalf("fingerprint %q, %v", fa, err)
	}
	if fb, _ := certFingerprint(b); fb == fa {
		t.Error("different certificates must have different fingerprints")
	}
	if _, err := certFingerprint([]byte("not pem")); err == nil {
		t.Error("garbage must be an error")
	}
}

// fakeTLSConn records reloads; after a successful one the server presents the
// new certificate unless stale is set (the kubelet has not projected it yet).
type fakeTLSConn struct {
	server *fakeTLSServer
}

func (c *fakeTLSConn) SettingValue(_ context.Context, name string) (string, bool, error) {
	if name != resources.TLSReloadSetting {
		return "", false, nil
	}
	if c.server.disabled {
		return "false", true, nil
	}
	return "true", true, nil
}

// ReloadTLS mimics 2026.08.1: it succeeds without doing anything when reload
// is not enabled.
func (c *fakeTLSConn) ReloadTLS(context.Context) error {
	c.server.reloads++
	if c.server.refuse {
		return errors.New("reload failed")
	}
	if !c.server.stale && !c.server.disabled {
		c.server.presenting = c.server.onDisk
	}
	return nil
}
func (c *fakeTLSConn) Close() error { return nil }

type fakeTLSServer struct {
	presenting, onDisk string
	refuse, stale      bool
	disabled           bool // dbms.security.tls_reload_enabled is false
	unreachable        bool
	reloads            int
}

func tlsFakes(servers map[string]*fakeTLSServer) (func(context.Context, string) (string, error), func(context.Context, string) (tlsReloadConn, error)) {
	presented := func(_ context.Context, hostPort string) (string, error) {
		return servers[hostPort].presenting, nil
	}
	dial := func(_ context.Context, pod string) (tlsReloadConn, error) {
		s := servers[pod+":7687"]
		if s.unreachable {
			return nil, errors.New("unreachable")
		}
		return &fakeTLSConn{server: s}, nil
	}
	return presented, dial
}

func TestReloadRenewedCertificate(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name                    string
		server                  fakeTLSServer
		wantReloaded, wantNeeds bool
		wantWaiting             bool
		wantReloads             int
	}{
		{"already current is left alone", fakeTLSServer{presenting: "new", onDisk: "new"}, false, false, false, 0},
		{"reloaded onto the new certificate", fakeTLSServer{presenting: "old", onDisk: "new"}, true, false, false, 1},
		{"files not projected yet: retry", fakeTLSServer{presenting: "old", onDisk: "new", stale: true}, false, false, true, 1},
		// reloadTLS would succeed and do nothing: never call it, report it.
		{"reload not enabled: restart needed", fakeTLSServer{presenting: "old", onDisk: "new", disabled: true}, false, true, false, 0},
		{"reload fails: restart needed", fakeTLSServer{presenting: "old", onDisk: "new", refuse: true}, false, true, false, 1},
		{"not reachable: retry", fakeTLSServer{presenting: "old", onDisk: "new", unreachable: true}, false, false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.server
			presented, dial := tlsFakes(map[string]*fakeTLSServer{"p:7687": &s})
			out := reloadRenewedCertificate(ctx, "new", []tlsTarget{{Pod: "p", HostPort: "p:7687"}}, presented, dial)
			if (len(out.Reloaded) == 1) != tc.wantReloaded || (len(out.NeedRestart) == 1) != tc.wantNeeds || (len(out.Waiting) == 1) != tc.wantWaiting {
				t.Errorf("outcome %+v", out)
			}
			if s.reloads != tc.wantReloads {
				t.Errorf("reloads %d, want %d", s.reloads, tc.wantReloads)
			}
		})
	}
}

// TestReconcileTLSReload pins #469 end to end on fakes: a renewed Secret is
// reloaded on every ready server, recorded on the StatefulSet, and announced;
// while it is recorded nothing is dialled; a server whose files lag is retried.
func TestReconcileTLSReload(t *testing.T) {
	ctx := context.Background()
	oldPEM, newPEM := testCertPEM(t, "old"), testCertPEM(t, "new")
	oldFP, _ := certFingerprint(oldPEM)
	newFP, _ := certFingerprint(newPEM)

	cluster := minimalCluster("tr", "default")
	sts := serverSTS("tr", "default")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tr-tls-secret", Namespace: "default"},
		Data: map[string][]byte{"tls.crt": newPEM}}
	objs := []client.Object{cluster, sts, secret}
	for i, ip := range []string{"10.0.0.1", "10.0.0.2"} {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "tr-server-" + string(rune('0'+i)), Namespace: "default", Labels: sts.Spec.Selector.MatchLabels},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		})
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).Build()

	servers := map[string]*fakeTLSServer{
		"10.0.0.1:7687": {presenting: oldFP, onDisk: newFP},
		"10.0.0.2:7687": {presenting: oldFP, onDisk: newFP, stale: true},
	}
	presented, _ := tlsFakes(servers)
	byPod := map[string]string{"tr-server-0": "10.0.0.1:7687", "tr-server-1": "10.0.0.2:7687"}
	dial := func(_ context.Context, pod string) (tlsReloadConn, error) {
		return &fakeTLSConn{server: servers[byPod[pod]]}, nil
	}
	rec := record.NewFakeRecorder(10)
	run := func() time.Duration {
		resetTLSReloadThrottle()
		return reconcileTLSReload(ctx, fc, rec, cluster, "tr-tls-secret", "tr-server", &tlsReloadDeps{presented: presented}, dial)
	}
	annotation := func() string {
		got := &appsv1.StatefulSet{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "tr-server", Namespace: "default"}, got); err != nil {
			t.Fatal(err)
		}
		return got.Annotations[tlsCertificateAnnotation]
	}

	if d := run(); d != tlsReloadRetry || annotation() != "" {
		t.Fatalf("server-1's files lag: retry without recording (requeue %v, annotation %q)", d, annotation())
	}
	servers["10.0.0.2:7687"].stale = false
	if d := run(); d != 0 || annotation() != newFP {
		t.Fatalf("both servers present the renewed certificate: record it (requeue %v, annotation %q)", d, annotation())
	}
	reloads := servers["10.0.0.1:7687"].reloads + servers["10.0.0.2:7687"].reloads
	if d := run(); d != 0 || servers["10.0.0.1:7687"].reloads+servers["10.0.0.2:7687"].reloads != reloads {
		t.Error("a recorded certificate must not be dialled or reloaded again")
	}
	close(rec.Events)
	var reloadedEvents int
	for e := range rec.Events {
		if strings.Contains(e, EventReasonTLSCertificateReloaded) {
			reloadedEvents++
		}
	}
	if reloadedEvents != 2 {
		t.Errorf("one %s event per pass that reloaded something, got %d", EventReasonTLSCertificateReloaded, reloadedEvents)
	}
}

// resetTLSReloadThrottle lets a test run passes back to back.
func resetTLSReloadThrottle() {
	tlsReloadAttempts.Range(func(k, _ any) bool { tlsReloadAttempts.Delete(k); return true })
}

// TestReconcileTLSReload_ThrottleAndGiveUp: passes are throttled, and a server
// still on the old certificate long after the renewed one was issued is
// reported instead of retried for ever.
func TestReconcileTLSReload_ThrottleAndGiveUp(t *testing.T) {
	ctx := context.Background()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "old-issue"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	issuedLongAgo := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	cluster := minimalCluster("tg", "default")
	sts := serverSTS("tg", "default")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tg-tls-secret", Namespace: "default"},
		Data: map[string][]byte{"tls.crt": issuedLongAgo}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "tg-server-0", Namespace: "default", Labels: sts.Spec.Selector.MatchLabels},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.1.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster, sts, secret, pod).Build()
	server := &fakeTLSServer{presenting: "old", onDisk: "new", stale: true}
	presented := func(context.Context, string) (string, error) { return server.presenting, nil }
	dial := func(context.Context, string) (tlsReloadConn, error) { return &fakeTLSConn{server: server}, nil }
	rec := record.NewFakeRecorder(10)

	resetTLSReloadThrottle()
	if d := reconcileTLSReload(ctx, fc, rec, cluster, "tg-tls-secret", "tg-server", &tlsReloadDeps{presented: presented}, dial); d != 0 {
		t.Errorf("past the give-up time the pass ends (requeue %v)", d)
	}
	close(rec.Events)
	var warned bool
	for e := range rec.Events {
		warned = warned || strings.Contains(e, EventReasonTLSCertificateNeedsRestart)
	}
	if !warned {
		t.Error("a server stuck on the old certificate must be reported")
	}

	// A second pass right away is throttled.
	got := &appsv1.StatefulSet{}
	_ = fc.Get(ctx, types.NamespacedName{Name: "tg-server", Namespace: "default"}, got)
	delete(got.Annotations, tlsCertificateAnnotation)
	_ = fc.Update(ctx, got)
	before := server.reloads
	if d := reconcileTLSReload(ctx, fc, nil, cluster, "tg-tls-secret", "tg-server", &tlsReloadDeps{presented: presented}, dial); d <= 0 || server.reloads != before {
		t.Errorf("a pass within %v of the last must be skipped (requeue %v, reloads %d→%d)", tlsReloadRetry, d, before, server.reloads)
	}
}

func TestTLSSecretOwnerRequest(t *testing.T) {
	got := tlsSecretOwnerRequest(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "c-tls-secret", Namespace: "ns"}})
	if len(got) != 1 || got[0].Name != "c" || got[0].Namespace != "ns" {
		t.Errorf("got %v", got)
	}
	if got := tlsSecretOwnerRequest(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin"}}); got != nil {
		t.Errorf("other Secrets are ignored, got %v", got)
	}
}

func TestClusterDeferredSettings_TLSReload(t *testing.T) {
	c := minimalCluster("c", "default")
	c.Spec.Image.Tag = "2026.08.1-enterprise"
	c.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: resources.CertManagerMode}
	got := clusterDeferredSettings(c)
	if len(got) != 1 || got[0].Name != resources.TLSReloadSetting || !got[0].Supported("2026.08.1") || got[0].Supported("2025.01.0") {
		t.Errorf("CalVer with cert-manager TLS defers the TLS reload setting, got %v", got)
	}
	c.Spec.TLS = nil
	if got := clusterDeferredSettings(c); len(got) != 0 {
		t.Errorf("no TLS, nothing deferred: %v", got)
	}
}

// TestStandaloneTLSReloadSetting pins #469 on the standalone: the setting is
// rendered for CalVer 2025.03+ with cert-manager TLS only, and an upgrade that
// adds it keeps the running pod's stamp even before the semantic hash exists.
func TestStandaloneTLSReloadSetting(t *testing.T) {
	ctx := context.Background()
	for tag, want := range map[string]bool{"2026.08.1-enterprise": true, "2025.03.0-enterprise": true, "2025.01.0-enterprise": false, "5.26-enterprise": false} {
		r, _ := standaloneCMTestReconciler(t)
		sa := standaloneForSTS(tag)
		sa.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: resources.CertManagerMode}
		conf := r.createConfigMap(sa).Data["neo4j.conf"]
		if got := strings.Contains(conf, resources.TLSReloadSetting+"=true"); got != want {
			t.Errorf("%s: setting rendered=%v, want %v", tag, got, want)
		}
	}

	r, c := standaloneCMTestReconciler(t)
	sa := standaloneForSTS("2026.08.1-enterprise")
	sa.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: resources.CertManagerMode}
	if err := r.reconcileConfigMap(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileStatefulSet(ctx, sa); err != nil {
		t.Fatal(err)
	}
	// Make the StatefulSet look like a previous operator's: stamped for the conf
	// without the setting, and no semantic hash yet.
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: "sa-config", Namespace: "default"}, cm); err != nil {
		t.Fatal(err)
	}
	oldStamp := standaloneConfHash(withoutDeferredConfKeys(cm.Data["neo4j.conf"]))
	if oldStamp == standaloneConfHash(cm.Data["neo4j.conf"]) {
		t.Fatal("test premise: the conf carries the deferred setting")
	}
	sts := getSTS(t, r)
	sts.Spec.Template.Annotations[standaloneConfigHashAnnotation] = oldStamp
	delete(sts.Annotations, standaloneConfigSemanticHashAnnotation)
	delete(sts.Annotations, standaloneTemplateHashAnnotation)
	if err := c.Update(ctx, sts); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileStatefulSet(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if got := getSTS(t, r).Spec.Template.Annotations[standaloneConfigHashAnnotation]; got != oldStamp {
		t.Errorf("adding the TLS reload setting must not restart the pod: stamp %s, want %s", got, oldStamp)
	}
}
