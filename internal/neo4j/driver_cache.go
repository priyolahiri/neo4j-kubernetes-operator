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

package neo4j

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Shared drivers (operations.md id 132).
//
// A reconcile pass used to build a fresh driver for every client and close it
// at the end of the pass, so every pass opened new connections — a TCP
// connect, a TLS handshake with TLS on, and a login, each a line in the
// server's security.log (about 22 a minute for one quiet 3-server cluster).
// The cluster and standalone clients now share one driver per deployment and
// credentials; each NewClientFor* still returns a Client of its own, with its
// own circuit breaker, and its Close releases the driver instead of closing it.
//
// The key holds everything the driver was built from: the deployment's UID,
// the URI, the username, the password and a digest of the TLS material.
// A deployment whose key changes (password rotated, certificate renewed, TLS
// switched on) gets a new driver, and the old one is closed once nobody holds
// it. A driver nobody has used for driverIdleTimeout is closed too, which also
// disposes of deleted deployments' drivers.

const driverIdleTimeout = 10 * time.Minute

type driverKey struct {
	uid      types.UID
	uri      string
	username string
	// password is compared as it is. An unsalted SHA-256 of it (as before)
	// protected nothing — the shared driver keeps the password for its auth
	// token in the same process — and CodeQL rightly reads it as a weak
	// password hash (go/weak-sensitive-data-hashing). The key is never
	// printed with it: String and GoString leave it out.
	password string
	tls      [sha256.Size]byte
}

// String and GoString print the key without its password, so no log line or
// error that formats a key can carry one.
func (k driverKey) String() string {
	return fmt.Sprintf("driverKey{uid:%s uri:%s username:%s password:<redacted> tls:%x}", k.uid, k.uri, k.username, k.tls[:4])
}

func (k driverKey) GoString() string { return k.String() }

type sharedDriver struct {
	driver                  neo4j.DriverWithContext
	tlsVerificationDisabled bool
	refs                    int
	lastUsed                time.Time
	retired                 bool
}

type driverCache struct {
	mu      sync.Mutex
	entries map[driverKey]*sharedDriver
	// current is each deployment's newest key (by target, "kind/ns/name"), so
	// a changed key retires the driver it replaces.
	current map[string]driverKey
	idle    time.Duration
	now     func() time.Time
}

func newDriverCache(idle time.Duration) *driverCache {
	return &driverCache{entries: map[driverKey]*sharedDriver{}, current: map[string]driverKey{}, idle: idle, now: time.Now}
}

// drivers is the process-wide cache; safe for concurrent reconciles.
var drivers = newDriverCache(driverIdleTimeout)

// acquire returns the shared driver for key, building it with build when there
// is none, and counts the caller as a holder until release.
func (c *driverCache) acquire(target string, key driverKey, build func() (neo4j.DriverWithContext, bool, error)) (*sharedDriver, error) {
	c.mu.Lock()
	now := c.now()
	toClose := c.sweepLocked(now)
	if old, ok := c.current[target]; ok && old != key {
		toClose = append(toClose, c.retireLocked(old)...)
	}
	e, ok := c.entries[key]
	if !ok {
		d, insecure, err := build()
		if err != nil {
			c.mu.Unlock()
			closeDrivers(toClose)
			return nil, err
		}
		e = &sharedDriver{driver: d, tlsVerificationDisabled: insecure}
		c.entries[key] = e
	}
	c.current[target] = key
	e.refs++
	e.lastUsed = now
	c.mu.Unlock()
	closeDrivers(toClose)
	return e, nil
}

// release drops a holder; a retired driver is closed by its last holder.
func (c *driverCache) release(e *sharedDriver) {
	c.mu.Lock()
	e.refs--
	e.lastUsed = c.now()
	closeIt := e.retired && e.refs == 0
	c.mu.Unlock()
	if closeIt {
		closeDrivers([]neo4j.DriverWithContext{e.driver})
	}
}

// forget retires a deployment's driver (its deletion).
func (c *driverCache) forget(target string) {
	c.mu.Lock()
	var toClose []neo4j.DriverWithContext
	if key, ok := c.current[target]; ok {
		toClose = c.retireLocked(key)
		delete(c.current, target)
	}
	c.mu.Unlock()
	closeDrivers(toClose)
}

// retireLocked removes key's driver from the cache; it is returned for
// closing when nobody holds it, or closed by its last holder otherwise.
func (c *driverCache) retireLocked(key driverKey) []neo4j.DriverWithContext {
	e, ok := c.entries[key]
	if !ok {
		return nil
	}
	delete(c.entries, key)
	e.retired = true
	if e.refs == 0 {
		return []neo4j.DriverWithContext{e.driver}
	}
	return nil
}

// sweepLocked retires the drivers nobody has used for the idle timeout.
func (c *driverCache) sweepLocked(now time.Time) []neo4j.DriverWithContext {
	var toClose []neo4j.DriverWithContext
	for key, e := range c.entries {
		if e.refs == 0 && now.Sub(e.lastUsed) >= c.idle {
			toClose = append(toClose, c.retireLocked(key)...)
			for t, k := range c.current {
				if k == key {
					delete(c.current, t)
				}
			}
		}
	}
	return toClose
}

func closeDrivers(ds []neo4j.DriverWithContext) {
	for _, d := range ds {
		_ = d.Close(context.Background())
	}
}

// ForgetCluster and ForgetStandalone close a deleted deployment's shared
// driver now rather than after the idle timeout.
func ForgetCluster(namespace, name string) { drivers.forget(driverTarget("cluster", namespace, name)) }

func ForgetStandalone(namespace, name string) {
	drivers.forget(driverTarget("standalone", namespace, name))
}

func driverTarget(kind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", kind, namespace, name)
}

func newDriverKey(uid types.UID, uri string, creds *Credentials, tlsDigest [sha256.Size]byte) driverKey {
	return driverKey{uid: uid, uri: uri, username: creds.Username, password: creds.Password, tls: tlsDigest}
}

// tlsMaterialDigest digests what buildTLSConfig builds a TLS config from: the
// mode and, for each Secret it reads, its ca.crt and tls.crt.
func tlsMaterialDigest(ctx context.Context, k8sClient client.Client, namespace, resourceName string, tlsSpec *neo4jv1beta1.TLSSpec) [sha256.Size]byte {
	h := sha256.New()
	if tlsSpec == nil || tlsSpec.Mode != "cert-manager" {
		h.Write([]byte("no-tls"))
		return [sha256.Size]byte(h.Sum(nil))
	}
	h.Write([]byte(tlsSpec.Mode))
	for _, name := range tlsSecretNames(resourceName, tlsSpec) {
		h.Write([]byte{0})
		h.Write([]byte(name))
		secret := &corev1.Secret{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
			h.Write([]byte("\x00missing"))
			continue
		}
		for _, k := range []string{"ca.crt", "tls.crt"} {
			h.Write([]byte{0})
			h.Write(secret.Data[k])
		}
	}
	return [sha256.Size]byte(h.Sum(nil))
}
