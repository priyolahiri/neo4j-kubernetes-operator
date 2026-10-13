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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// fakeDriver counts Close; every other method is the nil embedded interface.
type fakeDriver struct {
	neo4j.DriverWithContext
	closed atomic.Int32
}

func (f *fakeDriver) Close(context.Context) error { f.closed.Add(1); return nil }

// closes reports how many times a cached entry's fake driver was closed.
func closes(t *testing.T, e *sharedDriver) int32 {
	t.Helper()
	f, ok := e.driver.(*fakeDriver)
	require.True(t, ok)
	return f.closed.Load()
}

func keyFor(password string) driverKey {
	return driverKey{uid: "u", uri: "neo4j://c-client:7687", username: "neo4j", password: sha256.Sum256([]byte(password))}
}

func TestDriverCache(t *testing.T) {
	builds := 0
	build := func() (neo4j.DriverWithContext, bool, error) { builds++; return &fakeDriver{}, false, nil }

	t.Run("one driver per key, shared", func(t *testing.T) {
		c := newDriverCache(time.Hour)
		builds = 0
		a, err := c.acquire("cluster/ns/c", keyFor("p"), build)
		require.NoError(t, err)
		b, err := c.acquire("cluster/ns/c", keyFor("p"), build)
		require.NoError(t, err)
		assert.Same(t, a, b)
		assert.Equal(t, 1, builds)
		c.release(a)
		c.release(b)
		assert.Zero(t, closes(t, a), "released, not closed")
	})

	t.Run("a changed key retires the old driver after its last holder", func(t *testing.T) {
		c := newDriverCache(time.Hour)
		old, _ := c.acquire("cluster/ns/c", keyFor("p1"), build)
		fresh, _ := c.acquire("cluster/ns/c", keyFor("p2"), build)
		assert.NotSame(t, old, fresh, "password rotated: a new driver")
		assert.Zero(t, closes(t, old), "still held")
		c.release(old)
		assert.EqualValues(t, 1, closes(t, old), "closed by its last holder")
		c.release(fresh)
		assert.Zero(t, closes(t, fresh))
	})

	t.Run("an unheld driver is closed as soon as it is retired", func(t *testing.T) {
		c := newDriverCache(time.Hour)
		old, _ := c.acquire("cluster/ns/c", keyFor("p1"), build)
		c.release(old)
		fresh, _ := c.acquire("cluster/ns/c", keyFor("p2"), build)
		assert.EqualValues(t, 1, closes(t, old))
		c.release(fresh)
	})

	t.Run("idle drivers are closed", func(t *testing.T) {
		c := newDriverCache(10 * time.Minute)
		now := time.Unix(0, 0)
		c.now = func() time.Time { return now }
		e, _ := c.acquire("cluster/ns/gone", keyFor("p"), build)
		c.release(e)
		now = now.Add(11 * time.Minute)
		other, _ := c.acquire("cluster/ns/other", driverKey{uid: "x"}, build)
		assert.EqualValues(t, 1, closes(t, e), "unused for longer than the idle timeout")
		c.release(other)
	})

	t.Run("forget closes a deleted deployment's driver", func(t *testing.T) {
		c := newDriverCache(time.Hour)
		e, _ := c.acquire("cluster/ns/c", keyFor("p"), build)
		c.release(e)
		c.forget("cluster/ns/c")
		assert.EqualValues(t, 1, closes(t, e))
		again, _ := c.acquire("cluster/ns/c", keyFor("p"), build)
		assert.NotSame(t, e, again, "a forgotten driver is not handed out again")
		c.release(again)
	})
}

// Many reconciles at once (the cluster controller runs four workers, and the
// database, user and role controllers use the same clients): one build per
// key, and every driver retired by a key change is closed exactly once.
func TestDriverCacheConcurrent(t *testing.T) {
	c := newDriverCache(time.Hour)
	var mu sync.Mutex
	var built []*fakeDriver
	build := func() (neo4j.DriverWithContext, bool, error) {
		d := &fakeDriver{}
		mu.Lock()
		built = append(built, d)
		mu.Unlock()
		return d, false, nil
	}
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				pw := "p1"
				if i > 100 {
					pw = "p2"
				}
				e, err := c.acquire("cluster/ns/c", keyFor(pw), build)
				if err != nil {
					t.Error(err)
					return
				}
				c.release(e)
			}
		}(g)
	}
	wg.Wait()
	for _, d := range built {
		assert.LessOrEqual(t, d.closed.Load(), int32(1), "never closed twice")
	}
}

func clientTestCluster() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.Secret) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "uid-c"}}
	cluster.Spec.Image = neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin", Namespace: "ns"},
		Data: map[string][]byte{"username": []byte("neo4j"), "password": []byte("pw-1")}}
	return cluster, secret
}

// Clients of one cluster share its driver; closing a client releases it;
// a rotated password or a TLS change gets a new driver.
func TestNewClientForEnterpriseSharesTheDriver(t *testing.T) {
	saved := drivers
	drivers = newDriverCache(time.Hour)
	t.Cleanup(func() { drivers = saved })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	cluster, secret := clientTestCluster()
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	a, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	b, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	assert.Same(t, a.driver, b.driver, "one driver for the cluster")
	assert.NotSame(t, a.circuitBreaker, b.circuitBreaker, "each client keeps its own circuit breaker")
	shared := a.driver
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())

	c, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	assert.Same(t, shared, c.driver, "closing the clients left the driver to be reused")
	require.NoError(t, c.Close())

	secret.Data["password"] = []byte("pw-2")
	require.NoError(t, k8s.Update(context.Background(), secret))
	d, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	assert.NotSame(t, shared, d.driver, "a rotated password gets a new driver")
	rotated := d.driver
	require.NoError(t, d.Close())

	cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager"}
	require.NoError(t, k8s.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "c-tls-secret", Namespace: "ns"},
		Data: map[string][]byte{"ca.crt": []byte("not a real CA")}}))
	e, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	assert.NotSame(t, rotated, e.driver, "TLS switched on gets a new driver")
	withTLS := e.driver
	require.NoError(t, e.Close())

	// A renewed certificate: same URI, same password, new CA.
	tlsSecret := &corev1.Secret{}
	require.NoError(t, k8s.Get(context.Background(), client.ObjectKey{Name: "c-tls-secret", Namespace: "ns"}, tlsSecret))
	tlsSecret.Data["ca.crt"] = []byte("another CA")
	require.NoError(t, k8s.Update(context.Background(), tlsSecret))
	f, err := NewClientForEnterprise(cluster, k8s, "admin")
	require.NoError(t, err)
	assert.NotSame(t, withTLS, f.driver, "a renewed certificate gets a new driver")
	require.NoError(t, f.Close())
}
