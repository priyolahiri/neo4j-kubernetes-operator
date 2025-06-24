package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	K8sClient     client.Client
	Ctx           context.Context
	TestNamespace string
	Timeout       = time.Minute * 5
	Interval      = time.Second * 10
)

// SetupTestEnv initializes common test variables.
func SetupTestEnv() {
	Ctx = context.Background()
	TestNamespace = "test-" + RandString(8)
	// K8sClient should be set in the suite setup after envtest or real cluster client is created
}

// RandString generates a random string of length n.
func RandString(n int) string {
	// Use crypto/rand for better randomness
	bytes := make([]byte, n/2+1)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to a simple hash-based approach if crypto/rand fails
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)[:n]
}

// Add other shared helpers here, e.g., waitForNodeReadiness, createEKSCluster, etc.
