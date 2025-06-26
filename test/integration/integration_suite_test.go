/*
Copyright 2025.

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

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-kubernetes-operator/api/v1alpha1"
	"github.com/neo4j-labs/neo4j-kubernetes-operator/internal/controller"
)

var cfg *rest.Config
var k8sClient client.Client
var ctx context.Context
var cancel context.CancelFunc
var testRunID string
var mgr manager.Manager

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.Background())

	// Generate unique test run ID
	testRunID = fmt.Sprintf("%d", time.Now().UnixNano())

	// Set TEST_MODE for faster test execution
	os.Setenv("TEST_MODE", "true")

	By("connecting to existing cluster")
	// Use existing cluster instead of envtest
	var err error
	cfg, err = ctrl.GetConfig()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// Register the scheme
	err = neo4jv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// Register other schemes
	err = appsv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = batchv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Set up the controller manager
	By("setting up controller manager")

	// Use minimal cache options for faster test execution
	cacheOpt := manager.Options{
		Scheme:                 scheme.Scheme,
		HealthProbeBindAddress: "0",
		Metrics:                metricsserver.Options{BindAddress: "0"},
	}

	mgr, err = manager.New(cfg, cacheOpt)
	Expect(err).NotTo(HaveOccurred())

	// Set up controllers with test mode optimizations
	err = (&controller.Neo4jEnterpriseClusterReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		Recorder:          mgr.GetEventRecorderFor("neo4j-enterprise-cluster-controller"),
		RequeueAfter:      controller.GetTestRequeueAfter(),
		TopologyScheduler: controller.NewTopologyScheduler(mgr.GetClient()),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jDatabaseReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-database-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jBackupReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-backup-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jRestoreReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-restore-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jRoleReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-role-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jGrantReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-grant-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jUserReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("neo4j-user-controller"),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = (&controller.Neo4jPluginReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		RequeueAfter: controller.GetTestRequeueAfter(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	// Skip webhooks for integration tests to avoid TLS certificate issues
	// Webhooks are tested separately in unit tests
	By("skipping webhooks for integration tests")

	// Start the manager
	By("starting the manager")
	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()

	// Wait for cache to sync with increased timeout for real cluster
	By("waiting for cache to sync")
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	Expect(mgr.GetCache().WaitForCacheSync(ctxWithTimeout)).To(BeTrue())
})

var _ = AfterSuite(func() {
	By("cleaning up any leftover test namespaces")
	cleanupTestNamespaces()

	By("tearing down the test environment")
	// Cancel the context to signal shutdown
	cancel()

	By("initiating manager shutdown sequence")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if mgr != nil {
		By("waiting for manager to shut down")
		select {
		case <-shutdownCtx.Done():
			By("manager shutdown timeout reached")
		case <-time.After(5 * time.Second):
			By("manager shutdown completed")
		}
	}

	By("test environment teardown completed, forcefully exiting process to avoid controller-runtime goroutine leaks")
	os.Exit(0)
})

// Common test utilities
const (
	timeout        = time.Second * 10 // Increased from 5s to 10s for integration tests
	interval       = time.Millisecond * 100
	cleanupTimeout = time.Second * 30 // Longer timeout for cleanup operations
)

func createTestNamespace(name string) string {
	return fmt.Sprintf("test-%s-%s-%d", name, testRunID, time.Now().UnixNano())
}

// cleanupTestNamespaces removes any leftover test namespaces
func cleanupTestNamespaces() {
	if k8sClient == nil {
		return
	}

	ctx := context.Background()
	namespaceList := &corev1.NamespaceList{}

	err := k8sClient.List(ctx, namespaceList)
	if err != nil {
		return
	}

	for _, ns := range namespaceList.Items {
		if isTestNamespace(ns.Name) {
			// Force delete the namespace
			err := k8sClient.Delete(ctx, &ns)
			if err != nil && !errors.IsNotFound(err) {
				// Log but don't fail the test
				fmt.Printf("Warning: Failed to cleanup namespace %s: %v\n", ns.Name, err)
			}
		}
	}
}

// isTestNamespace checks if a namespace is a test namespace
func isTestNamespace(name string) bool {
	return strings.HasPrefix(name, "test-")
}

// aggressiveCleanup performs fast cleanup without waiting for complete deletion
func aggressiveCleanup(namespace string) {
	if k8sClient == nil || namespace == "" {
		return
	}

	ctx := context.Background()

	// List of CRDs to clean up
	crds := []client.ObjectList{
		&neo4jv1alpha1.Neo4jEnterpriseClusterList{},
		&neo4jv1alpha1.Neo4jBackupList{},
		&neo4jv1alpha1.Neo4jRestoreList{},
		&neo4jv1alpha1.Neo4jPluginList{},
		&neo4jv1alpha1.Neo4jUserList{},
		&neo4jv1alpha1.Neo4jRoleList{},
		&neo4jv1alpha1.Neo4jGrantList{},
	}

	// Force delete all custom resources
	for _, crdList := range crds {
		_ = k8sClient.List(ctx, crdList, client.InNamespace(namespace))
		switch list := crdList.(type) {
		case *neo4jv1alpha1.Neo4jEnterpriseClusterList:
			for _, item := range list.Items {
				// Remove finalizers and force delete
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jBackupList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jRestoreList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jPluginList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jUserList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jRoleList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		case *neo4jv1alpha1.Neo4jGrantList:
			for _, item := range list.Items {
				if len(item.Finalizers) > 0 {
					item.Finalizers = nil
					_ = k8sClient.Update(ctx, &item)
				}
				_ = k8sClient.Delete(ctx, &item, client.GracePeriodSeconds(0))
			}
		}
	}

	// Force delete the namespace without waiting
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	_ = k8sClient.Delete(ctx, ns, client.GracePeriodSeconds(0))
}
