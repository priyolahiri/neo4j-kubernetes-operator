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

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-operator/api/v1alpha1"
)

// SecurityCoordinator manages the ordering of security resource reconciliation
// Ensures: Roles → Grants → Users dependency chain
type SecurityCoordinator struct {
	client.Client

	// Channels for coordinating reconciliation order
	roleQueue  chan types.NamespacedName
	grantQueue chan types.NamespacedName
	userQueue  chan types.NamespacedName

	// Tracking maps
	rolesMutex  sync.RWMutex
	grantsMutex sync.RWMutex
	usersMutex  sync.RWMutex

	pendingRoles  map[string][]types.NamespacedName // cluster -> roles
	pendingGrants map[string][]types.NamespacedName // cluster -> grants
	pendingUsers  map[string][]types.NamespacedName // cluster -> users

	// Completed tracking
	completedRoles  map[types.NamespacedName]time.Time
	completedGrants map[types.NamespacedName]time.Time

	// Reconciler references
	roleReconciler  *Neo4jRoleReconciler
	grantReconciler *Neo4jGrantReconciler
	userReconciler  *Neo4jUserReconciler

	// Control channels
	stopChan chan struct{}
	started  bool
	mutex    sync.Mutex
}

// NewSecurityCoordinator creates a new security coordinator
func NewSecurityCoordinator(client client.Client) *SecurityCoordinator {
	return &SecurityCoordinator{
		Client:          client,
		roleQueue:       make(chan types.NamespacedName, 100),
		grantQueue:      make(chan types.NamespacedName, 100),
		userQueue:       make(chan types.NamespacedName, 100),
		pendingRoles:    make(map[string][]types.NamespacedName),
		pendingGrants:   make(map[string][]types.NamespacedName),
		pendingUsers:    make(map[string][]types.NamespacedName),
		completedRoles:  make(map[types.NamespacedName]time.Time),
		completedGrants: make(map[types.NamespacedName]time.Time),
		stopChan:        make(chan struct{}),
	}
}

// SetReconcilers sets the reconciler references
func (sc *SecurityCoordinator) SetReconcilers(
	roleReconciler *Neo4jRoleReconciler,
	grantReconciler *Neo4jGrantReconciler,
	userReconciler *Neo4jUserReconciler,
) {
	sc.roleReconciler = roleReconciler
	sc.grantReconciler = grantReconciler
	sc.userReconciler = userReconciler
}

// Start begins the security coordination process
func (sc *SecurityCoordinator) Start(ctx context.Context) error {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if sc.started {
		return fmt.Errorf("security coordinator already started")
	}

	sc.started = true

	// Start processing goroutines
	go sc.processRoles(ctx)
	go sc.processGrants(ctx)
	go sc.processUsers(ctx)
	go func() { sc.cleanupCompleted() }()

	return nil
}

// Stop stops the security coordination process
func (sc *SecurityCoordinator) Stop() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if !sc.started {
		return
	}

	close(sc.stopChan)
	sc.started = false
}

// ScheduleRoleReconcile schedules a role for reconciliation
func (sc *SecurityCoordinator) ScheduleRoleReconcile(req types.NamespacedName, clusterName string) {
	sc.rolesMutex.Lock()
	defer sc.rolesMutex.Unlock()

	// Add to pending roles for the cluster
	sc.pendingRoles[clusterName] = append(sc.pendingRoles[clusterName], req)

	// Queue for immediate processing
	select {
	case sc.roleQueue <- req:
	default:
		// Queue full, log warning
		log.Log.Info("Role reconcile queue full, dropping request", "role", req)
	}
}

// ScheduleGrantReconcile schedules a grant for reconciliation (after roles are ready)
func (sc *SecurityCoordinator) ScheduleGrantReconcile(req types.NamespacedName, clusterName string) {
	sc.grantsMutex.Lock()
	defer sc.grantsMutex.Unlock()

	// Add to pending grants for the cluster
	sc.pendingGrants[clusterName] = append(sc.pendingGrants[clusterName], req)

	// Check if roles are ready for this cluster
	if sc.areRolesReadyForCluster(clusterName) {
		select {
		case sc.grantQueue <- req:
		default:
			log.Log.Info("Grant reconcile queue full, dropping request", "grant", req)
		}
	}
}

// ScheduleUserReconcile schedules a user for reconciliation (after roles and grants are ready)
func (sc *SecurityCoordinator) ScheduleUserReconcile(req types.NamespacedName, clusterName string) {
	sc.usersMutex.Lock()
	defer sc.usersMutex.Unlock()

	// Add to pending users for the cluster
	sc.pendingUsers[clusterName] = append(sc.pendingUsers[clusterName], req)

	// Check if roles and grants are ready for this cluster
	if sc.areRolesReadyForCluster(clusterName) && sc.areGrantsReadyForCluster(clusterName) {
		select {
		case sc.userQueue <- req:
		default:
			log.Log.Info("User reconcile queue full, dropping request", "user", req)
		}
	}
}

// OnRoleReconcileComplete notifies that a role reconciliation is complete
func (sc *SecurityCoordinator) OnRoleReconcileComplete(req types.NamespacedName, clusterName string, success bool) {
	sc.rolesMutex.Lock()
	defer sc.rolesMutex.Unlock()

	if success {
		sc.completedRoles[req] = time.Now()

		// Remove from pending
		sc.removePendingRole(clusterName, req)

		// Check if we can now process grants for this cluster
		if sc.areRolesReadyForCluster(clusterName) {
			sc.triggerGrantsForCluster(clusterName)
		}
	}
}

// OnGrantReconcileComplete notifies that a grant reconciliation is complete
func (sc *SecurityCoordinator) OnGrantReconcileComplete(req types.NamespacedName, clusterName string, success bool) {
	sc.grantsMutex.Lock()
	defer sc.grantsMutex.Unlock()

	if success {
		sc.completedGrants[req] = time.Now()

		// Remove from pending
		sc.removePendingGrant(clusterName, req)

		// Check if we can now process users for this cluster
		if sc.areGrantsReadyForCluster(clusterName) {
			sc.triggerUsersForCluster(clusterName)
		}
	}
}

// OnUserReconcileComplete notifies that a user reconciliation is complete
func (sc *SecurityCoordinator) OnUserReconcileComplete(req types.NamespacedName, clusterName string, success bool) {
	sc.usersMutex.Lock()
	defer sc.usersMutex.Unlock()

	if success {
		// Remove from pending
		sc.removePendingUser(clusterName, req)
	}
}

// processRoles processes role reconciliation requests
func (sc *SecurityCoordinator) processRoles(ctx context.Context) {
	log := log.FromContext(ctx).WithName("security-coordinator").WithValues("component", "role-processor")

	for {
		select {
		case <-sc.stopChan:
			return
		case req := <-sc.roleQueue:
			if sc.roleReconciler != nil {
				log.Info("Processing role reconciliation", "role", req)

				// Create a context with timeout for reconciliation
				reconcileCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

				result, err := sc.roleReconciler.Reconcile(reconcileCtx, ctrl.Request{NamespacedName: req})
				cancel()

				if err != nil {
					log.Error(err, "Role reconciliation failed", "role", req)
					// Requeue after delay
					go func() {
						time.Sleep(30 * time.Second)
						select {
						case sc.roleQueue <- req:
						case <-sc.stopChan:
						}
					}()
				} else if result.RequeueAfter > 0 || result.Requeue {
					// Handle requeue
					go func() {
						delay := result.RequeueAfter
						if delay == 0 {
							delay = 30 * time.Second
						}
						time.Sleep(delay)
						select {
						case sc.roleQueue <- req:
						case <-sc.stopChan:
						}
					}()
				}
			}
		}
	}
}

// processGrants processes grant reconciliation requests
func (sc *SecurityCoordinator) processGrants(ctx context.Context) {
	log := log.FromContext(ctx).WithName("security-coordinator").WithValues("component", "grant-processor")

	for {
		select {
		case <-sc.stopChan:
			return
		case req := <-sc.grantQueue:
			if sc.grantReconciler != nil {
				log.Info("Processing grant reconciliation", "grant", req)

				reconcileCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				result, err := sc.grantReconciler.Reconcile(reconcileCtx, ctrl.Request{NamespacedName: req})
				cancel()

				if err != nil {
					log.Error(err, "Grant reconciliation failed", "grant", req)
					go func() {
						time.Sleep(30 * time.Second)
						select {
						case sc.grantQueue <- req:
						case <-sc.stopChan:
						}
					}()
				} else if result.RequeueAfter > 0 || result.Requeue {
					go func() {
						delay := result.RequeueAfter
						if delay == 0 {
							delay = 30 * time.Second
						}
						time.Sleep(delay)
						select {
						case sc.grantQueue <- req:
						case <-sc.stopChan:
						}
					}()
				}
			}
		}
	}
}

// processUsers processes user reconciliation requests
func (sc *SecurityCoordinator) processUsers(ctx context.Context) {
	log := log.FromContext(ctx).WithName("security-coordinator").WithValues("component", "user-processor")

	for {
		select {
		case <-sc.stopChan:
			return
		case req := <-sc.userQueue:
			if sc.userReconciler != nil {
				log.Info("Processing user reconciliation", "user", req)

				reconcileCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				result, err := sc.userReconciler.Reconcile(reconcileCtx, ctrl.Request{NamespacedName: req})
				cancel()

				if err != nil {
					log.Error(err, "User reconciliation failed", "user", req)
					go func() {
						time.Sleep(30 * time.Second)
						select {
						case sc.userQueue <- req:
						case <-sc.stopChan:
						}
					}()
				} else if result.RequeueAfter > 0 || result.Requeue {
					go func() {
						delay := result.RequeueAfter
						if delay == 0 {
							delay = 30 * time.Second
						}
						time.Sleep(delay)
						select {
						case sc.userQueue <- req:
						case <-sc.stopChan:
						}
					}()
				}
			}
		}
	}
}

// cleanupCompleted removes old completed entries
func (sc *SecurityCoordinator) cleanupCompleted() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-sc.stopChan:
			return
		case <-ticker.C:
			sc.cleanupOldEntries()
		}
	}
}

// Helper methods

func (sc *SecurityCoordinator) areRolesReadyForCluster(clusterName string) bool {
	sc.rolesMutex.RLock()
	defer sc.rolesMutex.RUnlock()

	pending, exists := sc.pendingRoles[clusterName]
	return !exists || len(pending) == 0
}

func (sc *SecurityCoordinator) areGrantsReadyForCluster(clusterName string) bool {
	sc.grantsMutex.RLock()
	defer sc.grantsMutex.RUnlock()

	pending, exists := sc.pendingGrants[clusterName]
	return !exists || len(pending) == 0
}

func (sc *SecurityCoordinator) triggerGrantsForCluster(clusterName string) {
	sc.grantsMutex.RLock()
	grants := make([]types.NamespacedName, len(sc.pendingGrants[clusterName]))
	copy(grants, sc.pendingGrants[clusterName])
	sc.grantsMutex.RUnlock()

	for _, grant := range grants {
		select {
		case sc.grantQueue <- grant:
		default:
			return // Queue full
		}
	}
}

func (sc *SecurityCoordinator) triggerUsersForCluster(clusterName string) {
	sc.usersMutex.RLock()
	users := make([]types.NamespacedName, len(sc.pendingUsers[clusterName]))
	copy(users, sc.pendingUsers[clusterName])
	sc.usersMutex.RUnlock()

	for _, user := range users {
		select {
		case sc.userQueue <- user:
		default:
			return // Queue full
		}
	}
}

func (sc *SecurityCoordinator) removePendingRole(clusterName string, req types.NamespacedName) {
	pending := sc.pendingRoles[clusterName]
	for i, r := range pending {
		if r == req {
			sc.pendingRoles[clusterName] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(sc.pendingRoles[clusterName]) == 0 {
		delete(sc.pendingRoles, clusterName)
	}
}

func (sc *SecurityCoordinator) removePendingGrant(clusterName string, req types.NamespacedName) {
	pending := sc.pendingGrants[clusterName]
	for i, r := range pending {
		if r == req {
			sc.pendingGrants[clusterName] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(sc.pendingGrants[clusterName]) == 0 {
		delete(sc.pendingGrants, clusterName)
	}
}

func (sc *SecurityCoordinator) removePendingUser(clusterName string, req types.NamespacedName) {
	pending := sc.pendingUsers[clusterName]
	for i, r := range pending {
		if r == req {
			sc.pendingUsers[clusterName] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(sc.pendingUsers[clusterName]) == 0 {
		delete(sc.pendingUsers, clusterName)
	}
}

func (sc *SecurityCoordinator) cleanupOldEntries() {
	cutoff := time.Now().Add(-1 * time.Hour)

	sc.rolesMutex.Lock()
	for req, timestamp := range sc.completedRoles {
		if timestamp.Before(cutoff) {
			delete(sc.completedRoles, req)
		}
	}
	sc.rolesMutex.Unlock()

	sc.grantsMutex.Lock()
	for req, timestamp := range sc.completedGrants {
		if timestamp.Before(cutoff) {
			delete(sc.completedGrants, req)
		}
	}
	sc.grantsMutex.Unlock()
}

// GetClusterNameFromRole extracts cluster name from role spec
func (sc *SecurityCoordinator) GetClusterNameFromRole(ctx context.Context, req types.NamespacedName) (string, error) {
	role := &neo4jv1alpha1.Neo4jRole{}
	if err := sc.Get(ctx, req, role); err != nil {
		return "", err
	}
	return role.Spec.ClusterRef, nil
}

// GetClusterNameFromGrant extracts cluster name from grant spec
func (sc *SecurityCoordinator) GetClusterNameFromGrant(ctx context.Context, req types.NamespacedName) (string, error) {
	grant := &neo4jv1alpha1.Neo4jGrant{}
	if err := sc.Get(ctx, req, grant); err != nil {
		return "", err
	}
	return grant.Spec.ClusterRef, nil
}

// GetClusterNameFromUser extracts cluster name from user spec
func (sc *SecurityCoordinator) GetClusterNameFromUser(ctx context.Context, req types.NamespacedName) (string, error) {
	user := &neo4jv1alpha1.Neo4jUser{}
	if err := sc.Get(ctx, req, user); err != nil {
		return "", err
	}
	return user.Spec.ClusterRef, nil
}
