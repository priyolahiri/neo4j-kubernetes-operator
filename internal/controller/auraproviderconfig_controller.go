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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraproviderconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraproviderconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// AuraProviderConfigReconciler validates Aura API credentials and reports their
// usability on status.conditions[Ready]. It creates no external resources; it
// exists so a bad credential is surfaced explicitly rather than only failing
// when an AuraInstance tries to use it.
type AuraProviderConfigReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	// ClientFactory builds the Aura API client; nil uses the real shared client.
	ClientFactory auraClientFactory
}

// Reconcile validates the referenced credentials by obtaining an access token
// and making one cheap API call, then re-checks periodically.
func (r *AuraProviderConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pc := &neo4jv1beta1.AuraProviderConfig{}
	if err := r.Get(ctx, req.NamespacedName, pc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, pc.Namespace, nil, &pc.Spec.CredentialsSecretRef)
	if err == nil {
		creds.baseURL = pc.Spec.BaseURL
		creds.projectID = pc.Spec.DefaultProjectID
		// Force a token exchange + a real call to prove the credentials work.
		_, err = resolveClient(r.ClientFactory, creds).ListInstances(ctx, creds.projectID)
	}

	ready := err == nil
	reason := "CredentialsValidated"
	msg := "Aura API credentials validated"
	if !ready {
		reason = "CredentialsInvalid"
		msg = "Aura API credentials invalid: " + err.Error()
		logger.Info("AuraProviderConfig validation failed", "error", err.Error())
		r.Recorder.Event(pc, corev1.EventTypeWarning, EventReasonAuraCredentialsInvalid, msg)
	} else {
		r.Recorder.Event(pc, corev1.EventTypeNormal, EventReasonAuraCredentialsValidated, msg)
	}

	if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraProviderConfig{}
		if getErr := r.Get(ctx, req.NamespacedName, latest); getErr != nil {
			return getErr
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             auraCondStatus(ready),
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: latest.Generation,
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}); updateErr != nil {
		return ctrl.Result{}, updateErr
	}

	requeue := r.RequeueAfter
	if requeue <= 0 {
		requeue = 10 * time.Minute
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// SetupWithManager wires the controller.
func (r *AuraProviderConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraProviderConfig{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
