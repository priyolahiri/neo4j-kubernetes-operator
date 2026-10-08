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

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// deleteIssuedTLSSecret deletes a deleted deployment's {name}-tls-secret when
// cert-manager issued it for that deployment's Certificate — {name}-tls for a
// cluster, {name}-tls-cert for a standalone (#479).
//
// The operator owns the Certificate, so it is garbage-collected with the
// deployment, but cert-manager leaves the Secrets it issues behind unless it
// runs with --enable-certificate-owner-ref (default false) — a certificate and
// private key with no owner. Only a Secret carrying cert-manager's
// certificate-name annotation for this Certificate is deleted; one the user
// created, or one issued for another Certificate, is left alone. Failure is
// logged and returned for the caller to decide; it never needs to block
// deletion.
func deleteIssuedTLSSecret(ctx context.Context, c client.Client, namespace, name, certificate string) error {
	logger := log.FromContext(ctx)
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: name + "-tls-secret"}
	if err := c.Get(ctx, key, secret); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if secret.Annotations[certv1.CertificateNameKey] != certificate {
		logger.Info("Leaving TLS Secret in place: cert-manager did not issue it for this deployment's Certificate",
			"secret", key.Name, "certificate", secret.Annotations[certv1.CertificateNameKey])
		return nil
	}
	if err := c.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) {
		return err
	}
	logger.Info("Deleted the TLS Secret cert-manager issued for this deployment", "secret", key.Name)
	return nil
}
