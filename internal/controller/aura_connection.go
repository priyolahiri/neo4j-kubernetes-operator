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
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// connectionSecretName is the Secret the operator writes connection details to.
func (r *AuraInstanceReconciler) connectionSecretName(inst *neo4jv1beta1.AuraInstance) string {
	if inst.Spec.ConnectionSecretName != "" {
		return inst.Spec.ConnectionSecretName
	}
	return inst.Name + "-conn"
}

// reconcileConnectionOutputs writes the connection Secret (and, if requested,
// the non-secret ConfigMap). Exactly one of observed/created is non-nil: created
// carries the one-time username/password (create path); observed carries the
// steady-state details (its password is never returned, so an existing Secret's
// password is preserved).
func (r *AuraInstanceReconciler) reconcileConnectionOutputs(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, observed *aura.Instance, created *aura.CreateInstanceResponse,
) error {
	var uri, username, password, id, name string
	switch {
	case created != nil:
		uri, username, password, id, name = created.ConnectionURL, created.Username, created.Password, created.ID, created.Name
	case observed != nil:
		uri, id, name = observed.ConnectionURL, observed.ID, observed.Name
	default:
		return nil
	}

	secretName := r.connectionSecretName(inst)

	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: inst.Namespace}, existing)
	switch {
	case err == nil:
		// Preserve a previously-captured password (Aura never re-returns it).
		if password == "" {
			password = existingPassword(inst.Spec.ConnectionSecretFormat, existing.Data)
		}
		existing.Data = buildAuraConnectionData(inst.Spec.ConnectionSecretFormat, uri, username, password, id, name)
		if err := controllerutil.SetControllerReference(inst, existing, r.Scheme); err != nil {
			return err
		}
		if err := r.Update(ctx, existing); err != nil {
			return err
		}
	case apierrors.IsNotFound(err):
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: inst.Namespace,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "neo4j-operator"},
			},
			Type: corev1.SecretTypeOpaque,
			Data: buildAuraConnectionData(inst.Spec.ConnectionSecretFormat, uri, username, password, id, name),
		}
		if err := controllerutil.SetControllerReference(inst, sec, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	default:
		return err
	}

	if inst.Spec.PublishConnectionDetailsTo != "" {
		if err := r.reconcileConnectionConfigMap(ctx, inst, uri, id, name); err != nil {
			return err
		}
	}
	return nil
}

func (r *AuraInstanceReconciler) reconcileConnectionConfigMap(ctx context.Context, inst *neo4jv1beta1.AuraInstance, uri, id, name string) error {
	data := map[string]string{
		"NEO4J_URI":         uri,
		"AURA_INSTANCEID":   id,
		"AURA_INSTANCENAME": name,
		"region":            inst.Spec.Region,
		"type":              inst.Spec.Type,
	}
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: inst.Spec.PublishConnectionDetailsTo, Namespace: inst.Namespace}, cm)
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      inst.Spec.PublishConnectionDetailsTo,
				Namespace: inst.Namespace,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "neo4j-operator"},
			},
			Data: data,
		}
		if err := controllerutil.SetControllerReference(inst, cm, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cm)
	} else if err != nil {
		return err
	}
	cm.Data = data
	if err := controllerutil.SetControllerReference(inst, cm, r.Scheme); err != nil {
		return err
	}
	return r.Update(ctx, cm)
}

// existingPassword recovers the password key for the configured format.
func existingPassword(format string, data map[string][]byte) string {
	if format == "servicebinding" {
		return string(data["password"])
	}
	return string(data["NEO4J_PASSWORD"])
}

// buildAuraConnectionData renders the connection Secret keys for the chosen
// format. Every format carries the Service Binding "type"/"provider" keys so
// the Secret is a compliant Provisioned Service binding regardless of format.
func buildAuraConnectionData(format, uri, username, password, instanceID, instanceName string) map[string][]byte {
	if username == "" {
		username = "neo4j"
	}
	host, scheme, port := parseNeo4jURI(uri)
	data := map[string][]byte{}
	put := func(k, v string) {
		if v != "" {
			data[k] = []byte(v)
		}
	}
	// Minimal Service Binding compliance on every format.
	put("type", "neo4j")
	put("provider", "aura")

	switch format {
	case "servicebinding":
		put("uri", uri)
		put("username", username)
		put("password", password)
		put("database", "neo4j")
		put("host", host)
		put("port", port)
	default: // neo4j-driver (default), jdbc, aura-dotenv, custom
		put("NEO4J_URI", uri)
		put("NEO4J_USERNAME", username)
		put("NEO4J_PASSWORD", password)
		put("NEO4J_DATABASE", "neo4j")
		put("AURA_INSTANCEID", instanceID)
		put("AURA_INSTANCENAME", instanceName)
		if format == "jdbc" && uri != "" {
			put("NEO4J_JDBC_URL", "jdbc:neo4j:"+uri)
		}
		if format == "aura-dotenv" {
			dotenv := fmt.Sprintf(
				"NEO4J_URI=%s\nNEO4J_USERNAME=%s\nNEO4J_PASSWORD=%s\nNEO4J_DATABASE=neo4j\nAURA_INSTANCEID=%s\nAURA_INSTANCENAME=%s\n",
				uri, username, password, instanceID, instanceName)
			data["credentials.env"] = []byte(dotenv)
		}
	}
	_ = scheme // scheme is embedded in NEO4J_URI; retained for parseNeo4jURI clarity
	return data
}

// parseNeo4jURI splits a neo4j+s://host[:port] URI into host, scheme, port.
func parseNeo4jURI(uri string) (host, scheme, port string) {
	if uri == "" {
		return "", "", ""
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", ""
	}
	scheme = u.Scheme
	host = u.Hostname()
	port = u.Port()
	if port == "" {
		port = "7687" // Aura Bolt default
	}
	if host == "" {
		// Fall back to a naive split if url.Parse didn't populate host.
		trimmed := strings.TrimPrefix(uri, scheme+"://")
		host = strings.SplitN(trimmed, ":", 2)[0]
	}
	return host, scheme, port
}
