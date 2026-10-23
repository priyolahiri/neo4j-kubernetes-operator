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

package resources_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// pluginWithSource returns a minimal Neo4jPlugin spec'd for
// VerifiedDownload, with optional authSecret. Centralised so each
// test case stays focused on the one shape it's asserting.
func pluginWithSource(name, url, checksum, authSecret string) *neo4jv1beta1.Neo4jPlugin {
	return &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			Name:        name,
			Version:     "1.0.0",
			InstallMode: "VerifiedDownload",
			Source: &neo4jv1beta1.PluginSource{
				Type:       "url",
				URL:        url,
				Checksum:   checksum,
				AuthSecret: authSecret,
			},
		},
	}
}

// TestBuildPluginVerifiedDownloadInitContainer_DefaultImage locks in
// the baked-in default when the caller passes "" — air-gapped clusters
// override via the helm value but everyone else gets curlimages/curl.
func TestBuildPluginVerifiedDownloadInitContainer_DefaultImage(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://example.com/gds.jar", validSHA256, "")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", nil)

	assert.Equal(t, resources.PluginInitContainerName("gds"), c.Name)
	assert.Equal(t, resources.DefaultPluginInitContainerImage, c.Image,
		"empty image override must fall back to baked-in default")
}

// TestBuildPluginVerifiedDownloadInitContainer_ImageOverride confirms
// the chart override flows through. Without this the air-gap escape
// hatch from the helm value silently fails.
func TestBuildPluginVerifiedDownloadInitContainer_ImageOverride(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://example.com/gds.jar", validSHA256, "")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "internal.mirror/curl:8.5.0", nil)

	assert.Equal(t, "internal.mirror/curl:8.5.0", c.Image)
}

// TestBuildPluginVerifiedDownloadInitContainer_EnvVarsCarryCheckpoint
// verifies the four env vars the init script reads are present with
// the expected values. The script depends on this contract — break it
// and the script either no-ops or downloads the wrong thing.
func TestBuildPluginVerifiedDownloadInitContainer_EnvVarsCarryCheckpoint(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://example.com/gds.jar", validSHA256, "")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", nil)

	envMap := map[string]string{}
	for _, e := range c.Env {
		envMap[e.Name] = e.Value
	}
	assert.Equal(t, "gds", envMap["PLUGIN_NAME"])
	assert.Equal(t, "https://example.com/gds.jar", envMap["PLUGIN_URL"])
	assert.Equal(t, validSHA256, envMap["PLUGIN_CHECKSUM"])
	assert.Equal(t, "/plugins/gds.jar", envMap["PLUGIN_TARGET"])
}

// TestBuildPluginVerifiedDownloadInitContainer_NoAuthNoCA asserts the
// minimal-volume case: no authSecret, no trustedCASecrets. Only the
// shared /plugins emptyDir mount should appear. Anything more is
// scope creep that grows the pod spec for no reason.
func TestBuildPluginVerifiedDownloadInitContainer_NoAuthNoCA(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://example.com/gds.jar", validSHA256, "")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", nil)

	require.Len(t, c.VolumeMounts, 1, "no auth + no CA must produce only the /plugins mount")
	assert.Equal(t, "/plugins", c.VolumeMounts[0].MountPath)
	// Script must not reference the auth/ca branches.
	assert.NotContains(t, c.Args[1], "AUTH_HEADER", "auth branch must be absent without authSecret")
	assert.NotContains(t, c.Args[1], "ca-bundle.crt", "CA branch must be absent without trustedCASecrets")
}

// TestBuildPluginVerifiedDownloadInitContainer_WithAuth verifies the
// authSecret volume mount + the script's auth-header branch land
// together. Half-wiring (mount without script, or script without
// mount) would silently fall back to anonymous downloads.
func TestBuildPluginVerifiedDownloadInitContainer_WithAuth(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://artifactory.example.com/gds.jar", validSHA256, "artifactory-creds")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", nil)

	var foundAuth bool
	for _, vm := range c.VolumeMounts {
		if vm.MountPath == "/etc/plugin-auth" {
			foundAuth = true
		}
	}
	assert.True(t, foundAuth, "authSecret must produce an /etc/plugin-auth mount")
	script := c.Args[1]
	assert.Contains(t, script, "/etc/plugin-auth/header")
	assert.Contains(t, script, "/etc/plugin-auth/token")
	assert.Contains(t, script, "Bearer")
}

// TestBuildPluginVerifiedDownloadInitContainer_WithCA verifies that
// when trustedCASecrets are supplied, the script concatenates them
// and points curl at the bundle. This is the path that lets internal
// Artifactory behind a corporate CA work without manual init-image
// customisation.
func TestBuildPluginVerifiedDownloadInitContainer_WithCA(t *testing.T) {
	const validSHA256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	plugin := pluginWithSource("gds", "https://internal/gds.jar", validSHA256, "")
	caSecrets := []neo4jv1beta1.TrustedCASecret{
		{Name: "corp-root-ca"},
		{Name: "intermediate-ca", Key: "tls.crt"},
	}

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", caSecrets)

	var foundCA bool
	for _, vm := range c.VolumeMounts {
		if vm.MountPath == "/etc/plugin-ca" {
			foundCA = true
		}
	}
	assert.True(t, foundCA, "trustedCASecrets must produce an /etc/plugin-ca mount")
	script := c.Args[1]
	assert.Contains(t, script, "/etc/plugin-ca/*.crt")
	assert.Contains(t, script, "--cacert")
	assert.Contains(t, script, "ca-bundle.crt")
}

// TestBuildPluginCAVolume_PerSecretKey asserts the projected volume
// honours per-Secret Key overrides (default "ca.crt"). cert-manager-
// issued Secrets use that exact key, so the default works; custom
// providers (Vault PKI, etc.) often emit tls.crt instead.
func TestBuildPluginCAVolume_PerSecretKey(t *testing.T) {
	caSecrets := []neo4jv1beta1.TrustedCASecret{
		{Name: "corp-root-ca"},                    // implicit ca.crt
		{Name: "intermediate-ca", Key: "tls.crt"}, // explicit override
	}
	vol := resources.BuildPluginCAVolume(caSecrets)

	require.NotNil(t, vol)
	require.NotNil(t, vol.Projected)
	require.Len(t, vol.Projected.Sources, 2)

	require.Len(t, vol.Projected.Sources[0].Secret.Items, 1)
	assert.Equal(t, "ca.crt", vol.Projected.Sources[0].Secret.Items[0].Key,
		"missing Key on TrustedCASecret must default to ca.crt")
	assert.Equal(t, "corp-root-ca.crt", vol.Projected.Sources[0].Secret.Items[0].Path)

	require.Len(t, vol.Projected.Sources[1].Secret.Items, 1)
	assert.Equal(t, "tls.crt", vol.Projected.Sources[1].Secret.Items[0].Key)
	assert.Equal(t, "intermediate-ca.crt", vol.Projected.Sources[1].Secret.Items[0].Path)
}

// The plugin controller updates the CA volume one write after the target's
// own controller drops a removed CA, and only once the target is Ready. A
// projection of a Secret the user deleted in between must not keep the
// restarting servers from starting, so every projection is optional.
func TestBuildPluginCAVolume_ProjectionsAreOptional(t *testing.T) {
	vol := resources.BuildPluginCAVolume([]neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}, {Name: "mirror-ca"}})
	require.NotNil(t, vol)
	for _, s := range vol.Projected.Sources {
		require.NotNil(t, s.Secret.Optional, s.Secret.Name)
		assert.True(t, *s.Secret.Optional, s.Secret.Name)
	}
}

// defaultCABundles are the paths the init script looks for the image's default
// CA bundle at, after $CURL_CA_BUNDLE.
var defaultCABundles = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/cert.pem",
	"/cacert.pem",
}

// initImage is what the init image holds, for runPluginInit: curlCABundle is
// $CURL_CA_BUNDLE ("" leaves it unset), and files maps a default bundle path
// to its content. Every other default path does not exist, whatever the host
// running the test has there.
type initImage struct {
	curlCABundle string
	files        map[string]string
}

// runPluginInit runs the init container's script with /bin/sh against a fake
// curl that records its arguments and writes a JAR with known content: the CA
// directory is caDir, the image's default CAs what image says. It returns the
// script's output, the curl arguments and the CA bundle the script built (""
// when none).
func runPluginInit(t *testing.T, caDir string, withCA bool, image ...initImage) (string, string, string) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	if _, err := exec.LookPath("sha256sum"); err != nil {
		t.Skip("sha256sum not available")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o755))
	// The fake curl: its arguments go to curl.args, "jar" to the -o target.
	fakeCurl := `#!/bin/sh
echo "$@" > "` + dir + `/curl.args"
while [ $# -gt 0 ]; do
  if [ "$1" = "-o" ]; then printf jar > "$2"; fi
  shift
done
`
	require.NoError(t, os.WriteFile(filepath.Join(bin, "curl"), []byte(fakeCurl), 0o755))
	sum := sha256.Sum256([]byte("jar"))
	plugin := pluginWithSource("custom", "https://mirror.example.com/custom.jar", "sha256:"+hex.EncodeToString(sum[:]), "")
	var cas []neo4jv1beta1.TrustedCASecret
	if withCA {
		cas = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
	}
	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", cas)
	var img initImage
	if len(image) > 0 {
		img = image[0]
	}
	// The image's file system: each default bundle path moves under dir, and
	// exists there only when the image has it.
	replace := []string{"/etc/plugin-ca", caDir, "/tmp/plugin-init", filepath.Join(dir, "work")}
	imageRoot := filepath.Join(dir, "image")
	for _, p := range defaultCABundles {
		replace = append(replace, p, filepath.Join(imageRoot, p))
	}
	for p, content := range img.files {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(imageRoot, p)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(imageRoot, p), []byte(content), 0o600))
	}
	script := strings.NewReplacer(replace...).Replace(c.Args[1])
	cmd := exec.CommandContext(t.Context(), "sh", "-c", script)
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "CURL_CA_BUNDLE=") {
			cmd.Env = append(cmd.Env, e)
		}
	}
	cmd.Env = append(cmd.Env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if img.curlCABundle != "" {
		cmd.Env = append(cmd.Env, "CURL_CA_BUNDLE="+filepath.Join(imageRoot, img.curlCABundle))
	}
	for _, e := range c.Env {
		v := e.Value
		if e.Name == "PLUGIN_TARGET" {
			v = filepath.Join(dir, "custom.jar")
		}
		cmd.Env = append(cmd.Env, e.Name+"="+v)
	}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	args, err := os.ReadFile(filepath.Join(dir, "curl.args"))
	require.NoError(t, err)
	bundle, _ := os.ReadFile(filepath.Join(dir, "work", "ca-bundle.crt"))
	return string(out), string(args), string(bundle)
}

// The CA directory can be empty: every Secret the volume still projects was
// deleted after it left spec.trustedCASecrets, before the plugin controller
// updated the volume. The download then uses the default CAs, as it will once
// the volume is updated, instead of failing the pod on an unmatched glob.
func TestPluginInitScript_TrustedCAs(t *testing.T) {
	t.Run("the CAs present are bundled", func(t *testing.T) {
		caDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(caDir, "corp-ca.crt"), []byte("CORP\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(caDir, "mirror-ca.crt"), []byte("MIRROR\n"), 0o600))
		out, args, bundle := runPluginInit(t, caDir, true)
		assert.Contains(t, out, "trusting ONLY the custom CAs")
		assert.Contains(t, args, "--cacert")
		assert.Equal(t, "CORP\n\nMIRROR\n\n", bundle)
	})
	t.Run("no CA left", func(t *testing.T) {
		out, args, bundle := runPluginInit(t, t.TempDir(), true)
		assert.Contains(t, out, "no CA certificate")
		assert.Contains(t, out, "verified custom", "the download goes ahead and is verified")
		assert.NotContains(t, args, "--cacert")
		assert.Empty(t, bundle)
	})
	t.Run("no CA listed", func(t *testing.T) {
		out, args, _ := runPluginInit(t, t.TempDir(), false)
		assert.NotContains(t, out, "CA bundle")
		assert.NotContains(t, out, "CA certificate")
		assert.NotContains(t, args, "--cacert")
	})
}

// The defect (v1.22.0 journey): the bundle held the custom CAs alone, and
// --cacert is curl's only trust store, so a target with any trustedCASecrets
// could not download a plugin from a public URL (GitHub: curl exit 60) and
// its restarting server crash-looped in the init container. Custom CAs now
// extend the image's defaults, found where the configurable init image keeps
// them.
func TestPluginInitScript_CustomCAsExtendTheDefaults(t *testing.T) {
	caDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(caDir, "corp-ca.crt"), []byte("CORP\n"), 0o600))
	for _, tc := range []struct {
		name   string
		image  initImage
		used   string
		bundle string
	}{
		{"curlimages/curl: CURL_CA_BUNDLE first", initImage{curlCABundle: "/cacert.pem", files: map[string]string{
			"/cacert.pem": "MOZILLA\n", "/etc/ssl/certs/ca-certificates.crt": "ALPINE\n",
		}}, "/cacert.pem", "MOZILLA\n\nCORP\n\n"},
		{"CURL_CA_BUNDLE naming no file", initImage{curlCABundle: "/missing.pem", files: map[string]string{
			"/etc/pki/tls/certs/ca-bundle.crt": "RHEL\n",
		}}, "/etc/pki/tls/certs/ca-bundle.crt", "RHEL\n\nCORP\n\n"},
		{"Debian and Alpine before /etc/ssl/cert.pem", initImage{files: map[string]string{
			"/etc/ssl/cert.pem": "OPENSSL\n", "/etc/ssl/certs/ca-certificates.crt": "DEBIAN\n",
		}}, "/etc/ssl/certs/ca-certificates.crt", "DEBIAN\n\nCORP\n\n"},
		{"only cert.pem", initImage{files: map[string]string{"/etc/ssl/cert.pem": "OPENSSL\n"}},
			"/etc/ssl/cert.pem", "OPENSSL\n\nCORP\n\n"},
		{"an empty bundle is no bundle", initImage{files: map[string]string{
			"/etc/ssl/certs/ca-certificates.crt": "", "/cacert.pem": "MOZILLA\n",
		}}, "/cacert.pem", "MOZILLA\n\nCORP\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, args, bundle := runPluginInit(t, caDir, true, tc.image)
			assert.Contains(t, out, "trusting the default CAs (")
			assert.Contains(t, out, filepath.FromSlash(tc.used)+") and the custom CAs")
			assert.NotContains(t, out, "WARNING")
			assert.Contains(t, args, "--cacert")
			assert.Equal(t, tc.bundle, bundle, "the defaults, then the custom CAs")
		})
	}

	t.Run("PEM blocks never share a line", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "a.crt"), []byte("-----END CERTIFICATE-----"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "b.crt"), []byte("-----BEGIN CERTIFICATE-----\n"), 0o600))
		_, _, bundle := runPluginInit(t, dir, true, initImage{files: map[string]string{"/cacert.pem": "-----END CERTIFICATE-----"}})
		assert.Equal(t, "-----END CERTIFICATE-----\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\n\n", bundle)
	})
}

// TestPluginInitScript_ChecksumAlgoBranching verifies the script
// dispatches to sha256sum vs sha512sum based on the checksum prefix.
// Locking this in via test means a future refactor of the script
// can't silently fall back to sha256 for a sha512:-prefixed value
// (which would still pass the validator but mismatch every time).
func TestPluginInitScript_ChecksumAlgoBranching(t *testing.T) {
	plugin := pluginWithSource("gds", "https://x/y.jar",
		"sha512:cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e", "")

	c := resources.BuildPluginVerifiedDownloadInitContainer(plugin, "", nil)
	script := c.Args[1]

	// Script must reference both algos in the case-statement (sha256
	// for backwards compat with most published checksums, sha512 for
	// the user's choice). We don't test against a downloaded file —
	// that's a live integration test, not a unit test. Lock in the
	// case statement instead.
	assert.True(t,
		strings.Contains(script, "sha256:*)") && strings.Contains(script, "sha512:*)"),
		"script must dispatch on both sha256: and sha512: prefixes")
	assert.Contains(t, script, "sha256sum")
	assert.Contains(t, script, "sha512sum")
}
