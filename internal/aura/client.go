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

package aura

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// DefaultBaseURL is the Aura Platform API v1 base URL.
	DefaultBaseURL = "https://api.neo4j.io/v1"
	// DefaultTokenURL is the OAuth2 client-credentials token endpoint.
	DefaultTokenURL = "https://api.neo4j.io/oauth/token"
	// DefaultPerMinute is the conservative per-credential rate limit (the Aura
	// API allows 25 or 125 requests/minute depending on the credential tier).
	DefaultPerMinute = 25
	// defaultHTTPTimeout bounds each individual HTTP request.
	defaultHTTPTimeout = 30 * time.Second
	// tokenExpirySkew is subtracted from the token's stated lifetime so we
	// refresh slightly early rather than racing the server's expiry.
	tokenExpirySkew = 60 * time.Second
)

// Config configures a Client.
type Config struct {
	// BaseURL is the Aura API base URL. Defaults to DefaultBaseURL if empty.
	BaseURL string
	// TokenURL is the OAuth2 token endpoint. Defaults to DefaultTokenURL if empty.
	TokenURL string
	// ClientID is the OAuth2 client-credentials client id.
	ClientID string
	// ClientSecret is the OAuth2 client-credentials client secret.
	ClientSecret string
	// PerMinute is the per-credential request rate limit (e.g. 25 or 125).
	// Defaults to DefaultPerMinute if <= 0.
	PerMinute int
	// HTTPClient is the HTTP client used for all requests. Defaults to a client
	// with a sane timeout if nil.
	HTTPClient *http.Client
	// Observer, if set, is invoked once per logical API call (after any 403
	// token-refresh retry) with a low-cardinality route label (e.g.
	// "POST /instances/{id}/upgrade"), the call's wall-clock duration, and its
	// error (nil on a 2xx). It lets the operator feed Prometheus metrics without
	// coupling this package to a metrics library.
	Observer func(operation string, duration time.Duration, err error)
}

// Client is a rate-limited, self-authenticating HTTP client for the Aura API v1.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	baseURL      string
	tokenURL     string
	clientID     string
	clientSecret string

	httpClient *http.Client
	limiter    *rate.Limiter
	observer   func(operation string, duration time.Duration, err error)

	// token cache, guarded by tokenMu.
	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// NewClient constructs a Client from cfg, applying defaults for any unset field.
func NewClient(cfg Config) *Client {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	tokenURL := cfg.TokenURL
	if tokenURL == "" {
		tokenURL = DefaultTokenURL
	}
	perMinute := cfg.PerMinute
	if perMinute <= 0 {
		perMinute = DefaultPerMinute
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	// Convert requests/minute into a per-second rate with a burst of 1 so that
	// Wait() paces calls evenly across the minute.
	limit := rate.Limit(float64(perMinute) / 60.0)

	return &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		tokenURL:     tokenURL,
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		httpClient:   httpClient,
		limiter:      rate.NewLimiter(limit, 1),
		observer:     cfg.Observer,
		now:          time.Now,
	}
}

// tokenResponse models the OAuth2 client-credentials token response.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// getToken returns a valid bearer token, fetching a new one if the cache is
// empty or expired. If forceRefresh is true the cached token is discarded and a
// fresh one is fetched unconditionally (used after a 403).
func (c *Client) getToken(ctx context.Context, forceRefresh bool) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if !forceRefresh && c.accessToken != "" && c.now().Before(c.tokenExpiry) {
		return c.accessToken, nil
	}

	tok, err := c.fetchToken(ctx)
	if err != nil {
		return "", err
	}
	c.accessToken = tok.AccessToken
	// Refresh a little before the stated expiry to avoid racing the server.
	lifetime := time.Duration(tok.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		// Degenerate/short lifetime — keep it but don't go negative.
		c.tokenExpiry = c.now().Add(lifetime)
	} else {
		c.tokenExpiry = c.now().Add(lifetime - tokenExpirySkew)
	}
	return c.accessToken, nil
}

// fetchToken performs the OAuth2 client-credentials exchange: HTTP Basic auth
// with clientId:clientSecret and an x-www-form-urlencoded body of
// grant_type=client_credentials.
func (c *Client) fetchToken(ctx context.Context) (*tokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL,
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building token request: %w", err)
	}
	req.SetBasicAuth(c.clientID, c.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading token response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newAPIError(resp.StatusCode, resp.Header.Get("X-Request-Id"), body)
	}

	var tok tokenResponse
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("token response contained no access_token")
	}
	return &tok, nil
}

// doJSON issues a rate-limited, authenticated JSON request to path (relative to
// the base URL). The optional body is JSON-encoded; if out is non-nil the 2xx
// response body is decoded into it. On a non-2xx it returns an *APIError parsed
// from the body. A 403 (Aura's signal for an expired token) triggers a single
// token refresh + retry.
func (c *Client) doJSON(ctx context.Context, method, path string, body, out any) error {
	start := time.Now()
	err := c.doJSONOnce(ctx, method, path, body, out, false)
	if IsForbidden(err) {
		// Aura returns 403 (not 401) for an expired/revoked token. Force a
		// fresh token and retry the request exactly once.
		err = c.doJSONOnce(ctx, method, path, body, out, true)
	}
	if c.observer != nil {
		c.observer(method+" "+normalizeAuraPath(path), time.Since(start), err)
	}
	return err
}

// normalizeAuraPath collapses a concrete request path into a low-cardinality
// route template so it can safely label a metric. The Aura routes alternate
// literal / identifier segments (collection, {id}, action, {id}, ...), so every
// odd-indexed segment is a resource ID and is replaced with "{id}"; the query
// string is dropped.
func normalizeAuraPath(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i := 1; i < len(segs); i += 2 {
		if segs[i] != "" {
			segs[i] = "{id}"
		}
	}
	return "/" + strings.Join(segs, "/")
}

// doJSONOnce performs a single attempt of doJSON. forceRefresh forces a fresh
// bearer token before the request.
func (c *Client) doJSONOnce(ctx context.Context, method, path string, body, out any, forceRefresh bool) error {
	// Rate-limit before every request (blocks until a token is available or ctx
	// is cancelled).
	if err := c.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	fullURL := c.baseURL + path
	if !strings.HasPrefix(fullURL, "https://") {
		return fmt.Errorf("refusing non-HTTPS request URL: %s", fullURL)
	}

	var bodyReader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		bodyReader = bytes.NewReader(encoded)
	}

	token, err := c.getToken(ctx, forceRefresh)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("performing %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newAPIError(resp.StatusCode, resp.Header.Get("X-Request-Id"), respBody)
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decoding response body: %w", err)
		}
	}
	return nil
}

// dataEnvelope is the standard Aura response wrapper: every payload is nested
// under a top-level "data" key.
type dataEnvelope[T any] struct {
	Data T `json:"data"`
}
