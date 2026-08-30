// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// substrate-release-verify performs the read-only, in-cluster checks used by
// the kagent/Substrate testbed release. It intentionally dials the internal
// Control API; validating the public Broker remains an external-host smoke test
// and does not depend on load-balancer hairpinning from a cluster Pod.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	defaultKubernetesAPIURL = "https://kubernetes.default.svc"
	defaultTimeout          = 45 * time.Second
	maximumTimeout          = 2 * time.Minute
	operationTimeout        = 10 * time.Second
	maximumSecretBytes      = 64 << 10
	maximumCABundleBytes    = 1 << 20
	maximumResponseBytes    = 2 << 20
)

type config struct {
	kagentHealthURL          string
	substrateEndpoint        string
	substrateCAFile          string
	substrateServerName      string
	tokenFile                string
	expectedAudience         string
	kubernetesAPIURL         string
	kubernetesTokenFile      string
	kubernetesCAFile         string
	gatewayNamespace         string
	gatewayName              string
	tlsRouteName             string
	requireGatewayProgrammed bool
	timeout                  time.Duration
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "substrate-release-verify: configuration error: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if err := verify(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "substrate-release-verify: verification failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("substrate-release-verify: verification succeeded")
}

func parseConfig(args []string) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("substrate-release-verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.kagentHealthURL, "kagent-health-url", "", "Internal kagent health URL (required).")
	flags.StringVar(&cfg.substrateEndpoint, "substrate-endpoint", "", "Internal Substrate Control API host:port (required).")
	flags.StringVar(&cfg.substrateCAFile, "substrate-ca-file", "", "PEM CA bundle for the internal Substrate endpoint (required).")
	flags.StringVar(&cfg.substrateServerName, "substrate-server-name", "", "Exact DNS name verified on the Substrate server certificate (required).")
	flags.StringVar(&cfg.tokenFile, "token-file", "", "Projected Substrate-audience ServiceAccount token file (required).")
	flags.StringVar(&cfg.expectedAudience, "expected-audience", "", "Only audience accepted in the projected Substrate token (required).")
	flags.StringVar(&cfg.kubernetesAPIURL, "kubernetes-api-url", defaultKubernetesAPIURL, "Kubernetes API base URL.")
	flags.StringVar(&cfg.kubernetesTokenFile, "kubernetes-token-file", "", "Separate projected Kubernetes API token file (required).")
	flags.StringVar(&cfg.kubernetesCAFile, "kubernetes-ca-file", "", "PEM CA bundle for the Kubernetes API (required).")
	flags.StringVar(&cfg.gatewayNamespace, "gateway-namespace", "", "Namespace containing the Gateway and TLSRoute (required).")
	flags.StringVar(&cfg.gatewayName, "gateway-name", "", "Gateway name (required).")
	flags.StringVar(&cfg.tlsRouteName, "tls-route-name", "", "TLSRoute name (required).")
	flags.BoolVar(&cfg.requireGatewayProgrammed, "require-gateway-programmed", false, "Require a current Programmed=True Gateway condition (required).")
	flags.DurationVar(&cfg.timeout, "timeout", defaultTimeout, "Overall verification timeout (1s to 2m).")
	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments")
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	required := []struct {
		name  string
		value string
	}{
		{"--kagent-health-url", cfg.kagentHealthURL},
		{"--substrate-endpoint", cfg.substrateEndpoint},
		{"--substrate-ca-file", cfg.substrateCAFile},
		{"--substrate-server-name", cfg.substrateServerName},
		{"--token-file", cfg.tokenFile},
		{"--expected-audience", cfg.expectedAudience},
		{"--kubernetes-api-url", cfg.kubernetesAPIURL},
		{"--kubernetes-token-file", cfg.kubernetesTokenFile},
		{"--kubernetes-ca-file", cfg.kubernetesCAFile},
		{"--gateway-namespace", cfg.gatewayNamespace},
		{"--gateway-name", cfg.gatewayName},
		{"--tls-route-name", cfg.tlsRouteName},
	}
	for _, field := range required {
		if field.value == "" || strings.TrimSpace(field.value) != field.value {
			return fmt.Errorf("%s is required and must not contain surrounding whitespace", field.name)
		}
	}
	if !cfg.requireGatewayProgrammed {
		return fmt.Errorf("--require-gateway-programmed must be set")
	}
	if cfg.timeout < time.Second || cfg.timeout > maximumTimeout {
		return fmt.Errorf("--timeout must be between 1s and 2m")
	}
	if err := validateHealthURL(cfg.kagentHealthURL); err != nil {
		return fmt.Errorf("--kagent-health-url: %w", err)
	}
	if err := validateKubernetesAPIURL(cfg.kubernetesAPIURL); err != nil {
		return fmt.Errorf("--kubernetes-api-url: %w", err)
	}
	host, port, err := net.SplitHostPort(cfg.substrateEndpoint)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("--substrate-endpoint must be an explicit host:port")
	}
	if net.ParseIP(host) == nil && !isDNS1123Subdomain(host) {
		return fmt.Errorf("--substrate-endpoint host must be a DNS name or IP address")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("--substrate-endpoint must use a numeric port from 1 to 65535")
	}
	if !isDNS1123Subdomain(cfg.substrateServerName) {
		return fmt.Errorf("--substrate-server-name must be a DNS-1123 name without a scheme or port")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"--gateway-namespace", cfg.gatewayNamespace},
		{"--gateway-name", cfg.gatewayName},
		{"--tls-route-name", cfg.tlsRouteName},
	} {
		if !isDNS1123Subdomain(field.value) {
			return fmt.Errorf("%s must be a DNS-1123 subdomain", field.name)
		}
	}
	return nil
}

func validateHealthURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("must be an absolute HTTP or HTTPS URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("userinfo and fragments are forbidden")
	}
	return nil
}

func validateKubernetesAPIURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL")
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("must be an absolute HTTPS URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return fmt.Errorf("userinfo, path, query, and fragment are forbidden")
	}
	return nil
}

func isDNS1123Subdomain(value string) bool {
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func verify(ctx context.Context, cfg config) error {
	if err := checkKagentHealth(ctx, cfg.kagentHealthURL); err != nil {
		return err
	}

	substrateToken, err := readBoundedFile(cfg.tokenFile, maximumSecretBytes, "Substrate token")
	if err != nil {
		return err
	}
	if err := requireExactJWTAudience(string(substrateToken), cfg.expectedAudience); err != nil {
		return fmt.Errorf("validate projected Substrate token: %w", err)
	}
	if err := checkSubstrate(ctx, cfg, strings.TrimSpace(string(substrateToken))); err != nil {
		return err
	}

	kubernetesToken, err := readBoundedFile(cfg.kubernetesTokenFile, maximumSecretBytes, "Kubernetes token")
	if err != nil {
		return err
	}
	kubernetesTokenString := strings.TrimSpace(string(kubernetesToken))
	if kubernetesTokenString == "" || strings.ContainsAny(kubernetesTokenString, " \t\r\n") {
		return fmt.Errorf("kubernetes token is empty or malformed")
	}
	client, err := newKubernetesClient(cfg.kubernetesCAFile)
	if err != nil {
		return err
	}
	if err := checkGatewayAPI(ctx, client, cfg, kubernetesTokenString); err != nil {
		return err
	}
	return nil
}

func checkKagentHealth(ctx context.Context, healthURL string) error {
	requestCtx, cancel := boundedOperationContext(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("create kagent health request")
	}
	client := &http.Client{
		Timeout: operationTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("kagent health request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("kagent health returned non-success status %d", resp.StatusCode)
	}
	return nil
}

func checkSubstrate(ctx context.Context, cfg config, token string) error {
	pool, err := loadCertPool(cfg.substrateCAFile, "Substrate CA")
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: cfg.substrateServerName,
	}
	conn, err := grpc.NewClient(
		cfg.substrateEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithPerRPCCredentials(bearerTokenCredentials(token)),
	)
	if err != nil {
		return fmt.Errorf("create Substrate Control client")
	}
	defer conn.Close()

	requestCtx, cancel := boundedOperationContext(ctx)
	defer cancel()
	_, err = ateapipb.NewControlClient(conn).ListActorTemplates(requestCtx, &ateapipb.ListActorTemplatesRequest{PageSize: 1})
	if err != nil {
		// gRPC status details are server-controlled and may accidentally echo a
		// credential. Report only the canonical code.
		return fmt.Errorf("Control.ListActorTemplates failed with gRPC code %s", status.Code(err))
	}
	return nil
}

type bearerTokenCredentials string

func (c bearerTokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if c == "" {
		return nil, fmt.Errorf("bearer token is empty")
	}
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (bearerTokenCredentials) RequireTransportSecurity() bool { return true }

func newKubernetesClient(caFile string) (*http.Client, error) {
	pool, err := loadCertPool(caFile, "Kubernetes CA")
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   operationTimeout,
			KeepAlive: operationTimeout,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
		},
		TLSHandshakeTimeout: operationTimeout,
		IdleConnTimeout:     operationTimeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   operationTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

type metadata struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	Generation int64  `json:"generation"`
}

type condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

type gatewayResource struct {
	Metadata metadata `json:"metadata"`
	Status   struct {
		Conditions []condition `json:"conditions"`
	} `json:"status"`
}

type parentReference struct {
	Group     *string `json:"group"`
	Kind      *string `json:"kind"`
	Namespace *string `json:"namespace"`
	Name      string  `json:"name"`
}

type routeParentStatus struct {
	ParentRef  parentReference `json:"parentRef"`
	Conditions []condition     `json:"conditions"`
}

type tlsRouteResource struct {
	Metadata metadata `json:"metadata"`
	Status   struct {
		Parents []routeParentStatus `json:"parents"`
	} `json:"status"`
}

func checkGatewayAPI(ctx context.Context, client *http.Client, cfg config, token string) error {
	base := strings.TrimSuffix(cfg.kubernetesAPIURL, "/")
	gatewayPath := fmt.Sprintf(
		"%s/apis/gateway.networking.k8s.io/v1/namespaces/%s/gateways/%s",
		base,
		url.PathEscape(cfg.gatewayNamespace),
		url.PathEscape(cfg.gatewayName),
	)
	var gateway gatewayResource
	if err := getKubernetesResource(ctx, client, gatewayPath, token, &gateway); err != nil {
		return fmt.Errorf("read Gateway: %w", err)
	}
	if err := validateGateway(gateway, cfg.gatewayNamespace, cfg.gatewayName); err != nil {
		return err
	}

	tlsRoutePath := fmt.Sprintf(
		"%s/apis/gateway.networking.k8s.io/v1/namespaces/%s/tlsroutes/%s",
		base,
		url.PathEscape(cfg.gatewayNamespace),
		url.PathEscape(cfg.tlsRouteName),
	)
	var tlsRoute tlsRouteResource
	if err := getKubernetesResource(ctx, client, tlsRoutePath, token, &tlsRoute); err != nil {
		return fmt.Errorf("read TLSRoute: %w", err)
	}
	if err := validateTLSRoute(tlsRoute, cfg.gatewayNamespace, cfg.tlsRouteName, cfg.gatewayName); err != nil {
		return err
	}
	return nil
}

func getKubernetesResource(ctx context.Context, client *http.Client, resourceURL, token string, target any) error {
	requestCtx, cancel := boundedOperationContext(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, resourceURL, nil)
	if err != nil {
		return fmt.Errorf("create request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("kubernetes API returned non-success status %d", resp.StatusCode)
	}
	limited := io.LimitReader(resp.Body, maximumResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read response")
	}
	if len(body) > maximumResponseBytes {
		return fmt.Errorf("kubernetes API response exceeds %d bytes", maximumResponseBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode Kubernetes API response")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode Kubernetes API response")
	}
	return nil
}

func validateGateway(gateway gatewayResource, namespace, name string) error {
	if gateway.Metadata.Name != name || gateway.Metadata.Namespace != namespace {
		return fmt.Errorf("gateway response identity does not match the requested object")
	}
	if gateway.Metadata.Generation < 1 {
		return fmt.Errorf("gateway metadata.generation is missing")
	}
	if !hasCurrentTrueCondition(gateway.Status.Conditions, "Programmed", gateway.Metadata.Generation) {
		return fmt.Errorf("gateway does not have current Programmed=True")
	}
	return nil
}

func validateTLSRoute(route tlsRouteResource, namespace, routeName, gatewayName string) error {
	if route.Metadata.Name != routeName || route.Metadata.Namespace != namespace {
		return fmt.Errorf("TLSRoute response identity does not match the requested object")
	}
	if route.Metadata.Generation < 1 {
		return fmt.Errorf("TLSRoute metadata.generation is missing")
	}
	for _, parent := range route.Status.Parents {
		if !parentRefMatchesGateway(parent.ParentRef, namespace, gatewayName) {
			continue
		}
		if hasCurrentTrueCondition(parent.Conditions, "Accepted", route.Metadata.Generation) &&
			hasCurrentTrueCondition(parent.Conditions, "ResolvedRefs", route.Metadata.Generation) {
			return nil
		}
	}
	return fmt.Errorf("TLSRoute has no current Accepted=True and ResolvedRefs=True status for the named Gateway")
}

func parentRefMatchesGateway(ref parentReference, namespace, name string) bool {
	group := "gateway.networking.k8s.io"
	if ref.Group != nil {
		group = *ref.Group
	}
	kind := "Gateway"
	if ref.Kind != nil {
		kind = *ref.Kind
	}
	parentNamespace := namespace
	if ref.Namespace != nil {
		parentNamespace = *ref.Namespace
	}
	return group == "gateway.networking.k8s.io" && kind == "Gateway" && parentNamespace == namespace && ref.Name == name
}

func hasCurrentTrueCondition(conditions []condition, conditionType string, generation int64) bool {
	for _, current := range conditions {
		if current.Type == conditionType && current.Status == "True" && current.ObservedGeneration == generation {
			return true
		}
	}
	return false
}

func requireExactJWTAudience(rawToken, expected string) error {
	token := strings.TrimSpace(rawToken)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("JWT is empty or contains whitespace")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return fmt.Errorf("JWT must contain exactly three non-empty segments")
	}
	decoded := make([][]byte, len(parts))
	for i, part := range parts {
		if strings.Contains(part, "=") {
			return fmt.Errorf("JWT segments must use unpadded base64url")
		}
		value, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil {
			return fmt.Errorf("JWT segment %d is not strict base64url", i+1)
		}
		decoded[i] = value
	}
	header, err := decodeStrictJSONObject(decoded[0])
	if err != nil {
		return fmt.Errorf("JWT header is invalid")
	}
	algorithm, ok := header["alg"].(string)
	if !ok || algorithm == "" || strings.EqualFold(algorithm, "none") {
		return fmt.Errorf("JWT header must declare a signing algorithm")
	}
	payload, err := decodeStrictJSONObject(decoded[1])
	if err != nil {
		return fmt.Errorf("JWT payload is invalid")
	}
	audience, ok := payload["aud"]
	if !ok {
		return fmt.Errorf("JWT audience is missing")
	}
	switch typed := audience.(type) {
	case string:
		if typed != expected {
			return fmt.Errorf("JWT audience does not exactly match the expected audience")
		}
	case []any:
		if len(typed) != 1 {
			return fmt.Errorf("JWT audience must contain exactly one value")
		}
		value, ok := typed[0].(string)
		if !ok || value != expected {
			return fmt.Errorf("JWT audience does not exactly match the expected audience")
		}
	default:
		return fmt.Errorf("JWT audience has an invalid type")
	}
	return nil
}

func decodeStrictJSONObject(raw []byte) (map[string]any, error) {
	if len(raw) == 0 || len(raw) > maximumSecretBytes {
		return nil, fmt.Errorf("JSON object has an invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeStrictJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("top-level JSON value is not an object")
	}
	return object, nil
}

func decodeStrictJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("JSON object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object key")
			}
			value, err := decodeStrictJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return nil, fmt.Errorf("JSON object is not terminated")
		}
		return object, nil
	case '[':
		var values []any
		for decoder.More() {
			value, err := decodeStrictJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return nil, fmt.Errorf("JSON array is not terminated")
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter")
	}
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("JSON contains trailing data")
	}
	return nil
}

func readBoundedFile(path string, limit int64, label string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file", label)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s file", label)
	}
	if int64(len(contents)) > limit {
		return nil, fmt.Errorf("%s file exceeds %d bytes", label, limit)
	}
	return contents, nil
}

func loadCertPool(path, label string) (*x509.CertPool, error) {
	contents, err := readBoundedFile(path, maximumCABundleBytes, label)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(contents) {
		return nil, fmt.Errorf("%s file contains no valid certificates", label)
	}
	return pool, nil
}

func boundedOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, operationTimeout)
}
