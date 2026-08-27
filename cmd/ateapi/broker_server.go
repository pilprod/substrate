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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type externalProviderBrokerConfig struct {
	ListenAddress          string
	ServerCredentialBundle string
	SessionTokenTTL        time.Duration
}

func (c externalProviderBrokerConfig) enabled() bool {
	return c.ListenAddress != ""
}

// externalProviderBrokerRuntime owns the dedicated listener and gRPC server.
// The server deliberately exposes only ExternalProviderBroker.
type externalProviderBrokerRuntime struct {
	listener net.Listener
	server   *grpc.Server
}

func startExternalProviderBroker(
	ctx context.Context,
	store externalprovider.ExternalProviderStore,
	sessionRuntime *externalprovider.SessionRuntime,
	config externalProviderBrokerConfig,
	logger *slog.Logger,
) (*externalProviderBrokerRuntime, error) {
	if !config.enabled() {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("external provider store is not configured")
	}
	if sessionRuntime == nil {
		return nil, errors.New("external provider session runtime is not configured")
	}
	serverCredentials, err := externalProviderBrokerServerCredentials(config.ServerCredentialBundle)
	if err != nil {
		return nil, err
	}
	server, err := newExternalProviderBrokerGRPCServer(store, sessionRuntime, serverCredentials, config.SessionTokenTTL, logger)
	if err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for external provider broker on %q: %w", config.ListenAddress, err)
	}
	return &externalProviderBrokerRuntime{listener: listener, server: server}, nil
}

func (r *externalProviderBrokerRuntime) Serve() error {
	return r.server.Serve(r.listener)
}

func newExternalProviderBrokerGRPCServer(
	store externalprovider.ExternalProviderStore,
	sessionRuntime *externalprovider.SessionRuntime,
	serverCredentials credentials.TransportCredentials,
	sessionTokenTTL time.Duration,
	logger *slog.Logger,
) (*grpc.Server, error) {
	if serverCredentials == nil {
		return nil, errors.New("external provider broker transport credentials are required")
	}
	brokerOptions := make([]externalprovider.BrokerOption, 0, 1)
	if sessionRuntime != nil {
		brokerOptions = append(brokerOptions, externalprovider.WithSessionRuntime(sessionRuntime))
	}
	broker, err := externalprovider.NewBroker(store, sessionTokenTTL, brokerOptions...)
	if err != nil {
		return nil, fmt.Errorf("create external provider broker: %w", err)
	}
	server := grpc.NewServer(
		grpc.Creds(serverCredentials),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      time.Hour,
			MaxConnectionAgeGrace: maxRPCDeadline + time.Minute,
		}),
		grpc.ChainUnaryInterceptor(
			externalprovider.MetadataOnlyUnaryLoggingInterceptor(logger),
			externalprovider.OpaqueBearerUnaryInterceptor(),
			ateinterceptors.MaxDeadlineUnaryInterceptor(maxRPCDeadline),
		),
		grpc.ChainStreamInterceptor(
			externalprovider.MetadataOnlyStreamLoggingInterceptor(logger),
			externalprovider.OpaqueBearerStreamInterceptor(),
		),
	)
	externalproviderpb.RegisterExternalProviderBrokerServer(server, broker)
	return server, nil
}

func externalProviderBrokerServerCredentials(bundlePath string) (credentials.TransportCredentials, error) {
	if bundlePath == "" {
		return nil, errors.New("external provider broker server credential bundle is required")
	}
	if _, err := credbundle.Parse(bundlePath); err != nil {
		return nil, fmt.Errorf("load external provider broker server credential bundle: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:     tls.VersionTLS12,
		ClientAuth:     tls.NoClientCert,
		GetCertificate: credbundle.Loader(bundlePath),
	}), nil
}
