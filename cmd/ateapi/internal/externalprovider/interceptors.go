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

package externalprovider

import (
	"context"

	"google.golang.org/grpc"
)

// OpaqueBearerUnaryInterceptor rejects a missing or malformed external
// provider credential before invoking a unary Broker handler. It validates
// only the credential envelope. Enroll and MintSessionToken authenticate the
// credential against their purpose-specific digests inside the handler.
func OpaqueBearerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		credential, err := bearerCredential(ctx)
		if err != nil {
			return nil, unauthenticated()
		}
		clear(credential)
		return handler(ctx, request)
	}
}

// OpaqueBearerStreamInterceptor applies the same credential-envelope check to
// Connect without interpreting or consuming the one-time session token. The
// Connect handler owns that atomic authentication step once session routing is
// enabled.
func OpaqueBearerStreamInterceptor() grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		credential, err := bearerCredential(stream.Context())
		if err != nil {
			return unauthenticated()
		}
		clear(credential)
		return handler(server, stream)
	}
}
