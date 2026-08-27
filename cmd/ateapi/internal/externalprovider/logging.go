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
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// MetadataOnlyUnaryLoggingInterceptor records only RPC method, result code,
// and elapsed time. It never logs request/response payloads, authorization
// metadata, returned credentials, or error messages.
func MetadataOnlyUnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, request)
		logger.InfoContext(ctx, "external provider RPC",
			slog.String("method", info.FullMethod),
			slog.String("code", status.Code(err).String()),
			slog.Duration("elapsed", time.Since(started)),
		)
		return response, err
	}
}
