// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package client

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nvidia/nvsentinel/store-client/pkg/config"
)

// ResetResumeTokenOnStartIfConfigured deletes a component's change stream
// resume token when explicitly requested via environment configuration.
func ResetResumeTokenOnStartIfConfigured(
	ctx context.Context,
	dbClient DatabaseClient,
	tokenConfig TokenConfig,
) error {
	resetOnStart, err := config.ChangeStreamResumeTokenResetOnStartFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load change stream resume token reset-on-start configuration: %w", err)
	}

	if !resetOnStart {
		return nil
	}

	slog.InfoContext(ctx, "Deleting change stream resume token on startup",
		"clientName", tokenConfig.ClientName,
		"tokenDatabase", tokenConfig.TokenDatabase,
		"tokenCollection", tokenConfig.TokenCollection)

	if err := dbClient.DeleteResumeToken(ctx, tokenConfig); err != nil {
		return fmt.Errorf("failed to delete change stream resume token: %w", err)
	}

	return nil
}
