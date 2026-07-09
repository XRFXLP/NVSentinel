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
	"errors"
	"strings"
	"testing"

	"github.com/nvidia/nvsentinel/store-client/pkg/config"
)

type mockResumeTokenDBClient struct {
	deleteCalls int
	tokenConfig TokenConfig
	deleteErr   error
}

func (m *mockResumeTokenDBClient) InsertMany(context.Context, []interface{}) (*InsertManyResult, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) UpdateDocumentStatus(context.Context, string, string, interface{}) error {
	return nil
}

func (m *mockResumeTokenDBClient) UpdateDocumentStatusFields(context.Context, string, map[string]interface{}) error {
	return nil
}

func (m *mockResumeTokenDBClient) UpdateDocument(context.Context, interface{}, interface{}) (*UpdateResult, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) UpdateManyDocuments(context.Context, interface{}, interface{}) (*UpdateResult, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) UpsertDocument(context.Context, interface{}, interface{}) (*UpdateResult, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) FindOne(context.Context, interface{}, *FindOneOptions) (SingleResult, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) Find(context.Context, interface{}, *FindOptions) (Cursor, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) CountDocuments(context.Context, interface{}, *CountOptions) (int64, error) {
	return 0, nil
}

func (m *mockResumeTokenDBClient) Aggregate(context.Context, interface{}) (Cursor, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) Ping(context.Context) error {
	return nil
}

func (m *mockResumeTokenDBClient) NewChangeStreamWatcher(
	context.Context, TokenConfig, interface{},
) (ChangeStreamWatcher, error) {
	return nil, nil
}

func (m *mockResumeTokenDBClient) DeleteResumeToken(_ context.Context, tokenConfig TokenConfig) error {
	m.deleteCalls++
	m.tokenConfig = tokenConfig

	return m.deleteErr
}

func (m *mockResumeTokenDBClient) Close(context.Context) error {
	return nil
}

func TestResetResumeTokenOnStartIfConfigured_DisabledNoop(t *testing.T) {
	t.Setenv(config.EnvChangeStreamResumeTokenResetOnStart, "false")

	dbClient := &mockResumeTokenDBClient{}
	err := ResetResumeTokenOnStartIfConfigured(context.Background(), dbClient, TokenConfig{
		ClientName:      "node-drainer",
		TokenDatabase:   "HealthEventsDatabase",
		TokenCollection: "ResumeTokens",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dbClient.deleteCalls != 0 {
		t.Fatalf("DeleteResumeToken called %d times, want 0", dbClient.deleteCalls)
	}
}

func TestResetResumeTokenOnStartIfConfigured_EnabledDeletesToken(t *testing.T) {
	t.Setenv(config.EnvChangeStreamResumeTokenResetOnStart, "true")

	tokenConfig := TokenConfig{
		ClientName:      "node-drainer",
		TokenDatabase:   "HealthEventsDatabase",
		TokenCollection: "ResumeTokens",
	}
	dbClient := &mockResumeTokenDBClient{}

	err := ResetResumeTokenOnStartIfConfigured(context.Background(), dbClient, tokenConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dbClient.deleteCalls != 1 {
		t.Fatalf("DeleteResumeToken called %d times, want 1", dbClient.deleteCalls)
	}

	if dbClient.tokenConfig != tokenConfig {
		t.Fatalf("DeleteResumeToken got token config %+v, want %+v", dbClient.tokenConfig, tokenConfig)
	}
}

func TestResetResumeTokenOnStartIfConfigured_ConfigParseError(t *testing.T) {
	t.Setenv(config.EnvChangeStreamResumeTokenResetOnStart, "sometimes")

	dbClient := &mockResumeTokenDBClient{}
	err := ResetResumeTokenOnStartIfConfigured(context.Background(), dbClient, TokenConfig{
		ClientName: "node-drainer",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "failed to load change stream resume token reset-on-start configuration") {
		t.Fatalf("error %q missing configuration context", err)
	}

	if dbClient.deleteCalls != 0 {
		t.Fatalf("DeleteResumeToken called %d times, want 0", dbClient.deleteCalls)
	}
}

func TestResetResumeTokenOnStartIfConfigured_DeleteError(t *testing.T) {
	t.Setenv(config.EnvChangeStreamResumeTokenResetOnStart, "true")

	deleteErr := errors.New("delete failed")
	dbClient := &mockResumeTokenDBClient{deleteErr: deleteErr}

	err := ResetResumeTokenOnStartIfConfigured(context.Background(), dbClient, TokenConfig{
		ClientName: "node-drainer",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, deleteErr) {
		t.Fatalf("errors.Is(err, deleteErr) = false, err=%v", err)
	}

	if !strings.Contains(err.Error(), "failed to delete change stream resume token") {
		t.Fatalf("error %q missing delete context", err)
	}

	if dbClient.deleteCalls != 1 {
		t.Fatalf("DeleteResumeToken called %d times, want 1", dbClient.deleteCalls)
	}
}
