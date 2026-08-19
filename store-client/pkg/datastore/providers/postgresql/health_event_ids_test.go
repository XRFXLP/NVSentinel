// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresql

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type healthEventIDQuery struct{}

func (healthEventIDQuery) ToMongo() map[string]interface{} { return nil }
func (healthEventIDQuery) ToSQL() (string, []interface{}) {
	return "created_at > $1", []interface{}{"2026-01-01"}
}

func TestPostgreSQLHealthEventStoreFindHealthEventIDsByQueryBatched(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	const firstQuery = "SELECT id FROM health_events WHERE created_at > $1 ORDER BY id LIMIT 2"
	mockDB.ExpectQuery(regexp.QuoteMeta(firstQuery)).
		WithArgs("2026-01-01").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("00000000-0000-0000-0000-000000000001").
			AddRow("00000000-0000-0000-0000-000000000002"))
	const nextQuery = "SELECT id FROM health_events WHERE created_at > $1 AND id > $2 ORDER BY id LIMIT 2"
	mockDB.ExpectQuery(regexp.QuoteMeta(nextQuery)).
		WithArgs("2026-01-01", "00000000-0000-0000-0000-000000000002").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	store := &PostgreSQLHealthEventStore{db: db}
	var batches [][]string
	err = store.FindHealthEventIDsByQueryBatched(
		context.Background(),
		healthEventIDQuery{},
		2,
		func(ids []string) error {
			batches = append(batches, ids)

			return nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, [][]string{{
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000002",
	}}, batches)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestPostgreSQLHealthEventStoreFindHealthEventByID(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	const id = "00000000-0000-0000-0000-000000000001"
	document, err := json.Marshal(model.HealthEventWithStatus{
		HealthEvent:       &protos.HealthEvent{NodeName: "node-1"},
		HealthEventStatus: &protos.HealthEventStatus{NodeQuarantined: "Quarantined"},
	})
	require.NoError(t, err)

	mockDB.ExpectQuery(regexp.QuoteMeta("SELECT document FROM health_events WHERE id = $1")).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"document"}).AddRow(document))

	store := &PostgreSQLHealthEventStore{db: db}
	event, err := store.FindHealthEventByID(context.Background(), id)

	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, "node-1", event.HealthEvent.NodeName)
	assert.Equal(t, "Quarantined", event.HealthEventStatus.NodeQuarantined)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestPostgreSQLHealthEventStoreFindHealthEventByIDJSONCompatibility(t *testing.T) {
	for _, test := range []struct {
		name               string
		faultRemediated    string
		expectedRemediated bool
	}{
		{
			name:               "historical plain boolean",
			faultRemediated:    "false",
			expectedRemediated: false,
		},
		{
			name:               "wrapped boolean",
			faultRemediated:    `{"value":true}`,
			expectedRemediated: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mockDB, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			const id = "00000000-0000-0000-0000-000000000001"
			document := []byte(`{
				"createdAt":"2023-11-14T22:13:20Z",
				"healthevent":{
					"id":"event-1",
					"nodeName":"node-1",
					"recommendedAction":24,
					"metadata":{"traceId":"trace-1"},
					"entitiesImpacted":[{"entityType":"GPU","entityValue":"0"}]
				},
				"healtheventstatus":{
					"nodequarantined":"Quarantined",
					"userpodsevictionstatus":{"status":"Succeeded","message":"drained"},
					"faultremediated":` + test.faultRemediated + `,
					"quarantinefinishtimestamp":{"seconds":1700000000,"nanos":0},
					"spanids":{"node_drainer":"span-1"}
				}
			}`)

			mockDB.ExpectQuery(regexp.QuoteMeta("SELECT document FROM health_events WHERE id = $1")).
				WithArgs(id).
				WillReturnRows(sqlmock.NewRows([]string{"document"}).AddRow(document))

			store := &PostgreSQLHealthEventStore{db: db}
			event, err := store.FindHealthEventByID(context.Background(), id)

			require.NoError(t, err)
			require.NotNil(t, event)
			assert.Equal(t, "node-1", event.HealthEvent.NodeName)
			assert.Equal(t, protos.RecommendedAction_RESTART_BM, event.HealthEvent.RecommendedAction)
			assert.Equal(t, "trace-1", event.HealthEvent.Metadata["traceId"])
			require.Len(t, event.HealthEvent.EntitiesImpacted, 1)
			assert.Equal(t, "GPU", event.HealthEvent.EntitiesImpacted[0].EntityType)
			assert.Equal(t, "Succeeded", event.HealthEventStatus.UserPodsEvictionStatus.Status)
			require.NotNil(t, event.HealthEventStatus.FaultRemediated)
			assert.Equal(t, test.expectedRemediated, event.HealthEventStatus.FaultRemediated.Value)
			assert.Equal(t, int64(1_700_000_000), event.HealthEventStatus.QuarantineFinishTimestamp.Seconds)
			assert.Equal(t, "span-1", event.HealthEventStatus.SpanIds["node_drainer"])
			assert.NoError(t, mockDB.ExpectationsWereMet())
		})
	}
}
