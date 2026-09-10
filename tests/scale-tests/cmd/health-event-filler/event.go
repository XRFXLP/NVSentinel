// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package main

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// Pipeline stages an event can be planted at. The stage decides which
// component would pick the event up next.
const (
	stageFresh       = "fresh"       // no quarantine status yet -> fault-quarantine
	stageQuarantined = "quarantined" // cordoned            -> node-drainer
	stageDrained     = "drained"     // drained             -> fault-remediation
	stageRemediated  = "remediated"  // terminal, no consumer
	stageNoise       = "noise"       // STORE_ONLY, matches no change stream
)

// Delivery modes. This distinction is load-bearing: on MongoDB the
// node-drainer and fault-remediation change streams match operationType
// "update" only, so a document INSERTed with its terminal state already set is
// invisible to their live path and is reachable only by a cold-start query.
// fault-quarantine is the inverse -- it watches inserts.
const (
	deliverSeed   = "seed"   // single insert; backlog only for ND/FR
	deliverStream = "stream" // insert precursor, then update -> fires the change stream
)

// Processing strategy enum values, from data-models/protobufs/health_event.proto.
// UNSPECIFIED is normalized to EXECUTE_REMEDIATION by platform-connector.
const (
	psUnspecified  = 0
	psExecute      = 1
	psStoreOnly    = 2
	psStoreAnalyse = 3
)

// RecommendedAction enum values, from data-models/protobufs/health_event.proto.
// fault-remediation skips any event whose action is NONE, and only acts on
// actions present in its remediationActions config (COMPONENT_RESET,
// RESTART_BM, RESTART_VM, REPLACE_VM).
var recommendedActions = map[string]int32{
	"NONE": 0, "COMPONENT_RESET": 2, "CONTACT_SUPPORT": 5, "RUN_FIELDDIAG": 6,
	"RESTART_VM": 15, "RESTART_BM": 24, "REPLACE_VM": 25, "RUN_DCGMEUD": 26,
}

// Quarantine status values, from data-models/pkg/model.
const (
	qNotQuarantined = "NotQuarantined"
	qQuarantined    = "Quarantined"
)

// Eviction status values, from data-models/pkg/model.
const (
	evSucceeded = "Succeeded"
)

// eventSpec is the content template for generated events.
type eventSpec struct {
	Agent          string
	ComponentClass string
	CheckName      string
	ErrorCodes     []string
	Message        string
	Entities       int
	FatalRatio     float64
	Strategy       int32
	Action         int32
}

// statusFor returns the healtheventstatus subdocument for a stage. When
// precursor is true it returns the state the document should be INSERTed in so
// that the follow-up update lands the right key in updateDescription.updatedFields.
func statusFor(stage string, precursor bool) bson.D {
	// Fields that would be nil are OMITTED rather than written as null.
	// node-drainer promotes an event with $set on
	// "healtheventstatus.userpodsevictionstatus.status"; if the parent exists
	// as an explicit null, MongoDB rejects that with
	// "Cannot create field 'status' in element {userpodsevictionstatus: null}"
	// and the event is dropped. A missing parent is created implicitly.
	base := func(quarantined string, eviction any, remediated any) bson.D {
		d := bson.D{{Key: "nodequarantined", Value: quarantined}}

		if eviction != nil {
			d = append(d, bson.E{Key: "userpodsevictionstatus", Value: eviction})
		}

		if remediated != nil {
			d = append(d, bson.E{Key: "faultremediated", Value: remediated})
		}

		return append(d, bson.E{Key: "spanids", Value: bson.D{}})
	}

	switch stage {
	case stageQuarantined:
		if precursor {
			// Insert un-quarantined so the update sets nodequarantined.
			return base(qNotQuarantined, nil, nil)
		}

		return base(qQuarantined, nil, nil)

	case stageDrained:
		if precursor {
			// Already quarantined on insert: fault-remediation requires
			// nodequarantined to be set in fullDocument while the update
			// itself touches userpodsevictionstatus.
			return base(qQuarantined, nil, nil)
		}

		return base(qQuarantined, bson.D{
			{Key: "status", Value: evSucceeded},
			{Key: "message", Value: ""},
		}, nil)

	case stageRemediated:
		return base(qQuarantined, bson.D{
			{Key: "status", Value: evSucceeded},
			{Key: "message", Value: ""},
		}, bson.D{{Key: "value", Value: true}})

	default: // stageFresh, stageNoise
		return base(qNotQuarantined, nil, nil)
	}
}

// updateFor returns the $set document that promotes a precursor into its
// target stage. The key that appears here is exactly what the change stream
// matches on, so it must not be over-broad.
func updateFor(stage string) (bson.D, error) {
	switch stage {
	case stageQuarantined:
		return bson.D{{Key: "$set", Value: bson.D{
			{Key: "healtheventstatus.nodequarantined", Value: qQuarantined},
		}}}, nil

	case stageDrained:
		return bson.D{{Key: "$set", Value: bson.D{
			{Key: "healtheventstatus.userpodsevictionstatus", Value: bson.D{
				{Key: "status", Value: evSucceeded},
				{Key: "message", Value: ""},
			}},
		}}}, nil

	default:
		return nil, fmt.Errorf("stage %q has no live change-stream consumer; use --deliver=seed", stage)
	}
}

// makeDoc builds one stored HealthEvents document. Field names are all
// lowercased by the bson marshaller except createdAt, which is tagged.
func (s *eventSpec) makeDoc(node string, seq int, stage string, precursor bool, now time.Time) bson.D {
	strategy := s.Strategy
	if stage == stageNoise {
		strategy = psStoreOnly
	}

	isFatal := s.FatalRatio >= 1.0 || float64(seq%100)/100.0 < s.FatalRatio

	code := ""
	if len(s.ErrorCodes) > 0 {
		code = s.ErrorCodes[seq%len(s.ErrorCodes)]
	}

	entity := 0
	if s.Entities > 0 {
		entity = seq % s.Entities
	}

	return bson.D{
		{Key: "createdAt", Value: now},
		{Key: "healthevent", Value: bson.D{
			{Key: "version", Value: 1},
			{Key: "agent", Value: s.Agent},
			{Key: "componentclass", Value: s.ComponentClass},
			{Key: "checkname", Value: s.CheckName},
			{Key: "isfatal", Value: isFatal},
			{Key: "ishealthy", Value: false},
			{Key: "message", Value: s.Message},
			{Key: "recommendedaction", Value: s.Action},
			{Key: "errorcode", Value: []string{code}},
			{Key: "entitiesimpacted", Value: bson.A{bson.D{
				{Key: "entitytype", Value: "GPU"},
				{Key: "entityvalue", Value: fmt.Sprintf("%d", entity)},
			}}},
			{Key: "metadata", Value: bson.D{}},
			{Key: "generatedtimestamp", Value: bson.D{
				{Key: "seconds", Value: now.Unix()},
				{Key: "nanos", Value: 0},
			}},
			{Key: "nodename", Value: node},
			{Key: "quarantineoverrides", Value: nil},
			{Key: "drainoverrides", Value: nil},
			{Key: "processingstrategy", Value: strategy},
			{Key: "id", Value: ""},
			{Key: "customrecommendedaction", Value: ""},
		}},
		{Key: "healtheventstatus", Value: statusFor(stage, precursor)},
	}
}

// validate rejects stage/delivery combinations that would silently do nothing,
// which is the single easiest way to lose an afternoon with this tool.
func validate(stage, deliver string) error {
	switch stage {
	case stageFresh, stageQuarantined, stageDrained, stageRemediated, stageNoise:
	default:
		return fmt.Errorf("unknown --stage %q", stage)
	}

	switch deliver {
	case deliverSeed, deliverStream:
	default:
		return fmt.Errorf("unknown --deliver %q", deliver)
	}

	if deliver == deliverStream {
		if _, err := updateFor(stage); err != nil {
			return err
		}
	}

	return nil
}

// consumerHint describes what will actually pick these events up, so the tool
// can say so at startup instead of leaving it to be discovered empirically.
func consumerHint(stage, deliver string) string {
	switch stage {
	case stageFresh:
		return "fault-quarantine (watches INSERT; seed and stream are equivalent here)"
	case stageQuarantined:
		if deliver == deliverSeed {
			return "node-drainer COLD START only (its change stream matches UPDATE, not INSERT)"
		}

		return "node-drainer live change stream"
	case stageDrained:
		if deliver == deliverSeed {
			return "fault-remediation COLD START only (its change stream matches UPDATE, not INSERT)"
		}

		return "fault-remediation live change stream"
	case stageRemediated:
		return "nobody (terminal state; use this to grow the collection)"
	default:
		return "nobody (STORE_ONLY noise; use this to measure scan/query cost)"
	}
}
