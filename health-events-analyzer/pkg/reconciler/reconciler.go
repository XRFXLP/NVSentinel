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

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	datamodels "github.com/nvidia/nvsentinel/data-models/pkg/model"
	protos "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	parser "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/parser"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/nvidia/nvsentinel/store-client/pkg/storewatcher"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionInterface interface {
	Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error)
}

type HealthEventsAnalyzerReconcilerConfig struct {
	MongoHealthEventCollectionConfig storewatcher.MongoDBConfig
	TokenConfig                      storewatcher.TokenConfig
	MongoPipeline                    mongo.Pipeline
	HealthEventsAnalyzerRules        *config.TomlConfig
	Publisher                        *publisher.PublisherConfig
	CollectionClient                 CollectionInterface
}

type Reconciler struct {
	config HealthEventsAnalyzerReconcilerConfig
}

func NewReconciler(cfg HealthEventsAnalyzerReconcilerConfig) *Reconciler {
	return &Reconciler{
		config: cfg,
	}
}

// Start begins the reconciliation process by listening to change stream events
// and processing them accordingly.
func (r *Reconciler) Start(ctx context.Context) error {
	watcher, err := storewatcher.NewChangeStreamWatcher(
		ctx,
		r.config.MongoHealthEventCollectionConfig,
		r.config.TokenConfig,
		r.config.MongoPipeline,
	)
	if err != nil {
		return fmt.Errorf("failed to create change stream watcher: %w", err)
	}
	defer watcher.Close(ctx)

	r.config.CollectionClient, err = storewatcher.GetCollectionClient(ctx, r.config.MongoHealthEventCollectionConfig)
	if err != nil {
		slog.Error(
			"Error initializing healthEventCollection client",
			"config", r.config.MongoHealthEventCollectionConfig,
			"error", err,
		)

		return fmt.Errorf("failed to initialize healthEventCollection client: %w", err)
	}

	watcher.Start(ctx)

	slog.Info("Listening for events on the channel...")

	for event := range watcher.Events() {
		slog.Info("Processing event", "event", event)

		err := r.processEvent(ctx, event)
		if err != nil {
			slog.Error("Error processing event", "error", err)
		}

		if err := watcher.MarkProcessed(ctx); err != nil {
			slog.Error("Error updating resume token", "error", err)
		}
	}

	return nil
}

func (r *Reconciler) processEvent(ctx context.Context, event bson.M) error {
	startTime := time.Now()

	healthEventWithStatus := datamodels.HealthEventWithStatus{}
	if err := storewatcher.UnmarshalFullDocumentFromEvent(
		event,
		&healthEventWithStatus,
	); err != nil {
		slog.Error("Failed to unmarshal event", "error", err)

		totalEventProcessingError.WithLabelValues("unmarshal_doc_error").Inc()

		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	slog.Debug("Received event", "event", healthEventWithStatus)

	totalEventsReceived.WithLabelValues(healthEventWithStatus.HealthEvent.NodeName).Inc()

	var err error

	var publishedNewEvent bool

	publishedNewEvent, err = r.handleEvent(ctx, &healthEventWithStatus)
	if err != nil {
		slog.Error("Error in handling the event", "event", healthEventWithStatus, "error", err)

		totalEventProcessingError.WithLabelValues("handle_event_error").Inc()
	} else {
		totalEventsSuccessfullyProcessed.Inc()

		if publishedNewEvent {
			slog.Info("New event successfully published.")
			newEventsPublishedTotal.WithLabelValues(healthEventWithStatus.HealthEvent.NodeName).Inc()
		} else {
			slog.Info("New event is not published, rule set criteria didn't match.")
		}
	}

	duration := time.Since(startTime).Seconds()

	eventHandlingDuration.Observe(duration)

	return err
}

func (r *Reconciler) handleEvent(ctx context.Context, event *datamodels.HealthEventWithStatus) (bool, error) {
	var multiErr *multierror.Error

	publishedNewEvent := false

	for _, rule := range r.config.HealthEventsAnalyzerRules.Rules {
		published, err := r.processRule(ctx, rule, event)
		if err != nil {
			multiErr = multierror.Append(multiErr, err)
			continue
		}

		if published {
			publishedNewEvent = true
		}
	}

	if multiErr.ErrorOrNil() != nil {
		slog.Error("Error in handling the event", "error", multiErr)
		return publishedNewEvent, fmt.Errorf("error in handling the event: %w", multiErr)
	}

	return publishedNewEvent, nil
}

// processRule handles the processing of a single rule against an event
func (r *Reconciler) processRule(ctx context.Context,
	rule config.HealthEventsAnalyzerRule,
	event *datamodels.HealthEventWithStatus) (bool, error) {
	// Validate all sequences from DB docs
	matchedSequences, err := r.validateAllSequenceCriteria(ctx, rule, *event)
	if err != nil {
		slog.Error("Error in validating all sequence criteria", "error", err)
		return false, fmt.Errorf("error in validating all sequence criteria: %w", err)
	}

	if !matchedSequences {
		return false, nil
	}

	err = r.publishMatchedEvent(ctx, rule, event)
	if err != nil {
		slog.Error("Error in publishing the matched event", "error", err)
		return false, fmt.Errorf("error in publishing the matched event: %w", err)
	}

	return true, nil
}

// publishMatchedEvent publishes an event when a rule matches
func (r *Reconciler) publishMatchedEvent(ctx context.Context,
	rule config.HealthEventsAnalyzerRule,
	event *datamodels.HealthEventWithStatus) error {
	slog.Info("Rule matched for event", "rule_name", rule.Name, "event", event)
	ruleMatchedTotal.WithLabelValues(rule.Name, event.HealthEvent.NodeName).Inc()

	actionVal := r.getRecommendedActionValue(rule.RecommendedAction, rule.Name)

	err := r.config.Publisher.Publish(ctx, event.HealthEvent, protos.RecommendedAction(actionVal), rule.Name)
	if err != nil {
		slog.Error("Error in publishing the new fatal event", "error", err)
		return fmt.Errorf("error in publishing the new fatal event: %w", err)
	}

	slog.Info("New event successfully published for matching rule", "rule_name", rule.Name)

	return nil
}

// getRecommendedActionValue returns the action value, with fallback to RecommendedAction_CONTACT_SUPPORT if invalid
func (r *Reconciler) getRecommendedActionValue(recommendedAction, ruleName string) int32 {
	actionVal, ok := protos.RecommendedAction_value[recommendedAction]
	if !ok {
		defaultAction := int32(protos.RecommendedAction_CONTACT_SUPPORT)
		slog.Warn("Invalid recommended_action in rule; defaulting to CONTACT_SUPPORT",
			"recommended_action", recommendedAction,
			"rule_name", ruleName,
			"default_action", protos.RecommendedAction_name[defaultAction])

		return defaultAction
	}

	return actionVal
}

func (r *Reconciler) validateAllSequenceCriteria(ctx context.Context, rule config.HealthEventsAnalyzerRule,
	healthEventWithStatus datamodels.HealthEventWithStatus) (bool, error) {
	slog.Debug("Evaluating rule for event", "rule_name", rule.Name, "event", healthEventWithStatus)

	timeWindow, err := time.ParseDuration(rule.TimeWindow)
	if err != nil {
		slog.Error("Failed to parse time window", "error", err)
		totalEventProcessingError.WithLabelValues("parse_time_window_error").Inc()

		return false, fmt.Errorf("failed to parse time window: %w", err)
	}

	facets := bson.D{}

	for i, seq := range rule.Sequence {
		slog.Debug("Evaluating sequence", "sequence", seq)

		facetName := "sequence_" + strconv.Itoa(i)

		matchCriteria, err := parser.ParseSequenceString(seq.Criteria, healthEventWithStatus)
		if err != nil {
			slog.Error("Failed to parse sequence criteria", "error", err)

			totalEventProcessingError.WithLabelValues("parse_criteria_error").Inc()

			continue
		}

		facets = append(facets, getFacet(facetName, timeWindow, matchCriteria))
	}

	if len(facets) == 0 {
		slog.Debug("No facets created for rule", "rule_name", rule.Name)
		totalEventProcessingError.WithLabelValues("no_facets_found_error").Inc()

		return false, nil
	}

	pipeline := getPipeline(facets, rule)

	var result []bson.M

	startTime := time.Now()

	cursor, err := r.config.CollectionClient.Aggregate(ctx, pipeline)
	if err != nil {
		slog.Error("Failed to execute aggregation pipeline", "error", err)
		totalEventProcessingError.WithLabelValues("execute_pipeline_error").Inc()

		return false, fmt.Errorf("failed to execute aggregation pipeline: %w", err)
	}

	duration := time.Since(startTime).Seconds()
	databaseQueryDuration.Observe(duration)

	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &result); err != nil {
		slog.Error("Failed to decode cursor", "error", err)
		totalEventProcessingError.WithLabelValues("decode_cursor_error").Inc()

		return false, fmt.Errorf("failed to decode cursor: %w", err)
	}

	if len(result) > 0 {
		// Check if all criteria are met
		slog.Debug("Query result", "result", result)

		if matched, ok := result[0]["ruleMatched"].(bool); ok && matched {
			slog.Debug("All sequence conditions met for rule", "rule_name", rule.Name)
			return true, nil
		}
	}

	return false, nil
}

func getFacet(facetName string, timeWindow time.Duration, matchCriteria bson.D) bson.E {
	return bson.E{
		Key: facetName,
		Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "healthevent.generatedtimestamp.seconds", Value: bson.D{
					{Key: "$gte", Value: time.Now().UTC().Add(-timeWindow).Unix()},
				}},
				{Key: "healthevent.agent", Value: bson.D{{Key: "$ne", Value: "health-events-analyzer"}}},
			}}},
			bson.D{{Key: "$match", Value: matchCriteria}},
			bson.D{{Key: "$count", Value: "count"}},
		},
	}
}

func getPipeline(facets bson.D, rule config.HealthEventsAnalyzerRule) mongo.Pipeline {
	return mongo.Pipeline{
		{{Key: "$facet", Value: facets}},
		{{Key: "$project", Value: bson.D{
			{Key: "ruleMatched", Value: bson.D{
				{Key: "$and", Value: func() bson.A {
					conditions := make(bson.A, len(rule.Sequence))
					for i, seq := range rule.Sequence {
						facetName := "sequence_" + strconv.Itoa(i)
						conditions[i] = bson.D{
							{Key: "$gte", Value: bson.A{
								bson.D{{Key: "$arrayElemAt", Value: bson.A{"$" + facetName + ".count", 0}}},
								seq.ErrorCount,
							}},
						}
					}

					return conditions
				}()},
			}},
		}}},
	}
}
