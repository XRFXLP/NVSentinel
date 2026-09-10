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
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// runGRPC pushes events through platform-connector's unix socket. This path
// exercises platform-connector itself and is bounded by its K8sConnectorQps /
// K8sConnectorBurst settings, so it tops out far below the direct mongo sink.
//
// Only the "fresh" stage is meaningful here: platform-connector writes new
// events, it cannot plant a document at a later pipeline stage. Later stages
// require the mongo sink.
func runGRPC(ctx context.Context, cfg config, spec *eventSpec, nodes []string) error {
	if cfg.stage != stageFresh && cfg.stage != stageNoise {
		return fmt.Errorf("--sink=grpc can only create fresh events; "+
			"stage %q must be planted with --sink=mongo", cfg.stage)
	}

	conn, err := grpc.NewClient(
		"unix://"+cfg.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial platform-connector at %s: %w", cfg.socket, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewPlatformConnectorClient(conn)

	type job struct{ start, n int }

	jobs := make(chan job, cfg.workers*2)
	pace := newPacer(cfg.rate, cfg.batch)

	var (
		wg  sync.WaitGroup
		cnt counters
	)

	for range cfg.workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := range jobs {
				if ctx.Err() != nil {
					return
				}

				pace.wait(ctx)

				events := make([]*pb.HealthEvent, 0, j.n)
				for i := range j.n {
					seq := j.start + i
					events = append(events, spec.makeProto(nodes[seq%len(nodes)], seq))
				}

				if _, err := client.HealthEventOccurredV1(ctx, &pb.HealthEvents{
					Version: 1,
					Events:  events,
				}); err != nil {
					cnt.failed.Add(int64(j.n))

					if cfg.verbose {
						log.Printf("send at %d: %v", j.start, err)
					}

					continue
				}

				cnt.written.Add(int64(j.n))
			}
		}()
	}

	start := time.Now()
	done := reportProgress(ctx, &cnt, cfg.count, start)

	for i := 0; i < cfg.count && ctx.Err() == nil; i += cfg.batch {
		jobs <- job{start: i, n: min(cfg.batch, cfg.count-i)}
	}

	close(jobs)
	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	log.Printf("done: sent=%d failed=%d in %s (%.0f events/s)",
		cnt.written.Load(), cnt.failed.Load(), elapsed.Round(time.Millisecond),
		float64(cnt.written.Load())/elapsed.Seconds())

	return nil
}

// makeProto builds the wire form of an event for the platform-connector sink.
func (s *eventSpec) makeProto(node string, seq int) *pb.HealthEvent {
	code := ""
	if len(s.ErrorCodes) > 0 {
		code = s.ErrorCodes[seq%len(s.ErrorCodes)]
	}

	entity := 0
	if s.Entities > 0 {
		entity = seq % s.Entities
	}

	isFatal := s.FatalRatio >= 1.0 || float64(seq%100)/100.0 < s.FatalRatio

	return &pb.HealthEvent{
		Version:            1,
		Agent:              s.Agent,
		ComponentClass:     s.ComponentClass,
		CheckName:          s.CheckName,
		IsFatal:            isFatal,
		IsHealthy:          false,
		Message:            s.Message,
		RecommendedAction:  pb.RecommendedAction(s.Action),
		ErrorCode:          []string{code},
		NodeName:           node,
		GeneratedTimestamp: timestamppb.Now(),
		ProcessingStrategy: pb.ProcessingStrategy(s.Strategy),
		EntitiesImpacted: []*pb.Entity{{
			EntityType:  "GPU",
			EntityValue: fmt.Sprintf("%d", entity),
		}},
	}
}
