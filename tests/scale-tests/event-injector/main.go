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

// event-injector: high-rate FQ benchmark event injector.
//
// Runs inside the cluster, connects directly to the platform-connector Unix
// socket and injects health events at configurable rates.
//
// Patterns:
//
//	fleet-storm    1 fatal event per N distinct KWOK nodes — cordon throughput
//	noisy-node     M events for node[0], then node[1], then node[2] — backlog P99
//	sustained      Max throughput non-fatal events — measure platform-connector ceiling
//	flappy         Alternating fatal/healthy per node — forces FQ K8s API on every event,
//	               maximally slows FQ processing to stress oplog resume token exhaustion
//	oplog-stress   Burst of non-fatal events burying one fatal — resume token test
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

var (
	socketPath  = flag.String("socket", "/var/run/nvsentinel.sock", "Platform-connector Unix socket path")
	pattern     = flag.String("pattern", "fleet-storm", "Pattern: fleet-storm|noisy-node|sustained|oplog-stress")
	workers     = flag.Int("workers", 20, "Number of parallel gRPC workers")
	nodePrefix  = flag.String("node-prefix", "kwok-node-", "KWOK node name prefix")
	nodeStart   = flag.Int("node-start", 89995, "First node index")
	nodeCount   = flag.Int("node-count", 100, "Number of distinct nodes")
	eventsPerNode = flag.Int("events-per-node", 100, "Events per node (noisy-node/oplog-stress)")
	rate        = flag.Float64("rate", 100, "Events per second (sustained mode)")
	duration    = flag.Int("duration", 60, "Duration in seconds (sustained mode)")
	fatalAt     = flag.Int("fatal-at", -1, "Insert fatal event after N non-fatal events (-1 = middle)")
	agent       = flag.String("agent", "gpu-health-monitor", "Health event agent name")
	batchSize   = flag.Int("batch", 10, "Events per gRPC call")
	verbose     = flag.Bool("v", false, "Verbose logging")
)

var errorCodes = []string{"79", "80", "81", "74", "92", "48", "31", "63"}
var checkNames = []string{"GpuXidError", "GpuHealthCheck", "GpuMemoryCheck", "GpuPerfCheck"}

func nodeName(i int) string {
	return fmt.Sprintf("%s%06d", *nodePrefix, *nodeStart+i)
}

func fatalEvent(node string, errorCode string, entityVal string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              *agent,
		ComponentClass:     "GPU",
		CheckName:          "GpuXidError",
		IsFatal:            true,
		IsHealthy:          false,
		Message:            fmt.Sprintf("XID %s - benchmark injection", errorCode),
		RecommendedAction:  pb.RecommendedAction_COMPONENT_RESET,
		ErrorCode:          []string{errorCode},
		EntitiesImpacted:   []*pb.Entity{{EntityType: "gpu", EntityValue: entityVal}},
		NodeName:           node,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

func nonFatalEvent(node string, checkName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              *agent,
		ComponentClass:     "System",
		CheckName:          checkName,
		IsFatal:            false,
		IsHealthy:          true,
		Message:            "heartbeat",
		RecommendedAction:  pb.RecommendedAction_NONE,
		NodeName:           node,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

type sender struct {
	client pb.PlatformConnectorClient
}

func (s *sender) send(ctx context.Context, events []*pb.HealthEvent) error {
	_, err := s.client.HealthEventOccurredV1(ctx, &pb.HealthEvents{
		Version: 1,
		Events:  events,
	})
	return err
}

func newSender(socketPath string) (*sender, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return &sender{client: pb.NewPlatformConnectorClient(conn)}, conn, nil
}

// workerPool dispatches events from a channel using N persistent connections.
type workerPool struct {
	jobs    chan []*pb.HealthEvent
	wg      sync.WaitGroup
	sent    atomic.Int64
	failed  atomic.Int64
	socket  string
}

func newPool(socket string, n int) *workerPool {
	p := &workerPool{
		jobs:   make(chan []*pb.HealthEvent, n*4),
		socket: socket,
	}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *workerPool) worker() {
	defer p.wg.Done()
	s, conn, err := newSender(p.socket)
	if err != nil {
		log.Printf("[worker] failed to connect: %v", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	for batch := range p.jobs {
		if err := s.send(ctx, batch); err != nil {
			p.failed.Add(int64(len(batch)))
			if *verbose {
				log.Printf("[worker] send error: %v", err)
			}
		} else {
			p.sent.Add(int64(len(batch)))
		}
	}
}

func (p *workerPool) submit(batch []*pb.HealthEvent) {
	p.jobs <- batch
}

func (p *workerPool) close() {
	close(p.jobs)
	p.wg.Wait()
}

// batcher accumulates events and flushes when batch is full or flush() called.
type batcher struct {
	pool  *workerPool
	buf   []*pb.HealthEvent
	size  int
}

func newBatcher(pool *workerPool, size int) *batcher {
	return &batcher{pool: pool, buf: make([]*pb.HealthEvent, 0, size), size: size}
}

func (b *batcher) add(e *pb.HealthEvent) {
	b.buf = append(b.buf, e)
	if len(b.buf) >= b.size {
		b.flush()
	}
}

func (b *batcher) flush() {
	if len(b.buf) == 0 {
		return
	}
	cp := make([]*pb.HealthEvent, len(b.buf))
	copy(cp, b.buf)
	b.pool.submit(cp)
	b.buf = b.buf[:0]
}

func healthyEvent(node string, checkName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              *agent,
		ComponentClass:     "GPU",
		CheckName:          checkName,
		IsFatal:            false,
		IsHealthy:          true,
		Message:            "GPU recovered",
		RecommendedAction:  pb.RecommendedAction_NONE,
		NodeName:           node,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

// ── Patterns ──────────────────────────────────────────────────────────────────

func runFleetStorm() {
	log.Printf("=== Fleet Storm: %d nodes ===", *nodeCount)
	pool := newPool(*socketPath, *workers)
	bat := newBatcher(pool, *batchSize)

	t0 := time.Now()
	for i := 0; i < *nodeCount; i++ {
		bat.add(fatalEvent(nodeName(i), "79", "0"))
	}
	bat.flush()
	pool.close()

	elapsed := time.Since(t0)
	log.Printf("Sent %d fatal events in %.2fs (%.0f events/s)",
		pool.sent.Load(), elapsed.Seconds(), float64(pool.sent.Load())/elapsed.Seconds())
	log.Printf("Failed: %d", pool.failed.Load())
}

func runNoisyNode() {
	log.Printf("=== Noisy Node Backlog: %d nodes × %d events ===", *nodeCount, *eventsPerNode)
	pool := newPool(*socketPath, *workers)

	t0 := time.Now()
	for n := 0; n < *nodeCount; n++ {
		name := nodeName(n)
		bat := newBatcher(pool, *batchSize)
		for j := 0; j < *eventsPerNode; j++ {
			if j == 0 {
				// First event is fatal (triggers cordon)
				bat.add(fatalEvent(name, errorCodes[j%len(errorCodes)], fmt.Sprintf("%d", j%8)))
			} else {
				// Subsequent events vary entity/errorcode to bypass HealthEventKey dedup
				bat.add(fatalEvent(name, errorCodes[j%len(errorCodes)], fmt.Sprintf("%d", j%8)))
			}
		}
		bat.flush()
		log.Printf("  Node %s: %d events queued", name, *eventsPerNode)
	}
	pool.close()

	elapsed := time.Since(t0)
	log.Printf("Sent %d events in %.2fs (%.0f events/s)",
		pool.sent.Load(), elapsed.Seconds(), float64(pool.sent.Load())/elapsed.Seconds())
	log.Printf("Failed: %d", pool.failed.Load())
	log.Printf("P99 cordon latency for node[%d] = processing time for ~%d events",
		*nodeCount-1, (*nodeCount-1)**eventsPerNode)
}

func runSustained() {
	log.Printf("=== Sustained Rate: max throughput for %ds (non-fatal events) ===", *duration)
	pool := newPool(*socketPath, *workers)

	deadline := time.Now().Add(time.Duration(*duration) * time.Second)
	t0 := time.Now()

	// Spin up producer goroutines — each fills the pool as fast as possible
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			count := int64(id)
			for time.Now().Before(deadline) {
				batch := make([]*pb.HealthEvent, *batchSize)
				for i := range batch {
					node := nodeName(int(count) % *nodeCount)
					check := checkNames[count%int64(len(checkNames))]
					batch[i] = nonFatalEvent(node, check)
					count += int64(*workers)
				}
				pool.jobs <- batch
			}
		}(w)
	}

	// Report every 5s
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for t := range tick.C {
			elapsed := t.Sub(t0)
			if elapsed > time.Duration(*duration)*time.Second {
				return
			}
			log.Printf("  t=%.0fs: sent=%d rate=%.0f/s failed=%d",
				elapsed.Seconds(), pool.sent.Load(),
				float64(pool.sent.Load())/elapsed.Seconds(),
				pool.failed.Load())
		}
	}()

	wg.Wait()
	pool.close()

	elapsed := time.Since(t0)
	log.Printf("Total: sent=%d failed=%d elapsed=%.1fs rate=%.0f/s",
		pool.sent.Load(), pool.failed.Load(), elapsed.Seconds(),
		float64(pool.sent.Load())/elapsed.Seconds())
}

func runFlappy() {
	log.Printf("=== Flappy Storm: %d nodes × %d cycles (fatal→healthy alternating) ===",
		*nodeCount, *eventsPerNode)
	log.Printf("    Forces FQ K8s API call on every event — max slowdown")

	pool := newPool(*socketPath, *workers)
	t0 := time.Now()
	deadline := time.Now().Add(time.Duration(*duration) * time.Second)

	// Producer goroutines — alternate fatal/healthy per node
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cycle := int64(id)
			for time.Now().Before(deadline) {
				batch := make([]*pb.HealthEvent, *batchSize)
				for i := range batch {
					node := nodeName(int(cycle) % *nodeCount)
					check := checkNames[cycle%int64(len(checkNames))]
					// Alternate: even cycles = fatal, odd = healthy
					if cycle%2 == 0 {
						batch[i] = fatalEvent(node, errorCodes[cycle%int64(len(errorCodes))], "0")
					} else {
						batch[i] = healthyEvent(node, check)
					}
					cycle += int64(*workers)
				}
				pool.jobs <- batch
			}
		}(w)
	}

	// Report every 5s
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for t := range tick.C {
			elapsed := t.Sub(t0)
			if elapsed > time.Duration(*duration)*time.Second {
				return
			}
			log.Printf("  t=%.0fs: sent=%d rate=%.0f/s failed=%d",
				elapsed.Seconds(), pool.sent.Load(),
				float64(pool.sent.Load())/elapsed.Seconds(),
				pool.failed.Load())
		}
	}()

	wg.Wait()
	pool.close()

	elapsed := time.Since(t0)
	log.Printf("Done: sent=%d failed=%d elapsed=%.1fs rate=%.0f/s",
		pool.sent.Load(), pool.failed.Load(), elapsed.Seconds(),
		float64(pool.sent.Load())/elapsed.Seconds())
	log.Printf("FQ should be severely behind — check fault_quarantine_event_backlog_count")
}

func runOplogStress() {
	total := *nodeCount * *eventsPerNode
	fatalPos := *fatalAt
	if fatalPos < 0 {
		fatalPos = total / 2
	}
	fatalNode := nodeName(0)

	log.Printf("=== Oplog Stress: %d nodes × %d events, fatal at #%d for %s ===",
		*nodeCount, *eventsPerNode, fatalPos, fatalNode)

	pool := newPool(*socketPath, *workers)
	bat := newBatcher(pool, *batchSize)

	t0 := time.Now()
	count := 0
	fatalSent := false
	var tFatal time.Time

	for n := 0; n < *nodeCount; n++ {
		name := nodeName(n)
		for j := 0; j < *eventsPerNode; j++ {
			if !fatalSent && count >= fatalPos {
				bat.flush()
				// Send fatal event as its own batch so we can timestamp it precisely
				pool.submit([]*pb.HealthEvent{
					fatalEvent(fatalNode, "79", "0"),
				})
				tFatal = time.Now()
				fatalSent = true
				log.Printf("  ⚡ Fatal event sent at t=%.3fs (position #%d)", time.Since(t0).Seconds(), count)
			}

			check := checkNames[rand.Intn(len(checkNames))]
			bat.add(nonFatalEvent(name, check))
			count++

			if count%5000 == 0 {
				log.Printf("  %d/%d non-fatal events sent (t=%.1fs)", count, total, time.Since(t0).Seconds())
			}
		}
	}

	if !fatalSent {
		bat.flush()
		pool.submit([]*pb.HealthEvent{fatalEvent(fatalNode, "79", "0")})
		tFatal = time.Now()
		log.Printf("  ⚡ Fatal event sent at END (t=%.3fs)", time.Since(t0).Seconds())
	}

	bat.flush()
	pool.close()

	elapsed := time.Since(t0)
	log.Printf("\nAll events sent in %.2fs (%.0f events/s)", elapsed.Seconds(),
		float64(pool.sent.Load())/elapsed.Seconds())
	log.Printf("Fatal event sent at t+%.3fs", tFatal.Sub(t0).Seconds())
	log.Printf("Non-fatal events ahead of fatal: ~%d", fatalPos)
	log.Printf("Now waiting for FQ to cordon %s ...", fatalNode)
	log.Printf("Monitor: kubectl logs -n nvsentinel -l app.kubernetes.io/name=fault-quarantine -f | grep %s", strings.Split(fatalNode, "-")[2])
}

func main() {
	flag.Parse()

	log.Printf("Event Injector — pattern=%s workers=%d socket=%s", *pattern, *workers, *socketPath)

	if _, err := os.Stat(*socketPath); err != nil {
		log.Fatalf("Socket not found: %s — is platform-connector running on this node?", *socketPath)
	}

	switch *pattern {
	case "fleet-storm":
		runFleetStorm()
	case "noisy-node":
		runNoisyNode()
	case "sustained":
		runSustained()
	case "flappy":
		runFlappy()
	case "oplog-stress":
		runOplogStress()
	default:
		log.Fatalf("Unknown pattern: %s (use fleet-storm|noisy-node|sustained|oplog-stress)", *pattern)
	}
}
