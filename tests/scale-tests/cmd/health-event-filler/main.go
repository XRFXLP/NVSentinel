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

// health-event-filler is one of the two atomic load generators for NVSentinel
// scale tests. It puts health events into the system at a controlled level
// (--count) and rate (--rate), planted at a chosen pipeline stage (--stage)
// and delivered so that either the live change stream fires or only a
// cold-start backlog is built (--deliver).
//
// The stage/delivery split matters because the MongoDB change streams differ
// per component:
//
//	fault-quarantine    watches INSERT
//	node-drainer        watches UPDATE of healtheventstatus.nodequarantined
//	fault-remediation   watches UPDATE of healtheventstatus.userpodsevictionstatus
//
// So a document inserted with nodequarantined already set is invisible to
// node-drainer's live path. Use --deliver=stream to drive the live path and
// --deliver=seed to build a backlog for cold-start scenarios.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	sinkMongo = "mongo"
	sinkGRPC  = "grpc"
)

type config struct {
	// what
	stage    string
	deliver  string
	count    int
	rate     float64
	duration time.Duration

	// targeting
	nodes      int
	nodePrefix string
	nodeStart  int

	// content
	agent          string
	componentClass string
	checkName      string
	errorCodes     string
	message        string
	entities       int
	fatalRatio     float64
	strategy       string
	action         string

	// sink
	sink   string
	socket string
	db     string
	col    string

	// throughput
	workers int
	batch   int

	// node state
	syncNodeState bool
	apiServer     string

	dryRun  bool
	verbose bool
}

func main() {
	var (
		cfg  config
		auth mongoAuth
	)

	flag.StringVar(&cfg.stage, "stage", stageFresh,
		"pipeline stage to plant events at: fresh|quarantined|drained|remediated|noise")
	flag.StringVar(&cfg.deliver, "deliver", deliverSeed,
		"seed (insert only; backlog) | stream (insert+update; fires the change stream)")
	flag.IntVar(&cfg.count, "count", 1000, "total events to create (the level, x)")
	flag.Float64Var(&cfg.rate, "rate", 0, "events/sec; 0 = unthrottled (the flux, dx/dt)")
	flag.DurationVar(&cfg.duration, "duration", 0, "stop after this long regardless of --count (0 = no limit)")

	flag.IntVar(&cfg.nodes, "nodes", 100, "number of distinct nodes to spread events over")
	flag.StringVar(&cfg.nodePrefix, "node-prefix", "kwok-node-", "node name prefix")
	flag.IntVar(&cfg.nodeStart, "node-start", 0, "first node index")

	flag.StringVar(&cfg.agent, "agent", "gpu-health-monitor",
		"event agent; fault-quarantine ignores events from 'event-generator'")
	flag.StringVar(&cfg.componentClass, "component-class", "GPU", "component class")
	flag.StringVar(&cfg.checkName, "check-name", "GpuXidError", "check name")
	flag.StringVar(&cfg.errorCodes, "error-codes", "XID79,XID80,XID81,XID74,XID92,XID48,XID31,XID63",
		"comma-separated error codes to cycle through (varies the dedup signature)")
	flag.StringVar(&cfg.message, "message", "", "event message")
	flag.IntVar(&cfg.entities, "entities", 8, "distinct entity values (GPU indices) to cycle")
	flag.Float64Var(&cfg.fatalRatio, "fatal-ratio", 1.0, "fraction of events marked isFatal")
	flag.StringVar(&cfg.action, "recommended-action", "RESTART_BM",
		"NONE|COMPONENT_RESET|RESTART_BM|RESTART_VM|REPLACE_VM|CONTACT_SUPPORT|RUN_FIELDDIAG|RUN_DCGMEUD "+
			"-- fault-remediation skips NONE")
	flag.StringVar(&cfg.strategy, "processing-strategy", "EXECUTE_REMEDIATION",
		"UNSPECIFIED|EXECUTE_REMEDIATION|STORE_ONLY|STORE_AND_ANALYSE")

	flag.StringVar(&cfg.sink, "sink", sinkMongo,
		"mongo (direct insert, high rate) | grpc (through platform-connector)")
	flag.StringVar(&cfg.socket, "socket", "/var/run/nvsentinel/nvsentinel.sock",
		"platform-connector UDS (grpc sink)")
	flag.StringVar(&cfg.db, "db", "HealthEventsDatabase", "database name")
	flag.StringVar(&cfg.col, "col", "HealthEvents", "collection name")

	flag.StringVar(&auth.URI, "mongo-uri", "", "MongoDB URI (or MONGO_URI)")
	flag.StringVar(&auth.CertDir, "mongo-cert-dir", "/etc/ssl/client-certs",
		"directory holding tls.crt, tls.key, ca.crt")
	flag.StringVar(&auth.User, "mongo-user", "", "SCRAM username (or MONGODB_USER)")
	flag.StringVar(&auth.Password, "mongo-password", "", "SCRAM password (or MONGODB_PASSWORD)")
	flag.StringVar(&auth.AuthDB, "mongo-auth-db", "admin", "SCRAM auth source database")
	flag.StringVar(&auth.Mech, "mongo-auth", authAuto, "auto|x509|scram|none")
	flag.BoolVar(&auth.Insecure, "tls-insecure", false,
		"skip server certificate verification (needed when connecting via port-forward)")
	flag.Uint64Var(&auth.PoolSize, "max-pool-size", 0, "MongoDB maxPoolSize (0 = driver default of 100)")
	flag.StringVar(&auth.WriteConcern, "write-concern", "",
		"default|majority|1|0 -- '1' skips waiting for replication and is much faster for seeding")
	flag.BoolVar(&auth.Journal, "journal", true, "require journal commit (ignored when --write-concern=0)")
	flag.StringVar(&auth.Compressors, "compressors", "",
		"wire compressors, e.g. snappy or zstd (empty = none)")

	flag.IntVar(&cfg.workers, "workers", 50, "parallel writers")
	flag.IntVar(&cfg.batch, "batch", 500, "documents per bulk write")

	flag.BoolVar(&cfg.syncNodeState, "sync-node-state", false,
		"also patch node labels/cordon to match --stage, so components do not no-op")
	flag.StringVar(&cfg.apiServer, "api-server", "https://kubernetes.default.svc",
		"Kubernetes API server for --sync-node-state")

	flag.BoolVar(&cfg.dryRun, "dry-run", false, "print the plan and exit")
	flag.BoolVar(&cfg.verbose, "v", false, "verbose logging")
	flag.Parse()

	if err := run(cfg, &auth); err != nil {
		log.Fatalf("health-event-filler: %v", err)
	}
}

func strategyValue(name string) (int32, error) {
	switch strings.ToUpper(name) {
	case "UNSPECIFIED":
		return psUnspecified, nil
	case "EXECUTE_REMEDIATION":
		return psExecute, nil
	case "STORE_ONLY":
		return psStoreOnly, nil
	case "STORE_AND_ANALYSE":
		return psStoreAnalyse, nil
	default:
		return 0, fmt.Errorf("unknown --processing-strategy %q", name)
	}
}

func run(cfg config, auth *mongoAuth) error {
	if err := validate(cfg.stage, cfg.deliver); err != nil {
		return err
	}

	strategy, err := strategyValue(cfg.strategy)
	if err != nil {
		return err
	}

	action, ok := recommendedActions[strings.ToUpper(cfg.action)]
	if !ok {
		return fmt.Errorf("unknown --recommended-action %q", cfg.action)
	}

	spec := &eventSpec{
		Agent:          cfg.agent,
		ComponentClass: cfg.componentClass,
		CheckName:      cfg.checkName,
		ErrorCodes:     strings.Split(cfg.errorCodes, ","),
		Message:        cfg.message,
		Entities:       cfg.entities,
		FatalRatio:     cfg.fatalRatio,
		Strategy:       strategy,
		Action:         action,
	}

	nodes := make([]string, cfg.nodes)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("%s%06d", cfg.nodePrefix, cfg.nodeStart+i)
	}

	log.Printf("plan: %d events over %d nodes | stage=%s deliver=%s sink=%s rate=%s",
		cfg.count, cfg.nodes, cfg.stage, cfg.deliver, cfg.sink, rateLabel(cfg.rate))
	log.Printf("consumed by: %s", consumerHint(cfg.stage, cfg.deliver))

	if cfg.deliver == deliverStream {
		log.Printf("note: stream mode writes 2 ops per event (insert + update), doubling oplog volume")
	}

	if cfg.dryRun {
		log.Printf("dry run: nothing written")
		return nil
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if cfg.duration > 0 {
		var stop context.CancelFunc

		ctx, stop = context.WithTimeout(ctx, cfg.duration)
		defer stop()
	}

	if cfg.syncNodeState {
		if err := syncNodeState(cfg, nodes); err != nil {
			return err
		}
	}

	auth.resolve()

	switch cfg.sink {
	case sinkMongo:
		return runMongo(ctx, cfg, auth, spec, nodes)
	case sinkGRPC:
		return runGRPC(ctx, cfg, spec, nodes)
	default:
		return fmt.Errorf("unknown --sink %q", cfg.sink)
	}
}

func syncNodeState(cfg config, nodes []string) error {
	label, cordon, ok := nodeStateFor(cfg.stage)
	if !ok {
		log.Printf("node sync: stage %q implies no node state; skipping", cfg.stage)
		return nil
	}

	patcher, err := newNodePatcher(cfg.apiServer)
	if err != nil {
		return fmt.Errorf("node sync: %w", err)
	}

	start := time.Now()
	patcher.syncNodes(nodes, label, cordon, cfg.workers)
	log.Printf("node sync: labelled %d, failed %d (%s=%s, cordon=%t) in %s",
		patcher.patched.Load(), patcher.failed.Load(),
		nodeStateLabelKey, label, cordon, time.Since(start).Round(time.Millisecond))

	return nil
}

// pacer throttles batch dispatch to approximate a target events/sec.
type pacer struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newPacer(ratePerSec float64, batch int) *pacer {
	if ratePerSec <= 0 {
		return nil
	}

	return &pacer{
		interval: time.Duration(float64(batch) / ratePerSec * float64(time.Second)),
		next:     time.Now(),
	}
}

func (p *pacer) wait(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	now := time.Now()

	if p.next.Before(now) {
		p.next = now
	}

	delay := p.next.Sub(now)
	p.next = p.next.Add(p.interval)
	p.mu.Unlock()

	if delay <= 0 {
		return
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type counters struct {
	written atomic.Int64
	updated atomic.Int64
	failed  atomic.Int64
}

func runMongo(ctx context.Context, cfg config, auth *mongoAuth, spec *eventSpec, nodes []string) error {
	client, err := auth.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(cfg.db).Collection(cfg.col)

	// Precursor state only differs from the terminal state in stream mode.
	precursor := cfg.deliver == deliverStream

	type job struct{ start, n int }

	jobs := make(chan job, cfg.workers*2)
	pace := newPacer(cfg.rate, cfg.batch)

	var (
		wg   sync.WaitGroup
		cnt  counters
		errs sync.Once
		perr error
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

				if err := writeBatch(ctx, coll, cfg, spec, nodes, j.start, j.n, precursor, &cnt); err != nil {
					if cnt.failed.Add(int64(j.n)); cfg.verbose {
						log.Printf("batch at %d: %v", j.start, err)
					}

					errs.Do(func() { perr = err })
				}
			}
		}()
	}

	start := time.Now()
	done := reportProgress(ctx, &cnt, cfg.count, start)

	for i := 0; i < cfg.count && ctx.Err() == nil; i += cfg.batch {
		n := min(cfg.batch, cfg.count-i)
		jobs <- job{start: i, n: n}
	}

	close(jobs)
	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	log.Printf("done: inserted=%d updated=%d failed=%d in %s (%.0f events/s)",
		cnt.written.Load(), cnt.updated.Load(), cnt.failed.Load(),
		elapsed.Round(time.Millisecond),
		float64(cnt.written.Load())/elapsed.Seconds())

	if cnt.written.Load() == 0 && perr != nil {
		return perr
	}

	return nil
}

// writeBatch inserts a batch and, in stream mode, promotes it with a second
// update so the change stream sees the transition.
func writeBatch(
	ctx context.Context,
	coll *mongo.Collection,
	cfg config,
	spec *eventSpec,
	nodes []string,
	start, n int,
	precursor bool,
	cnt *counters,
) error {
	now := time.Now().UTC()
	docs := make([]any, 0, n)

	for i := range n {
		seq := start + i
		docs = append(docs, spec.makeDoc(nodes[seq%len(nodes)], seq, cfg.stage, precursor, now))
	}

	res, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if res != nil {
		cnt.written.Add(int64(len(res.InsertedIDs)))
	}

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	if !precursor {
		return nil
	}

	update, err := updateFor(cfg.stage)
	if err != nil {
		return err
	}

	// Update by the ids just inserted so each document produces its own
	// change stream event with the right key in updatedFields.
	ures, err := coll.UpdateMany(ctx, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$in", Value: res.InsertedIDs},
	}}}, update)
	if err != nil {
		return fmt.Errorf("promote to %s: %w", cfg.stage, err)
	}

	cnt.updated.Add(ures.ModifiedCount)

	return nil
}

func reportProgress(ctx context.Context, cnt *counters, total int, start time.Time) chan struct{} {
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				w := cnt.written.Load()
				log.Printf("progress: %d/%d (%.0f events/s, failed=%d)",
					w, total, float64(w)/time.Since(start).Seconds(), cnt.failed.Load())
			}
		}
	}()

	return done
}

func rateLabel(r float64) string {
	if r <= 0 {
		return "unthrottled"
	}

	return fmt.Sprintf("%.0f/s", r)
}
