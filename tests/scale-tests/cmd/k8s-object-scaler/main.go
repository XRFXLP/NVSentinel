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

// k8s-object-scaler is one of the two atomic load generators for NVSentinel
// scale tests. It drives the number of Kubernetes objects that exist (--count,
// the level) and how fast they arrive or turn over (--rate / --churn-rate, the
// flux). Those are separate controls on purpose: "100k nodes exist" and "100k
// nodes are changing" load completely different paths.
//
// Modes:
//
//	ramp   0 -> count at --rate            (transient; measures time-to-N)
//	hold   sit at count, no writes         (steady level; measures resident cost)
//	churn  hold count, replace at --churn-rate (steady level, nonzero flux)
//	drain  delete count objects
//
// It talks to the API server over plain HTTP with the pod's service account
// token rather than through client-go, because client-go's default QPS 5 /
// burst 10 limiter is exactly the bottleneck this tool exists to avoid.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	kindNode = "node"
	kindPod  = "pod"

	modeRamp     = "ramp"
	modeHold     = "hold"
	modeChurn    = "churn"
	modeDrain    = "drain"
	modeResize   = "resize"
	modeWorkload = "workload"
)

type config struct {
	kind         string
	mode         string
	jobRate      float64
	gangFraction float64
	gangMin      int
	gangMax      int
	jobLifetime  time.Duration
	nodeCount    int
	count        int
	startIdx     int
	rate         float64
	churnRate    float64
	duration     time.Duration

	namePrefix   string
	namespace    string
	nodePrefix   string
	spreadNodes  int
	nodeStartIdx int
	gpus         string

	retained       int
	stripped       int
	profile        string
	managedFields  int
	nodeLabels     int
	prevNodeLabels int
	nodeLabelBytes int
	ownerKind      string
	podProfile     string
	podLabels   string

	concurrency int
	apiServer   string
	insecure    bool
	dryRun      bool
}

func main() {
	var cfg config

	flag.StringVar(&cfg.kind, "kind", kindNode, "node | pod")
	flag.StringVar(&cfg.mode, "mode", modeRamp, "ramp | hold | churn | drain | resize")
	flag.IntVar(&cfg.count, "count", 100, "target object count (the level, x)")
	flag.IntVar(&cfg.startIdx, "start-index", 0, "first object index")
	flag.Float64Var(&cfg.rate, "rate", 0, "objects/sec during ramp; 0 = unthrottled (dx/dt)")
	flag.Float64Var(&cfg.churnRate, "churn-rate", 10, "replacements/sec in churn mode")
	flag.DurationVar(&cfg.duration, "duration", 0, "how long to hold or churn (0 = forever)")

	flag.StringVar(&cfg.namePrefix, "name-prefix", "", "object name prefix (default kwok-node- / bench-pod-)")
	flag.StringVar(&cfg.namespace, "namespace", "benchmark", "namespace (pods only)")
	flag.StringVar(&cfg.nodePrefix, "node-prefix", "kwok-node-", "node name prefix pods are bound to")
	flag.IntVar(&cfg.spreadNodes, "spread-nodes", 100, "spread pods across this many nodes")
	flag.IntVar(&cfg.nodeStartIdx, "node-start-index", 0,
		"first node index pods bind to; must match the nodes' --start-index")
	flag.StringVar(&cfg.gpus, "gpus", "", "nvidia.com/gpu limit per pod (empty = none)")

	flag.IntVar(&cfg.retained, "pad-retained-bytes", 0,
		"padding placed in fields transforms KEEP (annotations)")
	flag.IntVar(&cfg.prevNodeLabels, "prev-node-labels", 0,
		"in resize mode, the --node-labels of the point being replaced, so its extra labels are deleted")
	flag.IntVar(&cfg.stripped, "pad-stripped-bytes", 0,
		"padding placed in fields transforms DROP (node status.images / pod container env)")
	flag.StringVar(&cfg.profile, "pad-profile", profileNone,
		"none|kom|janitor|nd|preflight -- warns when padding is invisible to that component")
	flag.IntVar(&cfg.managedFields, "managed-fields-bytes", 0,
		"bytes to account for metadata.managedFields; the API server owns that field "+
			"so the budget is folded into labels (nodes) or spec (pods), which the same "+
			"transforms retain or drop. 0 uses production defaults")
	flag.IntVar(&cfg.nodeLabels, "node-labels", 180,
		"number of labels per node; production GPU worker nodes carry ~180 (~9 KB)")
	flag.IntVar(&cfg.nodeLabelBytes, "node-label-bytes", 9000,
		"total size of node labels; production is ~9 KB across ~180 labels")
	flag.StringVar(&cfg.ownerKind, "owner-kind", "none",
		"pod ownerReferences kind, e.g. DaemonSet. DaemonSet-owned pods are not "+
			"evicted by node-drainer, which is ~80% of a production fleet")
	flag.Float64Var(&cfg.jobRate, "job-rate", 1.0,
		"workload mode: customer jobs arriving per second")
	flag.Float64Var(&cfg.gangFraction, "gang-fraction", 0.3,
		"workload mode: fraction of jobs that are gangs placed across consecutive nodes; the rest are single pods")
	flag.IntVar(&cfg.gangMin, "gang-min", 8,
		"workload mode: smallest gang size")
	flag.IntVar(&cfg.gangMax, "gang-max", 64,
		"workload mode: largest gang size")
	flag.DurationVar(&cfg.jobLifetime, "job-lifetime", 30*time.Minute,
		"workload mode: how long a job's pods live before being deleted; sets the steady-state population with --job-rate")
	flag.IntVar(&cfg.nodeCount, "node-count", 0,
		"workload mode: number of fleet nodes jobs may land on, starting at --node-start-index")
	flag.StringVar(&cfg.podLabels, "pod-labels", "",
		"extra pod labels as k=v,k=v; merged over the benchmark defaults. Components "+
			"select the pods they cache by label, so a pod without the right label is "+
			"invisible to them (labeler selects app in (nvidia-dcgm,nvidia-driver-daemonset))")
	flag.StringVar(&cfg.podProfile, "pod-profile", profileUser,
		"user (~50 KB, ~80% spec) | system (~14 KB)")

	flag.IntVar(&cfg.concurrency, "concurrency", 200, "parallel in-flight requests")
	flag.StringVar(&cfg.apiServer, "api-server", "https://kubernetes.default.svc", "API server URL")
	flag.BoolVar(&cfg.insecure, "insecure", false, "skip API server certificate verification")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "print the plan and one sample object, then exit")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatalf("k8s-object-scaler: %v", err)
	}
}

func defaultPrefix(kind string) string {
	if kind == kindNode {
		return "kwok-node-"
	}

	return "bench-pod-"
}

func run(cfg config) error {
	if cfg.kind != kindNode && cfg.kind != kindPod {
		return fmt.Errorf("--kind must be node or pod, got %q", cfg.kind)
	}

	if cfg.namePrefix == "" {
		cfg.namePrefix = defaultPrefix(cfg.kind)
	}

	spec := &objectSpec{
		Kind:        cfg.kind,
		NamePrefix:  cfg.namePrefix,
		Namespace:   cfg.namespace,
		SpreadNodes: cfg.spreadNodes,
		NodePrefix:  cfg.nodePrefix,
		NodeStart:   cfg.nodeStartIdx,
		Retained:    cfg.retained,
		Stripped:    cfg.stripped,
		Profile:     cfg.profile,
		GPUs:        cfg.gpus,

		ManagedFields:  cfg.managedFields,
		NodeLabels:     cfg.nodeLabels,
		NodeLabelBytes: cfg.nodeLabelBytes,
		OwnerKind:      cfg.ownerKind,
		PodProfile:     cfg.podProfile,
		PodLabels:      parsePodLabels(cfg.podLabels),
	}

	// Nodes only need the shortfall in managedFields compensated. The API
	// server generates the field itself as controllers touch the object -- a
	// created node accumulates ~13 KB across four managers (the creating
	// client, gpu-operator, kube-controller-manager and kwok) within seconds.
	// Production carries ~18 KB, so ~5 KB is the gap. Compensating the full
	// 18 KB double-counts and produces a 65 KB node against a 50-55 KB target.
	// The defaults apply only when the flag was not given at all. Testing for a
	// zero value instead would make an explicit --managed-fields-bytes=0 mean
	// 5000, which silently removes the low end of a node-size sweep: the point
	// meant to carry no padding carries 9 KB like every other point.
	if cfg.kind == kindNode {
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })

		if !set["managed-fields-bytes"] {
			spec.ManagedFields = 5000
		}

		if !set["pad-retained-bytes"] {
			spec.Retained = 4000 // production nodes carry ~4 KB of annotations
		}
	}

	log.Printf("plan: mode=%s kind=%s count=%d rate=%s concurrency=%d",
		cfg.mode, cfg.kind, cfg.count, rateLabel(cfg.rate), cfg.concurrency)

	if cfg.retained > 0 && !profileRetains(cfg.profile) {
		log.Printf("WARNING: --pad-retained-bytes=%d is placed in annotations, but the %q "+
			"transform drops annotations. That padding will be invisible to it; "+
			"use --pad-stripped-bytes to model cost it does not pay, or pick another profile.",
			cfg.retained, cfg.profile)
	}

	if cfg.dryRun {
		body, path, err := spec.build(cfg.startIdx)
		if err != nil {
			return err
		}

		log.Printf("sample POST %s (%d bytes):\n%s", path, len(body), string(body))

		return nil
	}

	client, err := newAPIClient(cfg.apiServer, cfg.concurrency, cfg.insecure)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// A pod carrying an ownerReference to a UID that does not exist is deleted
	// by the garbage collector as an orphan, so the owner must exist first.
	if cfg.kind == kindPod && cfg.ownerKind != "" && cfg.ownerKind != "none" {
		spec.OwnerName = "bench-owner-" + strings.ToLower(cfg.ownerKind)

		uid, err := client.ensureOwner(ctx, cfg.namespace, cfg.ownerKind, spec.OwnerName)
		if err != nil {
			return err
		}

		spec.OwnerUID = uid

		log.Printf("owner %s/%s uid=%s", cfg.ownerKind, spec.OwnerName, uid)
	}

	if cfg.duration > 0 && (cfg.mode == modeHold || cfg.mode == modeChurn) {
		var stop context.CancelFunc

		ctx, stop = context.WithTimeout(ctx, cfg.duration)
		defer stop()
	}

	start := time.Now()

	switch cfg.mode {
	case modeRamp:
		err = ramp(ctx, client, spec, cfg)
	case modeDrain:
		err = drain(ctx, client, spec, cfg)
	case modeHold:
		log.Printf("holding at %d objects; nothing is being written", cfg.count)
		<-ctx.Done()
	case modeChurn:
		err = churn(ctx, client, spec, cfg)
	case modeResize:
		err = resize(ctx, client, spec, cfg)
	case modeWorkload:
		err = workload(ctx, client, spec, cfg)
	default:
		return fmt.Errorf("unknown --mode %q", cfg.mode)
	}

	report(client, start)

	return err
}

// ramp creates objects [startIdx, startIdx+count) with bounded concurrency.
func ramp(ctx context.Context, c *apiClient, spec *objectSpec, cfg config) error {
	idx := make(chan int, cfg.concurrency*2)
	pace := newPacer(cfg.rate)

	var wg sync.WaitGroup

	for range cfg.concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range idx {
				if ctx.Err() != nil {
					return
				}

				pace.wait(ctx)

				body, path, err := spec.build(i)
				if err != nil {
					c.failed.Add(1)
					continue
				}

				if err := c.do(ctx, http.MethodPost, path, body); err != nil {
					if c.failed.Load() <= 5 {
						log.Printf("create %d: %v", i, err)
					}

					continue
				}

				c.created.Add(1)
			}
		}()
	}

	done := progress(ctx, c, cfg.count)

	for i := cfg.startIdx; i < cfg.startIdx+cfg.count && ctx.Err() == nil; i++ {
		idx <- i
	}

	close(idx)
	wg.Wait()
	close(done)

	return nil
}

// drain deletes objects [startIdx, startIdx+count).
func drain(ctx context.Context, c *apiClient, spec *objectSpec, cfg config) error {
	idx := make(chan int, cfg.concurrency*2)
	pace := newPacer(cfg.rate)

	var wg sync.WaitGroup

	for range cfg.concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range idx {
				if ctx.Err() != nil {
					return
				}

				pace.wait(ctx)

				if err := c.do(ctx, http.MethodDelete, spec.deletePath(i), nil); err != nil {
					continue
				}

				c.deleted.Add(1)
			}
		}()
	}

	done := progress(ctx, c, cfg.count)

	for i := cfg.startIdx; i < cfg.startIdx+cfg.count && ctx.Err() == nil; i++ {
		idx <- i
	}

	close(idx)
	wg.Wait()
	close(done)

	return nil
}

// resize rewrites the size-bearing metadata of objects [startIdx, startIdx+count)
// without recreating them, so a size sweep can hold everything except node bytes
// constant. --prev-node-labels names the label count of the point being replaced
// so labels from a larger previous point are removed rather than left behind.
func resize(ctx context.Context, c *apiClient, spec *objectSpec, cfg config) error {
	patch, err := spec.resizePatch(cfg.prevNodeLabels)
	if err != nil {
		return fmt.Errorf("build resize patch: %w", err)
	}

	idx := make(chan int, cfg.concurrency*2)
	pace := newPacer(cfg.rate)

	var wg sync.WaitGroup

	for range cfg.concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range idx {
				if ctx.Err() != nil {
					return
				}

				pace.wait(ctx)

				if err := c.patch(ctx, spec.patchPath(i), patch); err != nil {
					if c.failed.Load() <= 5 {
						log.Printf("resize %d: %v", i, err)
					}

					continue
				}

				c.created.Add(1)
			}
		}()
	}

	done := progress(ctx, c, cfg.count)

	for i := cfg.startIdx; i < cfg.startIdx+cfg.count && ctx.Err() == nil; i++ {
		idx <- i
	}

	close(idx)
	wg.Wait()
	close(done)

	return nil
}

// churn holds the object count steady while replacing objects at a fixed rate,
// which separates "N objects exist" from "N objects are changing".
func churn(ctx context.Context, c *apiClient, spec *objectSpec, cfg config) error {
	if cfg.churnRate <= 0 {
		return fmt.Errorf("--churn-rate must be > 0 in churn mode")
	}

	interval := time.Duration(float64(time.Second) / cfg.churnRate)
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	log.Printf("churning %d objects at %.1f/s (delete+recreate)", cfg.count, cfg.churnRate)

	next := cfg.startIdx

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			i := next
			next++

			if next >= cfg.startIdx+cfg.count {
				next = cfg.startIdx
			}

			go func(i int) {
				if err := c.do(ctx, http.MethodDelete, spec.deletePath(i), nil); err == nil {
					c.deleted.Add(1)
				}

				body, path, err := spec.build(i)
				if err != nil {
					return
				}

				if err := c.do(ctx, http.MethodPost, path, body); err == nil {
					c.created.Add(1)
				}
			}(i)
		}
	}
}

// pacer throttles dispatch to approximate a target objects/sec.
type pacer struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newPacer(ratePerSec float64) *pacer {
	if ratePerSec <= 0 {
		return nil
	}

	return &pacer{
		interval: time.Duration(float64(time.Second) / ratePerSec),
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

func progress(ctx context.Context, c *apiClient, total int) chan struct{} {
	done := make(chan struct{})
	start := time.Now()

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
				n := c.created.Load() + c.deleted.Load()
				log.Printf("progress: %d/%d (%.0f obj/s, 409=%d, 429=%d, failed=%d)",
					n, total, float64(n)/time.Since(start).Seconds(),
					c.conflicts.Load(), c.throttled.Load(), c.failed.Load())
			}
		}
	}()

	return done
}

func report(c *apiClient, start time.Time) {
	elapsed := time.Since(start)
	n := c.created.Load() + c.deleted.Load()

	log.Printf("done: created=%d deleted=%d existed=%d throttled=%d failed=%d in %s (%.0f obj/s)",
		c.created.Load(), c.deleted.Load(), c.conflicts.Load(),
		c.throttled.Load(), c.failed.Load(),
		elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())

	if c.throttled.Load() > 0 {
		log.Printf("note: %d requests hit API Priority and Fairness throttling; "+
			"lower --concurrency or accept this as the cluster's write ceiling", c.throttled.Load())
	}
}

func rateLabel(r float64) string {
	if r <= 0 {
		return "unthrottled"
	}

	return fmt.Sprintf("%.0f/s", r)
}

// workload simulates customer jobs arriving on the fleet, rather than a static
// population of pods created once.
//
// This matters because node-drainer caches the whole pod object only for pods it
// could actually evict, and reduces system-namespace and DaemonSet-owned pods to
// bare identity. A fleet whose pods are all DaemonSet fan-out therefore holds
// node-drainer at its floor and drains nothing, so both its memory figure and
// the drain latency are unrepresentative.
//
// Jobs arrive at --job-rate. A fraction --gang-fraction of them are gangs: a
// single job placing --gang-min..--gang-max pods on consecutive nodes at once,
// the way a distributed training job lands. The rest are single pods, the way
// inference and batch work lands. Every job is deleted after --job-lifetime, so
// the fleet reaches a steady state of roughly job-rate x lifetime jobs rather
// than growing without bound.
func workload(ctx context.Context, c *apiClient, spec *objectSpec, cfg config) error {
	if cfg.jobRate <= 0 {
		return fmt.Errorf("--job-rate must be > 0 in workload mode")
	}

	if cfg.nodeCount <= 0 {
		return fmt.Errorf("--node-count must be > 0 in workload mode")
	}

	interval := time.Duration(float64(time.Second) / cfg.jobRate)
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	avgGang := float64(cfg.gangMin+cfg.gangMax) / 2
	avgPods := cfg.gangFraction*avgGang + (1 - cfg.gangFraction)
	steady := cfg.jobRate * cfg.jobLifetime.Seconds() * avgPods

	log.Printf("workload: %.2f jobs/s, %.0f%% gangs of %d-%d, lifetime %s -> steady state ~%.0f pods (%.2f/node)",
		cfg.jobRate, cfg.gangFraction*100, cfg.gangMin, cfg.gangMax,
		cfg.jobLifetime, steady, steady/float64(cfg.nodeCount))

	var (
		seq  atomic.Int64
		live sync.WaitGroup
	)

	for {
		select {
		case <-ctx.Done():
			log.Printf("workload stopping; waiting for in-flight jobs to be cleaned up")
			live.Wait()

			return nil
		case <-ticker.C:
			jobID := seq.Add(1)

			size := 1
			if rand.Float64() < cfg.gangFraction { //nolint:gosec
				size = cfg.gangMin
				if cfg.gangMax > cfg.gangMin {
					size += rand.Intn(cfg.gangMax - cfg.gangMin + 1) //nolint:gosec
				}
			}

			// Place the gang on consecutive nodes from a random offset, which is
			// what a scheduler honouring pod affinity tends to produce.
			offset := rand.Intn(cfg.nodeCount) //nolint:gosec

			live.Add(1)

			go func(jobID int64, size, offset int) {
				defer live.Done()
				runJob(ctx, c, spec, cfg, jobID, size, offset)
			}(jobID, size, offset)
		}
	}
}

// runJob creates a job's pods, holds them for the configured lifetime, then
// deletes them. Deletion uses a background context so a cancelled run still
// cleans up rather than stranding pods on the fleet.
func runJob(ctx context.Context, c *apiClient, spec *objectSpec, cfg config, jobID int64, size, offset int) {
	names := make([]string, 0, size)

	for i := range size {
		node := (offset + i) % cfg.nodeCount
		name := fmt.Sprintf("%sjob%d-%d", cfg.namePrefix, jobID, i)

		body, path, err := spec.buildWorkloadPod(name, cfg.nodeStartIdx+node)
		if err != nil {
			continue
		}

		if err := c.do(ctx, http.MethodPost, path, body); err == nil {
			c.created.Add(1)

			names = append(names, name)
		}
	}

	select {
	case <-ctx.Done():
	case <-time.After(cfg.jobLifetime):
	}

	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()

	for _, name := range names {
		if err := c.do(cleanup, http.MethodDelete, spec.namedDeletePath(name), nil); err == nil {
			c.deleted.Add(1)
		}
	}
}

// parsePodLabels turns "k=v,k=v" into a map. Empty input yields nil, which
// leaves the benchmark defaults untouched.
func parsePodLabels(spec string) map[string]string {
	if spec == "" {
		return nil
	}

	out := map[string]string{}

	for _, pair := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || k == "" {
			continue
		}

		out[k] = v
	}

	return out
}
