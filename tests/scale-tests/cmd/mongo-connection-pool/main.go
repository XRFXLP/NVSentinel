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

// mongo-connection-pool simulates many platform-connector MongoDB clients from a
// single process.
//
// Each simulated connector is one goroutine holding its own mongo.Client, and
// therefore its own connection pool, which is what determines the server-side
// connection count. Running the real platform-connector binary once per
// simulated connector needs a container each and about 10 MB of pod memory, so
// five worker nodes cap out near ten thousand connectors. Goroutines carry the
// connection without the container, so a single pod holds thousands.
//
// The real connector sets no maxPoolSize, so it gets the driver default of 100.
// Its insert path is a single sequential goroutine, so the pool holds one socket
// in steady state and only grows under retry overlap -- which is exactly the
// behaviour a burst is meant to expose. --pool-size therefore defaults to 0
// (leave the driver alone); setting it clamps that burst response and makes the
// simulated fleet look better behaved than a real one.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type stats struct {
	connected atomic.Int64
	failed    atomic.Int64
	inserts   atomic.Int64
	errors    atomic.Int64
}

// podOrdinal reads the trailing ordinal from the pod name, which for a
// StatefulSet is stable across restarts and unique per replica. POD_NAME is
// preferred over the hostname so the behaviour does not change if the pod's
// hostname is overridden.
func podOrdinal() (int, error) {
	name := os.Getenv("POD_NAME")
	if name == "" {
		h, err := os.Hostname()
		if err != nil {
			return 0, err
		}

		name = h
	}

	i := strings.LastIndex(name, "-")
	if i < 0 {
		return 0, fmt.Errorf("no ordinal suffix in %q", name)
	}

	return strconv.Atoi(name[i+1:])
}

func main() {
	var (
		connectors = flag.Int("connectors", 100, "simulated connectors, one goroutine and one client each")
		appName    = flag.String("app-name", "platform-connector", "value sent as appName in the client handshake; matches the real connector's APP_NAME")
		burstNodes = flag.Int("burst-nodes", 0,
			"one-shot mode: emit a single fatal event for this many distinct nodes as fast as possible, then exit; simulates a correlated failure such as a rack or switch loss")
		burstClients = flag.Int("burst-clients", 20,
			"clients used to spread a burst; a burst is about how many nodes fail at once, not how many connections are held")
		prime = flag.Bool("prime", true,
			"open the application connection to the primary at startup with a no-match read, so the connection count matches a real fleet without injecting a burst of events")
		poolSize = flag.Uint64("pool-size", 0, "maxPoolSize per client; 0 leaves the driver default of 100, which is what the real connector uses")
		rate     = flag.Float64("rate", 0.1, "health events per second per connector")
		fatalPM  = flag.Int("fatal-permille", 1675,
			"share of events marked fatal, per ten thousand; 1675 is the production share, 2000 is a 1:4 fatal:non-fatal ratio")
		nodeSpan = flag.Int("node-span", 1,
			"nodes each connector writes for, cycling through them. Decouples the "+
				"fleet-wide event rate from the connection count: one connector per node "+
				"means the event rate cannot be raised without also raising connections, "+
				"and the datastore's connection ceiling then blocks measurements that have "+
				"nothing to do with connections")
		shardStride = flag.Int("shard-stride", 0,
			"if >0, derive --node-start-index from the pod's StatefulSet ordinal as ordinal*stride; "+
				"lets one StatefulSet cover a fleet with each replica writing for a distinct node range")
		database   = flag.String("database", "HealthEventsDatabase", "database")
		collection = flag.String("collection", "HealthEvents", "collection")
		rampMS     = flag.Int("ramp-ms", 20, "delay between starting connectors, to avoid a connection storm")
		duration   = flag.Duration("duration", 0, "run time; 0 runs until signalled")
		agent      = flag.String("agent", "gpu-health-monitor", "health event agent; fault-quarantine rulesets match on this")
		nodePrefix = flag.String("node-prefix", "pool-node-", "node name prefix for generated events")
		nodeStart  = flag.Int("node-start-index", 0, "first node index; connector i writes for node-start-index+i")
		nodeWidth  = flag.Int("node-index-width", 6, "zero-padded width of the node index")
	)

	auth := &mongoAuth{}
	flag.StringVar(&auth.URI, "mongo-uri", "", "MongoDB URI (or MONGO_URI)")
	flag.StringVar(&auth.CertDir, "mongo-cert-dir", "/etc/ssl/client-certs", "directory holding tls.crt/tls.key/ca.crt")
	flag.StringVar(&auth.AuthDB, "mongo-auth-db", "admin", "SCRAM auth source database")
	flag.StringVar(&auth.Mech, "mongo-auth", "auto", "auto|x509|scram|none")
	flag.StringVar(&auth.User, "mongo-user", "", "SCRAM user (or MONGODB_USER)")
	flag.StringVar(&auth.Password, "mongo-password", "", "SCRAM password (or MONGODB_PASSWORD)")
	flag.BoolVar(&auth.Insecure, "mongo-insecure", false, "skip TLS verification")
	flag.Parse()

	auth.resolve()
	auth.PoolSize = *poolSize
	auth.AppName = *appName

	if *fatalPM < 0 || *fatalPM > 10000 {
		log.Fatalf("--fatal-permille must be between 0 and 10000, got %d", *fatalPM)
	}

	fatalPermille = *fatalPM

	// A Deployment gives every replica identical arguments, so every pod would
	// write for the same node range and the fleet would see one shard's worth of
	// load however many replicas ran. A StatefulSet's pod name carries an
	// ordinal, and the image is distroless so there is no shell to compute the
	// offset in; the binary therefore reads its own ordinal and shards itself.
	if *shardStride > 0 {
		ord, err := podOrdinal()
		if err != nil {
			log.Fatalf("--shard-stride set but the pod ordinal is unreadable: %v", err)
		}

		*nodeStart = ord * *shardStride
		log.Printf("shard: ordinal=%d stride=%d node-start-index=%d", ord, *shardStride, *nodeStart)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		log.Printf("signal received, closing clients")
		cancel()
	}()

	if *duration > 0 {
		go func() { time.Sleep(*duration); cancel() }()
	}

	s := &stats{}

	if *burstNodes > 0 {
		runBurst(ctx, auth, s, *burstNodes, *burstClients, *agent,
			*nodePrefix, *nodeStart, *nodeWidth, *database, *collection)

		return
	}

	log.Printf("plan: connectors=%d poolSize=%d rate=%.3f/s/connector total=%.1f events/s "+
		"fatal=%.2f%% (%.1f fatal events/s)",
		*connectors, *poolSize, *rate, float64(*connectors)*(*rate),
		float64(fatalPermille)/100, float64(*connectors)*(*rate)*float64(fatalPermille)/10000)

	var wg sync.WaitGroup

	for i := range *connectors {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			// Each connector owns a contiguous block of node names and cycles
			// through them, so N connectors cover N*span nodes.
			nodes := make([]string, *nodeSpan)
			for j := range nodes {
				nodes[j] = fmt.Sprintf("%s%0*d", *nodePrefix, *nodeWidth,
					*nodeStart+id*(*nodeSpan)+j)
			}

			runConnector(ctx, nodes, *agent, auth, *rate, *database, *collection, s, *prime)
		}(i)

		if *rampMS > 0 {
			time.Sleep(time.Duration(*rampMS) * time.Millisecond)
		}
	}

	go report(ctx, s, *connectors)
	wg.Wait()

	log.Printf("done: connected=%d failed=%d inserts=%d errors=%d",
		s.connected.Load(), s.failed.Load(), s.inserts.Load(), s.errors.Load())
}

// runConnector holds one client open for the life of the run, writing at the
// configured rate. The client is deliberately not shared: sharing one client
// across goroutines would multiplex them onto a single pool and the server would
// see a fraction of the intended connections.
// runConnector holds one client open for the life of the run, writing events for
// one node. The node name must match a node that exists in the cluster, or the
// fault-handling pipeline has nothing to act on: fault-quarantine looks the node
// up when it evaluates an event.
func runConnector(ctx context.Context, nodes []string, agent string, auth *mongoAuth,
	rate float64, database, collection string, s *stats,
	prime bool,
) {
	// Connect with retries rather than giving up on the first failure. At fleet
	// scale every connector in every pod dials at once, and a run at 50,000
	// connectors lost a third of them inside a single fifteen-second window
	// during that storm: clients that could not be served inside the driver's
	// server-selection timeout returned an error and this function simply
	// returned, so the fleet ran at two thirds of its intended rate for the
	// whole run. The failures were transient -- inserts on the connectors that
	// did get through ran with zero errors -- so a retry recovers them.
	//
	// The error was also discarded, which made the cause unrecoverable after the
	// fact. It is logged now, once per connector, rate-limited by the fact that a
	// connector only reaches the final attempt after several minutes of backoff.
	var client *mongo.Client

	for attempt := 1; ; attempt++ {
		c, err := auth.connect(ctx)
		if err == nil {
			if err = c.Ping(ctx, nil); err == nil {
				client = c
				break
			}

			_ = c.Disconnect(context.Background())
		}

		if attempt >= connectAttempts {
			log.Printf("connector %s: giving up after %d attempts: %v", nodes[0], attempt, err)
			s.failed.Add(1)

			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(connectBackoff(attempt)):
		}
	}

	defer func() { _ = client.Disconnect(context.Background()) }()

	s.connected.Add(1)

	// GetCollectionClient sets these on the real connector's collection handle.
	// They change the wire protocol (writeConcern/readConcern in every insert and
	// the server the driver selects), so a simulated connector that omits them
	// does not put the same load on the replica set.
	collOpts := options.Collection().
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority()).
		SetReadPreference(readpref.Primary())
	coll := client.Database(database).Collection(collection, collOpts)

	interval := time.Duration(float64(time.Second) / rate)

	// The first write is what creates the application connection to the primary:
	// the pool starts empty because minPoolSize is 0, so a connector that has not
	// yet written holds only its two monitoring connections. A real connector
	// writes within moments of starting and then keeps that connection for the
	// life of the process, because the URI sets no maxIdleTimeMS.
	//
	// At a realistic fleet-wide fault rate the interval is hours long, so
	// spreading the first write across a full interval would leave most
	// connectors having never written and the replica set showing 2 connections
	// per connector instead of 3. Prime inside the ramp window instead, then
	// settle to the configured rate.
	// Priming opens the application connection with a read rather than a write.
	// A write would work too, but at fleet scale every connector priming at once
	// injects one event per node: 50,000 events inside the prime window, of
	// which the production fatal share is nearly 17%, which would cordon
	// thousands of nodes in a burst no real fleet produces. A read checks a
	// connection out of the pool for the primary exactly as a write does, and
	// the connection stays because maxIdleTimeMS is unset, so the connection
	// count is the same and the event stream is left alone.
	if prime {
		primeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = coll.FindOne(primeCtx, bson.D{{Key: "_id", Value: "connection-prime-no-match"}}).Err()

		cancel()
	}

	timer := time.NewTimer(time.Duration(rand.Int63n(int64(interval)))) //nolint:gosec
	defer timer.Stop()

	// Round-robin rather than random, so every node in the block receives events
	// at the same rate. A random pick would leave some nodes unvisited for long
	// stretches, and fault-quarantine's work is per node, not per event.
	next := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			node := nodes[next]
			next = (next + 1) % len(nodes)

			// The real connector calls InsertMany with whatever the ring buffer
			// dequeued, never InsertOne.
			batch := []any{healthEvent(node, agent)}
			if _, err := coll.InsertMany(ctx, batch); err != nil {
				s.errors.Add(1)
			} else {
				s.inserts.Add(1)
			}

			timer.Reset(interval)
		}
	}
}

// actionMix is the RecommendedAction distribution observed in production,
// keyed by the protobuf enum value because RecommendedAction has no bson tag
// and serialises as its integer, not its name.
var actionMix = []struct {
	action int32
	weight int
}{
	{2, 6238}, // COMPONENT_RESET
	{5, 3452}, // CONTACT_SUPPORT
	{15, 237}, // RESTART_VM
	{24, 73},  // RESTART_BM
	{25, 0},   // REPLACE_VM
}

const actionTotal = 10000

// connectAttempts bounds the retry loop in runConnector. Six attempts with the
// backoff below spans about two minutes, which covers the connection storm at
// the start of a fleet-scale run without letting a genuinely unreachable
// datastore hold connectors open indefinitely.
const connectAttempts = 6

// connectBackoff grows the wait between connect attempts and jitters it, so
// retries from tens of thousands of connectors do not re-converge into the same
// storm that caused the first failure.
func connectBackoff(attempt int) time.Duration {
	base := time.Duration(1<<attempt) * time.Second
	return base + time.Duration(rand.Intn(1000))*time.Millisecond //nolint:gosec
}

// fatalPermille is the share of events marked fatal, per ten thousand. It
// defaults to the production share of 16.75% and is a variable rather than a
// constant so a run can dial it to a chosen fatal:non-fatal ratio: the fatal
// share is what decides how much work reaches fault-quarantine's quarantine
// path, so it is the knob that matters when sizing that component.
var fatalPermille = 1675

// fatalChecks is the observed distribution of fatal events by check name and
// error code. Weights are per ten thousand. Several error codes are truncated in
// the source chart and are spelled out here as their most likely full names.
var fatalChecks = []struct {
	check, class, code string
	weight             int
}{
	{"SysLogsXIDError", "GPU", "95", 6131},
	{"GpuThermalMarginWatch", "GPU", "GPU_TEMP_HW_SLOWDOWN", 3007},
	{"GpuDcgmConnectivityFailure", "GPU", "DCGM_CONNECTIVITY_FAILURE", 232},
	{"OsmoLFSMountFailure", "Node", "OSMO_LFS_MOUNT_FAILURE", 185},
	{"SysLogsXIDError", "GPU", "45", 108},
	{"GpuDcgmFieldWatch", "GPU", "DCGM_FR_FIELD_VIOLATION", 72},
	{"GpuNvlinkWatch", "GPU", "NVLINK_ERROR", 90},
	{"GpuNvswitchFatalWatch", "NVSwitch", "NVSWITCH_FATAL", 60},
	{"SysLogsSXIDError", "NVSwitch", "SXID_ERROR", 50},
	{"PodStuckAfterDeletion", "Node", "POD_STUCK_AFTER_DELETION", 40},
	{"OsmoReportedBadNode", "Node", "OSMO_BAD_NODE", 20},
	{"NCCLLoopbackTest", "GPU", "NCCL_LOOPBACK_FAIL", 5},
}

// pickFatalCheck returns a check name, component class and error code drawn from
// the fatal distribution.
func pickFatalCheck() (string, string, string) {
	total := 0
	for _, c := range fatalChecks {
		total += c.weight
	}

	n := rand.Intn(total) //nolint:gosec
	for _, c := range fatalChecks {
		if n < c.weight {
			return c.check, c.class, c.code
		}

		n -= c.weight
	}

	return fatalChecks[0].check, fatalChecks[0].class, fatalChecks[0].code
}

func pickAction() int32 {
	n := rand.Intn(actionTotal) //nolint:gosec
	for _, a := range actionMix {
		if n < a.weight {
			return a.action
		}

		n -= a.weight
	}

	return 2
}

// healthEvent builds the document platform-connector writes, taken from
// model.HealthEventWithStatus and store_connector.go. The event is nested under
// "healthevent" rather than flattened, and the protobuf structs carry no bson
// tags, so field names are their Go names lowercased.
//
// healtheventstatus is initialised the way the connector initialises it: an
// empty UserPodsEvictionStatus and a span map. Writing an explicit null there
// instead of an empty document breaks node-drainer, which promotes the event
// with $set on "healtheventstatus.userpodsevictionstatus.status" and cannot
// create a field inside a null.
// randHex returns n hex characters, matching the width of an OpenTelemetry
// trace id (32) or span id (16).
func randHex(n int) string {
	const hexDigits = "0123456789abcdef"

	b := make([]byte, n)
	for i := range b {
		b[i] = hexDigits[rand.Intn(len(hexDigits))] //nolint:gosec
	}

	return string(b)
}

func healthEvent(node, agent string) bson.D {
	now := time.Now().UTC()
	fatal := rand.Intn(10000) < fatalPermille //nolint:gosec
	check, class, code := pickFatalCheck()

	return bson.D{
		{Key: "createdAt", Value: now},
		{Key: "healthevent", Value: bson.D{
			{Key: "version", Value: 1},
			{Key: "agent", Value: agent},
			{Key: "componentclass", Value: class},
			{Key: "checkname", Value: check},
			{Key: "isfatal", Value: fatal},
			{Key: "ishealthy", Value: false},
			{Key: "message", Value: check + " reported"},
			{Key: "recommendedaction", Value: pickAction()},
			{Key: "errorcode", Value: []string{code}},
			{Key: "nodename", Value: node},
			// insertHealthEvents stamps the trace id into Metadata on every event,
			// so a real document always carries this map. Omitting it understates
			// the document size, and document size is what drives collection and
			// oplog growth at fleet scale.
			{Key: "metadata", Value: bson.D{
				{Key: "trace-id", Value: randHex(32)},
			}},
			// timestamppb.Timestamp, not a BSON datetime: the field decodes into
			// *timestamppb.Timestamp, which has no bson tags, so it is a nested
			// document of its lowercased Go fields.
			{Key: "generatedtimestamp", Value: bson.D{
				{Key: "seconds", Value: now.Unix()},
				{Key: "nanos", Value: int32(now.Nanosecond())},
			}},
		}},
		{Key: "healtheventstatus", Value: bson.D{
			{Key: "userpodsevictionstatus", Value: bson.D{
				{Key: "status", Value: ""},
				{Key: "message", Value: ""},
			}},
			// The real connector records its own span id here under the
			// "platform-connector" key; downstream components add theirs.
			{Key: "spanids", Value: bson.D{
				{Key: "platform-connector", Value: randHex(16)},
			}},
		}},
	}
}

func report(ctx context.Context, s *stats, want int) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			el := time.Since(start).Seconds()
			log.Printf("connected=%d/%d failed=%d inserts=%d (%.1f/s) errors=%d",
				s.connected.Load(), want, s.failed.Load(),
				s.inserts.Load(), float64(s.inserts.Load())/el, s.errors.Load())
		}
	}
}

// runBurst emits one fatal event per node for a contiguous range of nodes, as
// fast as the clients allow, and exits. It models a correlated failure -- a rack
// or a switch taking a block of nodes out at once -- which is what the burst
// absorption figure measures: how long the pipeline takes to cordon and clear a
// step change rather than a steady trickle.
//
// Every event is forced fatal. A burst drawn from the production mix would be
// only about 17% fatal, so a burst of 1000 would cordon 170 nodes and the size
// in the report would not mean what it says.
func runBurst(
	ctx context.Context, auth *mongoAuth, s *stats,
	nodes, clients int, agent, prefix string, start, width int,
	database, collection string,
) {
	if clients > nodes {
		clients = nodes
	}

	log.Printf("burst: %d nodes over %d clients, first=%s%0*d", nodes, clients, prefix, width, start)

	collOpts := options.Collection().
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority()).
		SetReadPreference(readpref.Primary())

	var wg sync.WaitGroup

	began := time.Now()

	for c := range clients {
		wg.Add(1)

		go func(c int) {
			defer wg.Done()

			client, err := auth.connect(ctx)
			if err != nil {
				s.failed.Add(1)
				return
			}
			defer func() { _ = client.Disconnect(context.Background()) }()

			coll := client.Database(database).Collection(collection, collOpts)

			for i := c; i < nodes; i += clients {
				node := fmt.Sprintf("%s%0*d", prefix, width, start+i)

				ev := healthEvent(node, agent)
				forceFatal(ev)

				if _, err := coll.InsertMany(ctx, []any{ev}); err != nil {
					s.errors.Add(1)
				} else {
					s.inserts.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()

	took := time.Since(began)
	log.Printf("burst complete: inserted=%d errors=%d in %s (%.0f events/s)",
		s.inserts.Load(), s.errors.Load(), took.Round(time.Millisecond),
		float64(s.inserts.Load())/took.Seconds())
}

// forceFatal rewrites the generated event so it is fatal and matches the
// fault-quarantine ruleset, leaving the rest of the production shape intact.
func forceFatal(ev bson.D) {
	for i := range ev {
		if ev[i].Key != "healthevent" {
			continue
		}

		inner, ok := ev[i].Value.(bson.D)
		if !ok {
			return
		}

		for j := range inner {
			switch inner[j].Key {
			case "isfatal":
				inner[j].Value = true
			case "componentclass":
				inner[j].Value = "GPU"
			}
		}

		ev[i].Value = inner
	}
}
