// mongo-bench: NVSentinel MongoDB microbenchmark tool
//
// Modes:
//   connections  — open N clients, hold idle, report serverStatus
//   write        — insert HealthEvent docs at controlled rate, report latency
//   burst        — burst inserts at peak rate for -duration, report recovery

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mode        = flag.String("mode", "connections", "connections | write | burst")
	uri         = flag.String("uri", "", "MongoDB URI (required)")
	caFile      = flag.String("ca", "", "TLS CA cert file")
	certFile    = flag.String("cert", "", "TLS client cert file (tls.crt)")
	keyFile     = flag.String("key", "", "TLS client key file (tls.key)")
	nConn       = flag.Int("n", 100, "number of connections (connections mode)")
	rate        = flag.Float64("rate", 30, "events/s (write mode)")
	duration    = flag.Duration("duration", 5*time.Minute, "test duration")
	holdAfter   = flag.Duration("hold", 30*time.Second, "hold time after connections established")
	database    = flag.String("db", "HealthEventsDatabase", "database name")
	collection  = flag.String("col", "HealthEvents", "collection name")
	maxPoolSize = flag.Uint64("maxPoolSize", 0, "maxPoolSize per client (0 = driver default of 100)")
)

func buildTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec

	if caPath != "" {
		ca, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		cfg.RootCAs = pool
		cfg.InsecureSkipVerify = false
	}

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func newClient(ctx context.Context, tlsCfg *tls.Config, poolSize uint64) (*mongo.Client, error) {
	opts := options.Client().
		ApplyURI(*uri).
		SetTLSConfig(tlsCfg).
		SetServerMonitoringMode(options.ServerMonitoringModeAuto).
		SetHeartbeatInterval(10 * time.Second)

	if poolSize > 0 {
		opts.SetMaxPoolSize(poolSize)
		opts.SetMinPoolSize(1)
	}
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func serverStatus(ctx context.Context, client *mongo.Client) (bson.M, error) {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func getInt64(m bson.M, key string) int64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

func getSubdoc(m bson.M, key string) bson.M {
	v, ok := m[key]
	if !ok {
		return bson.M{}
	}
	sub, ok := v.(bson.M)
	if !ok {
		return bson.M{}
	}
	return sub
}

func fakeHealthEvent(nodeName string) bson.M {
	checks := []string{"GpuXidError", "GpuMemoryError", "GpuNvlinkWatch", "GpuPowerError",
		"NVSwitchHealth", "GpuThermalError", "GpuDriverError", "GpuEccError"}
	return bson.M{
		"createdAt": primitive.NewDateTimeFromTime(time.Now()),
		"healthevent": bson.M{
			"nodename":  nodeName,
			"checkname": checks[rand.Intn(len(checks))],
			"ishealthy": false,
			"isfatal":   false,
			"agent":     "mongo-bench",
			"generatedtimestamp": bson.M{
				"seconds": time.Now().Unix(),
				"nanos":   0,
			},
			"entitiesimpacted": bson.A{bson.M{
				"entitytype":  "GPU",
				"entityvalue": fmt.Sprintf("GPU-%d", rand.Intn(8)),
			}},
			"message": "benchmark synthetic event",
		},
	}
}

func printServerStatus(ctx context.Context, client *mongo.Client, label string) {
	ss, err := serverStatus(ctx, client)
	if err != nil {
		log.Printf("%s: serverStatus error: %v", label, err)
		return
	}
	conns := getSubdoc(ss, "connections")
	mem := getSubdoc(ss, "mem")
	wt := getSubdoc(getSubdoc(ss, "wiredTiger"), "cache")
	log.Printf("%s: connections_current=%d active=%d rejected=%d | mem_resident_MB=%d | wt_cache_MB=%d",
		label,
		getInt64(conns, "current"),
		getInt64(conns, "active"),
		getInt64(conns, "rejected"),
		getInt64(mem, "resident"),
		getInt64(wt, "bytes currently in the cache")/1048576,
	)
}

func runConnections(ctx context.Context, tlsCfg *tls.Config) {
	log.Printf("=== CONNECTION SCALING: n=%d, maxPoolSize=%d ===", *nConn, *maxPoolSize)

	baseClient, err := newClient(ctx, tlsCfg, 1)
	if err != nil {
		log.Fatalf("base client: %v", err)
	}
	printServerStatus(ctx, baseClient, "baseline")

	clients := make([]*mongo.Client, 0, *nConn)
	for i := 0; i < *nConn; i++ {
		c, err := newClient(ctx, tlsCfg, *maxPoolSize)
		if err != nil {
			log.Printf("  client %d: connect error: %v", i, err)
			continue
		}
		if err := c.Ping(ctx, nil); err != nil {
			log.Printf("  client %d: ping error: %v", i, err)
		}
		clients = append(clients, c)

		if (i+1)%100 == 0 || i+1 == *nConn {
			printServerStatus(ctx, baseClient, fmt.Sprintf("n=%d", i+1))
		}
	}

	log.Printf("All %d clients connected. Holding for %s...", len(clients), *holdAfter)
	time.Sleep(*holdAfter)

	printServerStatus(ctx, baseClient, "=== FINAL")

	for _, c := range clients {
		_ = c.Disconnect(ctx)
	}
	_ = baseClient.Disconnect(ctx)
}

func runWrite(ctx context.Context, tlsCfg *tls.Config) {
	log.Printf("=== SUSTAINED WRITE: rate=%.0f/s duration=%s ===", *rate, *duration)

	client, err := newClient(ctx, tlsCfg, 50)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(ctx) //nolint:errcheck

	col := client.Database(*database).Collection(*collection)
	interval := time.Duration(float64(time.Second) / *rate)
	deadline := time.Now().Add(*duration)

	var totalInserts, totalErrors int64
	var latencySum int64
	var wg sync.WaitGroup

	reportTicker := time.NewTicker(10 * time.Second)
	defer reportTicker.Stop()
	rateTicker := time.NewTicker(interval)
	defer rateTicker.Stop()

	var lastReport int64
	lastReportTime := time.Now()

	for {
		select {
		case t := <-rateTicker.C:
			if t.After(deadline) {
				goto done
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				node := fmt.Sprintf("bench-node-%06d", rand.Intn(1000))
				_, err := col.InsertOne(ctx, fakeHealthEvent(node))
				elapsed := time.Since(start)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalInserts, 1)
					atomic.AddInt64(&latencySum, elapsed.Milliseconds())
				}
			}()
		case <-reportTicker.C:
			n := atomic.LoadInt64(&totalInserts)
			delta := n - lastReport
			dt := time.Since(lastReportTime).Seconds()
			avgLat := int64(0)
			if n > 0 {
				avgLat = atomic.LoadInt64(&latencySum) / n
			}
			log.Printf("  inserts=%d  rate=%.1f/s  avg_lat=%dms  errors=%d",
				n, float64(delta)/dt, avgLat, atomic.LoadInt64(&totalErrors))
			lastReport = n
			lastReportTime = time.Now()
		}
	}
done:
	wg.Wait()
	n := atomic.LoadInt64(&totalInserts)
	avgLat := int64(0)
	if n > 0 {
		avgLat = atomic.LoadInt64(&latencySum) / n
	}
	log.Printf("=== FINAL: inserts=%d  avg_lat=%dms  errors=%d ===",
		n, avgLat, atomic.LoadInt64(&totalErrors))
}

func runBurst(ctx context.Context, tlsCfg *tls.Config) {
	log.Printf("=== BURST WRITE: rate=%.0f/s duration=%s ===", *rate, *duration)

	client, err := newClient(ctx, tlsCfg, 50)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(ctx) //nolint:errcheck

	col := client.Database(*database).Collection(*collection)
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	var totalInserts, totalErrors int64
	var latencySum int64

	// burst: spawn goroutines at target rate
	interval := time.Duration(float64(time.Second) / *rate)
	for time.Now().Before(deadline) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			node := fmt.Sprintf("bench-node-%06d", rand.Intn(1000))
			_, err := col.InsertOne(ctx, fakeHealthEvent(node))
			elapsed := time.Since(start)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
			} else {
				atomic.AddInt64(&totalInserts, 1)
				atomic.AddInt64(&latencySum, elapsed.Milliseconds())
			}
		}()
		time.Sleep(interval)
	}

	wg.Wait()
	n := atomic.LoadInt64(&totalInserts)
	avgLat := int64(0)
	if n > 0 {
		avgLat = atomic.LoadInt64(&latencySum) / n
	}
	log.Printf("=== BURST FINAL: inserts=%d  avg_lat=%dms  errors=%d ===",
		n, avgLat, atomic.LoadInt64(&totalErrors))

	// recovery: measure latency of single inserts returning to baseline
	log.Printf("Measuring recovery...")
	for i := 0; i < 10; i++ {
		start := time.Now()
		col.InsertOne(ctx, fakeHealthEvent("bench-recovery")) //nolint:errcheck
		log.Printf("  recovery insert %d: %dms", i+1, time.Since(start).Milliseconds())
		time.Sleep(time.Second)
	}
}

func main() {
	flag.Parse()
	if *uri == "" {
		log.Fatal("-uri is required")
	}

	tlsCfg, err := buildTLSConfig(*caFile, *certFile, *keyFile)
	if err != nil {
		log.Fatalf("TLS: %v", err)
	}

	ctx := context.Background()

	// print baseline serverStatus summary
	baseClient, err := newClient(ctx, tlsCfg, 1)
	if err != nil {
		log.Fatalf("baseline connect: %v", err)
	}
	ss, ssErr := serverStatus(ctx, baseClient)
	if ssErr != nil {
		log.Fatalf("serverStatus: %v", ssErr)
	}
	connsBase := getSubdoc(ss, "connections")
	memBase := getSubdoc(ss, "mem")
	log.Printf("Baseline: connections_current=%d mem_resident_MB=%d",
		getInt64(connsBase, "current"), getInt64(memBase, "resident"))
	baseClient.Disconnect(ctx) //nolint:errcheck

	switch *mode {
	case "connections":
		runConnections(ctx, tlsCfg)
	case "write":
		runWrite(ctx, tlsCfg)
	case "burst":
		runBurst(ctx, tlsCfg)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}
