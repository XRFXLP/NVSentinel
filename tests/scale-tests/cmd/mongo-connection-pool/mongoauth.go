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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Auth mechanisms this tool knows how to negotiate.
const (
	authAuto  = "auto"
	authX509  = "x509"
	authSCRAM = "scram"
	authNone  = "none"
)

// mongoAuth carries everything needed to reach the HealthEvents collection.
// The cluster runs Percona with allowTLS and both an X.509 app identity and
// SCRAM operator users, so the tool has to handle either.
type mongoAuth struct {
	URI      string
	CertDir  string
	User     string
	Password string
	AuthDB   string
	Mech     string
	Insecure bool
	PoolSize uint64

	// AppName is sent in the client handshake and shows up as appName in
	// currentOp and in the server log, which is the only way to attribute
	// connections and slow queries to a component. The real connector sets it
	// from APP_NAME.
	AppName string

	// WriteConcern is "majority", "1" or "0". The replica-set default is
	// majority, which blocks every insert until a second member acknowledges.
	// For seeding a backlog that durability is not worth the throughput.
	WriteConcern string
	Journal      bool
	Compressors  string
}

// resolve fills in anything not passed explicitly from the environment, then
// decides which mechanism to use. Explicit flags always win over env vars.
func (a *mongoAuth) resolve() {
	if a.URI == "" {
		a.URI = os.Getenv("MONGO_URI")
	}

	if a.User == "" {
		a.User = firstNonEmpty(
			os.Getenv("MONGODB_USER"),
			os.Getenv("MONGODB_DATABASE_ADMIN_USER"),
		)
	}

	if a.Password == "" {
		a.Password = firstNonEmpty(
			os.Getenv("MONGODB_PASSWORD"),
			os.Getenv("MONGODB_DATABASE_ADMIN_PASSWORD"),
		)
	}

	if a.Mech != authAuto {
		return
	}

	switch {
	case a.hasClientCert():
		a.Mech = authX509
	case a.User != "" && a.Password != "":
		a.Mech = authSCRAM
	default:
		a.Mech = authNone
	}
}

func (a *mongoAuth) hasClientCert() bool {
	if a.CertDir == "" {
		return false
	}

	for _, f := range []string{"tls.crt", "tls.key"} {
		if _, err := os.Stat(filepath.Join(a.CertDir, f)); err != nil {
			return false
		}
	}

	return true
}

// tlsConfig builds the TLS config. The CA is used when present; the client
// certificate is loaded whenever it exists, because Percona's clusterAuthMode
// can require it even on SCRAM connections.
func (a *mongoAuth) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	caPath := filepath.Join(a.CertDir, "ca.crt")
	if ca, err := os.ReadFile(caPath); err == nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse CA %s: no certificates found", caPath)
		}

		cfg.RootCAs = pool
	} else if a.CertDir != "" && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read CA %s: %w", caPath, err)
	}

	if a.hasClientCert() {
		cert, err := tls.LoadX509KeyPair(
			filepath.Join(a.CertDir, "tls.crt"),
			filepath.Join(a.CertDir, "tls.key"),
		)
		if err != nil {
			return nil, fmt.Errorf("load client keypair from %s: %w", a.CertDir, err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	// The server certificate is issued for the StatefulSet pod DNS names, which
	// will not match when connecting through a port-forward to localhost.
	if a.Insecure {
		cfg.InsecureSkipVerify = true
	}

	return cfg, nil
}

// connect dials MongoDB and verifies the connection with a primary-read ping,
// so an auth failure surfaces here rather than on the first insert.
func (a *mongoAuth) connect(ctx context.Context) (*mongo.Client, error) {
	if a.URI == "" {
		return nil, fmt.Errorf("no MongoDB URI: pass --mongo-uri or set MONGO_URI")
	}

	tlsCfg, err := a.tlsConfig()
	if err != nil {
		return nil, err
	}

	opts := options.Client().
		ApplyURI(a.URI).
		SetTLSConfig(tlsCfg).
		SetServerMonitoringMode(options.ServerMonitoringModeAuto).
		SetHeartbeatInterval(10 * time.Second).
		SetServerSelectionTimeout(20 * time.Second)

	if a.AppName != "" {
		opts.SetAppName(a.AppName)
	}

	if a.PoolSize > 0 {
		opts.SetMaxPoolSize(a.PoolSize)
		// No minimum. SetMinPoolSize applies per server, so a minimum keeps a
		// pooled connection open to every replica set member and spreads
		// connections evenly across them. The platform-connector leaves the
		// pool lazy, so it holds one monitoring connection per member plus
		// pooled connections only to the member it writes to, which is the
		// primary. Forcing a minimum here would hide that asymmetry.
	}

	if wc := a.writeConcern(); wc != nil {
		opts.SetWriteConcern(wc)
	}

	if a.Compressors != "" {
		opts.SetCompressors(strings.Split(a.Compressors, ","))
	}

	switch a.Mech {
	case authX509:
		if !a.hasClientCert() {
			return nil, fmt.Errorf("--mongo-auth=x509 but %s has no tls.crt/tls.key", a.CertDir)
		}

		opts.SetAuth(options.Credential{
			AuthMechanism: "MONGODB-X509",
			AuthSource:    "$external",
		})
	case authSCRAM:
		if a.User == "" || a.Password == "" {
			return nil, fmt.Errorf("--mongo-auth=scram requires --mongo-user and --mongo-password " +
				"(or MONGODB_USER / MONGODB_PASSWORD)")
		}

		opts.SetAuth(options.Credential{
			AuthMechanism: "SCRAM-SHA-256",
			AuthSource:    a.AuthDB,
			Username:      a.User,
			Password:      a.Password,
		})
	case authNone:
		// URI may already carry credentials, or the server may be unauthenticated.
	default:
		return nil, fmt.Errorf("unknown --mongo-auth %q", a.Mech)
	}

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("connect (mech=%s): %w", a.Mech, err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping primary (mech=%s, certDir=%s): %w", a.Mech, a.CertDir, err)
	}

	log.Printf("mongo: connected (mech=%s, tls=%t, insecure=%t)",
		a.Mech, len(tlsCfg.Certificates) > 0 || tlsCfg.RootCAs != nil, a.Insecure)

	return client, nil
}

// writeConcern maps the flag onto a driver write concern. Returning nil leaves
// the replica-set default (majority) in place.
func (a *mongoAuth) writeConcern() *writeconcern.WriteConcern {
	switch a.WriteConcern {
	case "", "default":
		return nil
	case "majority":
		return &writeconcern.WriteConcern{W: "majority", Journal: &a.Journal}
	case "0":
		// Unacknowledged: fastest, but insert errors are invisible and
		// InsertMany reports no inserted ids.
		return &writeconcern.WriteConcern{W: 0}
	default:
		n, err := strconv.Atoi(a.WriteConcern)
		if err != nil || n < 0 {
			return nil
		}

		return &writeconcern.WriteConcern{W: n, Journal: &a.Journal}
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}
