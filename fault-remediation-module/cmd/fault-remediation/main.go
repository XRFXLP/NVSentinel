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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/nvidia/nvsentinel/fault-remediation-module/pkg/reconciler"
	"github.com/nvidia/nvsentinel/platform-connectors/pkg/connectors/store"
	"github.com/nvidia/nvsentinel/statemanager"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/storewatcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"k8s.io/klog"
)

type config struct {
	namespace                string
	version                  string
	apiGroup                 string
	templateMountPath        string
	templateFileName         string
	metricsPort              string
	mongoClientCertMountPath string
	kubeconfigPath           string
	dryRun                   bool
	enableLogCollector       bool
	updateMaxRetries         int
	updateRetryDelaySeconds  int
}

func parseFlags() *config {
	cfg := &config{}

	flag.StringVar(&cfg.metricsPort, "metrics-port", "2112", "port to expose Prometheus metrics on")
	flag.StringVar(&cfg.mongoClientCertMountPath, "mongo-client-cert-mount-path", "/etc/ssl/mongo-client",
		"path where the mongodb client cert is mounted")
	flag.StringVar(&cfg.kubeconfigPath, "kubeconfig-path", "", "path to kubeconfig file")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "flag to run node drainer module in dry-run mode")
	flag.IntVar(&cfg.updateMaxRetries, "update-max-retries", 5,
		"maximum attempts to update remediation status per event")
	flag.IntVar(&cfg.updateRetryDelaySeconds, "update-retry-delay-seconds", 10,
		"delay in seconds between remediation status update retries")
	flag.Parse()

	return cfg
}

func getRequiredEnvVars() (*config, error) {
	cfg := &config{}

	requiredVars := map[string]*string{
		"MAINTENANCE_NAMESPACE": &cfg.namespace,
		"MAINTENANCE_VERSION":   &cfg.version,
		"MAINTENANCE_API_GROUP": &cfg.apiGroup,
		"TEMPLATE_MOUNT_PATH":   &cfg.templateMountPath,
		"TEMPLATE_FILE_NAME":    &cfg.templateFileName,
	}

	for envVar, ptr := range requiredVars {
		*ptr = os.Getenv(envVar)
		if *ptr == "" {
			return nil, fmt.Errorf("%s is not provided", envVar)
		}
	}

	// Feature flag: default disabled; only "true" enables it
	if v := os.Getenv("ENABLE_LOG_COLLECTOR"); v == "true" {
		cfg.enableLogCollector = true
	}

	log.Printf("namespace: %s, version: %s, apigroup: %s, templateMountPath: %s, templateFileName: %s",
		cfg.namespace, cfg.version, cfg.apiGroup, cfg.templateMountPath, cfg.templateFileName)

	return cfg, nil
}

func getMongoDBConfig(mongoClientCertMountPath string) (*storewatcher.MongoDBConfig, error) {
	requiredEnvVars := map[string]string{
		"MONGODB_URI":                   "MongoDB URI",
		"MONGODB_DATABASE_NAME":         "MongoDB Database name",
		"MONGODB_COLLECTION_NAME":       "MongoDB collection name",
		"MONGODB_TOKEN_COLLECTION_NAME": "MongoDB token collection name",
	}

	envVars := make(map[string]string)

	for envVar, description := range requiredEnvVars {
		value := os.Getenv(envVar)
		if value == "" {
			return nil, fmt.Errorf("%s is not provided", description)
		}

		envVars[envVar] = value
	}

	totalTimeoutSeconds, err := getEnvAsInt("MONGODB_PING_TIMEOUT_TOTAL_SECONDS", 300)
	if err != nil {
		return nil, fmt.Errorf("invalid MONGODB_PING_TIMEOUT_TOTAL_SECONDS: %w", err)
	}

	intervalSeconds, err := getEnvAsInt("MONGODB_PING_INTERVAL_SECONDS", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid MONGODB_PING_INTERVAL_SECONDS: %w", err)
	}

	totalCACertTimeoutSeconds, err := getEnvAsInt("CA_CERT_MOUNT_TIMEOUT_TOTAL_SECONDS", 360)
	if err != nil {
		return nil, fmt.Errorf("invalid CA_CERT_MOUNT_TIMEOUT_TOTAL_SECONDS: %w", err)
	}

	intervalCACertSeconds, err := getEnvAsInt("CA_CERT_READ_INTERVAL_SECONDS", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid CA_CERT_READ_INTERVAL_SECONDS: %w", err)
	}

	return &storewatcher.MongoDBConfig{
		URI:        envVars["MONGODB_URI"],
		Database:   envVars["MONGODB_DATABASE_NAME"],
		Collection: envVars["MONGODB_COLLECTION_NAME"],
		ClientTLSCertConfig: storewatcher.MongoDBClientTLSCertConfig{
			TlsCertPath: filepath.Join(mongoClientCertMountPath, "tls.crt"),
			TlsKeyPath:  filepath.Join(mongoClientCertMountPath, "tls.key"),
			CaCertPath:  filepath.Join(mongoClientCertMountPath, "ca.crt"),
		},
		TotalPingTimeoutSeconds:    totalTimeoutSeconds,
		TotalPingIntervalSeconds:   intervalSeconds,
		TotalCACertTimeoutSeconds:  totalCACertTimeoutSeconds,
		TotalCACertIntervalSeconds: intervalCACertSeconds,
	}, nil
}

func getTokenConfig() (*storewatcher.TokenConfig, error) {
	tokenDatabase := os.Getenv("MONGODB_DATABASE_NAME")
	if tokenDatabase == "" {
		return nil, fmt.Errorf("MongoDB token database name is not provided")
	}

	tokenCollection := os.Getenv("MONGODB_TOKEN_COLLECTION_NAME")
	if tokenCollection == "" {
		return nil, fmt.Errorf("MongoDB token collection name is not provided")
	}

	return &storewatcher.TokenConfig{
		ClientName:      "fault-remediation-module",
		TokenDatabase:   tokenDatabase,
		TokenCollection: tokenCollection,
	}, nil
}

func startMetricsServer(metricsPort string) {
	klog.Infof("Starting a metrics port on : %s", metricsPort)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+metricsPort, nil)
		if err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
	}()
}

func getMongoPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{
			bson.E{Key: "$match", Value: bson.D{
				bson.E{Key: "operationType", Value: "update"},
				bson.E{Key: "$or", Value: bson.A{
					// Watch for quarantine events (for remediation)
					bson.D{
						bson.E{Key: "fullDocument.healtheventstatus.userpodsevictionstatus.status", Value: bson.D{
							bson.E{Key: "$in", Value: bson.A{store.StatusSucceeded, store.AlreadyDrained}},
						}},
						bson.E{Key: "fullDocument.healtheventstatus.nodequarantined", Value: bson.D{
							bson.E{Key: "$in", Value: bson.A{store.Quarantined, store.AlreadyQuarantined}},
						}},
					},
					// Watch for unquarantine events (for annotation cleanup)
					bson.D{
						bson.E{Key: "fullDocument.healtheventstatus.nodequarantined", Value: store.UnQuarantined},
						bson.E{Key: "fullDocument.healtheventstatus.userpodsevictionstatus.status", Value: store.StatusSucceeded},
					},
				}},
			}},
		},
	}
}

func main() {
	ctx := context.Background()

	// Parse flags and get configuration
	cfg := parseFlags()

	// Get required environment variables
	envCfg, err := getRequiredEnvVars()
	if err != nil {
		log.Fatalf("Failed to get required environment variables: %v", err)
	}

	// Get MongoDB configuration
	mongoConfig, err := getMongoDBConfig(cfg.mongoClientCertMountPath)
	if err != nil {
		log.Fatalf("Failed to get MongoDB configuration: %v", err)
	}

	// Get token configuration
	tokenConfig, err := getTokenConfig()
	if err != nil {
		log.Fatalf("Failed to get token configuration: %v", err)
	}

	// Get MongoDB pipeline
	pipeline := getMongoPipeline()

	// Initialize k8s client
	k8sClient, clientSet, err := reconciler.NewK8sClient(cfg.kubeconfigPath, cfg.dryRun, reconciler.TemplateData{
		Namespace:         envCfg.namespace,
		Version:           envCfg.version,
		ApiGroup:          envCfg.apiGroup,
		TemplateMountPath: envCfg.templateMountPath,
		TemplateFileName:  envCfg.templateFileName,
	})
	if err != nil {
		log.Fatalf("error while initializing kubernetes client: %v", err)
	}

	log.Println("Successfully initialized k8sclient")

	// Initialize and start reconciler
	reconcilerCfg := reconciler.ReconcilerConfig{
		MongoConfig:        *mongoConfig,
		TokenConfig:        *tokenConfig,
		MongoPipeline:      pipeline,
		RemediationClient:  k8sClient,
		StateManager:       statemanager.NewStateManager(clientSet),
		EnableLogCollector: envCfg.enableLogCollector,
		UpdateMaxRetries:   cfg.updateMaxRetries,
		UpdateRetryDelay:   time.Duration(cfg.updateRetryDelaySeconds) * time.Second,
	}

	reconciler := reconciler.NewReconciler(reconcilerCfg, cfg.dryRun)

	startMetricsServer(cfg.metricsPort)

	reconciler.Start(ctx)
}

func getEnvAsInt(name string, defaultValue int) (int, error) {
	valueStr, exists := os.LookupEnv(name)
	if !exists {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("error converting %s to integer: %w", name, err)
	}

	if value <= 0 {
		return 0, fmt.Errorf("value of %s must be a positive integer", name)
	}

	return value, nil
}
