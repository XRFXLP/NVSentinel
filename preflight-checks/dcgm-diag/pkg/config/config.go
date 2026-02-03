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

package config

import (
	"flag"
	"fmt"
	"os"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type Config struct {
	DiagLevel          int
	HostengineAddr     string
	ConnectorSocket    string
	NodeName           string
	ProcessingStrategy pb.ProcessingStrategy
}

func Parse() (*Config, error) {
	cfg := &Config{}

	diagLevelDefault := getEnvInt("DCGM_DIAG_LEVEL", 1)
	hostengineDefault := getEnv("DCGM_HOSTENGINE_ADDR", "")
	connectorDefault := getEnv("PLATFORM_CONNECTOR_SOCKET", "")
	strategyDefault := getEnv("PROCESSING_STRATEGY", "EXECUTE_REMEDIATION")

	var strategyStr string

	flag.IntVar(&cfg.DiagLevel, "level", diagLevelDefault,
		"DCGM diagnostic level (1=quick ~30s, 2=medium ~2min, 3=long ~15min, 4=extended)")
	flag.StringVar(&cfg.HostengineAddr, "hostengine", hostengineDefault,
		"DCGM hostengine address (e.g., localhost:5555). If empty, uses embedded mode.")
	flag.StringVar(&cfg.ConnectorSocket, "connector-socket", connectorDefault,
		"Platform connector socket path for health event reporting")
	flag.StringVar(&strategyStr, "processing-strategy", strategyDefault,
		"Event processing strategy: EXECUTE_REMEDIATION or STORE_ONLY")
	flag.Parse()

	cfg.NodeName = os.Getenv("NODE_NAME")

	if err := cfg.validate(strategyStr); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate(strategyStr string) error {
	if c.ConnectorSocket == "" {
		return fmt.Errorf("platform connector socket is required (set PLATFORM_CONNECTOR_SOCKET or --connector-socket)")
	}

	if c.NodeName == "" {
		return fmt.Errorf("node name is required (set NODE_NAME environment variable)")
	}

	if c.DiagLevel < 1 || c.DiagLevel > 4 {
		return fmt.Errorf("invalid diagnostic level %d: must be 1, 2, 3, or 4", c.DiagLevel)
	}

	value, ok := pb.ProcessingStrategy_value[strategyStr]
	if !ok {
		return fmt.Errorf("invalid processing strategy %q, valid options: EXECUTE_REMEDIATION, STORE_ONLY", strategyStr)
	}

	c.ProcessingStrategy = pb.ProcessingStrategy(value)

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var result int
		if _, err := fmt.Sscanf(value, "%d", &result); err == nil {
			return result
		}
	}

	return defaultValue
}
