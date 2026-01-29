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
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type Config struct {
	Port             int
	CertDir          string
	ConfigFile       string
	InitContainers   []corev1.Container `json:"initContainers"`
	GPUResourceNames []string           `json:"gpuResourceNames"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if len(cfg.InitContainers) == 0 {
		cfg.InitContainers = []corev1.Container{{
			Name:  "preflight-ping",
			Image: "ghcr.io/nvidia/nvsentinel/preflight-checks-ping:latest",
		}}
	}

	if len(cfg.GPUResourceNames) == 0 {
		cfg.GPUResourceNames = []string{"nvidia.com/gpu"}
	}

	return cfg, nil
}
