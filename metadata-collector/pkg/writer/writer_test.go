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

package writer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nvidia/nvsentinel/metadata-collector/pkg/types"
)

func TestWriterAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "metadata.json")

	w, err := NewWriter(outputPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	metadata := &types.GPUMetadata{
		Version:    "1.0",
		Timestamp:  "2025-11-05T12:00:00Z",
		NodeName:   "test-node",
		GPUs:       []types.GPUInfo{},
		NVSwitches: []string{},
	}

	if err := w.Write(metadata); err != nil {
		t.Fatalf("Failed to write metadata: %v", err)
	}

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Fatalf("Output file was not created")
	}

	tmpPath := outputPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary file was not cleaned up")
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var readMetadata types.GPUMetadata
	if err := json.Unmarshal(data, &readMetadata); err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}

	if readMetadata.Version != metadata.Version {
		t.Errorf("Version mismatch: got %s, want %s", readMetadata.Version, metadata.Version)
	}
}

func TestWriterCreateDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "subdir", "metadata.json")

	w, err := NewWriter(outputPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	metadata := &types.GPUMetadata{
		Version:    "1.0",
		Timestamp:  "2025-11-05T12:00:00Z",
		NodeName:   "test-node",
		GPUs:       []types.GPUInfo{},
		NVSwitches: []string{},
	}

	if err := w.Write(metadata); err != nil {
		t.Fatalf("Failed to write metadata: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "subdir")); os.IsNotExist(err) {
		t.Errorf("Output directory was not created")
	}
}
