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

package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

const (
	MetadataFilePath = "/var/lib/nvsentinel/gpu_metadata.json"
)

type GPUMetadata struct {
	Version       string   `json:"version"`
	Timestamp     string   `json:"timestamp"`
	NodeName      string   `json:"node_name"`
	ChassisSerial *string  `json:"chassis_serial"`
	GPUs          []GPU    `json:"gpus"`
	NVSwitches    []string `json:"nvswitches"`
}

type GPU struct {
	GPUID        int      `json:"gpu_id"`
	UUID         string   `json:"uuid"`
	PCIAddress   string   `json:"pci_address"`
	SerialNumber string   `json:"serial_number"`
	DeviceName   string   `json:"device_name"`
	NVLinks      []NVLink `json:"nvlinks"`
}

type NVLink struct {
	LinkID           int    `json:"link_id"`
	RemotePCIAddress string `json:"remote_pci_address"`
	RemoteLinkID     int    `json:"remote_link_id"`
}

func CreateTestMetadata(nodeName string) *GPUMetadata {
	chassisSerial := "TEST-CHASSIS-12345"

	return &GPUMetadata{
		Version:       "1.0",
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		NodeName:      nodeName,
		ChassisSerial: &chassisSerial,
		GPUs: []GPU{
			{
				GPUID:        0,
				UUID:         "GPU-00000000-0000-0000-0000-000000000000",
				PCIAddress:   "0000:17:00.0",
				SerialNumber: "SN-GPU-0",
				DeviceName:   "NVIDIA A100",
				NVLinks: []NVLink{
					{LinkID: 0, RemotePCIAddress: "0000:c3:00.0", RemoteLinkID: 28},
					{LinkID: 1, RemotePCIAddress: "0000:c3:00.0", RemoteLinkID: 29},
				},
			},
			{
				GPUID:        1,
				UUID:         "GPU-11111111-1111-1111-1111-111111111111",
				PCIAddress:   "0001:00:00.0",
				SerialNumber: "SN-GPU-1",
				DeviceName:   "NVIDIA A100",
				NVLinks: []NVLink{
					{LinkID: 0, RemotePCIAddress: "0000:c3:00.0", RemoteLinkID: 30},
				},
			},
			{
				GPUID:        2,
				UUID:         "GPU-22222222-2222-2222-2222-222222222222",
				PCIAddress:   "0002:00:00.0",
				SerialNumber: "SN-GPU-2",
				DeviceName:   "NVIDIA A100",
				NVLinks:      []NVLink{},
			},
			{
				GPUID:        3,
				UUID:         "GPU-33333333-3333-3333-3333-333333333333",
				PCIAddress:   "0000:19:00.0",
				SerialNumber: "SN-GPU-3",
				DeviceName:   "NVIDIA A100",
				NVLinks:      []NVLink{},
			},
		},
		NVSwitches: []string{"0000:c3:00.0"},
	}
}

func InjectMetadata(t *testing.T, ctx context.Context,
	restConfig *rest.Config, namespace, podName, containerName string, metadata *GPUMetadata) {
	t.Helper()

	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err, "failed to marshal metadata")

	cmd := []string{"sh", "-c",
		fmt.Sprintf("mkdir -p /var/lib/nvsentinel && cat > %s <<'EOF'\n%s\nEOF", MetadataFilePath, string(metadataJSON))}
	stdout, stderr, err := ExecInPod(ctx, restConfig, namespace, podName, containerName, cmd)
	require.NoError(t, err, "failed to inject metadata file: stdout=%s, stderr=%s", stdout, stderr)

	t.Logf("Injected metadata file to %s in pod %s", MetadataFilePath, podName)
}
