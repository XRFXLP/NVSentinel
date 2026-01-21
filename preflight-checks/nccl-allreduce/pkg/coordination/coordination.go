// Package coordination handles gang coordination via ConfigMap for NCCL tests.
// Each pod in a gang registers itself, then waits for all peers before starting.
//
// Flow:
// 1. Webhook creates ConfigMap with expected_count
// 2. Each pod's init container calls RegisterAndWait()
// 3. RegisterAndWait patches ConfigMap to add this pod's IP
// 4. RegisterAndWait polls until all peers are registered
// 5. Master IP is rank 0's IP (first pod alphabetically)
package coordination

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// ConfigMap keys
	KeyExpectedCount = "expected_count"
	KeyMasterAddr    = "master_addr"
	KeyMasterPort    = "master_port"
	KeyPeers         = "peers"

	// Default values
	DefaultMasterPort   = "29500"
	DefaultPollInterval = 5 * time.Second

	// Kubernetes API paths
	k8sServiceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	k8sAPIServer          = "https://kubernetes.default.svc"
)

// PeerInfo contains information about a gang peer.
type PeerInfo struct {
	PodName string
	PodIP   string
}

// GangConfig contains the gang coordination configuration.
type GangConfig struct {
	ExpectedCount int
	MasterAddr    string
	MasterPort    string
	Peers         []PeerInfo
	MyRank        int
	MyPodName     string
	MyPodIP       string
}

// Coordinator handles gang coordination via ConfigMap.
type Coordinator struct {
	configDir    string
	pollInterval time.Duration
}

// NewCoordinator creates a new gang coordinator.
func NewCoordinator(configDir string) *Coordinator {
	return &Coordinator{
		configDir:    configDir,
		pollInterval: DefaultPollInterval,
	}
}

// WaitForGang waits until all gang members have registered and returns the configuration.
func (c *Coordinator) WaitForGang(ctx context.Context, myPodName, myPodIP string, timeout time.Duration) (*GangConfig, error) {
	deadline := time.Now().Add(timeout)

	slog.Info("Waiting for gang formation",
		"pod", myPodName,
		"ip", myPodIP,
		"config_dir", c.configDir,
		"timeout", timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for gang formation after %v", timeout)
		}

		config, err := c.readConfig(myPodName, myPodIP)
		if err != nil {
			slog.Debug("Error reading config, will retry", "error", err)
			time.Sleep(c.pollInterval)
			continue
		}

		if len(config.Peers) >= config.ExpectedCount {
			slog.Info("Gang formation complete",
				"expected", config.ExpectedCount,
				"actual", len(config.Peers),
				"my_rank", config.MyRank)
			return config, nil
		}

		slog.Info("Waiting for more peers",
			"expected", config.ExpectedCount,
			"current", len(config.Peers),
			"remaining", config.ExpectedCount-len(config.Peers))
		time.Sleep(c.pollInterval)
	}
}

// readConfig reads the gang configuration from the ConfigMap volume.
func (c *Coordinator) readConfig(myPodName, myPodIP string) (*GangConfig, error) {
	config := &GangConfig{
		MyPodName:  myPodName,
		MyPodIP:    myPodIP,
		MasterPort: DefaultMasterPort,
	}

	// Read expected count
	expectedStr, err := c.readFile(KeyExpectedCount)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", KeyExpectedCount, err)
	}
	config.ExpectedCount, err = strconv.Atoi(strings.TrimSpace(expectedStr))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", KeyExpectedCount, err)
	}

	// Read master address
	masterAddr, err := c.readFile(KeyMasterAddr)
	if err == nil {
		config.MasterAddr = strings.TrimSpace(masterAddr)
	}

	// Read master port (optional)
	masterPort, err := c.readFile(KeyMasterPort)
	if err == nil && masterPort != "" {
		config.MasterPort = strings.TrimSpace(masterPort)
	}

	// Read peers list
	peersData, err := c.readFile(KeyPeers)
	if err != nil {
		// Peers file might not exist yet
		config.Peers = []PeerInfo{}
	} else {
		config.Peers = c.parsePeers(peersData)
	}

	// Calculate rank based on sorted pod names
	config.MyRank = c.calculateRank(config.Peers, myPodName)

	// If master address not set, use rank 0's IP
	if config.MasterAddr == "" && len(config.Peers) > 0 {
		config.MasterAddr = c.getMasterAddr(config.Peers)
	}

	return config, nil
}

// readFile reads a file from the config directory.
func (c *Coordinator) readFile(name string) (string, error) {
	path := filepath.Join(c.configDir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// parsePeers parses the peers data from the ConfigMap.
// Format: "pod-name:ip\npod-name:ip\n..."
func (c *Coordinator) parsePeers(data string) []PeerInfo {
	var peers []PeerInfo
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			peers = append(peers, PeerInfo{
				PodName: strings.TrimSpace(parts[0]),
				PodIP:   strings.TrimSpace(parts[1]),
			})
		}
	}
	return peers
}

// calculateRank calculates the rank for a pod by sorting pod names alphabetically.
func (c *Coordinator) calculateRank(peers []PeerInfo, myPodName string) int {
	if len(peers) == 0 {
		return -1
	}

	// Sort pod names alphabetically
	podNames := make([]string, len(peers))
	for i, peer := range peers {
		podNames[i] = peer.PodName
	}
	sort.Strings(podNames)

	// Find my rank
	for i, name := range podNames {
		if name == myPodName {
			return i
		}
	}

	return -1 // Pod not in peers list yet
}

// getMasterAddr returns the IP of rank 0 (first pod alphabetically).
func (c *Coordinator) getMasterAddr(peers []PeerInfo) string {
	if len(peers) == 0 {
		return ""
	}

	// Sort by pod name
	sorted := make([]PeerInfo, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PodName < sorted[j].PodName
	})

	return sorted[0].PodIP
}

// GetTorchrunArgs builds the torchrun command arguments from the gang config.
func (config *GangConfig) GetTorchrunArgs(nprocsPerNode int, scriptPath string) []string {
	return []string{
		"torchrun",
		fmt.Sprintf("--nnodes=%d", config.ExpectedCount),
		fmt.Sprintf("--nproc_per_node=%d", nprocsPerNode),
		fmt.Sprintf("--node_rank=%d", config.MyRank),
		fmt.Sprintf("--master_addr=%s", config.MasterAddr),
		fmt.Sprintf("--master_port=%s", config.MasterPort),
		scriptPath,
	}
}

// String returns a string representation of the gang config.
func (config *GangConfig) String() string {
	return fmt.Sprintf("GangConfig{nodes=%d, rank=%d, master=%s:%s, peers=%d}",
		config.ExpectedCount, config.MyRank, config.MasterAddr, config.MasterPort, len(config.Peers))
}

// =============================================================================
// K8sCoordinator - Uses Kubernetes API to register and coordinate gang members
// =============================================================================

// K8sCoordinator coordinates gang members via Kubernetes ConfigMap API.
// It registers the current pod's IP and waits for all peers to register.
type K8sCoordinator struct {
	namespace     string
	configMapName string
	pollInterval  time.Duration
	httpClient    *http.Client
	token         string
}

// NewK8sCoordinator creates a coordinator that uses the Kubernetes API.
func NewK8sCoordinator(namespace, gangID string) (*K8sCoordinator, error) {
	// Read service account token
	tokenPath := filepath.Join(k8sServiceAccountPath, "token")
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read service account token: %w", err)
	}

	// Read CA cert for TLS
	caPath := filepath.Join(k8sServiceAccountPath, "ca.crt")
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
	}

	return &K8sCoordinator{
		namespace:     namespace,
		configMapName: fmt.Sprintf("nvsentinel-gang-%s", gangID),
		pollInterval:  DefaultPollInterval,
		httpClient:    httpClient,
		token:         string(tokenBytes),
	}, nil
}

// RegisterAndWait registers this pod and waits for all gang members.
func (k *K8sCoordinator) RegisterAndWait(ctx context.Context, podName, podIP string, timeout time.Duration) (*GangConfig, error) {
	deadline := time.Now().Add(timeout)

	slog.Info("Starting gang coordination via Kubernetes API",
		"namespace", k.namespace,
		"configmap", k.configMapName,
		"pod", podName,
		"ip", podIP,
		"timeout", timeout)

	// First, register ourselves
	if err := k.registerPod(ctx, podName, podIP); err != nil {
		return nil, fmt.Errorf("failed to register pod: %w", err)
	}

	slog.Info("Successfully registered with gang ConfigMap")

	// Now wait for all peers
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for gang formation after %v", timeout)
		}

		config, err := k.getGangConfig(ctx, podName, podIP)
		if err != nil {
			slog.Warn("Error reading gang config, will retry", "error", err)
			time.Sleep(k.pollInterval)
			continue
		}

		if len(config.Peers) >= config.ExpectedCount {
			slog.Info("Gang formation complete",
				"expected", config.ExpectedCount,
				"actual", len(config.Peers),
				"my_rank", config.MyRank,
				"master_addr", config.MasterAddr)
			return config, nil
		}

		slog.Info("Waiting for more peers",
			"expected", config.ExpectedCount,
			"current", len(config.Peers),
			"remaining", config.ExpectedCount-len(config.Peers))
		time.Sleep(k.pollInterval)
	}
}

// registerPod adds this pod to the ConfigMap's peers list.
func (k *K8sCoordinator) registerPod(ctx context.Context, podName, podIP string) error {
	// Get current ConfigMap
	cm, err := k.getConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	}

	// Parse current peers
	currentPeers := cm.Data[KeyPeers]
	peerEntry := fmt.Sprintf("%s:%s", podName, podIP)

	// Check if already registered
	if strings.Contains(currentPeers, peerEntry) {
		slog.Info("Pod already registered in ConfigMap")
		return nil
	}

	// Add this pod to peers
	var newPeers string
	if currentPeers == "" {
		newPeers = peerEntry
	} else {
		newPeers = currentPeers + "\n" + peerEntry
	}

	// Patch ConfigMap
	return k.patchConfigMap(ctx, map[string]string{KeyPeers: newPeers})
}

// ConfigMapResponse represents the Kubernetes ConfigMap API response.
type ConfigMapResponse struct {
	Data     map[string]string `json:"data"`
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
}

// getConfigMap fetches the ConfigMap from Kubernetes API.
func (k *K8sCoordinator) getConfigMap(ctx context.Context) (*ConfigMapResponse, error) {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/configmaps/%s",
		k8sAPIServer, k.namespace, k.configMapName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	var cm ConfigMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&cm); err != nil {
		return nil, err
	}

	return &cm, nil
}

// patchConfigMap patches the ConfigMap data.
func (k *K8sCoordinator) patchConfigMap(ctx context.Context, data map[string]string) error {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/configmaps/%s",
		k8sAPIServer, k.namespace, k.configMapName)

	// Use strategic merge patch
	patch := map[string]interface{}{
		"data": data,
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(patchBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("patch failed with %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// getGangConfig reads and parses the gang configuration from ConfigMap.
func (k *K8sCoordinator) getGangConfig(ctx context.Context, myPodName, myPodIP string) (*GangConfig, error) {
	cm, err := k.getConfigMap(ctx)
	if err != nil {
		return nil, err
	}

	config := &GangConfig{
		MyPodName:  myPodName,
		MyPodIP:    myPodIP,
		MasterPort: DefaultMasterPort,
	}

	// Parse expected count
	if expStr, ok := cm.Data[KeyExpectedCount]; ok {
		config.ExpectedCount, err = strconv.Atoi(strings.TrimSpace(expStr))
		if err != nil {
			return nil, fmt.Errorf("invalid expected_count: %w", err)
		}
	}

	// Parse master port
	if port, ok := cm.Data[KeyMasterPort]; ok && port != "" {
		config.MasterPort = strings.TrimSpace(port)
	}

	// Parse peers
	if peersData, ok := cm.Data[KeyPeers]; ok {
		config.Peers = parsePeersData(peersData)
	}

	// Calculate rank and master address
	config.MyRank = calculateRankFromPeers(config.Peers, myPodName)
	config.MasterAddr = getMasterAddrFromPeers(config.Peers)

	return config, nil
}

// Helper functions for parsing (shared with file-based coordinator)

func parsePeersData(data string) []PeerInfo {
	var peers []PeerInfo
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			peers = append(peers, PeerInfo{
				PodName: strings.TrimSpace(parts[0]),
				PodIP:   strings.TrimSpace(parts[1]),
			})
		}
	}
	return peers
}

func calculateRankFromPeers(peers []PeerInfo, myPodName string) int {
	if len(peers) == 0 {
		return -1
	}

	podNames := make([]string, len(peers))
	for i, peer := range peers {
		podNames[i] = peer.PodName
	}
	sort.Strings(podNames)

	for i, name := range podNames {
		if name == myPodName {
			return i
		}
	}
	return -1
}

func getMasterAddrFromPeers(peers []PeerInfo) string {
	if len(peers) == 0 {
		return ""
	}

	sorted := make([]PeerInfo, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PodName < sorted[j].PodName
	})

	return sorted[0].PodIP
}
