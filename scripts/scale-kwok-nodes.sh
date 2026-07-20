#!/usr/bin/env bash
# Scales KWOK fake GPU nodes to TARGET_NODES (default 1000).
# Each node gets labels matching real GPU nodes so NVSentinel DaemonSets
# (platform-connector, gpu-health-monitor, syslog-health-monitor,
# metadata-collector) all schedule correctly.
#
# Usage:
#   TARGET_NODES=1000 ./scripts/scale-kwok-nodes.sh
#   TARGET_NODES=500 DRY_RUN=1 ./scripts/scale-kwok-nodes.sh   # print manifests only

set -euo pipefail

TARGET_NODES="${TARGET_NODES:-1000}"
DRY_RUN="${DRY_RUN:-0}"
NAMESPACE="nvsentinel-scale"
WORKLOAD_PODS_PER_NODE=10   # simulates GPU job pods for O(P) informer pressure

current=$(kubectl get nodes --no-headers 2>/dev/null | grep "^nvs-kwok-node" | wc -l)
echo "Current KWOK nodes: $current  →  target: $TARGET_NODES"

# Generate a single node manifest.
# Labels match a real A100 GPU node on this cluster, stripped of
# cluster-specific values (region, subscription, etc.) that are irrelevant
# for scale testing.
make_node() {
    local i="$1"
    local name
    name=$(printf "nvs-kwok-node-%03d" "$i")
    cat <<EOF
---
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    node.alpha.kubernetes.io/ttl: "15"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    kubernetes.io/hostname: ${name}
    kubernetes.io/os: linux
    node-role.kubernetes.io/gpu: ""
    type: kwok
    nvsentinel-scale-test: "true"
    # NVSentinel DaemonSet scheduling labels
    nvsentinel.dgxc.nvidia.com/dcgm.version: 4.x
    nvsentinel.dgxc.nvidia.com/driver.installed: "true"
    nvsentinel.dgxc.nvidia.com/kata.enabled: "false"
    nvsentinel.dgxc.nvidia.com/gpu.count.current: "8"
    nvsentinel.dgxc.nvidia.com/gpu.count.expected: "8"
    nvsentinel.dgxc.nvidia.com/nic.count.current: "8"
    nvsentinel.dgxc.nvidia.com/nic.count.expected: "8"
    # GPU labels (matching real A100 nodes)
    nvidia.com/gpu.present: "true"
    nvidia.com/gpu.count: "8"
    nvidia.com/gpu.family: ampere
    nvidia.com/gpu.product: NVIDIA-A100-SXM4-80GB
    nvidia.com/gpu.memory: "81920"
    nvidia.com/gpu.compute.major: "8"
    nvidia.com/gpu.compute.minor: "0"
    nvidia.com/gpu.deploy.dcgm: "true"
    nvidia.com/gpu.deploy.driver: "true"
    nvidia.com/gpu.deploy.device-plugin: "true"
    nvidia.com/gpu.deploy.container-toolkit: "true"
    nvidia.com/mig.capable: "true"
    nvidia.com/mig.config: all-disabled
    nvidia.com/mig.strategy: mixed
    skyhook.nvidia.com/status_cluster: complete
  name: ${name}
spec:
  providerID: kwok://${name}
  taints:
  - effect: NoSchedule
    key: nvidia.com/gpu
    value: "true"
status:
  allocatable:
    cpu: "96"
    memory: 1843200Mi
    nvidia.com/gpu: "8"
    nvidia.com/mlnxnics: "8"
    pods: "250"
  capacity:
    cpu: "96"
    memory: 1843200Mi
    nvidia.com/gpu: "8"
    nvidia.com/mlnxnics: "8"
    pods: "250"
  conditions:
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: kwok fake node ready
    reason: KubeletReady
    status: "True"
    type: Ready
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: KubeletHasNoDiskPressure
    status: "False"
    type: DiskPressure
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: KubeletHasSufficientMemory
    status: "False"
    type: MemoryPressure
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: KubeletHasSufficientPID
    status: "False"
    type: PIDPressure
  # NVSentinel-specific conditions (matching real GPU node set)
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuDcgmConnectivityFailure
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuDriverWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuMemWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuNvlinkWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuPowerWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuThermalWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuXidError
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: SysLogsXIDError
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: SysLogsSXIDError
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuAllWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuPmuWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuSmWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuNvswitchFatalWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuNvswitchNonfatalWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuPcieWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuInforomWatch
  - lastHeartbeatTime: "2026-01-01T00:00:00Z"
    lastTransitionTime: "2026-01-01T00:00:00Z"
    message: ""
    reason: ""
    status: "False"
    type: GpuMcuWatch
  nodeInfo:
    architecture: amd64
    bootID: ""
    containerRuntimeVersion: containerd://1.7.0
    kernelVersion: 5.15.0-fake
    kubeProxyVersion: fake
    kubeletVersion: fake
    machineID: ""
    operatingSystem: linux
    osImage: Ubuntu 22.04
    systemUUID: ""
EOF
}

# Generate workload pods that simulate GPU jobs on each node.
# These inflate the pod count (O(P)) for informer cache testing.
make_pods() {
    local node_num="$1"
    local node_name
    node_name=$(printf "nvs-kwok-node-%03d" "$node_num")
    for j in $(seq 1 "$WORKLOAD_PODS_PER_NODE"); do
        local pod_name
        pod_name=$(printf "nvs-kwok-pod-%03d-%02d" "$node_num" "$j")
        cat <<EOF
---
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${NAMESPACE}
  labels:
    nvsentinel-scale-test: "true"
    app: fake-gpu-workload
spec:
  nodeName: ${node_name}
  tolerations:
  - operator: Exists
  containers:
  - name: workload
    image: registry.k8s.io/pause:3.9
    resources:
      limits:
        nvidia.com/gpu: "1"
EOF
    done
}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1 || true
kubectl label namespace "$NAMESPACE" \
    app.kubernetes.io/name=nvsentinel-scale-harness \
    nvsentinel-scale-test=true \
    --overwrite >/dev/null 2>&1 || true

# Apply nodes and pods in batches of 50 to avoid overwhelming the API server
BATCH=50
for start in $(seq 1 "$BATCH" "$TARGET_NODES"); do
    end=$((start + BATCH - 1))
    if [ "$end" -gt "$TARGET_NODES" ]; then end="$TARGET_NODES"; fi

    echo "Applying nodes $start–$end ..."
    manifest=""
    for i in $(seq "$start" "$end"); do
        node_num=$((current + i))
        manifest+="$(make_node "$node_num")"$'\n'
        manifest+="$(make_pods "$node_num")"$'\n'
    done

    if [ "$DRY_RUN" = "1" ]; then
        echo "$manifest"
    else
        echo "$manifest" | kubectl apply -f - >/dev/null
    fi
done

echo ""
echo "Done. Total KWOK nodes now: $((current + TARGET_NODES))"
echo ""
echo "What this tests:"
echo "  O(N) - platform-connector MongoDB connections: $((current + TARGET_NODES)) × 7"
echo "  O(P) - KOM/node-drainer pod cache: $((WORKLOAD_PODS_PER_NODE * TARGET_NODES)) new workload pods"
echo "  O(N) - central informer node cache: $((current + TARGET_NODES)) node objects × ~34 KB each"
echo ""
echo "What this does NOT test (still needs synthetic health event injector):"
echo "  O(R)   - etcd write saturation, consumer throughput"
echo "  O(R²)  - K8s Events LIST amplification"
