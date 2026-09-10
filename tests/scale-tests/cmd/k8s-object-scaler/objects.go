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
	"encoding/json"
	"fmt"
	"strings"
)

// Padding profiles. Which fields a component keeps in its informer cache
// decides what an object actually costs it, so padding "to 9 KB" is meaningless
// without saying where the bytes go. These profiles place padding so that a
// synthetic object costs what a real one costs *in the named component*.
//
// Retained-field padding goes into annotations; stripped-field padding goes
// into fields every transform drops (node status.images, pod container env).
const (
	profileNone      = "none"
	profileKOM       = "kom"       // no transform: everything is retained
	profileJanitor   = "janitor"   // keeps name/uid/rv/labels only; drops annotations
	profileND        = "nd"        // keeps one allow-listed annotation; drops the rest
	profilePreflight = "preflight" // keeps annotations, drops spec/status detail
)

// padAnnotationKey holds retained-field padding.
const padAnnotationKey = "benchmark.nvsentinel.io/padding"

// profileRetains reports whether the named component's transform keeps
// annotations. When it does not, retained-byte padding placed in annotations is
// invisible to it and the tool says so rather than silently measuring nothing.
func profileRetains(profile string) bool {
	switch profile {
	case profileKOM, profilePreflight:
		return true
	case profileJanitor, profileND:
		return false
	default:
		return true
	}
}

func pad(n int) string {
	if n <= 0 {
		return ""
	}

	return strings.Repeat("x", n)
}

type objectSpec struct {
	Kind        string
	NamePrefix  string
	Namespace   string
	SpreadNodes int
	NodePrefix  string
	NodeStart   int
	Retained    int
	Stripped    int
	Profile     string
	GPUs        string

	// ManagedFields is the synthesised size of metadata.managedFields. Real
	// objects carry 27-38% of their bytes here (18 KB on a node, 5-7 KB on a
	// pod). It is retained by fault-quarantine and stripped by node-drainer and
	// janitor, so omitting it compresses the measured difference between them.
	ManagedFields int

	// NodeLabels is how many labels to put on a node, and NodeLabelBytes the
	// total size they should occupy. Production GPU worker nodes carry ~180
	// labels totalling ~9 KB, which janitor retains.
	NodeLabels     int
	NodeLabelBytes int

	// OwnerKind sets metadata.ownerReferences[0].kind. "DaemonSet" makes the
	// pod non-evictable, which is what ~80% of production pods are.
	OwnerKind string
	OwnerUID  string
	OwnerName string

	// PodProfile selects the size/shape profile: "user" (~50 KB, ~80% spec) or
	// "system" (~14 KB).
	PodProfile string

	// PodLabels are merged over the benchmark defaults, so a pod can carry the
	// labels a component's informer selects on.
	PodLabels map[string]string
}

// node builds a KWOK node. The taint and the kwok.x-k8s.io/node annotation are
// what make the KWOK controller adopt it; without them it stays NotReady.
func (s *objectSpec) node(idx int) ([]byte, error) {
	name := fmt.Sprintf("%s%06d", s.NamePrefix, idx)

	// metadata.managedFields cannot be injected: the API server owns that field
	// and recomputes it from apply operations, so anything POSTed there is
	// discarded. Production nodes carry ~18 KB of it, and fault-quarantine
	// retains it alongside labels and annotations. Its budget is folded into
	// annotations, which are settable, unbounded in length, and retained by the
	// same components. Labels cannot absorb it -- Kubernetes caps a label value
	// at 63 characters.
	annotations := map[string]string{"kwok.x-k8s.io/node": "fake"}
	if n := s.Retained + s.ManagedFields; n > 0 {
		annotations[padAnnotationKey] = pad(n)
	}

	// The kwok.x-k8s.io/node key must be a LABEL as well as an annotation: the
	// KWOK controller adopts the node via the annotation, but the Stage
	// selectors (node-initialize, node-heartbeat) are label selectors. With
	// only the annotation the node is created but never initialised.
	//
	// nvidia.com/gpu.present is what fault-quarantine's --gpu-node-label
	// defaults to; without it the node is not treated as a GPU node.
	labels := labelsFor(map[string]string{
		"kwok.x-k8s.io/node":               "fake",
		"type":                             "kwok",
		"benchmark":                        "true",
		"nvidia.com/gpu.present":           "true",
		"kubernetes.io/os":                 "linux",
		"kubernetes.io/arch":               "amd64",
		"kubernetes.io/hostname":           name,
		"node.kubernetes.io/instance-type": "gpu-worker",
	}, s.NodeLabels, s.NodeLabelBytes)

	meta := map[string]any{
		"name":        name,
		"annotations": annotations,
		"labels":      labels,
	}

	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata":   meta,
		"spec": map[string]any{
			"taints": []map[string]any{{
				"key":    "kwok.x-k8s.io/node",
				"value":  "fake",
				"effect": "NoSchedule",
			}},
		},
	}

	// Stripped-field padding rides in status.images, which every node
	// transform discards, and where a real node genuinely carries several KB.
	if s.Stripped > 0 {
		obj["status"] = map[string]any{"images": imagesFor(s.Stripped)}
	}

	return json.Marshal(obj)
}

// managedFieldsFor synthesises a metadata.managedFields list of roughly n
// bytes. Real objects accumulate one entry per controller that has touched
// them; the exact contents do not matter, only that the bytes are present in
// the field that transforms either keep or drop.
func managedFieldsFor(n int) []map[string]any {
	if n <= 0 {
		return nil
	}

	managers := []string{
		"kubelet", "kube-controller-manager", "node-problem-detector",
		"gpu-operator", "network-operator", "nvsentinel", "cloud-controller",
		"skyhook-operator",
	}

	// Entries are appended until the serialised list reaches n bytes, so the
	// caller gets the size it asked for rather than an estimate.
	const chunk = 1500

	out := []map[string]any{}

	for i := 0; sizeOf(out) < n; i++ {
		out = append(out, map[string]any{
			"manager":    managers[i%len(managers)],
			"operation":  "Update",
			"apiVersion": "v1",
			"time":       "2026-01-01T00:00:00Z",
			"fieldsType": "FieldsV1",
			"fieldsV1": map[string]any{
				"f:metadata": map[string]any{
					"f:labels":      map[string]any{"f:pad": pad(chunk / 2)},
					"f:annotations": map[string]any{"f:pad": pad(chunk / 2)},
				},
			},
		})
	}

	return out
}

// sizeOf returns the compact JSON size of v, used to hit byte targets exactly
// rather than by estimation.
func sizeOf(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}

	return len(b)
}

// labelsFor pads a label map out to n entries totalling roughly targetBytes,
// which is how real nodes carry several KB in metadata.labels.
// baseNodeLabelCount is how many real (non-synthesised) labels node() puts on
// every node. Synthesised padding labels are numbered from here upward, so the
// creation path and the resize path generate identical key names.
const baseNodeLabelCount = 8

// podLabels returns the pod's labels: the benchmark defaults, with any
// caller-supplied labels merged over them.
//
// Components select the pods they cache by label, so a benchmark pod carrying
// only "app: bench-pod" is invisible to them. labeler is the clearest case: its
// four pod informers select app in (nvidia-dcgm, nvidia-driver-daemonset), a
// driver-component label, and k8s-app=<gke-installer>, so a fleet of benchmark
// pods produced no pod term for it at all and the term read as zero. Setting
// the labels the component actually selects on is what makes that term
// measurable.
func (s *objectSpec) podLabels() map[string]string {
	labels := map[string]string{"benchmark": "true", "app": "bench-pod"}
	for k, v := range s.PodLabels {
		labels[k] = v
	}

	return labels
}

// syntheticLabels returns the padding labels numbered [from, n), sized so the
// whole label set totals roughly targetBytes.
func syntheticLabels(from, n, targetBytes int) map[string]string {
	out := map[string]string{}
	if n <= from {
		return out
	}

	// Budget per synthesised label, minus key and JSON punctuation. Kubernetes
	// caps a label value at 63 characters, so labels can carry at most about
	// 63 bytes each however large the target is.
	const maxLabelValue = 63

	valLen := 8
	if targetBytes > 0 {
		valLen = min(max(targetBytes/(n-from)-30, 1), maxLabelValue)
	}

	for i := from; i < n; i++ {
		out[fmt.Sprintf("bench.nvsentinel.io/l%03d", i)] = pad(valLen)
	}

	return out
}

func labelsFor(base map[string]string, n, targetBytes int) map[string]string {
	for k, v := range syntheticLabels(len(base), n, targetBytes) {
		base[k] = v
	}

	return base
}

// imagesFor synthesises a node image list of roughly n bytes, mirroring the
// shape real nodes carry.
func imagesFor(n int) []map[string]any {
	const perEntry = 120

	count := max(n/perEntry, 1)
	images := make([]map[string]any, 0, count)

	for i := range count {
		images = append(images, map[string]any{
			"names":     []string{fmt.Sprintf("registry.local/pad/image-%04d@sha256:%064d", i, i)},
			"sizeBytes": 100000000 + i,
		})
	}

	return images
}

// Pod profiles observed in production. User workload pods are ~50 KB with
// roughly 80% of bytes in spec; system and platform pods are ~14 KB.
const (
	profileUser   = "user"
	profileSystem = "system"
)

// podProfileBytes returns the spec-padding and managedFields sizes for a
// profile when the caller has not set them explicitly.
func podProfileBytes(profile string) (specPad, managedFields int) {
	if profile == profileSystem {
		return 4000, 5000
	}

	return 40000, 7000
}

// pod builds a benchmark pod already bound to a node. spec.nodeName is set
// directly so the scheduler is bypassed entirely -- KWOK nodes are tainted, and
// the scheduler would become the bottleneck long before the API server does.
func (s *objectSpec) pod(idx int) ([]byte, error) {
	// Pods are spread across [NodeStart, NodeStart+SpreadNodes). The offset
	// matters: without it pods bind to node indices starting at 0, which
	// usually do not exist, and they sit Pending forever because KWOK only
	// plays stages for pods on nodes it manages.
	nodeIdx := s.NodeStart
	if s.SpreadNodes > 0 {
		nodeIdx = s.NodeStart + idx%s.SpreadNodes
	}

	return s.podNamed(fmt.Sprintf("%s%09d", s.NamePrefix, idx), nodeIdx)
}

// buildWorkloadPod builds a pod for workload mode, where the name comes from the
// job rather than a global index and the node is chosen by the caller. Workload
// pods deliberately carry no ownerReferences and live in a user namespace, so
// node-drainer treats them as drain-eligible and caches them in full.
func (s *objectSpec) buildWorkloadPod(name string, nodeIdx int) (body []byte, path string, err error) {
	body, err = s.podNamed(name, nodeIdx)

	return body, fmt.Sprintf("/api/v1/namespaces/%s/pods", s.Namespace), err
}

// namedDeletePath addresses a pod by name rather than by index.
func (s *objectSpec) namedDeletePath(name string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", s.Namespace, name)
}

// podNamed builds a pod with an explicit name and an explicit node index, so
// callers that already know where the pod belongs are not forced through the
// index-derived spreading above.
func (s *objectSpec) podNamed(name string, nodeIdx int) ([]byte, error) {

	// Pods are spread across [NodeStart, NodeStart+SpreadNodes). The offset
	// matters: without it pods bind to node indices starting at 0, which
	// usually do not exist, and they sit Pending forever because KWOK only
	// plays stages for pods on nodes it manages.
	defSpec, defMF := podProfileBytes(s.PodProfile)

	specPad := s.Stripped
	if specPad == 0 {
		specPad = defSpec
	}

	mfBytes := s.ManagedFields
	if mfBytes == 0 {
		mfBytes = defMF
	}

	// Real pods carry ~2 bytes of annotations, so retained padding on a pod
	// goes into spec rather than annotations. Only set annotations when the
	// caller explicitly asks for them.
	annotations := map[string]string{}
	if s.Retained > 0 {
		annotations[padAnnotationKey] = pad(s.Retained)
	}

	container := map[string]any{
		"name":  "pause",
		"image": "registry.k8s.io/pause:3.9",
		"resources": map[string]any{
			"requests": map[string]string{"cpu": "1m", "memory": "4Mi"},
		},
	}

	// Bulk pod bytes live in spec on real pods (78-92% for user workloads),
	// carried by container env, and every pod transform discards them.
	if specPad > 0 {
		container["env"] = []map[string]string{{
			"name":  "BENCH_PADDING",
			"value": pad(specPad),
		}}
	}

	if s.GPUs != "" && s.GPUs != "0" {
		container["resources"].(map[string]any)["limits"] = map[string]string{
			"nvidia.com/gpu": s.GPUs,
		}
	}

	// As with nodes, managedFields is server-owned and cannot be injected. Its
	// budget is folded into spec padding, which every pod transform discards
	// just as it discards managedFields, so the retained totals still line up.
	specPad += mfBytes

	meta := map[string]any{
		"name":        name,
		"namespace":   s.Namespace,
		"annotations": annotations,
		"labels":      s.podLabels(),
	}

	// node-drainer skips DaemonSet-owned pods, which is ~80% of a production
	// fleet. Setting the owner kind is what makes a generated pod
	// non-evictable, and it is also what selects node-drainer's identity-stub
	// transform rather than its drain-eligible one.
	//
	// controller is deliberately left false. With controller=true the owning
	// controller applies ControllerRef adoption semantics: it sees a pod
	// claiming its control that does not match its selector and releases it,
	// stripping the ownerReference within seconds. A plain owner reference is
	// enough here, because node-drainer's isDaemonSetOwned only inspects
	// owner.Kind, and the garbage collector only needs the owner to exist.
	if s.OwnerKind != "" && s.OwnerKind != "none" {
		meta["ownerReferences"] = []map[string]any{{
			"apiVersion": "apps/v1",
			"kind":       s.OwnerKind,
			"name":       s.OwnerName,
			"uid":        s.OwnerUID,
		}}
	}

	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   meta,
		"spec": map[string]any{
			"nodeName":      fmt.Sprintf("%s%06d", s.NodePrefix, nodeIdx),
			"restartPolicy": "Never",
			"containers":    []map[string]any{container},
			"tolerations": []map[string]any{
				{"key": "kwok.x-k8s.io/node", "operator": "Exists", "effect": "NoSchedule"},
				{"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute"},
				{"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute"},
			},
		},
	}

	return json.Marshal(obj)
}

// build returns the serialised object and the API path to POST it to.
func (s *objectSpec) build(idx int) (body []byte, path string, err error) {
	switch s.Kind {
	case kindNode:
		body, err = s.node(idx)
		return body, "/api/v1/nodes", err
	case kindPod:
		body, err = s.pod(idx)
		return body, fmt.Sprintf("/api/v1/namespaces/%s/pods", s.Namespace), err
	default:
		return nil, "", fmt.Errorf("unknown kind %q", s.Kind)
	}
}

// resizePatch returns a strategic-merge patch that changes only the size-bearing
// parts of an existing object's metadata: the padding annotation and the
// synthesised labels.
//
// This exists so a node-size sweep can mutate one fleet in place instead of
// recreating it per size point. Recreating the fleet means the DaemonSets that
// blanket-tolerate every taint re-fan-out onto the new nodes (11 of them, so
// ~11 pods per node) and the previous point's pods are still being garbage
// collected while the next point is measured. Pod count then moves with node
// size, and components that watch pods cannot be attributed to size alone.
// Patching in place holds node identity, pod placement and pod count exactly
// constant, so node bytes are the only variable.
//
// Only metadata is touched. KWOK's node-heartbeat and node-initialize stages
// write status, so status padding would not survive; metadata is never
// rewritten by them. Stale synthesised labels from a larger previous point are
// explicitly set to null, which is how a strategic-merge patch deletes a map
// key -- otherwise sizes could only ever grow.
func (s *objectSpec) resizePatch(prevLabels int) ([]byte, error) {
	annotations := map[string]any{}
	if n := s.Retained + s.ManagedFields; n > 0 {
		annotations[padAnnotationKey] = pad(n)
	} else {
		annotations[padAnnotationKey] = nil
	}

	labels := map[string]any{}
	for k, v := range syntheticLabels(baseNodeLabelCount, s.NodeLabels, s.NodeLabelBytes) {
		labels[k] = v
	}

	for i := max(s.NodeLabels, baseNodeLabelCount); i < prevLabels; i++ {
		labels[fmt.Sprintf("bench.nvsentinel.io/l%03d", i)] = nil
	}

	return json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
			"labels":      labels,
		},
	})
}

// patchPath returns the API path that patches object idx.
func (s *objectSpec) patchPath(idx int) string {
	return s.deletePath(idx)
}

// deletePath returns the API path that removes object idx.
func (s *objectSpec) deletePath(idx int) string {
	// gracePeriodSeconds=0 is required to remove pods whose node was deleted and
	// recreated: KWOK stops confirming their deletion, so they keep a
	// deletionTimestamp and stay Running indefinitely. With no finalizer set, a
	// zero grace period makes the API server drop them immediately.
	switch s.Kind {
	case kindNode:
		return fmt.Sprintf("/api/v1/nodes/%s%06d", s.NamePrefix, idx)
	default:
		return fmt.Sprintf("/api/v1/namespaces/%s/pods/%s%09d?gracePeriodSeconds=0",
			s.Namespace, s.NamePrefix, idx)
	}
}
