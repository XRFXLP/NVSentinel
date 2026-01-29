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

package webhook

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nvidia/nvsentinel/preflight/pkg/config"
)

type Handler struct {
	injector *Injector
}

func NewHandler(cfg *config.Config) *Handler {
	return &Handler{
		injector: NewInjector(cfg),
	}
}

func (h *Handler) HandleMutate(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read request body", "error", err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var admissionReview admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &admissionReview); err != nil {
		slog.Error("Failed to unmarshal admission review", "error", err)
		http.Error(w, "failed to unmarshal", http.StatusBadRequest)
		return
	}

	response := h.mutate(admissionReview.Request)
	admissionReview.Response = response

	respBytes, err := json.Marshal(admissionReview)
	if err != nil {
		slog.Error("Failed to marshal response", "error", err)
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respBytes)
}

func (h *Handler) mutate(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		return denyResponse("empty request")
	}

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		slog.Error("Failed to unmarshal pod", "error", err)
		return denyResponse(fmt.Sprintf("failed to unmarshal pod: %v", err))
	}

	slog.Info("Processing pod",
		"namespace", req.Namespace,
		"name", pod.Name,
		"generateName", pod.GenerateName)

	patch, err := h.injector.CreatePatch(&pod)
	if err != nil {
		slog.Error("Failed to create patch", "error", err)
		return denyResponse(fmt.Sprintf("failed to create patch: %v", err))
	}

	if patch == nil {
		slog.Info("No injection needed", "namespace", req.Namespace, "pod", pod.Name)
		return allowResponse(req.UID)
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		slog.Error("Failed to marshal patch", "error", err)
		return denyResponse(fmt.Sprintf("failed to marshal patch: %v", err))
	}

	slog.Info("Injecting preflight init containers",
		"namespace", req.Namespace,
		"pod", pod.Name,
		"patchSize", len(patchBytes))

	return patchResponse(req.UID, patchBytes)
}

func allowResponse(uid types.UID) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		UID:     uid,
		Allowed: true,
	}
}

func denyResponse(message string) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: message,
		},
	}
}

func patchResponse(uid types.UID, patch []byte) *admissionv1.AdmissionResponse {
	patchType := admissionv1.PatchTypeJSONPatch
	return &admissionv1.AdmissionResponse{
		UID:       uid,
		Allowed:   true,
		Patch:     patch,
		PatchType: &patchType,
	}
}
