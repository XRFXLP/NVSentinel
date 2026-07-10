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

package client

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

const (
	ResumeControlConfigMapName = "resume-control"
	ResumeControlModeResume    = "RESUME"
	ResumeControlModeCreate    = "CREATE"
)

type resumeControlStore interface {
	GetMode(ctx context.Context, clientName string) (string, error)
	SetMode(ctx context.Context, clientName, mode string) error
}

// ResetResumeTokenOnStartIfConfigured deletes a component's change stream
// resume token when the shared resume-control ConfigMap requests CREATE.
func ResetResumeTokenOnStartIfConfigured(
	ctx context.Context,
	dbClient DatabaseClient,
	tokenConfig TokenConfig,
) error {
	if tokenConfig.ClientName == "event-exporter" {
		return nil
	}

	store, err := newKubernetesResumeControlStore()
	if err != nil {
		return fmt.Errorf("failed to initialize change stream resume control: %w", err)
	}

	return resetResumeTokenOnStartWithStore(ctx, dbClient, tokenConfig, store)
}

func resetResumeTokenOnStartWithStore(
	ctx context.Context,
	dbClient DatabaseClient,
	tokenConfig TokenConfig,
	store resumeControlStore,
) error {
	mode, err := store.GetMode(ctx, tokenConfig.ClientName)
	if err != nil {
		return fmt.Errorf("failed to read change stream resume control for %s: %w", tokenConfig.ClientName, err)
	}

	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" || mode == ResumeControlModeResume {
		return nil
	}

	if mode != ResumeControlModeCreate {
		return fmt.Errorf("invalid change stream resume control mode %q for %s", mode, tokenConfig.ClientName)
	}

	slog.InfoContext(ctx, "Deleting change stream resume token on startup",
		"clientName", tokenConfig.ClientName,
		"tokenDatabase", tokenConfig.TokenDatabase,
		"tokenCollection", tokenConfig.TokenCollection)

	if err := dbClient.DeleteResumeToken(ctx, tokenConfig); err != nil {
		return fmt.Errorf("failed to delete change stream resume token: %w", err)
	}

	if err := store.SetMode(ctx, tokenConfig.ClientName, ResumeControlModeResume); err != nil {
		return fmt.Errorf("failed to reset change stream resume control for %s to %s: %w",
			tokenConfig.ClientName, ResumeControlModeResume, err)
	}

	return nil
}

type kubernetesResumeControlStore struct {
	client    kubernetes.Interface
	name      string
	namespace string
}

func newKubernetesResumeControlStore() (*kubernetesResumeControlStore, error) {
	namespace, err := resumeControlNamespace()
	if err != nil {
		return nil, err
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load in-cluster Kubernetes config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return &kubernetesResumeControlStore{
		client:    clientset,
		name:      ResumeControlConfigMapName,
		namespace: namespace,
	}, nil
}

func resumeControlNamespace() (string, error) {
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		return namespace, nil
	}

	namespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("failed to determine namespace for change stream resume control: %w", err)
	}

	namespace := strings.TrimSpace(string(namespaceBytes))
	if namespace == "" {
		return "", fmt.Errorf("empty namespace for change stream resume control")
	}

	return namespace, nil
}

func (s *kubernetesResumeControlStore) GetMode(ctx context.Context, clientName string) (string, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if err := s.SetMode(ctx, clientName, ResumeControlModeResume); err != nil {
			return "", fmt.Errorf("failed to create default resume control config map: %w", err)
		}

		return ResumeControlModeResume, nil
	}

	if err != nil {
		return "", fmt.Errorf("failed to get resume control config map %s in namespace %s: %w",
			s.name, s.namespace, err)
	}

	if cm.Data == nil {
		if err := s.SetMode(ctx, clientName, ResumeControlModeResume); err != nil {
			return "", fmt.Errorf("failed to set default resume control mode for %s: %w", clientName, err)
		}

		return ResumeControlModeResume, nil
	}

	mode, ok := cm.Data[clientName]
	if !ok || strings.TrimSpace(mode) == "" {
		if err := s.SetMode(ctx, clientName, ResumeControlModeResume); err != nil {
			return "", fmt.Errorf("failed to set default resume control mode for %s: %w", clientName, err)
		}

		return ResumeControlModeResume, nil
	}

	return mode, nil
}

func (s *kubernetesResumeControlStore) SetMode(ctx context.Context, clientName, mode string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			cm = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace},
				Data:       map[string]string{},
			}
			cm.Data[clientName] = mode

			_, createErr := s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, cm, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(createErr) {
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, s.name, createErr)
			}

			return createErr
		}

		if err != nil {
			return err
		}

		if cm.Data == nil {
			cm.Data = map[string]string{}
		}

		cm.Data[clientName] = mode

		_, err = s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})

		return err
	})
}
