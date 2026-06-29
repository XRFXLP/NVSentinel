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

package gang

import (
	"fmt"
	"log/slog"

	"github.com/nvidia/nvsentinel/preflight/pkg/config"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DiscovererResolver returns the gang discoverer that applies to a given
// namespace. It holds a cluster-wide default discoverer plus zero or more
// namespace-scoped overrides, allowing different namespaces to use different
// gang-scheduling systems (e.g. Volcano in one namespace, native Kubernetes
// in another).
type DiscovererResolver struct {
	defaultDiscoverer GangDiscoverer
	byNamespace       map[string]GangDiscoverer
}

// NewResolver creates a DiscovererResolver from an already-constructed default
// discoverer and an optional map of namespace -> discoverer overrides. It is
// primarily useful for tests and callers that build discoverers themselves;
// production code typically uses NewResolverFromConfig.
func NewResolver(defaultDiscoverer GangDiscoverer, overrides map[string]GangDiscoverer) *DiscovererResolver {
	byNamespace := make(map[string]GangDiscoverer, len(overrides))
	for ns, d := range overrides {
		byNamespace[ns] = d
	}

	return &DiscovererResolver{
		defaultDiscoverer: defaultDiscoverer,
		byNamespace:       byNamespace,
	}
}

// NewResolverFromConfig builds a DiscovererResolver from the preflight config.
// It constructs the cluster-wide default discoverer from cfg.GangDiscovery and
// one discoverer per namespace override. Each distinct configuration is
// validated against the cluster (via NewDiscovererFromConfig), so an invalid
// or unavailable scheduler CRD fails fast at startup rather than at admission
// time.
func NewResolverFromConfig(
	cfg *config.Config,
	c client.Client,
	restMapper meta.RESTMapper,
) (*DiscovererResolver, error) {
	// Enforce the same override rules as config.Load, in case the Config was
	// built programmatically rather than parsed and validated by Load.
	if err := config.ValidateGangDiscoveryOverrides(cfg.GangDiscoveryOverrides); err != nil {
		return nil, err
	}

	defaultDiscoverer, err := NewDiscovererFromConfig(cfg.GangDiscovery, c, restMapper)
	if err != nil {
		return nil, fmt.Errorf("failed to create default gang discoverer: %w", err)
	}

	byNamespace := make(map[string]GangDiscoverer)

	// Each override entry maps one discoverer to one or more namespaces.
	// Namespaces sharing a single override entry reuse one discoverer instance.
	for i := range cfg.GangDiscoveryOverrides {
		override := cfg.GangDiscoveryOverrides[i]

		discoverer, err := NewDiscovererFromConfig(override.GangDiscovery, c, restMapper)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create gang discoverer for gangDiscoveryOverrides[%d] (namespaces %v): %w",
				i, override.Namespaces, err,
			)
		}

		for _, ns := range override.Namespaces {
			byNamespace[ns] = discoverer

			slog.Info("Registered namespace-scoped gang discoverer",
				"namespace", ns,
				"discoverer", discoverer.Name())
		}
	}

	return NewResolver(defaultDiscoverer, byNamespace), nil
}

// For returns the gang discoverer for the given namespace, falling back to the
// cluster-wide default discoverer when the namespace has no override.
func (r *DiscovererResolver) For(namespace string) GangDiscoverer {
	if r == nil {
		return nil
	}

	if discoverer, ok := r.byNamespace[namespace]; ok {
		return discoverer
	}

	return r.defaultDiscoverer
}
