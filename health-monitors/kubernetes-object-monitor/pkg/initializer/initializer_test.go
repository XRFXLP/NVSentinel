// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package initializer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
)

func TestBuildManagerOptions_CacheSyncTimeout_PreservesConfiguredValue(t *testing.T) {
	timeout := 10 * time.Minute

	opts := buildManagerOptions(Params{CacheSyncTimeout: timeout}, cache.Options{})

	require.Equal(t, timeout, opts.Controller.CacheSyncTimeout)
}

func TestBuildManagerOptions_UnstructuredReads_AreServedFromCache(t *testing.T) {
	opts := buildManagerOptions(Params{}, cache.Options{})

	// Without this the reconciler's Get of an unstructured object is a live
	// call to the API server on every reconcile.
	require.NotNil(t, opts.Client.Cache)
	require.True(t, opts.Client.Cache.Unstructured)
}

func TestBuildCacheOptionsLimitsGVKToConfiguredNamespaces(t *testing.T) {
	resyncPeriod := time.Minute
	opts, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{
		testPolicy("gpu-operator-pod-health", "", "v1", "Pod", "gpu-operator"),
		testPolicy("monitoring-pod-health", "", "v1", "Pod", "monitoring"),
		testPolicy("node-not-ready", "", "v1", "Node", ""),
	}, resyncPeriod)
	require.NoError(t, err)

	require.NotNil(t, opts.SyncPeriod)
	require.Equal(t, resyncPeriod, *opts.SyncPeriod)

	byObj, ok := byObjectForGVK(opts, schema.GroupVersionKind{Version: "v1", Kind: "Pod"})
	require.True(t, ok)
	require.Contains(t, byObj.Namespaces, "gpu-operator")
	require.Contains(t, byObj.Namespaces, "monitoring")

	// The cluster-scoped Node still gets an entry so that it has somewhere to
	// carry a transform, and its Namespaces stays nil, which controller-runtime
	// requires for cluster-scoped kinds and which caches cluster-wide.
	byObj, ok = byObjectForGVK(opts, schema.GroupVersionKind{Version: "v1", Kind: "Node"})
	require.True(t, ok)
	require.Nil(t, byObj.Namespaces)
}

func TestBuildCacheOptionsKeepsGVKAllNamespacesWhenAnyPolicyOmitsNamespace(t *testing.T) {
	opts, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{
		testPolicy("gpu-operator-pod-health", "", "v1", "Pod", "gpu-operator"),
		testPolicy("all-pod-health", "", "v1", "Pod", ""),
	}, time.Minute)
	require.NoError(t, err)

	byObj, ok := byObjectForGVK(opts, schema.GroupVersionKind{Version: "v1", Kind: "Pod"})
	require.True(t, ok)
	require.Empty(t, byObj.Namespaces)
}

func TestBuildCacheOptions_WatchedGVKs_EachGetATransform(t *testing.T) {
	opts, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{
		policyWithExpressions("node-not-ready", nodeGVK, nodeNotReadyPredicate, ""),
		policyWithExpressions("pod-health", podGVK, `resource.status.phase != 'Running'`, ""),
	}, time.Minute)
	require.NoError(t, err)

	for _, gvk := range []schema.GroupVersionKind{nodeGVK, podGVK} {
		byObj, ok := byObjectForGVK(opts, gvk)
		require.True(t, ok, "no cache entry for %s", gvk)
		require.NotNil(t, byObj.Transform, "no transform for %s", gvk)
	}
}

func TestBuildCacheOptions_UnderivablePolicyFields_OmitTransform(t *testing.T) {
	opts, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{
		policyWithExpressions("node-opaque", nodeGVK, `size(resource) > 3`, ""),
	}, time.Minute)
	require.NoError(t, err)

	byObj, ok := byObjectForGVK(opts, nodeGVK)
	require.True(t, ok)
	require.Nil(t, byObj.Transform)
}

func TestBuildCacheOptions_NoEnabledPolicies_HasNoEntries(t *testing.T) {
	disabled := testPolicy("node-not-ready", "", "v1", "Node", "")
	disabled.Enabled = false

	opts, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{disabled}, time.Minute)
	require.NoError(t, err)

	require.Empty(t, opts.ByObject)
}

func TestBuildCacheOptionsRejectsNamespaceForClusterScopedGVK(t *testing.T) {
	_, err := buildCacheOptionsWithRESTMapper(testRESTMapper(), []config.Policy{
		testPolicy("cluster-thing-health", "example.com", "v1", "ClusterThing", "gpu-operator"),
	}, time.Minute)

	require.Error(t, err)
	require.Contains(t, err.Error(), "resource.namespace cannot be set for cluster-scoped resource example.com/v1, Kind=ClusterThing")
}

func testRESTMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Version: "v1"},
		{Group: "example.com", Version: "v1"},
	})
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Node"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "ClusterThing"}, meta.RESTScopeRoot)

	return mapper
}

func byObjectForGVK(opts cache.Options, gvk schema.GroupVersionKind) (cache.ByObject, bool) {
	for obj, byObj := range opts.ByObject {
		if obj.GetObjectKind().GroupVersionKind() == gvk {
			return byObj, true
		}
	}

	return cache.ByObject{}, false
}

func testPolicy(name, group, version, kind, namespace string) config.Policy {
	return config.Policy{
		Name:    name,
		Enabled: true,
		Resource: config.ResourceSpec{
			Group:     group,
			Version:   version,
			Kind:      kind,
			Namespace: namespace,
		},
		Predicate: config.PredicateSpec{
			Expression: "true",
		},
		HealthEvent: config.HealthEventSpec{
			ComponentClass: "Software",
			Message:        "test",
		},
	}
}
