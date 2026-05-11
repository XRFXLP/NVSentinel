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

package cancellation

import "slices"

// Resolver maps a source error code to the list of error codes it cancels.
//
// A nil Resolver behaves identically to one with no rules: Lookup returns nil.
// Handlers should treat a nil Resolver as "cancellations not configured".
type Resolver struct {
	rules map[string][]string
}

// NewResolver builds a Resolver from a single CheckCancellations entry.
//
// Passing nil yields a Resolver whose Lookup always returns nil — useful as a
// default when the parent Config has no entry for the handler's check.
func NewResolver(check *CheckCancellations) *Resolver {
	if check == nil || !check.Enabled {
		return &Resolver{rules: map[string][]string{}}
	}

	rules := make(map[string][]string, len(check.Rules))

	for _, rule := range check.Rules {
		// Defensive copy so callers cannot mutate the resolver via the
		// originating Config slice.
		targets := slices.Clone(rule.CancelErrorCodes)
		rules[rule.OnErrorCode] = targets
	}

	return &Resolver{rules: rules}
}

// Lookup returns the list of error codes that should be cancelled when the
// given source error code is observed. Returns nil when no rule matches.
//
// The returned slice is a defensive copy; callers may mutate it freely
// without affecting the resolver's internal state, mirroring the input-side
// copy that NewResolver performs on construction.
func (r *Resolver) Lookup(sourceErrorCode string) []string {
	if r == nil {
		return nil
	}

	targets, ok := r.rules[sourceErrorCode]
	if !ok {
		return nil
	}

	return slices.Clone(targets)
}

// Empty returns true when the resolver carries no rules.
func (r *Resolver) Empty() bool {
	return r == nil || len(r.rules) == 0
}
