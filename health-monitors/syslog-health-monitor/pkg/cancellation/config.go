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

// Package cancellation declares per-monitor "observation A clears observation
// B" rules: when a check emits a fault carrying onErrorCode, the handler
// additionally emits one synthetic healthy event per cancelErrorCodes entry
// to clear matching prior faults on the same impacted entities.
package cancellation

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

// CancellationRule declares that a single source error code should emit
// synthetic healthy events for one or more target error codes.
type CancellationRule struct {
	OnErrorCode      string   `toml:"onErrorCode"`
	CancelErrorCodes []string `toml:"cancelErrorCodes"`
}

// CheckCancellations groups all cancellation rules owned by a single check.
type CheckCancellations struct {
	Name    string             `toml:"name"`
	Enabled bool               `toml:"enabled"`
	Rules   []CancellationRule `toml:"cancellations"`
}

// Config is the top-level TOML schema for the cancellations file.
type Config struct {
	Checks []CheckCancellations `toml:"checks"`
}

// LoadConfig reads and validates a cancellations TOML file.
//
// A non-existent path is treated as "no cancellations configured" and returns
// an empty Config with no error: cancellations are an optional feature and the
// monitor must remain functional when the file is absent.
func LoadConfig(path string) (*Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Config{}, nil
		}

		return nil, fmt.Errorf("failed to read cancellations config %s: %w", path, err)
	}

	var cfg Config

	if _, err := toml.Decode(string(content), &cfg); err != nil {
		return nil, fmt.Errorf("failed to decode cancellations config %s: %w", path, err)
	}

	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("cancellations config %s validation failed: %w", path, err)
	}

	return &cfg, nil
}

// Validate enforces the following load-time invariants and rejects:
//   - empty or whitespace-padded check.Name / OnErrorCode / CancelErrorCodes
//   - empty CancelErrorCodes list
//   - duplicate OnErrorCode within a check
//   - self-cancel (OnErrorCode appears in CancelErrorCodes)
//   - duplicate CancelErrorCodes within a single rule
//   - duplicate Name across checks
//
// Padded values (e.g. "162 ") are rejected rather than silently trimmed
// because they would never match a real error code emitted at runtime; failing
// fast surfaces typos in the operator's TOML.
func Validate(cfg *Config) error {
	if cfg == nil {
		return nil
	}

	seenChecks := make(map[string]struct{}, len(cfg.Checks))

	for i := range cfg.Checks {
		check := &cfg.Checks[i]
		if err := validateNonEmptyTrimmed(check.Name); err != nil {
			return fmt.Errorf("checks[%d]: name: %w", i, err)
		}

		if _, dup := seenChecks[check.Name]; dup {
			return fmt.Errorf("checks[%d]: duplicate check name %q", i, check.Name)
		}

		seenChecks[check.Name] = struct{}{}

		if err := validateCheckRules(check); err != nil {
			return fmt.Errorf("checks[%d] (%s): %w", i, check.Name, err)
		}
	}

	return nil
}

func validateCheckRules(check *CheckCancellations) error {
	seenOn := make(map[string]struct{}, len(check.Rules))

	for j := range check.Rules {
		rule := &check.Rules[j]
		if err := validateNonEmptyTrimmed(rule.OnErrorCode); err != nil {
			return fmt.Errorf("cancellations[%d]: onErrorCode: %w", j, err)
		}

		if _, dup := seenOn[rule.OnErrorCode]; dup {
			return fmt.Errorf("cancellations[%d]: duplicate onErrorCode %q", j, rule.OnErrorCode)
		}

		seenOn[rule.OnErrorCode] = struct{}{}

		if len(rule.CancelErrorCodes) == 0 {
			return fmt.Errorf("cancellations[%d] (onErrorCode=%s): cancelErrorCodes must be non-empty",
				j, rule.OnErrorCode)
		}

		seenTargets := make(map[string]struct{}, len(rule.CancelErrorCodes))

		for k, target := range rule.CancelErrorCodes {
			if err := validateNonEmptyTrimmed(target); err != nil {
				return fmt.Errorf("cancellations[%d] (onErrorCode=%s): cancelErrorCodes[%d]: %w",
					j, rule.OnErrorCode, k, err)
			}

			if target == rule.OnErrorCode {
				return fmt.Errorf("cancellations[%d] (onErrorCode=%s): rule cancels its own source error code",
					j, rule.OnErrorCode)
			}

			if _, dup := seenTargets[target]; dup {
				return fmt.Errorf("cancellations[%d] (onErrorCode=%s): duplicate cancelErrorCode %q",
					j, rule.OnErrorCode, target)
			}

			seenTargets[target] = struct{}{}
		}
	}

	return nil
}

// validateNonEmptyTrimmed returns nil iff value is non-empty and equals its
// strings.TrimSpace(value) representation, i.e. the operator wrote no
// leading/trailing whitespace. Returned errors are intentionally short so
// callers can wrap them with positional context.
func validateNonEmptyTrimmed(value string) error {
	if value == "" {
		return fmt.Errorf("must be set")
	}

	if strings.TrimSpace(value) != value {
		return fmt.Errorf("must not have leading or trailing whitespace (got %q)", value)
	}

	return nil
}

// FindCheck returns the configured cancellations for the named check, or nil if
// the check is not configured or is explicitly disabled.
func (c *Config) FindCheck(name string) *CheckCancellations {
	if c == nil {
		return nil
	}

	for i := range c.Checks {
		check := &c.Checks[i]
		if check.Name != name {
			continue
		}

		if !check.Enabled {
			return nil
		}

		return check
	}

	return nil
}
