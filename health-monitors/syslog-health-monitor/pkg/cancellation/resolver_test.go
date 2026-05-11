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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolver_Lookup(t *testing.T) {
	check := &CheckCancellations{
		Name:    "SysLogsXIDError",
		Enabled: true,
		Rules: []CancellationRule{
			{OnErrorCode: "162", CancelErrorCodes: []string{"163"}},
			{OnErrorCode: "31", CancelErrorCodes: []string{"13", "43"}},
		},
	}

	r := NewResolver(check)

	assert.Equal(t, []string{"163"}, r.Lookup("162"))
	assert.Equal(t, []string{"13", "43"}, r.Lookup("31"))
	assert.Nil(t, r.Lookup("999"))
	assert.False(t, r.Empty())
}

func TestResolver_NilCheckIsEmpty(t *testing.T) {
	r := NewResolver(nil)
	assert.True(t, r.Empty())
	assert.Nil(t, r.Lookup("162"))
}

func TestResolver_DisabledCheckIsEmpty(t *testing.T) {
	r := NewResolver(&CheckCancellations{
		Name:    "SysLogsXIDError",
		Enabled: false,
		Rules: []CancellationRule{
			{OnErrorCode: "162", CancelErrorCodes: []string{"163"}},
		},
	})

	assert.True(t, r.Empty())
	assert.Nil(t, r.Lookup("162"))
}

func TestResolver_NilReceiverSafe(t *testing.T) {
	var r *Resolver
	assert.Nil(t, r.Lookup("162"))
	assert.True(t, r.Empty())
}

// Mutating the input rule slice after constructing the resolver must not
// affect the resolver's view — Lookup returns the resolver's own copy.
func TestResolver_DefensiveCopy(t *testing.T) {
	check := &CheckCancellations{
		Name:    "SysLogsXIDError",
		Enabled: true,
		Rules: []CancellationRule{
			{OnErrorCode: "162", CancelErrorCodes: []string{"163"}},
		},
	}

	r := NewResolver(check)
	check.Rules[0].CancelErrorCodes[0] = "999"

	assert.Equal(t, []string{"163"}, r.Lookup("162"))
}
