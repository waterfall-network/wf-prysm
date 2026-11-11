// Copyright 2026 Digital Clever Solution LLC
// SPDX-License-Identifier: Apache-2.0
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

package blockchain

import (
	"context"
	"testing"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/async/abool"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/assert"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	gwatTypes "gitlab.waterfall.network/waterfall/protocol/gwat/core/types"
)

func newResyncTestService(t *testing.T, synced bool) *Service {
	t.Helper()
	// A canceled context makes the restarted initGwatSync return at once.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &Service{
		ctx:           ctx,
		isGwatSyncing: abool.New(),
		isGwatSynced:  abool.NewBool(synced),
	}
	s.CacheGwatCoordinatedState(&gwatTypes.Checkpoint{Epoch: 5})
	return s
}

func TestResyncGwat_RestartsSync(t *testing.T) {
	s := newResyncTestService(t, true)

	s.ResyncGwat()

	assert.Equal(t, false, s.IsGwatSynced())
	require.Equal(t, (*gwatTypes.Checkpoint)(nil), s.GetCachedGwatCoordinatedState())
}

func TestResyncGwat_NoopWhileSyncing(t *testing.T) {
	s := newResyncTestService(t, false)

	s.ResyncGwat()

	assert.Equal(t, false, s.IsGwatSynced())
	require.NotNil(t, s.GetCachedGwatCoordinatedState(), "a running synchronization must keep its state")
}

func TestRunProcessDagFinalize_DrainsOnce(t *testing.T) {
	s := &Service{ctx: context.Background(), newHeadCh: make(chan *head)}
	calls := 0
	for i := 0; i < 3; i++ {
		s.runProcessDagFinalize()
		s.drainNewHeadsOnce.Do(func() { calls++ })
	}
	assert.Equal(t, 0, calls, "the drain must be started by the first call only")
}
