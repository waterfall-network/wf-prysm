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
	"sync"
	"testing"

	types "github.com/prysmaticlabs/eth2-types"
	gwatTypes "gitlab.waterfall.network/waterfall/protocol/gwat/core/types"
)

// TestSpineData_ConcurrentAccess must pass under -race: the cache setters write
// shared fields and are called from block processing, sync and gRPC handlers.
func TestSpineData_ConcurrentAccess(t *testing.T) {
	s := &Service{}
	cp := &gwatTypes.Checkpoint{Epoch: 1}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.CacheGwatCoordinatedState(cp)
				s.ResetCachedGwatCoordinatedState()
				_ = s.GetCachedGwatCoordinatedState()

				s.CacheGwatCheckpoint(cp)
				_ = s.GetCachedGwatCheckpoint(cp.Root.Bytes())

				s.SetValidatedBlockInfo([]byte{byte(i)}, types.Slot(j))
				_, _ = s.GetValidatedBlockInfo()
			}
		}(i)
	}
	wg.Wait()
}
