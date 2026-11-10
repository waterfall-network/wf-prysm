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

package protoarray

import (
	"sync"
	"testing"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
)

func TestAttestationsData_CopyIsIsolated(t *testing.T) {
	orig := &AttestationsData{votes: map[uint64]Vote{1: {nextEpoch: 1}}}

	cpy := orig.Copy()
	orig.setVote(2, Vote{nextEpoch: 2})
	cpy.setVote(3, Vote{nextEpoch: 3})

	require.Equal(t, 2, len(orig.Votes()))
	require.Equal(t, 2, len(cpy.Votes()))
	_, ok := orig.Votes()[3]
	require.Equal(t, false, ok)
	_, ok = cpy.Votes()[2]
	require.Equal(t, false, ok)
	require.Equal(t, uint64(1), uint64(cpy.votes[1].nextEpoch))
}

func TestAttestationsData_SetVoteOnNil(t *testing.T) {
	ad := &AttestationsData{}
	ad.setVote(5, Vote{nextEpoch: 5})
	require.Equal(t, 1, len(ad.Votes()))
}

func TestAttestationsData_ConcurrentCopyAndWrite(t *testing.T) {
	orig := &AttestationsData{votes: map[uint64]Vote{}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 2000; i++ {
			orig.setVote(i, Vote{nextEpoch: 1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			cpy := orig.Copy()
			// Copies are read without the lock, as in calculateHeadRootByNodesIndexes.
			for vi := range cpy.votes {
				_ = cpy.votes[vi]
			}
		}
	}()
	wg.Wait()
	require.Equal(t, 2000, len(orig.Votes()))
}
