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
	"testing"
	"time"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
)

func TestParentByOptSpinesKey(t *testing.T) {
	key := func(fc *ForkChoice, spines []gwatCommon.HashArray, jCp [32]byte) string {
		k, _ := parentByOptSpinesKey(fc, spines, jCp)
		return k
	}
	f := setup(1, 1)
	spines := []gwatCommon.HashArray{{gwatCommon.Hash{0x01}}, {gwatCommon.Hash{0x02}, gwatCommon.Hash{0x03}}}
	jCp := [32]byte{'j'}

	k1 := key(f, spines, jCp)
	require.Equal(t, k1, key(f, spines, jCp), "same input must give same key")

	otherSpines := []gwatCommon.HashArray{{gwatCommon.Hash{0x01}}, {gwatCommon.Hash{0x02}}, {gwatCommon.Hash{0x03}}}
	require.NotEqual(t, k1, key(f, otherSpines, jCp), "spines grouping must change the key")
	require.NotEqual(t, k1, key(f, spines, [32]byte{'k'}), "justified root must change the key")

	f.store.nodesIndices[[32]byte{'A'}] = uint64(len(f.store.nodes))
	f.store.nodes = append(f.store.nodes, &Node{root: [32]byte{'A'}, parent: 0, bestChild: NonExistentNode, bestDescendant: NonExistentNode})
	require.NotEqual(t, k1, key(f, spines, jCp), "a new node must change the key")

	require.NotEqual(t, key(setup(1, 1), spines, jCp), key(setup(1, 1), spines, jCp), "different fork choice instances must not share keys")
}

func TestParentByOptSpinesCacheTTL(t *testing.T) {
	key := "test-ttl-key"
	root := [32]byte{'r'}

	_, ok := getCachedParentByOptSpines(key)
	require.Equal(t, false, ok)

	putCachedParentByOptSpines(key, root, time.Minute)
	got, ok := getCachedParentByOptSpines(key)
	require.Equal(t, true, ok)
	require.Equal(t, root, got)

	parentByOptSpinesMu.Lock()
	parentByOptSpinesCache[key] = parentByOptSpinesResult{root: root, expires: time.Now().Add(-time.Second)}
	parentByOptSpinesMu.Unlock()
	_, ok = getCachedParentByOptSpines(key)
	require.Equal(t, false, ok, "expired entry must not be returned")
}

func TestParentByOptSpinesTTLIsOneSlot(t *testing.T) {
	want := time.Duration(params.BeaconConfig().GetSecondsPerSlot(0)) * time.Second
	require.Equal(t, want, parentByOptSpinesTTL(0))
}
