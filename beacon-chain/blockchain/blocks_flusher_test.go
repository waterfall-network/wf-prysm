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
	"time"

	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/db"
	testDB "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/db/testing"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/wrapper"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/util"
)

func newCachedTestBlock(t *testing.T, slot types.Slot) ([32]byte, block.SignedBeaconBlock) {
	b := util.NewBeaconBlock()
	b.Block.Slot = slot
	r, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	wb, err := wrapper.WrappedSignedBeaconBlock(b)
	require.NoError(t, err)
	return r, wb
}

func waitFor(t *testing.T, cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition is not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestService_flushCachedBlocks(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	r1, b1 := newCachedTestBlock(t, 1)
	r2, b2 := newCachedTestBlock(t, 2)
	s.saveInitSyncBlock(r1, b1)
	s.saveInitSyncBlock(r2, b2)

	// cached blocks are available before the flush.
	require.Equal(t, false, s.cfg.BeaconDB.HasBlock(ctx, r1))
	require.Equal(t, true, s.hasBlockInInitSyncOrDB(ctx, r1))
	got, err := s.getBlock(ctx, r2)
	require.NoError(t, err)
	require.DeepEqual(t, b2, got)

	require.NoError(t, s.flushCachedBlocks(ctx))

	require.Equal(t, true, s.cfg.BeaconDB.HasBlock(ctx, r1))
	require.Equal(t, true, s.cfg.BeaconDB.HasBlock(ctx, r2))
	require.Equal(t, false, s.hasInitSyncBlock(r1))
	require.Equal(t, false, s.hasInitSyncBlock(r2))
	got, err = s.getBlock(ctx, r2)
	require.NoError(t, err)
	require.DeepEqual(t, b2, got)

	// empty cache flush is no-op.
	require.NoError(t, s.flushCachedBlocks(ctx))
}

func TestService_requestBlocksFlush_NotRunningFlushesSync(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	r, b := newCachedTestBlock(t, 3)
	s.saveInitSyncBlock(r, b)
	require.NoError(t, s.cfg.BeaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 3, Root: r[:]}))
	s.setPendingHeadRoot(r)

	require.NoError(t, s.requestBlocksFlush(ctx))

	require.Equal(t, true, s.cfg.BeaconDB.HasBlock(ctx, r))
	require.Equal(t, false, s.hasInitSyncBlock(r))
	headBlk, err := s.cfg.BeaconDB.HeadBlock(ctx)
	require.NoError(t, err)
	headRoot, err := headBlk.Block().HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, r, headRoot)
}

func TestService_blocksFlusher_Background(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))
	s.startBlocksFlusher()
	waitFor(t, s.blocksFlusherRunning.Load)

	r, b := newCachedTestBlock(t, 4)
	s.saveInitSyncBlock(r, b)
	require.NoError(t, s.cfg.BeaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 4, Root: r[:]}))
	s.setPendingHeadRoot(r)

	require.NoError(t, s.requestBlocksFlush(ctx))
	// the block is available all the time: in the cache or in the DB.
	_, err := s.getBlock(ctx, r)
	require.NoError(t, err)

	waitFor(t, func() bool {
		return s.cfg.BeaconDB.HasBlock(ctx, r) && !s.hasInitSyncBlock(r)
	})
	waitFor(t, func() bool {
		headBlk, err := s.cfg.BeaconDB.HeadBlock(ctx)
		if err != nil || headBlk == nil || headBlk.IsNil() {
			return false
		}
		headRoot, err := headBlk.Block().HashTreeRoot()
		return err == nil && headRoot == r
	})

	s.cancel()
	waitFor(t, func() bool { return !s.blocksFlusherRunning.Load() })
}

func TestService_saveInitSyncBlock_BlockInfoAvailableBeforeFlush(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	b := util.NewBeaconBlock()
	b.Block.Slot = 5
	b.Block.ProposerIndex = 7
	r, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	wb, err := wrapper.WrappedSignedBeaconBlock(b)
	require.NoError(t, err)
	s.saveInitSyncBlock(r, wb)
	require.Equal(t, false, s.cfg.BeaconDB.HasBlock(ctx, r))

	// proposer rewards read the block info via the db fetcher.
	proposerIndex, slot, _, err := db.BlockInfoFetcherFunc(s.cfg.BeaconDB)(ctx, r)
	require.NoError(t, err)
	require.Equal(t, types.ValidatorIndex(7), proposerIndex)
	require.Equal(t, types.Slot(5), slot)
}

func TestService_blockRootsBySlot(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	rDB, bDB := newCachedTestBlock(t, 10)
	require.NoError(t, s.cfg.BeaconDB.SaveBlock(ctx, bDB))
	b := util.NewBeaconBlock()
	b.Block.Slot = 10
	b.Block.ProposerIndex = 1
	rCached, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	wb, err := wrapper.WrappedSignedBeaconBlock(b)
	require.NoError(t, err)
	s.saveInitSyncBlock(rCached, wb)
	// a block of another slot in cache.
	rOther, bOther := newCachedTestBlock(t, 11)
	s.saveInitSyncBlock(rOther, bOther)
	// a block in both cache and DB is returned once.
	s.saveInitSyncBlock(rDB, bDB)

	roots, err := s.blockRootsBySlot(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 2, len(roots))
	require.Equal(t, true, roots[0] == rDB || roots[1] == rDB)
	require.Equal(t, true, roots[0] == rCached || roots[1] == rCached)
}

func TestService_removeCachedBlockAndFromDB(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	rCached, bCached := newCachedTestBlock(t, 12)
	s.saveInitSyncBlock(rCached, bCached)
	rDB, bDB := newCachedTestBlock(t, 13)
	require.NoError(t, s.cfg.BeaconDB.SaveBlock(ctx, bDB))

	require.NoError(t, s.removeCachedBlockAndFromDB(ctx, rCached))
	require.NoError(t, s.removeCachedBlockAndFromDB(ctx, rDB))
	// the removed block is not written back by flush.
	require.NoError(t, s.flushCachedBlocks(ctx))

	require.Equal(t, false, s.hasBlockInInitSyncOrDB(ctx, rCached))
	require.Equal(t, false, s.hasBlockInInitSyncOrDB(ctx, rDB))
}

func TestService_CachedBlock(t *testing.T) {
	ctx := context.Background()
	s := setupBeaconChain(t, testDB.SetupDB(t))

	r, b := newCachedTestBlock(t, 14)
	require.Equal(t, true, s.CachedBlock(r) == nil)
	s.saveInitSyncBlock(r, b)
	require.DeepEqual(t, b, s.CachedBlock(r))

	require.NoError(t, s.FlushCachedBlocks(ctx))
	require.Equal(t, true, s.CachedBlock(r) == nil)
	require.Equal(t, true, s.cfg.BeaconDB.HasBlock(ctx, r))
}
