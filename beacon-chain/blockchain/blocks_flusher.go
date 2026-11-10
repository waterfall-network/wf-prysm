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

	"github.com/pkg/errors"
	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
)

// The processed blocks are kept in the blocks cache (initSyncBlocks) and written to the DB
// by the background flusher, so the disk write (fsync) is out of the onBlock critical path
// and does not delay the head update. The new head root is persisted by the flusher as well,
// right after the blocks, so the DB head never points to a block which is not in the DB.

// startBlocksFlusher runs the background routine which writes the cached blocks
// and the latest head root to the DB on request.
func (s *Service) startBlocksFlusher() {
	if s.flushBlocksCh == nil {
		s.flushBlocksCh = make(chan struct{}, 1)
	}
	if !s.blocksFlusherRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.blocksFlusherRunning.Store(false)
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.flushBlocksCh:
				if err := s.flushCachedBlocksAndHead(s.ctx); err != nil {
					log.WithError(err).Error("Blocks flusher: could not flush cached blocks")
				}
			}
		}
	}()
}

// requestBlocksFlush schedules the background flush of the cached blocks and the head root.
// If the flusher is not running (the service is not started yet), it flushes synchronously.
func (s *Service) requestBlocksFlush(ctx context.Context) error {
	if !s.blocksFlusherRunning.Load() {
		return s.flushCachedBlocksAndHead(ctx)
	}
	select {
	case s.flushBlocksCh <- struct{}{}:
	default:
		// flush is already requested
	}
	return nil
}

// flushCachedBlocksAndHead writes the cached blocks and then the pending head root to the DB.
func (s *Service) flushCachedBlocksAndHead(ctx context.Context) error {
	if err := s.flushCachedBlocks(ctx); err != nil {
		return err
	}
	return s.flushPendingHeadRoot(ctx)
}

// flushCachedBlocks writes the cached blocks to the DB and removes from the cache only the written ones,
// so the blocks added to the cache during the write are kept. A block is removed from the cache
// only after the DB transaction is committed, so it's always available either in the cache or in the DB.
func (s *Service) flushCachedBlocks(ctx context.Context) error {
	s.flushBlocksMu.Lock()
	defer s.flushBlocksMu.Unlock()

	s.initSyncBlocksLock.RLock()
	roots := make([][32]byte, 0, len(s.initSyncBlocks))
	blks := make([]block.SignedBeaconBlock, 0, len(s.initSyncBlocks))
	for r, b := range s.initSyncBlocks {
		roots = append(roots, r)
		blks = append(blks, b)
	}
	s.initSyncBlocksLock.RUnlock()

	if len(blks) == 0 {
		return nil
	}
	if err := s.cfg.BeaconDB.SaveBlocks(ctx, blks); err != nil {
		return errors.Wrap(err, "could not save cached blocks")
	}

	s.initSyncBlocksLock.Lock()
	for _, r := range roots {
		delete(s.initSyncBlocks, r)
	}
	s.initSyncBlocksLock.Unlock()
	return nil
}

// setPendingHeadRoot sets the head root to be persisted by the flusher.
func (s *Service) setPendingHeadRoot(r [32]byte) {
	s.pendingHeadLock.Lock()
	defer s.pendingHeadLock.Unlock()
	s.pendingHeadRoot = &r
}

// flushPendingHeadRoot writes the pending head root to the DB.
func (s *Service) flushPendingHeadRoot(ctx context.Context) error {
	s.pendingHeadLock.Lock()
	r := s.pendingHeadRoot
	s.pendingHeadRoot = nil
	s.pendingHeadLock.Unlock()

	if r == nil {
		return nil
	}
	if err := s.cfg.BeaconDB.SaveHeadBlockRoot(ctx, *r); err != nil {
		// restore the pending root to retry on the next flush, if no newer one was set.
		s.pendingHeadLock.Lock()
		if s.pendingHeadRoot == nil {
			s.pendingHeadRoot = r
		}
		s.pendingHeadLock.Unlock()
		return errors.Wrap(err, "could not save head root in DB")
	}
	return nil
}

// blockRootsBySlot returns the roots of the blocks at the slot from the DB and the blocks cache.
func (s *Service) blockRootsBySlot(ctx context.Context, slot types.Slot) ([][32]byte, error) {
	// read the cache first: a block is removed from the cache only after it is written to the DB.
	s.initSyncBlocksLock.RLock()
	var cached [][32]byte
	for r, b := range s.initSyncBlocks {
		if b.Block().Slot() == slot {
			cached = append(cached, r)
		}
	}
	s.initSyncBlocksLock.RUnlock()

	_, roots, err := s.cfg.BeaconDB.BlockRootsBySlot(ctx, slot)
	if err != nil {
		return nil, err
	}
	for _, r := range cached {
		has := false
		for _, dr := range roots {
			if dr == r {
				has = true
				break
			}
		}
		if !has {
			roots = append(roots, r)
		}
	}
	return roots, nil
}

// removeCachedBlockAndFromDB removes the block from the blocks cache and the DB.
// It holds the flush lock, so the flusher can't write the block back to the DB.
func (s *Service) removeCachedBlockAndFromDB(ctx context.Context, root [32]byte) error {
	s.flushBlocksMu.Lock()
	defer s.flushBlocksMu.Unlock()

	s.initSyncBlocksLock.Lock()
	delete(s.initSyncBlocks, root)
	s.initSyncBlocksLock.Unlock()

	return s.cfg.BeaconDB.DeleteBlock(ctx, root)
}
