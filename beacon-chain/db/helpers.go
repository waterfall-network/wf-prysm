//Copyright 2026 Digital Clever Solution LLC
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package db

import (
	"context"

	lru "github.com/hashicorp/golang-lru"
	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
)

// blockInfoCacheSize bounds the number of cached block infos.
const blockInfoCacheSize = 1 << 16

// blockInfo is the part of a block required to reward its proposer.
type blockInfo struct {
	proposerIndex types.ValidatorIndex
	slot          types.Slot
	votesIncluded uint64
}

// blockInfoCache maps a block root to its info. A block is immutable by root, so
// entries never go stale. State transition asks for the voted block of every
// attestation, which otherwise reads and unmarshals a whole block from the DB
// many times per processed block, and replaying blocks repeats it for each replay.
var blockInfoCache = func() *lru.Cache {
	c, err := lru.New(blockInfoCacheSize)
	if err != nil {
		panic(err)
	}
	return c
}()

func BlockInfoFetcherFunc(db ReadOnlyDatabase) params.CtxBlockFetcher {
	return func(ctx context.Context, blockRoot [32]byte) (types.ValidatorIndex, types.Slot, uint64, error) {
		if v, ok := blockInfoCache.Get(blockRoot); ok {
			info := v.(blockInfo)
			return info.proposerIndex, info.slot, info.votesIncluded, nil
		}
		block, err := db.Block(ctx, blockRoot)
		if err != nil {
			return 0, 0, 0, err
		}
		if block == nil {
			return 0, 0, 0, ErrNotFound
		}
		votesIncluded := uint64(0)
		for _, att := range block.Block().Body().Attestations() {
			votesIncluded += att.AggregationBits.Count()
		}
		info := blockInfo{
			proposerIndex: block.Block().ProposerIndex(),
			slot:          block.Block().Slot(),
			votesIncluded: votesIncluded,
		}
		blockInfoCache.Add(blockRoot, info)
		return info.proposerIndex, info.slot, info.votesIncluded, nil
	}
}
