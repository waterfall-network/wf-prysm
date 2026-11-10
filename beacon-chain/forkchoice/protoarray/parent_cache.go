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
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	types "github.com/prysmaticlabs/eth2-types"
	"github.com/sirupsen/logrus"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/crypto/hash"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
	"golang.org/x/sync/singleflight"
)

// Caching of GetParentByOptimisticSpines.
//
// The computation copies the whole fork choice store and walks every fork from the
// justified checkpoint. While finality stalls the store keeps growing, one call takes
// seconds, and attestation data, prevote data and block proposals all request it for
// the same slot concurrently.
//
// The key includes the store size and the latest node, the justified root and the
// optimistic spines, so a new block or new spines always miss the cache. A result is
// reused for at most one slot: within it only votes processed in between can change
// the answer, which is the same as computing it a moment earlier.

type parentByOptSpinesResult struct {
	root    [32]byte
	expires time.Time
}

var (
	parentByOptSpinesGroup singleflight.Group
	parentByOptSpinesMu    sync.Mutex
	parentByOptSpinesCache = make(map[string]parentByOptSpinesResult)
)

// GetParentByOptimisticSpines returns the parent root for the optimistic spines.
// Concurrent identical requests share one computation, and the result is reused for
// one slot while the fork choice store and the spines stay the same.
func (fc *ForkChoice) GetParentByOptimisticSpines(ctx context.Context, optSpines []gwatCommon.HashArray, jCpRoot [32]byte) ([32]byte, error) {
	key, lastSlot := parentByOptSpinesKey(fc, optSpines, jCpRoot)

	if root, ok := getCachedParentByOptSpines(key); ok {
		log.WithFields(logrus.Fields{
			"headRoot": fmt.Sprintf("%#x", root),
		}).Debug("FC: GetParentByOptimisticSpines cache hit")
		return root, nil
	}

	v, err, _ := parentByOptSpinesGroup.Do(key, func() (interface{}, error) {
		root, err := fc.getParentByOptimisticSpines(ctx, optSpines, jCpRoot)
		if err != nil {
			return root, err
		}
		putCachedParentByOptSpines(key, root, parentByOptSpinesTTL(lastSlot))
		return root, nil
	})
	if err != nil {
		return [32]byte{}, err
	}
	return v.([32]byte), nil
}

// parentByOptSpinesTTL is one slot duration at the slot of the latest node.
func parentByOptSpinesTTL(slot types.Slot) time.Duration {
	return time.Duration(params.BeaconConfig().GetSecondsPerSlot(slot)) * time.Second
}

// parentByOptSpinesKey identifies the fork choice instance, its store state (node count
// and latest node), the justified root and the optimistic spines. It also returns the
// slot of the latest node.
func parentByOptSpinesKey(fc *ForkChoice, optSpines []gwatCommon.HashArray, jCpRoot [32]byte) (string, types.Slot) {
	var (
		count    int
		last     [32]byte
		lastSlot types.Slot
	)
	fc.store.nodesLock.RLock()
	count = len(fc.store.nodes)
	if count > 0 {
		last = fc.store.nodes[count-1].root
		lastSlot = fc.store.nodes[count-1].slot
	}
	fc.store.nodesLock.RUnlock()

	size := 8 + 32 + 32
	for _, ha := range optSpines {
		size += 8 + 32*len(ha)
	}
	data := make([]byte, 0, size)
	data = binary.LittleEndian.AppendUint64(data, uint64(count))
	data = append(data, last[:]...)
	data = append(data, jCpRoot[:]...)
	for _, ha := range optSpines {
		data = binary.LittleEndian.AppendUint64(data, uint64(len(ha)))
		for _, h := range ha {
			data = append(data, h[:]...)
		}
	}
	sum := hash.FastSum256(data)
	return fmt.Sprintf("%p:%x", fc, sum), lastSlot
}

func getCachedParentByOptSpines(key string) ([32]byte, bool) {
	parentByOptSpinesMu.Lock()
	defer parentByOptSpinesMu.Unlock()
	res, ok := parentByOptSpinesCache[key]
	if !ok || time.Now().After(res.expires) {
		return [32]byte{}, false
	}
	return res.root, true
}

func putCachedParentByOptSpines(key string, root [32]byte, ttl time.Duration) {
	parentByOptSpinesMu.Lock()
	defer parentByOptSpinesMu.Unlock()
	now := time.Now()
	for k, v := range parentByOptSpinesCache {
		if now.After(v.expires) {
			delete(parentByOptSpinesCache, k)
		}
	}
	parentByOptSpinesCache[key] = parentByOptSpinesResult{root: root, expires: now.Add(ttl)}
}
