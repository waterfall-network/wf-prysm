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

package validator

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed"
	statefeed "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
)

// warmUpTimeout bounds one precomputation of the parent root.
const warmUpTimeout = 10 * time.Second

// WarmUpParentByOptimisticSpines precomputes the parent root by optimistic spines.
//
// The computation copies the whole fork choice store and takes seconds while finality
// stalls, and block proposals and attestation data need it at the start of a slot.
// Running it right after every processed block, and once per slot to take new votes into
// account, puts the result into the fork choice cache before validators ask for it.
// Requests that arrive during a precomputation wait for it instead of starting another.
func (vs *Server) WarmUpParentByOptimisticSpines() {
	stateCh := make(chan *feed.Event, 16)
	sub := vs.StateNotifier.StateFeed().Subscribe(stateCh)
	defer sub.Unsubscribe()

	period := time.Duration(params.BeaconConfig().GetSecondsPerSlot(vs.TimeFetcher.CurrentSlot())) * time.Second
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	runner := newWarmUpRunner(func() {
		ctx, cancel := context.WithTimeout(vs.Ctx, warmUpTimeout)
		defer cancel()
		vs.warmUpParentByOptimisticSpines(ctx)
	})

	for {
		select {
		case <-vs.Ctx.Done():
			return
		case err := <-sub.Err():
			log.WithError(err).Warn("Parent warm-up: state feed subscription closed")
			return
		case ev := <-stateCh:
			if ev.Type != statefeed.BlockProcessed {
				continue
			}
			if data, ok := ev.Data.(*statefeed.BlockProcessedData); ok && data != nil && data.InitialSync {
				continue
			}
			runner.trigger()
		case <-ticker.C:
			runner.trigger()
		}
	}
}

// warmUpParentByOptimisticSpines requests the parent root with the same inputs as
// GetAttestationData, GetPrevoteData and block proposals, so the result is cached under
// the same key.
func (vs *Server) warmUpParentByOptimisticSpines(ctx context.Context) {
	if vs.SyncChecker != nil && vs.SyncChecker.Syncing() {
		return
	}
	start := time.Now()
	optSpines, err := vs.getOptimisticSpine(ctx)
	if err != nil {
		return
	}
	currHead, err := vs.HeadFetcher.HeadState(ctx)
	if err != nil || currHead == nil {
		return
	}
	jCpRoot := bytesutil.ToBytes32(currHead.CurrentJustifiedCheckpoint().Root)
	if currHead.CurrentJustifiedCheckpoint().Epoch == 0 {
		jCpRoot, err = vs.BeaconDB.GenesisBlockRoot(ctx)
		if err != nil {
			return
		}
	}
	if _, err := vs.HeadFetcher.ForkChoicer().GetParentByOptimisticSpines(ctx, optSpines, jCpRoot); err != nil {
		log.WithError(err).Debug("Parent warm-up: computation failed")
		return
	}
	log.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
	}).Debug("Parent warm-up: done")
}

// warmUpRunner runs fn in the background, at most one at a time. A trigger that arrives
// while fn is running schedules exactly one more run after it finishes, so a block
// processed during a precomputation is not missed.
type warmUpRunner struct {
	fn      func()
	mu      sync.Mutex
	running bool
	pending bool
}

func newWarmUpRunner(fn func()) *warmUpRunner {
	return &warmUpRunner{fn: fn}
}

func (r *warmUpRunner) trigger() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		r.pending = true
		return
	}
	r.running = true
	go r.loop()
}

func (r *warmUpRunner) loop() {
	for {
		r.fn()
		r.mu.Lock()
		if !r.pending {
			r.running = false
			r.mu.Unlock()
			return
		}
		r.pending = false
		r.mu.Unlock()
	}
}
