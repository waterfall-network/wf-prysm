//Copyright 2026 Digital Clever Solution Inc.
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

package blocks

import (
	"context"
	"errors"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/adapter"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/helpers"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
	wfdag "gitlab.waterfall.network/waterfall/protocol/wf-consensus/dag"
)

// ProcessDagConsensus updates the beacon state's DAG-consensus fields
// (spine data, block voting, eth1 candidates) for the given signed block.
// It delegates to the Apache-2.0 wf-consensus library via the adapter layer.
func ProcessDagConsensus(ctx context.Context, beaconState state.BeaconState, signed block.SignedBeaconBlock) (state.BeaconState, error) {
	if beaconState == nil || beaconState.IsNil() {
		return nil, errors.New("nil state")
	}
	cc := &committeeCounterAdapter{state: beaconState}
	cfg := adapter.ConfigFromParams()
	if _, err := wfdag.ProcessDagConsensus(
		ctx,
		adapter.WrapDagState(beaconState),
		adapter.WrapSignedBlock(signed),
		cc,
		cfg,
	); err != nil {
		return nil, err
	}
	// wf-consensus sorts BlockVoting with a SHA256-based key; the canonical
	// on-chain ordering uses SSZ HashTreeRoot keys (old behavior).
	// Re-sort here so that replayed blocks produce the same state root.
	bv, err := helpers.BlockVotingArrStateOrder(beaconState.BlockVoting())
	if err != nil {
		return nil, err
	}
	return beaconState, beaconState.SetBlockVoting(bv)
}
