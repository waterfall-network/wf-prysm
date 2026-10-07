// Copyright 2026 Digital Clever Solution LLC
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

package adapter

import (
	eth2types "github.com/prysmaticlabs/eth2-types"
	beaconstate "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	wfiface "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/iface"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

var (
	_ wfiface.BeaconState    = (*StateAdapter)(nil)
	_ wfiface.DagBeaconState = (*DagStateAdapter)(nil)
)

// StateAdapter adapts a Prysm beaconstate.BeaconState to iface.BeaconState.
type StateAdapter struct {
	inner beaconstate.BeaconState
}

// WrapState returns a StateAdapter for the given Prysm state.
func WrapState(s beaconstate.BeaconState) *StateAdapter {
	return &StateAdapter{inner: s}
}

func (a *StateAdapter) Slot() wftypes.Slot {
	return wftypes.Slot(a.inner.Slot())
}

func (a *StateAdapter) SpineData() *wftypes.SpineData {
	return SpineDataToWF(a.inner.SpineData())
}

func (a *StateAdapter) BlockVoting() []*wftypes.BlockVoting {
	return BlockVotingToWF(a.inner.BlockVoting())
}

func (a *StateAdapter) FinalizedCheckpoint() *wftypes.Checkpoint {
	return CheckpointToWF(a.inner.FinalizedCheckpoint())
}

func (a *StateAdapter) CurrentJustifiedCheckpoint() *wftypes.Checkpoint {
	return CheckpointToWF(a.inner.CurrentJustifiedCheckpoint())
}

func (a *StateAdapter) PreviousJustifiedCheckpoint() *wftypes.Checkpoint {
	return CheckpointToWF(a.inner.PreviousJustifiedCheckpoint())
}

func (a *StateAdapter) NumValidators() int {
	return a.inner.NumValidators()
}

func (a *StateAdapter) ValidatorAtIndex(idx wftypes.ValidatorIndex) (*wftypes.Validator, error) {
	v, err := a.inner.ValidatorAtIndex(eth2types.ValidatorIndex(idx))
	if err != nil {
		return nil, err
	}
	return ValidatorToWF(v), nil
}

func (a *StateAdapter) ValidatorAtIndexReadOnly(idx wftypes.ValidatorIndex) (*wftypes.Validator, error) {
	v, err := a.inner.ValidatorAtIndex(eth2types.ValidatorIndex(idx))
	if err != nil {
		return nil, err
	}
	return ValidatorToWF(v), nil
}

func (a *StateAdapter) Balances() []uint64 {
	return a.inner.Balances()
}

func (a *StateAdapter) BalanceAtIndex(idx wftypes.ValidatorIndex) (uint64, error) {
	return a.inner.BalanceAtIndex(eth2types.ValidatorIndex(idx))
}

func (a *StateAdapter) SetSpineData(sd *wftypes.SpineData) error {
	return a.inner.SetSpineData(SpineDataFromWF(sd))
}

func (a *StateAdapter) SetBlockVoting(bvs []*wftypes.BlockVoting) error {
	return a.inner.SetBlockVoting(BlockVotingFromWF(bvs))
}

func (a *StateAdapter) SetFinalizedCheckpoint(cp *wftypes.Checkpoint) error {
	return a.inner.SetFinalizedCheckpoint(CheckpointFromWF(cp))
}

func (a *StateAdapter) SetCurrentJustifiedCheckpoint(cp *wftypes.Checkpoint) error {
	return a.inner.SetCurrentJustifiedCheckpoint(CheckpointFromWF(cp))
}

func (a *StateAdapter) UpdateValidatorAtIndex(idx wftypes.ValidatorIndex, val *wftypes.Validator) error {
	return a.inner.UpdateValidatorAtIndex(eth2types.ValidatorIndex(idx), ValidatorFromWF(val))
}

func (a *StateAdapter) UpdateBalancesAtIndex(idx wftypes.ValidatorIndex, balance uint64) error {
	return a.inner.UpdateBalancesAtIndex(eth2types.ValidatorIndex(idx), balance)
}

// DagStateAdapter extends StateAdapter with Eth1Candidates access for
// iface.DagBeaconState.
type DagStateAdapter struct {
	*StateAdapter
}

// WrapDagState returns a DagStateAdapter for the given Prysm state.
func WrapDagState(s beaconstate.BeaconState) *DagStateAdapter {
	return &DagStateAdapter{StateAdapter: WrapState(s)}
}

func (a *DagStateAdapter) Eth1Candidates() []byte {
	eth1Data := a.inner.Eth1Data()
	if eth1Data == nil {
		return nil
	}
	return append([]byte(nil), eth1Data.Candidates...)
}

func (a *DagStateAdapter) SetEth1Candidates(candidates []byte) error {
	eth1Data := a.inner.Eth1Data()
	if eth1Data == nil {
		eth1Data = &ethpb.Eth1Data{}
	} else {
		eth1Data = ethpb.CopyETH1Data(eth1Data)
	}
	eth1Data.Candidates = append([]byte(nil), candidates...)
	return a.inner.SetEth1Data(eth1Data)
}
