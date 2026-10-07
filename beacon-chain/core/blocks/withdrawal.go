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

package blocks

import (
	"bytes"
	"context"
	"math"

	"github.com/pkg/errors"
	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/adapter"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/helpers"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/time/slots"
	wfwithdrawal "gitlab.waterfall.network/waterfall/protocol/wf-consensus/withdrawal"
)

var (
	ErrWithdrawalIsNil             = errors.New("nil withdrawal")
	ErrWithdrawalBadValidatorIndex = errors.New("withdrawal bad ValidatorIndex")
	ErrWithdrawalBadPublicKey      = errors.New("withdrawal bad PublicKey")
	ErrWithdrawalBadEpoch          = errors.New("withdrawal bad Epoch")
	ErrWithdrawalLowBalance        = errors.New("withdrawal low balance")
	ErrWithdrawalBadAmount         = errors.New("withdrawal bad amount")
	ErrWithdrawalAlreadyApplied    = errors.New("withdrawal already applied")
)

// ProcessWithdrawal delegates to the Apache-2.0 wf-consensus library via
// the adapter layer.
func ProcessWithdrawal(
	ctx context.Context,
	beaconState state.BeaconState,
	withdrawals []*ethpb.Withdrawal,
) (state.BeaconState, error) {
	_ = ctx
	cfg := adapter.ConfigFromParams()
	if _, err := wfwithdrawal.ProcessWithdrawal(
		adapter.WrapState(beaconState),
		adapter.WithdrawalsToWF(withdrawals),
		cfg,
	); err != nil {
		return nil, err
	}
	return beaconState, nil
}

// VerifyWithdrawalData verifies is validator's exit data acceptable.
func VerifyWithdrawalData(
	withdrawal *ethpb.Withdrawal,
	validator state.ReadOnlyValidator,
	currentSlot types.Slot,
	availableBalance uint64,
) error {
	if withdrawal == nil {
		return ErrWithdrawalIsNil
	}
	if withdrawal.ValidatorIndex == math.MaxUint64 {
		return ErrWithdrawalBadValidatorIndex
	}

	if bytesutil.ToBytes48(withdrawal.PublicKey) != validator.PublicKey() {
		return ErrWithdrawalBadPublicKey
	}

	currentEpoch := slots.ToEpoch(currentSlot)
	if withdrawal.Epoch > currentEpoch {
		return ErrWithdrawalBadEpoch
	}

	// refunds of insufficient deposit to activate validator
	if validator.ActivationEligibilityEpoch() == params.BeaconConfig().FarFutureEpoch &&
		availableBalance < params.BeaconConfig().MaxEffectiveBalance {
		// for this mod amount must be strictly defined by shard node.
		// withdrawal whole balance (by set op.Amount to 0) is not acceptable.
		if withdrawal.Amount == 0 {
			return ErrWithdrawalBadAmount
		}
	}

	if availableBalance < withdrawal.Amount {
		return ErrWithdrawalLowBalance
	}

	for _, v := range validator.WithdrawalOps() {
		if bytes.Equal(v.Hash, withdrawal.InitTxHash) {
			return ErrWithdrawalAlreadyApplied
		}
	}

	return nil
}

func ApplyWithdrawals(state state.BeaconState, idx types.ValidatorIndex, delta uint64) error {
	balAtIdx, err := state.BalanceAtIndex(idx)
	if err != nil {
		return err
	}
	return state.UpdateBalancesAtIndex(idx, helpers.DecreaseBalanceWithVal(balAtIdx, delta))
}
