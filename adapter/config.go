// Copyright 2026 Digital Clever Solution Inc.
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
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	wfhelpers "gitlab.waterfall.network/waterfall/protocol/wf-consensus/helpers"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

// ConfigFromParams builds a wf-consensus helpers.Config from the active Prysm
// beacon-chain and network parameter sets.  Call this at the start of each
// block/epoch transition so that runtime config overrides (e.g. in tests) are
// always picked up.
func ConfigFromParams() *wfhelpers.Config {
	bc := params.BeaconConfig()
	nc := params.BeaconNetworkConfig()
	return &wfhelpers.Config{
		DelegateForkSlot:               wftypes.Slot(bc.DelegateForkSlot),
		SlotsPerEpoch:                  wftypes.Slot(bc.SlotsPerEpoch),
		CleanWithdrawalsAftEpochs:      wftypes.Epoch(bc.CleanWithdrawalsAftEpochs),
		SpinePublicationsPrefixSupport: bc.SpinePublicationsPefixSupport,
		AttestationSubnetCount:         nc.AttestationSubnetCount,
		TargetCommitteeSize:            bc.TargetCommitteeSize,
		MaxCommitteesPerSlot:           bc.MaxCommitteesPerSlot,
		FarFutureEpoch:                 wftypes.Epoch(bc.FarFutureEpoch),
		MaxEffectiveBalance:            bc.MaxEffectiveBalance,
		WithdrawalOpsLimit:             bc.WithdrawalOpsLimit,
		VotingRequiredSlots:            bc.VotingRequiredSlots,
		BlockVotingMinSupportPrc:       bc.BlockVotingMinSupportPrc,
		PrefixFinForkSlot:              wftypes.Slot(bc.PrefixFinForkSlot),
		BlockVotingForkSlot:            wftypes.Slot(bc.BlockVotingForkSlot),
		FcTgTreeForkSlot:               wftypes.Slot(bc.FcTgTreeForkSlot),
	}
}
