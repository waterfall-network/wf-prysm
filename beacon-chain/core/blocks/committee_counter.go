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

package blocks

import (
	"context"

	eth2types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/helpers"
	beaconstate "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

// committeeCounterAdapter implements iface.CommitteeCounter using Prysm's
// shuffling-based committee calculation.  It is used solely by
// ProcessDagConsensus to provide BlockVotingMinSupport calculations to
// wf-consensus without introducing a circular import.
type committeeCounterAdapter struct {
	state beaconstate.BeaconState
}

func (a *committeeCounterAdapter) CountSlotAttestors(ctx context.Context, slot wftypes.Slot) (int, error) {
	committees, err := helpers.CalcSlotCommitteesIndexes(ctx, a.state, eth2types.Slot(slot))
	if err != nil {
		return 0, err
	}
	total := 0
	for _, cmt := range committees {
		total += len(cmt)
	}
	return total, nil
}
