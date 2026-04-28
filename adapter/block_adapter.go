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
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	prysmblock "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
	wfiface "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/iface"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

var (
	_ wfiface.SignedBeaconBlock = (*SignedBlockAdapter)(nil)
	_ wfiface.BeaconBlock       = (*BlockAdapter)(nil)
	_ wfiface.BeaconBlockBody   = (*BlockBodyAdapter)(nil)
	_ wfiface.Attestation       = (*AttestationAdapter)(nil)
)

// SignedBlockAdapter adapts a Prysm block.SignedBeaconBlock to iface.SignedBeaconBlock.
type SignedBlockAdapter struct {
	inner prysmblock.SignedBeaconBlock
}

// WrapSignedBlock returns a SignedBlockAdapter for the given Prysm signed block.
func WrapSignedBlock(b prysmblock.SignedBeaconBlock) *SignedBlockAdapter {
	return &SignedBlockAdapter{inner: b}
}

func (a *SignedBlockAdapter) Block() wfiface.BeaconBlock {
	return &BlockAdapter{inner: a.inner.Block()}
}

// BlockAdapter adapts a Prysm block.BeaconBlock to iface.BeaconBlock.
type BlockAdapter struct {
	inner prysmblock.BeaconBlock
}

func (a *BlockAdapter) Slot() wftypes.Slot {
	return wftypes.Slot(a.inner.Slot())
}

func (a *BlockAdapter) ParentRoot() []byte {
	return a.inner.ParentRoot()
}

func (a *BlockAdapter) Body() wfiface.BeaconBlockBody {
	return &BlockBodyAdapter{inner: a.inner.Body()}
}

// BlockBodyAdapter adapts a Prysm block.BeaconBlockBody to iface.BeaconBlockBody.
type BlockBodyAdapter struct {
	inner prysmblock.BeaconBlockBody
}

func (a *BlockBodyAdapter) Attestations() []wfiface.Attestation {
	atts := a.inner.Attestations()
	res := make([]wfiface.Attestation, len(atts))
	for i, att := range atts {
		res[i] = &AttestationAdapter{inner: att}
	}
	return res
}

func (a *BlockBodyAdapter) Eth1Candidates() []byte {
	eth1Data := a.inner.Eth1Data()
	if eth1Data == nil {
		return nil
	}
	return eth1Data.Candidates
}

// AttestationAdapter adapts a Prysm *ethpb.Attestation to iface.Attestation.
type AttestationAdapter struct {
	inner *ethpb.Attestation
}

func (a *AttestationAdapter) BeaconBlockRoot() []byte {
	if a.inner.Data == nil {
		return nil
	}
	return a.inner.Data.BeaconBlockRoot
}

func (a *AttestationAdapter) AggregationBits() []byte {
	return a.inner.AggregationBits
}

func (a *AttestationAdapter) AttestationSlot() wftypes.Slot {
	if a.inner.Data == nil {
		return 0
	}
	return wftypes.Slot(a.inner.Data.Slot)
}

func (a *AttestationAdapter) CommitteeIndex() wftypes.CommitteeIndex {
	if a.inner.Data == nil {
		return 0
	}
	return wftypes.CommitteeIndex(a.inner.Data.CommitteeIndex)
}
