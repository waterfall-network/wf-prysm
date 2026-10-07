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

package helpers

import (
	"math/big"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	coradapter "gitlab.waterfall.network/waterfall/protocol/coordinator/adapter"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
	wfhelpers "gitlab.waterfall.network/waterfall/protocol/wf-consensus/helpers"
)

var ErrBadUnpublishedChains = errors.New("bad unpublished chains")

type mapPublications map[gwatCommon.Hash]int

// ConsensusUpdateStateSpineFinalization delegates to the Apache-2.0
// wf-consensus library via the adapter layer.
func ConsensusUpdateStateSpineFinalization(beaconState state.BeaconState, preJustRoot, preFinRoot []byte) (state.BeaconState, error) {
	if _, err := wfhelpers.ConsensusUpdateStateSpineFinalization(
		coradapter.WrapState(beaconState),
		preJustRoot,
		preFinRoot,
	); err != nil {
		return nil, err
	}
	return beaconState, nil
}

// ProcessWithdrawalOps delegates to the Apache-2.0 wf-consensus library via
// the adapter layer.
func ProcessWithdrawalOps(bState state.BeaconState, preFinRoot []byte) (state.BeaconState, error) {
	cfg := coradapter.ConfigFromParams()
	if _, err := wfhelpers.ProcessWithdrawalOps(
		coradapter.WrapState(bState),
		cfg,
		preFinRoot,
	); err != nil {
		return nil, err
	}
	return bState, nil
}

// CalculateCandidates candidates sequence from optimistic spines for publication in block.
func CalculateCandidates(parentState state.BeaconState, optSpines []gwatCommon.HashArray) gwatCommon.HashArray {
	//find terminal spine
	var terminalSpine gwatCommon.Hash
	sd := parentState.SpineData()
	// 1. from prefix
	if len(sd.Prefix) > 0 {
		terminalSpine = gwatCommon.BytesToHash(sd.Prefix[len(sd.Prefix)-gwatCommon.HashLength:])
	} else {
		// 2. from finalization or checkpoint finalized spines
		terminalSpine = GetTerminalFinalizedSpine(parentState)
	}
	//calc candidates
	candidates := make(gwatCommon.HashArray, 0, len(optSpines))
	for _, spineList := range optSpines {
		// reset candidates if reach terminal finalized spine
		if spineList.Has(terminalSpine) {
			candidates = make(gwatCommon.HashArray, 0, len(optSpines))
			continue
		}
		if len(spineList) > 0 {
			candidates = append(candidates, spineList[0])
		}
	}
	return candidates
}

// GetTerminalFinalizedSpine retrieve last optimistic finalized spine
func GetTerminalFinalizedSpine(beaconState state.BeaconState) gwatCommon.Hash {
	finalization := beaconState.SpineData().Finalization
	if len(finalization) > 0 {
		return gwatCommon.BytesToHash(finalization[len(finalization)-32:])
	}
	cpFinalized := beaconState.SpineData().CpFinalized
	return gwatCommon.BytesToHash(cpFinalized[len(cpFinalized)-32:])
}

// GetTerminalFinalizedSpine returns finalization spines sequence from state.
func GetFinalizationSequence(beaconState state.BeaconState) gwatCommon.HashArray {
	cpFinalized := gwatCommon.HashArrayFromBytes(beaconState.SpineData().CpFinalized)
	finalization := gwatCommon.HashArrayFromBytes(beaconState.SpineData().Finalization)
	baseSpine := cpFinalized[0]
	finalizationSeq := append(cpFinalized, finalization...)
	if baseIx := finalizationSeq.IndexOf(baseSpine); baseIx > -1 {
		finalizationSeq = finalizationSeq[baseIx+1:]
	}
	return finalizationSeq
}

// GetBaseSpine returns base spine.
func GetBaseSpine(beaconState state.BeaconState) gwatCommon.Hash {
	cpFinalized := gwatCommon.HashArrayFromBytes(beaconState.SpineData().CpFinalized)
	baseSpine := cpFinalized[0]
	return baseSpine
}

// ConsensusCalcPrefix calculates sequence of prefix from array of unpublished spines sequences.
func ConsensusCalcPrefix(unpublishedChains []gwatCommon.HashArray) (gwatCommon.HashArray, error) {
	if err := ConsensusValidateUnpublishedChains(unpublishedChains); err != nil {
		return gwatCommon.HashArray{}, err
	}
	var (
		publicationsMap = mapPublications{}
		commonChain     = gwatCommon.HashArray{}
		prefix          = gwatCommon.HashArray{}
	)
	for i, chain := range unpublishedChains {
		if i == 0 {
			commonChain = chain
		} else {
			commonChain = commonChain.SequenceIntersection(chain)
		}
		for _, spine := range chain {
			publicationsMap[spine]++
		}
	}

	for _, spine := range commonChain {
		if publicationsMap[spine] >= params.BeaconConfig().SpinePublicationsPefixSupport {
			prefix = append(prefix, spine)
		}
	}

	log.WithFields(log.Fields{
		"calcPrefix":        prefix,
		"unpublishedChains": unpublishedChains,
		"commonChain":       commonChain,
		"publicationsMap":   publicationsMap,
	}).Info("Calculate pefix")

	return prefix, nil
}

// ConsensusValidateUnpublishedChains validate unpublished chains
func ConsensusValidateUnpublishedChains(unpublishedChains []gwatCommon.HashArray) error {
	var firstVal gwatCommon.Hash
	for _, chain := range unpublishedChains {
		// empty chains must be removed
		if len(chain) == 0 {
			return errors.Wrap(ErrBadUnpublishedChains, "contains empty chain")
		}
		// chains must be uniq
		if !chain.IsUniq() {
			return errors.Wrap(ErrBadUnpublishedChains, "chain is not uniq")
		}
		//	the first values of each chain must be equal
		if firstVal == (gwatCommon.Hash{}) {
			firstVal = chain[0]
			continue
		}
		if chain[0] != firstVal {
			return errors.Wrap(ErrBadUnpublishedChains, "the first values of chain are not equal")
		}
	}
	return nil
}

func ConsensusCopyUnpublishedChains(unpublishedChains []gwatCommon.HashArray) []gwatCommon.HashArray {
	cpy := make([]gwatCommon.HashArray, len(unpublishedChains))
	for i, chain := range unpublishedChains {
		cpy[i] = chain.Copy()
	}
	return cpy
}

func GweiToWei(gwei uint64) *big.Int {
	return new(big.Int).Mul(new(big.Int).SetUint64(gwei), new(big.Int).SetUint64(1000000000))
}

func CountUniqSpines(beaconState state.BeaconState) int {
	finSeq := GetFinalizationSequence(beaconState)
	prefixSeq := gwatCommon.HashArrayFromBytes(beaconState.SpineData().Prefix)
	spineSeq := gwatCommon.HashArrayFromBytes(beaconState.SpineData().Spines)
	fullSeq := make(gwatCommon.HashArray, 0, len(finSeq)+len(prefixSeq)+len(spineSeq))
	fullSeq = append(fullSeq, finSeq...)
	fullSeq = append(fullSeq, prefixSeq...)
	fullSeq = append(fullSeq, spineSeq...)
	fullSeq.Deduplicate()
	return len(fullSeq)
}
