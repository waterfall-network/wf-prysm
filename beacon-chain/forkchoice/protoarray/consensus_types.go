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

package protoarray

import (
	"sync"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
)

type Fork struct {
	roots    [][32]byte
	nodesMap map[[32]byte]*Node
}

// AttestationsData represents data related with attestations in Node.
type AttestationsData struct {
	justifiedRoot [32]byte
	finalizedRoot [32]byte
	votes         map[uint64]Vote
	// shared is set when votes is referenced by a copy. A shared map is never
	// mutated: setVote clones it first (copy-on-write).
	shared bool
	mu     sync.Mutex
}

// setVote sets the validator vote, cloning the votes map first if a copy shares it.
func (ad *AttestationsData) setVote(validator uint64, vote Vote) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	if ad.shared || ad.votes == nil {
		ad.votes = copyVotes(ad.votes)
		ad.shared = false
	}
	ad.votes[validator] = vote
}

func copyVotes(votes map[uint64]Vote) map[uint64]Vote {
	cpy := make(map[uint64]Vote, len(votes))
	for vix, v := range votes {
		cpy[vix] = Vote{
			currentRoot: bytesutil.ToBytes32(bytesutil.SafeCopyBytes(v.currentRoot[:])),
			nextRoot:    bytesutil.ToBytes32(bytesutil.SafeCopyBytes(v.nextRoot[:])),
			nextEpoch:   v.nextEpoch,
		}
	}
	return cpy
}

func (ad *AttestationsData) Votes() map[uint64]Vote {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	return copyVotes(ad.votes)
}

func (ad *AttestationsData) JustifiedRoot() [32]byte {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	return bytesutil.ToBytes32(bytesutil.SafeCopyBytes(ad.justifiedRoot[:]))
}
func (ad *AttestationsData) FinalizedRoot() [32]byte {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	return bytesutil.ToBytes32(bytesutil.SafeCopyBytes(ad.finalizedRoot[:]))
}

// Copy returns a copy sharing the votes map with the original. Copying the
// maps of every fork choice node took seconds per copy while finality stalls;
// the map is cloned only when either side writes to it.
func (ad *AttestationsData) Copy() *AttestationsData {
	if ad == nil {
		return nil
	}
	ad.mu.Lock()
	defer ad.mu.Unlock()
	ad.shared = true
	return &AttestationsData{
		justifiedRoot: ad.justifiedRoot,
		finalizedRoot: ad.finalizedRoot,
		votes:         ad.votes,
		shared:        true,
	}
}

// SpinesData represents data related with spines in Node.
type SpinesData struct {
	spines       gwatCommon.HashArray // spines from block.Spines
	prefix       gwatCommon.HashArray // cache for calculated prefix
	finalization gwatCommon.HashArray // finalization sequence block.Finalization
	cpFinalized  gwatCommon.HashArray
}

func (rc *SpinesData) Spines() gwatCommon.HashArray       { return rc.spines.Copy() }
func (rc *SpinesData) Prefix() gwatCommon.HashArray       { return rc.prefix.Copy() }
func (rc *SpinesData) Finalization() gwatCommon.HashArray { return rc.finalization.Copy() }
func (rc *SpinesData) CpFinalized() gwatCommon.HashArray  { return rc.cpFinalized.Copy() }

func (rc *SpinesData) Copy() *SpinesData {
	if rc == nil {
		return nil
	}
	return &SpinesData{
		spines:       rc.Spines(),
		prefix:       rc.Prefix(),
		finalization: rc.Finalization(),
		cpFinalized:  rc.CpFinalized(),
	}
}

// Roots returns the ordered slice of block roots in this fork (tip first).
func (f *Fork) Roots() [][32]byte { return f.roots }

// NodeByRoot returns the Node for a given root, or nil if not found.
func (f *Fork) NodeByRoot(root [32]byte) *Node { return f.nodesMap[root] }
