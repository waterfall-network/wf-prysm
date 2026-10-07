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

// Package adapter bridges Prysm/coordinator types to the Apache 2.0
// wf-types interfaces consumed by wf-consensus processors.
package adapter

import (
	eth2types "github.com/prysmaticlabs/eth2-types"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

// ---- SpineData --------------------------------------------------------------

func SpineDataToWF(sd *ethpb.SpineData) *wftypes.SpineData {
	if sd == nil {
		return &wftypes.SpineData{}
	}
	ps := make([]*wftypes.SpinesSeq, len(sd.ParentSpines))
	for i, p := range sd.ParentSpines {
		if p == nil {
			ps[i] = &wftypes.SpinesSeq{}
		} else {
			ps[i] = &wftypes.SpinesSeq{Spines: append([]byte(nil), p.Spines...)}
		}
	}
	return &wftypes.SpineData{
		Spines:       append([]byte(nil), sd.Spines...),
		Prefix:       append([]byte(nil), sd.Prefix...),
		Finalization: append([]byte(nil), sd.Finalization...),
		CpFinalized:  append([]byte(nil), sd.CpFinalized...),
		ParentSpines: ps,
	}
}

func SpineDataFromWF(sd *wftypes.SpineData) *ethpb.SpineData {
	if sd == nil {
		return &ethpb.SpineData{}
	}
	ps := make([]*ethpb.SpinesSeq, len(sd.ParentSpines))
	for i, p := range sd.ParentSpines {
		if p == nil {
			ps[i] = &ethpb.SpinesSeq{}
		} else {
			ps[i] = &ethpb.SpinesSeq{Spines: append([]byte(nil), p.Spines...)}
		}
	}
	return &ethpb.SpineData{
		Spines:       append([]byte(nil), sd.Spines...),
		Prefix:       append([]byte(nil), sd.Prefix...),
		Finalization: append([]byte(nil), sd.Finalization...),
		CpFinalized:  append([]byte(nil), sd.CpFinalized...),
		ParentSpines: ps,
	}
}

// ---- BlockVoting ------------------------------------------------------------

func BlockVotingToWF(bvs []*ethpb.BlockVoting) []*wftypes.BlockVoting {
	if bvs == nil {
		return nil
	}
	res := make([]*wftypes.BlockVoting, len(bvs))
	for i, bv := range bvs {
		if bv == nil {
			continue
		}
		votes := make([]*wftypes.CommitteeVote, len(bv.Votes))
		for j, v := range bv.Votes {
			if v == nil {
				continue
			}
			votes[j] = &wftypes.CommitteeVote{
				AggregationBits: append([]byte(nil), v.AggregationBits...),
				Slot:            wftypes.Slot(v.Slot),
				Index:           wftypes.CommitteeIndex(v.Index),
			}
		}
		res[i] = &wftypes.BlockVoting{
			Root:       append([]byte(nil), bv.Root...),
			Slot:       wftypes.Slot(bv.Slot),
			Candidates: append([]byte(nil), bv.Candidates...),
			Votes:      votes,
		}
	}
	return res
}

func BlockVotingFromWF(bvs []*wftypes.BlockVoting) []*ethpb.BlockVoting {
	if bvs == nil {
		return nil
	}
	res := make([]*ethpb.BlockVoting, len(bvs))
	for i, bv := range bvs {
		if bv == nil {
			continue
		}
		votes := make([]*ethpb.CommitteeVote, len(bv.Votes))
		for j, v := range bv.Votes {
			if v == nil {
				continue
			}
			votes[j] = &ethpb.CommitteeVote{
				AggregationBits: append([]byte(nil), v.AggregationBits...),
				Slot:            eth2types.Slot(v.Slot),
				Index:           eth2types.CommitteeIndex(v.Index),
			}
		}
		res[i] = &ethpb.BlockVoting{
			Root:       append([]byte(nil), bv.Root...),
			Slot:       eth2types.Slot(bv.Slot),
			Candidates: append([]byte(nil), bv.Candidates...),
			Votes:      votes,
		}
	}
	return res
}

// ---- Checkpoint -------------------------------------------------------------

func CheckpointToWF(cp *ethpb.Checkpoint) *wftypes.Checkpoint {
	if cp == nil {
		return &wftypes.Checkpoint{}
	}
	var root [32]byte
	copy(root[:], cp.Root)
	return &wftypes.Checkpoint{
		Epoch: wftypes.Epoch(cp.Epoch),
		Root:  root,
	}
}

func CheckpointFromWF(cp *wftypes.Checkpoint) *ethpb.Checkpoint {
	if cp == nil {
		return &ethpb.Checkpoint{}
	}
	return &ethpb.Checkpoint{
		Epoch: eth2types.Epoch(cp.Epoch),
		Root:  append([]byte(nil), cp.Root[:]...),
	}
}

// ---- Validator --------------------------------------------------------------

func ValidatorToWF(v *ethpb.Validator) *wftypes.Validator {
	if v == nil {
		return nil
	}
	ops := make([]*wftypes.WithdrawalOp, len(v.WithdrawalOps))
	for i, op := range v.WithdrawalOps {
		if op == nil {
			continue
		}
		ops[i] = &wftypes.WithdrawalOp{
			Amount: op.Amount,
			Hash:   append([]byte(nil), op.Hash...),
			Slot:   wftypes.Slot(op.Slot),
		}
	}
	return &wftypes.Validator{
		PublicKey:                  append([]byte(nil), v.PublicKey...),
		CreatorAddress:             append([]byte(nil), v.CreatorAddress...),
		WithdrawalCredentials:      append([]byte(nil), v.WithdrawalCredentials...),
		EffectiveBalance:           v.EffectiveBalance,
		Slashed:                    v.Slashed,
		ActivationEligibilityEpoch: wftypes.Epoch(v.ActivationEligibilityEpoch),
		ActivationEpoch:            wftypes.Epoch(v.ActivationEpoch),
		ExitEpoch:                  wftypes.Epoch(v.ExitEpoch),
		WithdrawableEpoch:          wftypes.Epoch(v.WithdrawableEpoch),
		ActivationHash:             append([]byte(nil), v.ActivationHash...),
		ExitHash:                   append([]byte(nil), v.ExitHash...),
		WithdrawalOps:              ops,
	}
}

func ValidatorFromWF(v *wftypes.Validator) *ethpb.Validator {
	if v == nil {
		return nil
	}
	ops := make([]*ethpb.WithdrawalOp, len(v.WithdrawalOps))
	for i, op := range v.WithdrawalOps {
		if op == nil {
			continue
		}
		ops[i] = &ethpb.WithdrawalOp{
			Amount: op.Amount,
			Hash:   append([]byte(nil), op.Hash...),
			Slot:   eth2types.Slot(op.Slot),
		}
	}
	return &ethpb.Validator{
		PublicKey:                  append([]byte(nil), v.PublicKey...),
		CreatorAddress:             append([]byte(nil), v.CreatorAddress...),
		WithdrawalCredentials:      append([]byte(nil), v.WithdrawalCredentials...),
		EffectiveBalance:           v.EffectiveBalance,
		Slashed:                    v.Slashed,
		ActivationEligibilityEpoch: eth2types.Epoch(v.ActivationEligibilityEpoch),
		ActivationEpoch:            eth2types.Epoch(v.ActivationEpoch),
		ExitEpoch:                  eth2types.Epoch(v.ExitEpoch),
		WithdrawableEpoch:          eth2types.Epoch(v.WithdrawableEpoch),
		ActivationHash:             append([]byte(nil), v.ActivationHash...),
		ExitHash:                   append([]byte(nil), v.ExitHash...),
		WithdrawalOps:              ops,
	}
}

// ---- Withdrawal (block operation, coordinator → wf-consensus) ---------------

func WithdrawalToWF(w *ethpb.Withdrawal) *wftypes.Withdrawal {
	if w == nil {
		return nil
	}
	return &wftypes.Withdrawal{
		PublicKey:      append([]byte(nil), w.PublicKey...),
		ValidatorIndex: wftypes.ValidatorIndex(w.ValidatorIndex),
		Amount:         w.Amount,
		InitTxHash:     append([]byte(nil), w.InitTxHash...),
		Epoch:          wftypes.Epoch(w.Epoch),
	}
}

func WithdrawalsToWF(ws []*ethpb.Withdrawal) []*wftypes.Withdrawal {
	res := make([]*wftypes.Withdrawal, len(ws))
	for i, w := range ws {
		res[i] = WithdrawalToWF(w)
	}
	return res
}
