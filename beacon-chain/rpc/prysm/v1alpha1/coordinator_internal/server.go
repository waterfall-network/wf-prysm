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

// Package coordinator_internal provides the CoordinatorInternal gRPC server
// used by the wf-coordinator sidecar to interact with the beacon node's
// dag-finalization state without going through the public validator API.
package coordinator_internal

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed"
	statefeed "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state/stategen"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
	ethpbv1 "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/eth/v1"
	eth "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
	gwatTypes "gitlab.waterfall.network/waterfall/protocol/gwat/core/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var log = logrus.WithField("prefix", "coordinator-internal")

// DagFinalizationFetcher is implemented by blockchain.Service and provides the
// dag-finalization-specific methods needed by the CoordinatorInternal server.
type DagFinalizationFetcher interface {
	CollectFinalizationParams(ctx context.Context, headState state.BeaconState, syncMode gwatTypes.SyncMode) (*gwatTypes.FinalizationParams, error)
	CacheGwatCoordinatedState(cp *gwatTypes.Checkpoint)
}

// Server implements eth.CoordinatorInternalServer for the wf-coordinator sidecar.
type Server struct {
	eth.UnimplementedCoordinatorInternalServer

	Ctx           context.Context
	StateNotifier statefeed.Notifier
	StateGen      *stategen.State
	DagFinalizer  DagFinalizationFetcher
}

// StreamNewHeads streams a NewHeadEvent for every canonical head advancement.
// The sidecar subscribes once on startup and drives its dag-finalization loop
// from the events it receives.
func (s *Server) StreamNewHeads(_ *emptypb.Empty, stream eth.CoordinatorInternal_StreamNewHeadsServer) error {
	stateCh := make(chan *feed.Event, 1)
	sub := s.StateNotifier.StateFeed().Subscribe(stateCh)
	defer sub.Unsubscribe()

	for {
		select {
		case event := <-stateCh:
			if event.Type != statefeed.NewHead {
				continue
			}
			head, ok := event.Data.(*ethpbv1.EventHead)
			if !ok {
				continue
			}
			if err := stream.Send(&eth.NewHeadEvent{
				Slot:      uint64(head.GetSlot()),
				BlockRoot: head.GetBlock(),
				StateRoot: head.GetState(),
			}); err != nil {
				return status.Errorf(codes.Unavailable, "StreamNewHeads: send failed: %v", err)
			}
		case err := <-sub.Err():
			return status.Errorf(codes.Aborted, "StreamNewHeads: subscription error: %v", err)
		case <-s.Ctx.Done():
			return status.Error(codes.Canceled, "server context canceled")
		case <-stream.Context().Done():
			return status.Error(codes.Canceled, "client disconnected")
		}
	}
}

// GetFinalizationParams collects the finalization parameters for the head
// identified by block_root, ready for the sidecar to pass to gwat.
func (s *Server) GetFinalizationParams(ctx context.Context, req *eth.GetFinalizationParamsRequest) (*eth.FinalizationParamsResponse, error) {
	if len(req.GetBlockRoot()) != 32 {
		return nil, status.Error(codes.InvalidArgument, "block_root must be 32 bytes")
	}
	blockRoot := bytesutil.ToBytes32(req.GetBlockRoot())
	syncMode := gwatTypes.SyncMode(req.GetSyncMode())

	headState, err := s.StateGen.SyncStateByRoot(ctx, blockRoot)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "state for root %x not found: %v", blockRoot, err)
	}
	if headState == nil || headState.IsNil() {
		return nil, status.Errorf(codes.NotFound, "nil state for root %x", blockRoot)
	}
	if err := s.StateGen.AddSyncStateCache(blockRoot, headState); err != nil {
		log.WithError(err).WithField("root", blockRoot).Warn("GetFinalizationParams: failed to cache state")
		s.StateGen.RemoveSyncStateCache(blockRoot)
	}

	params, err := s.DagFinalizer.CollectFinalizationParams(ctx, headState, syncMode)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "collect finalization params: %v", err)
	}

	return finalizationParamsToProto(params), nil
}

// SubmitFinalizationResult receives the outcome of gwat ExecutionDagFinalize
// from the sidecar and caches the resulting gwat coordinated checkpoint.
func (s *Server) SubmitFinalizationResult(ctx context.Context, req *eth.SubmitFinalizationResultRequest) (*emptypb.Empty, error) {
	paramCp := req.GetParamCheckpoint()
	if paramCp == nil {
		return nil, status.Error(codes.InvalidArgument, "param_checkpoint is required")
	}

	// Nothing to cache if gwat did not return a checkpoint.
	if len(req.GetResultCpRoot()) == 0 {
		return &emptypb.Empty{}, nil
	}

	paramRoot := gwatCommon.BytesToHash(paramCp.GetRoot())
	resultRoot := gwatCommon.BytesToHash(req.GetResultCpRoot())

	if paramRoot == resultRoot && paramCp.GetEpoch() == req.GetResultCpEpoch() {
		s.DagFinalizer.CacheGwatCoordinatedState(&gwatTypes.Checkpoint{
			FinEpoch: paramCp.GetFinEpoch(),
			Epoch:    paramCp.GetEpoch(),
			Root:     paramRoot,
			Spine:    gwatCommon.BytesToHash(paramCp.GetSpine()),
		})
		return &emptypb.Empty{}, nil
	}

	// The checkpoint returned by gwat differs from what was in the params.
	// Look up the beacon state for the gwat-returned checkpoint root to derive
	// the correct spine, matching the logic in processDagFinalization.
	cpRoot32 := bytesutil.ToBytes32(req.GetResultCpRoot())
	cpState, err := s.StateGen.SyncStateByRoot(ctx, cpRoot32)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "state for result cp root %x not found: %v", cpRoot32, err)
	}
	if cpState == nil || cpState.IsNil() {
		return nil, status.Errorf(codes.NotFound, "nil state for result cp root %x", cpRoot32)
	}
	if err := s.StateGen.AddSyncStateCache(cpRoot32, cpState); err != nil {
		log.WithError(errors.Wrapf(err, "cache state for root %x", cpRoot32)).Warn("SubmitFinalizationResult: cache failed")
		s.StateGen.RemoveSyncStateCache(cpRoot32)
	}

	finSeq := gwatCommon.HashArrayFromBytes(cpState.SpineData().Finalization)
	if len(finSeq) == 0 {
		finSeq = gwatCommon.HashArrayFromBytes(cpState.SpineData().CpFinalized)
	}
	var spine gwatCommon.Hash
	if len(finSeq) > 0 {
		spine = finSeq[len(finSeq)-1]
	}

	s.DagFinalizer.CacheGwatCoordinatedState(&gwatTypes.Checkpoint{
		Epoch: req.GetResultCpEpoch(),
		Root:  gwatCommon.BytesToHash(cpRoot32[:]),
		Spine: spine,
	})

	log.WithFields(logrus.Fields{
		"resultCpEpoch": req.GetResultCpEpoch(),
		"resultCpRoot":  resultRoot.Hex(),
		"spine":         spine.Hex(),
	}).Info("SubmitFinalizationResult: cached gwat coordinated checkpoint")

	return &emptypb.Empty{}, nil
}

// finalizationParamsToProto converts gwatTypes.FinalizationParams to its proto wire form.
func finalizationParamsToProto(fp *gwatTypes.FinalizationParams) *eth.FinalizationParamsResponse {
	resp := &eth.FinalizationParamsResponse{
		SyncMode: uint32(fp.SyncMode),
	}
	if fp.BaseSpine != nil {
		resp.BaseSpine = fp.BaseSpine.Bytes()
	}
	for _, spine := range fp.Spines {
		resp.Spines = append(resp.Spines, spine.Bytes())
	}
	if fp.Checkpoint != nil {
		resp.Checkpoint = checkpointToProto(fp.Checkpoint)
	}
	for _, op := range fp.ValSyncData {
		resp.ValSyncData = append(resp.ValSyncData, validatorSyncOpToProto(op))
	}
	return resp
}

func checkpointToProto(cp *gwatTypes.Checkpoint) *eth.FinalizationCheckpoint {
	return &eth.FinalizationCheckpoint{
		FinEpoch: cp.FinEpoch,
		Epoch:    cp.Epoch,
		Root:     cp.Root.Bytes(),
		Spine:    cp.Spine.Bytes(),
	}
}

func validatorSyncOpToProto(op *gwatTypes.ValidatorSync) *eth.ValidatorSyncOp {
	proto := &eth.ValidatorSyncOp{
		OpType:          uint32(op.OpType),
		ProcEpoch:       op.ProcEpoch,
		Index:           op.Index,
		CreatorAddress:  op.Creator.Bytes(),
		InitTxHash:      op.InitTxHash.Bytes(),
		ActivationEpoch: op.ActivationEpoch,
		ExitEpoch:       op.ExitEpoch,
	}
	if op.Amount != nil {
		proto.AmountWei = op.Amount.Bytes()
	}
	if op.Balance != nil {
		proto.BalanceWei = op.Balance.Bytes()
	}
	return proto
}
