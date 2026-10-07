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

package coordinator_internal

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/async/event"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed"
	statefeed "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/feed/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	ethpbv1 "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/eth/v1"
	eth "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
	gwatTypes "gitlab.waterfall.network/waterfall/protocol/gwat/core/types"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// ---- fakes ----

type fakeStateNotifier struct {
	mu   sync.Mutex
	feed *event.Feed
}

func (f *fakeStateNotifier) StateFeed() *event.Feed {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.feed == nil {
		f.feed = new(event.Feed)
	}
	return f.feed
}

type fakeDagFinalizer struct {
	params           *gwatTypes.FinalizationParams
	cachedCheckpoint *gwatTypes.Checkpoint
}

func (f *fakeDagFinalizer) CollectFinalizationParams(_ context.Context, _ state.BeaconState, mode gwatTypes.SyncMode) (*gwatTypes.FinalizationParams, error) {
	if f.params == nil {
		return &gwatTypes.FinalizationParams{SyncMode: mode}, nil
	}
	cp := *f.params
	cp.SyncMode = mode
	return &cp, nil
}

func (f *fakeDagFinalizer) CacheGwatCoordinatedState(cp *gwatTypes.Checkpoint) {
	f.cachedCheckpoint = cp
}

// streamCollector implements CoordinatorInternal_StreamNewHeadsServer.
type streamCollector struct {
	grpc.ServerStream
	ctx    context.Context
	events []*eth.NewHeadEvent
	mu     sync.Mutex
}

func (s *streamCollector) Send(e *eth.NewHeadEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *streamCollector) Context() context.Context { return s.ctx }

func (s *streamCollector) collected() []*eth.NewHeadEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*eth.NewHeadEvent, len(s.events))
	copy(cp, s.events)
	return cp
}

// ---- tests ----

func TestStreamNewHeads_SendsEventOnNewHead(t *testing.T) {
	notifier := &fakeStateNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &Server{
		Ctx:           ctx,
		StateNotifier: notifier,
		DagFinalizer:  &fakeDagFinalizer{},
	}

	collector := &streamCollector{ctx: ctx}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.StreamNewHeads(&emptypb.Empty{}, collector)
	}()

	// Give the goroutine time to subscribe.
	time.Sleep(20 * time.Millisecond)

	blockRoot := [32]byte{0x01}
	stateRoot := [32]byte{0x02}
	notifier.StateFeed().Send(&feed.Event{
		Type: statefeed.NewHead,
		Data: &ethpbv1.EventHead{
			Slot:  5,
			Block: blockRoot[:],
			State: stateRoot[:],
		},
	})

	// Allow delivery.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Canceled")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StreamNewHeads did not return after context cancel")
	}

	got := collector.collected()
	require.Len(t, got, 1)
	assert.Equal(t, uint64(5), got[0].GetSlot())
	assert.Equal(t, blockRoot[:], got[0].GetBlockRoot())
	assert.Equal(t, stateRoot[:], got[0].GetStateRoot())
}

func TestStreamNewHeads_IgnoresNonHeadEvents(t *testing.T) {
	notifier := &fakeStateNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &Server{
		Ctx:           ctx,
		StateNotifier: notifier,
		DagFinalizer:  &fakeDagFinalizer{},
	}

	collector := &streamCollector{ctx: ctx}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.StreamNewHeads(&emptypb.Empty{}, collector)
	}()

	time.Sleep(20 * time.Millisecond)

	notifier.StateFeed().Send(&feed.Event{Type: statefeed.BlockProcessed})
	notifier.StateFeed().Send(&feed.Event{Type: statefeed.Synced})

	time.Sleep(20 * time.Millisecond)
	cancel()

	<-errCh
	assert.Empty(t, collector.collected(), "non-head events must not produce output")
}

func TestSubmitFinalizationResult_MatchingCheckpoint(t *testing.T) {
	fakeRoot := gwatCommon.HexToHash("0xaabb")
	fakeSpine := gwatCommon.HexToHash("0xccdd")

	finalizer := &fakeDagFinalizer{}
	srv := &Server{
		Ctx:          context.Background(),
		DagFinalizer: finalizer,
	}

	_, err := srv.SubmitFinalizationResult(context.Background(), &eth.SubmitFinalizationResultRequest{
		LfSpine:       fakeSpine.Bytes(),
		ResultCpEpoch: 7,
		ResultCpRoot:  fakeRoot.Bytes(),
		ParamCheckpoint: &eth.FinalizationCheckpoint{
			FinEpoch: 6,
			Epoch:    7,
			Root:     fakeRoot.Bytes(),
			Spine:    fakeSpine.Bytes(),
		},
	})
	require.NoError(t, err)

	cached := finalizer.cachedCheckpoint
	require.NotNil(t, cached)
	assert.Equal(t, uint64(7), cached.Epoch)
	assert.Equal(t, fakeRoot, cached.Root)
	assert.Equal(t, fakeSpine, cached.Spine)
}

func TestSubmitFinalizationResult_EmptyResultRoot_NoCache(t *testing.T) {
	finalizer := &fakeDagFinalizer{}
	srv := &Server{
		Ctx:          context.Background(),
		DagFinalizer: finalizer,
	}

	_, err := srv.SubmitFinalizationResult(context.Background(), &eth.SubmitFinalizationResultRequest{
		// result_cp_root is empty → nothing to cache
		ParamCheckpoint: &eth.FinalizationCheckpoint{Epoch: 1, Root: make([]byte, 32)},
	})
	require.NoError(t, err)
	assert.Nil(t, finalizer.cachedCheckpoint)
}

func TestFinalizationParamsToProto_RoundTrip(t *testing.T) {
	spine1 := gwatCommon.HexToHash("0x1111")
	spine2 := gwatCommon.HexToHash("0x2222")
	base := gwatCommon.HexToHash("0x3333")
	cpRoot := gwatCommon.HexToHash("0x4444")
	cpSpine := gwatCommon.HexToHash("0x5555")

	creator := gwatCommon.HexToAddress("0xabc")
	txHash := gwatCommon.HexToHash("0x6666")
	amount := big.NewInt(1000)
	balance := big.NewInt(5000)

	fp := &gwatTypes.FinalizationParams{
		Spines:    gwatCommon.HashArray{spine1, spine2},
		BaseSpine: &base,
		Checkpoint: &gwatTypes.Checkpoint{
			FinEpoch: 3,
			Epoch:    4,
			Root:     cpRoot,
			Spine:    cpSpine,
		},
		ValSyncData: []*gwatTypes.ValidatorSync{
			{
				OpType:          gwatTypes.UpdateBalance,
				ProcEpoch:       5,
				Index:           42,
				Creator:         creator,
				Amount:          amount,
				InitTxHash:      txHash,
				ActivationEpoch: 2,
				ExitEpoch:       100,
				Balance:         balance,
			},
		},
		SyncMode: gwatTypes.HeadSync,
	}

	proto := finalizationParamsToProto(fp)

	assert.Equal(t, uint32(gwatTypes.HeadSync), proto.GetSyncMode())
	require.Len(t, proto.GetSpines(), 2)
	assert.Equal(t, spine1.Bytes(), proto.GetSpines()[0])
	assert.Equal(t, spine2.Bytes(), proto.GetSpines()[1])
	assert.Equal(t, base.Bytes(), proto.GetBaseSpine())

	cp := proto.GetCheckpoint()
	require.NotNil(t, cp)
	assert.Equal(t, uint64(3), cp.GetFinEpoch())
	assert.Equal(t, uint64(4), cp.GetEpoch())
	assert.Equal(t, cpRoot.Bytes(), cp.GetRoot())
	assert.Equal(t, cpSpine.Bytes(), cp.GetSpine())

	require.Len(t, proto.GetValSyncData(), 1)
	op := proto.GetValSyncData()[0]
	assert.Equal(t, uint32(gwatTypes.UpdateBalance), op.GetOpType())
	assert.Equal(t, uint64(5), op.GetProcEpoch())
	assert.Equal(t, uint64(42), op.GetIndex())
	assert.Equal(t, creator.Bytes(), op.GetCreatorAddress())
	assert.Equal(t, amount.Bytes(), op.GetAmountWei())
	assert.Equal(t, txHash.Bytes(), op.GetInitTxHash())
	assert.Equal(t, uint64(2), op.GetActivationEpoch())
	assert.Equal(t, uint64(100), op.GetExitEpoch())
	assert.Equal(t, balance.Bytes(), op.GetBalanceWei())
}
