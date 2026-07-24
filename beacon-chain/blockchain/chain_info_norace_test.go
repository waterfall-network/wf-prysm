package blockchain

import (
	"context"
	"testing"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/async/abool"
	testDB "gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/db/testing"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state/stategen"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/wrapper"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/util"
)

func TestHeadSlot_DataRace(t *testing.T) {
	beaconDB := testDB.SetupDB(t)
	s := &Service{
		cfg:           &config{BeaconDB: beaconDB},
		isGwatSyncing: abool.New(),
	}
	b, err := wrapper.WrappedSignedBeaconBlock(util.NewBeaconBlock())
	require.NoError(t, err)
	st, _ := util.DeterministicGenesisState(t, 1)
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		require.NoError(t, s.saveHead(context.Background(), [32]byte{}, b, st))
	}()
	s.HeadSlot()
	<-wait
}

func TestHeadRoot_DataRace(t *testing.T) {
	beaconDB := testDB.SetupDB(t)
	s := &Service{
		cfg:           &config{BeaconDB: beaconDB, StateGen: stategen.New(beaconDB)},
		head:          &head{root: [32]byte{'A'}},
		isGwatSyncing: abool.New(),
	}
	b, err := wrapper.WrappedSignedBeaconBlock(util.NewBeaconBlock())
	require.NoError(t, err)
	wait := make(chan struct{})
	st, _ := util.DeterministicGenesisState(t, 1)
	go func() {
		defer close(wait)
		require.NoError(t, s.saveHead(context.Background(), [32]byte{}, b, st))

	}()
	_, err = s.HeadRoot(context.Background())
	require.NoError(t, err)
	<-wait
}

func TestHeadBlock_DataRace(t *testing.T) {
	beaconDB := testDB.SetupDB(t)
	wsb, err := wrapper.WrappedSignedBeaconBlock(&ethpb.SignedBeaconBlock{})
	require.NoError(t, err)
	s := &Service{
		cfg:           &config{BeaconDB: beaconDB, StateGen: stategen.New(beaconDB)},
		head:          &head{block: wsb},
		isGwatSyncing: abool.New(),
	}
	b, err := wrapper.WrappedSignedBeaconBlock(util.NewBeaconBlock())
	require.NoError(t, err)
	wait := make(chan struct{})
	st, _ := util.DeterministicGenesisState(t, 1)
	go func() {
		defer close(wait)
		require.NoError(t, s.saveHead(context.Background(), [32]byte{}, b, st))

	}()
	_, err = s.HeadBlock(context.Background())
	require.NoError(t, err)
	<-wait
}

func TestHeadState_DataRace(t *testing.T) {
	beaconDB := testDB.SetupDB(t)
	s := &Service{
		cfg:           &config{BeaconDB: beaconDB, StateGen: stategen.New(beaconDB)},
		isGwatSyncing: abool.New(),
	}
	b, err := wrapper.WrappedSignedBeaconBlock(util.NewBeaconBlock())
	require.NoError(t, err)
	wait := make(chan struct{})
	st, _ := util.DeterministicGenesisState(t, 1)
	go func() {
		defer close(wait)
		require.NoError(t, s.saveHead(context.Background(), [32]byte{}, b, st))

	}()
	_, err = s.HeadState(context.Background())
	require.NoError(t, err)
	<-wait
}
