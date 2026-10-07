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

// Command replay-slot replays a single block from an existing beacon-chain
// database and reports whether the resulting post-state root matches the one
// committed in the block. It is an offline debugging aid for localising
// state-transition divergences: no network, no execution engine, no sync.
//
// Build from the wf-prysm module root:
//
//	go build ./tools/replay-slot/
//
// Then run it:
//
//	./replay-slot --datadir=/path/to/coordinator --slot=657700
//
// Point --datadir at a COPY of the datadir, not at the live one. Despite only
// reading chain data, the tool opens the store read-write: kv.NewKVStore takes
// bolt's exclusive file lock and runs an Update transaction that creates the
// buckets, so the database file is modified. The beacon node must also not be
// running, or the lock cannot be acquired.
//
//	cp -r <datadir>/beaconchaindata /tmp/replay/beaconchaindata
//	./replay-slot --datadir=/tmp/replay --slot=657700
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/helpers"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/transition"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/db"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/db/kv"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state/stategen"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
)

func main() {
	datadir := flag.String("datadir", "", "beacon node datadir (the one holding beaconchaindata/)")
	slot := flag.Uint64("slot", 0, "slot of the block to replay")
	dumpVoting := flag.Bool("dump-voting", true, "print the post-state BlockVoting array")
	flag.Parse()

	if *datadir == "" || *slot == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: replay-slot --datadir=DIR --slot=N")
		os.Exit(2)
	}

	if err := run(context.Background(), *datadir, types.Slot(*slot), *dumpVoting); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "\nFAILED: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, datadir string, slot types.Slot, dumpVoting bool) error {
	params.UseMainnetConfig()

	dbPath := datadir + "/beaconchaindata"
	beaconDB, err := kv.NewKVStore(ctx, dbPath, &kv.Config{})
	if err != nil {
		return fmt.Errorf("open db at %s: %w", dbPath, err)
	}
	defer func() {
		if cerr := beaconDB.Close(); cerr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "close db: %v\n", cerr)
		}
	}()

	hasBlocks, blocks, err := beaconDB.BlocksBySlot(ctx, slot)
	if err != nil {
		return fmt.Errorf("read blocks at slot %d: %w", slot, err)
	}
	if !hasBlocks || len(blocks) == 0 {
		return fmt.Errorf("no block stored at slot %d", slot)
	}
	fmt.Printf("slot %d: %d block(s) in db\n", slot, len(blocks))

	// Attestation and proposer-reward processing resolve block metadata through
	// the context, the same way the blockchain service wires it up.
	ctx = context.WithValue(ctx, params.BeaconConfig().CtxBlockFetcherKey, db.BlockInfoFetcherFunc(beaconDB))

	// Every block stored at the slot is replayed and every result is reported.
	// A slot can hold more than one block (competing forks), and the whole
	// point of this tool is to surface divergences — so a mismatch on any of
	// them fails the run, even if a sibling block reproduced correctly.
	gen := stategen.New(beaconDB)
	mismatched := 0
	for i, signed := range blocks {
		fmt.Printf("\n=== block %d/%d ===\n", i+1, len(blocks))
		if err := replay(ctx, gen, signed, dumpVoting); err != nil {
			fmt.Printf("result: MISMATCH — %v\n", err)
			mismatched++
			continue
		}
		fmt.Println("result: MATCH — post-state root equals the block's committed state root")
	}
	if mismatched > 0 {
		return fmt.Errorf("%d of %d block(s) at slot %d did not reproduce their committed state root",
			mismatched, len(blocks), slot)
	}
	return nil
}

func replay(ctx context.Context, gen *stategen.State, signed block.SignedBeaconBlock, dumpVoting bool) error {
	blk := signed.Block()
	root, err := blk.HashTreeRoot()
	if err != nil {
		return fmt.Errorf("block htr: %w", err)
	}
	parentRoot := blkParentRoot(blk)
	fmt.Printf("block root  : %#x\n", root)
	fmt.Printf("parent root : %#x\n", parentRoot)
	fmt.Printf("wanted root : %#x\n", blk.StateRoot())

	preState, err := gen.StateByRoot(ctx, parentRoot)
	if err != nil {
		return fmt.Errorf("load parent state: %w", err)
	}
	if preState == nil || preState.IsNil() {
		return fmt.Errorf("parent state %#x not available", parentRoot)
	}
	fmt.Printf("pre-state slot: %d\n", preState.Slot())

	_, postState, err := transition.ExecuteStateTransitionNoVerifyAnySig(ctx, preState, signed)
	if postState != nil && !postState.IsNil() {
		dumpState(postState, dumpVoting)
	}
	if err != nil {
		return err
	}
	return nil
}

func blkParentRoot(blk block.BeaconBlock) [32]byte {
	var out [32]byte
	copy(out[:], blk.ParentRoot())
	return out
}

// dumpState prints the parts of the post-state most likely to diverge, so two
// runs (or two builds) can be diffed line by line.
func dumpState(st state.BeaconState, dumpVoting bool) {
	if root, err := st.HashTreeRoot(context.Background()); err == nil {
		fmt.Printf("got root    : %#x\n", root)
	}
	sd := st.SpineData()
	fmt.Printf("spine.prefix       : %#x\n", sd.GetPrefix())
	fmt.Printf("spine.finalization : %#x\n", sd.GetFinalization())
	fmt.Printf("spine.cpFinalized  : %#x\n", sd.GetCpFinalized())
	for i, ps := range sd.GetParentSpines() {
		fmt.Printf("spine.parent[%d]    : %#x\n", i, ps.GetSpines())
	}
	if dumpVoting {
		fmt.Printf("blockVoting        : %s\n", helpers.PrintBlockVotingArr(st.BlockVoting()))
	}
}
