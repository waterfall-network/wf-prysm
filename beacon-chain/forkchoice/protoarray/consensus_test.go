//Copyright 2024   Blue Wave Inc.
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
	"context"
	"fmt"
	"testing"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/assert"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
)

func TestStore_GetFork(t *testing.T) {
	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		[32]byte{'A'}: 0,
		[32]byte{'B'}: 1,
		[32]byte{'C'}: 2,
		[32]byte{'D'}: 3,
		[32]byte{'E'}: 4,
		[32]byte{'F'}: 5,
		[32]byte{'G'}: 6,
		[32]byte{'H'}: 7,
		[32]byte{'I'}: 8,
		[32]byte{'J'}: 9,
		[32]byte{'K'}: 10,
	}
	f.store.nodes = []*Node{
		//fork 0
		{slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
		{slot: 2, root: [32]byte{'B'}, parent: 0},
		{slot: 3, root: [32]byte{'C'}, parent: 1},
		{slot: 4, root: [32]byte{'D'}, parent: 2},
		{slot: 5, root: [32]byte{'E'}, parent: 3},
		{slot: 6, root: [32]byte{'F'}, parent: 4},
		//fork 1
		{slot: 7, root: [32]byte{'G'}, parent: 2},
		{slot: 8, root: [32]byte{'H'}, parent: 6},
		//fork 2
		{slot: 9, root: [32]byte{'I'}, parent: 3},
		{slot: 10, root: [32]byte{'J'}, parent: 8},
		{slot: 11, root: [32]byte{'K'}, parent: 9},
	}
	want := &Fork{
		roots: [][32]byte{
			{'H'},
			{'G'},
			{'C'},
			{'B'},
			{'A'},
		},
		nodesMap: map[[32]byte]*Node{
			[32]byte{'H'}: {slot: 8, root: [32]byte{'H'}, parent: 6},
			[32]byte{'G'}: {slot: 7, root: [32]byte{'G'}, parent: 2},
			[32]byte{'C'}: {slot: 3, root: [32]byte{'C'}, parent: 1},
			[32]byte{'B'}: {slot: 2, root: [32]byte{'B'}, parent: 0},
			[32]byte{'A'}: {slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
		},
	}
	got := f.GetFork([32]byte{'H'})
	require.DeepEqual(t, want, got)
}

func TestStore_GetForks(t *testing.T) {
	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		[32]byte{'A'}: 0,
		[32]byte{'B'}: 1,
		[32]byte{'C'}: 2,
		[32]byte{'D'}: 3,
		[32]byte{'E'}: 4,
		[32]byte{'F'}: 5,
		[32]byte{'G'}: 6,
		[32]byte{'H'}: 7,
		[32]byte{'I'}: 8,
		[32]byte{'J'}: 9,
		[32]byte{'K'}: 10,
	}
	f.store.nodes = []*Node{
		//fork 0
		{slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
		{slot: 2, root: [32]byte{'B'}, parent: 0},
		{slot: 3, root: [32]byte{'C'}, parent: 1},
		{slot: 4, root: [32]byte{'D'}, parent: 2},
		{slot: 5, root: [32]byte{'E'}, parent: 3},
		{slot: 6, root: [32]byte{'F'}, parent: 4},
		//fork 1
		{slot: 7, root: [32]byte{'G'}, parent: 2},
		{slot: 8, root: [32]byte{'H'}, parent: 6},
		//fork 2
		{slot: 9, root: [32]byte{'I'}, parent: 3},
		{slot: 10, root: [32]byte{'J'}, parent: 8},
		{slot: 11, root: [32]byte{'K'}, parent: 9},
	}
	want := []*Fork{
		{
			roots: [][32]byte{
				{'K'},
				{'J'},
				{'I'},
				{'D'},
				{'C'},
				{'B'},
				{'A'},
			},
			nodesMap: map[[32]byte]*Node{
				[32]byte{'K'}: {slot: 11, root: [32]byte{'K'}, parent: 9},
				[32]byte{'J'}: {slot: 10, root: [32]byte{'J'}, parent: 8},
				[32]byte{'I'}: {slot: 9, root: [32]byte{'I'}, parent: 3},
				[32]byte{'D'}: {slot: 4, root: [32]byte{'D'}, parent: 2},
				[32]byte{'C'}: {slot: 3, root: [32]byte{'C'}, parent: 1},
				[32]byte{'B'}: {slot: 2, root: [32]byte{'B'}, parent: 0},
				[32]byte{'A'}: {slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
			},
		},
		{
			roots: [][32]byte{
				{'H'},
				{'G'},
				{'C'},
				{'B'},
				{'A'},
			},
			nodesMap: map[[32]byte]*Node{
				[32]byte{'H'}: {slot: 8, root: [32]byte{'H'}, parent: 6},
				[32]byte{'G'}: {slot: 7, root: [32]byte{'G'}, parent: 2},
				[32]byte{'C'}: {slot: 3, root: [32]byte{'C'}, parent: 1},
				[32]byte{'B'}: {slot: 2, root: [32]byte{'B'}, parent: 0},
				[32]byte{'A'}: {slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
			},
		},
		{
			roots: [][32]byte{
				{'F'},
				{'E'},
				{'D'},
				{'C'},
				{'B'},
				{'A'},
			},
			nodesMap: map[[32]byte]*Node{
				[32]byte{'F'}: {slot: 6, root: [32]byte{'F'}, parent: 4},
				[32]byte{'E'}: {slot: 5, root: [32]byte{'E'}, parent: 3},
				[32]byte{'D'}: {slot: 4, root: [32]byte{'D'}, parent: 2},
				[32]byte{'C'}: {slot: 3, root: [32]byte{'C'}, parent: 1},
				[32]byte{'B'}: {slot: 2, root: [32]byte{'B'}, parent: 0},
				[32]byte{'A'}: {slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
			},
		},
	}

	got := f.GetForks()
	require.DeepEqual(t, want, got)
}

func TestStore_GetForks_from_one(t *testing.T) {
	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		[32]byte{'A'}: 0,
	}
	f.store.nodes = []*Node{
		//fork 0
		{slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
	}
	want := []*Fork{
		{
			roots: [][32]byte{{'A'}},
			nodesMap: map[[32]byte]*Node{
				[32]byte{'A'}: {slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
			},
		},
	}

	got := f.GetForks()
	require.DeepEqual(t, want, got)
}

func TestStore_GetFirstFork(t *testing.T) {
	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		[32]byte{'A'}: 0,
		[32]byte{'B'}: 1,
		[32]byte{'C'}: 2,
		[32]byte{'D'}: 3,
		[32]byte{'E'}: 4,
		[32]byte{'F'}: 5,
		[32]byte{'G'}: 6,
		[32]byte{'H'}: 7,
		[32]byte{'I'}: 8,
		[32]byte{'J'}: 9,
		[32]byte{'K'}: 10,
	}
	f.store.nodes = []*Node{
		//fork 0
		{slot: 1, root: [32]byte{'A'}, parent: NonExistentNode},
		{slot: 2, root: [32]byte{'B'}, parent: 0},
		{slot: 3, root: [32]byte{'C'}, parent: 1},
		{slot: 4, root: [32]byte{'D'}, parent: 2},
		{slot: 5, root: [32]byte{'E'}, parent: 3},
		{slot: 6, root: [32]byte{'F'}, parent: 4},
		//fork 1
		{slot: 7, root: [32]byte{'G'}, parent: 2},
		{slot: 8, root: [32]byte{'H'}, parent: 6},
		//fork 2
		{slot: 9, root: [32]byte{'I'}, parent: 3},
		{slot: 10, root: [32]byte{'J'}, parent: 8},
		{slot: 11, root: [32]byte{'K'}, parent: 9},
	}
	want := &Node{slot: 3, root: [32]byte{'C'}, parent: 1}
	got := f.GetCommonAncestor()
	require.DeepEqual(t, want, got)
}

func nrToHash(i int) [32]byte {
	return bytesutil.ToBytes32([]byte(fmt.Sprintf("%d", i)))
}

func Test_collectTgTreeNodesByOptimisticSpines_prefix_extension(t *testing.T) {

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	f.store.nodes = []*Node{

		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 4, root: nrToHash(4), parent: 3, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 0
		{slot: 6, root: nrToHash(6), parent: 5, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 8, root: nrToHash(8), parent: 7, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '6'}, {'a', '7'}, {'a', '8'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 9, root: nrToHash(9), parent: 8, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '7'}, {'a', '8'}, {'a', '9'}, {'a', '1', '0'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'a', '3'}, nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{nrToHash(0), {'a', '5'}, nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	wantLeafs := map[[32]byte]int{nrToHash(9): 10}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

func Test_collectTgTreeNodesByOptimisticSpines_prefix_not_extension(t *testing.T) {

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	f.store.nodes = []*Node{

		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 4, root: nrToHash(4), parent: 3, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 0
		{slot: 6, root: nrToHash(6), parent: 5, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 8, root: nrToHash(8), parent: 7, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '6'}, {'a', '7'}, {'a', '8'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 9, root: nrToHash(9), parent: 8, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '8'}, {'a', '9'}, {'a', '1', '0'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'a', '3'}, nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{nrToHash(0), {'a', '5'}, nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	wantLeafs := map[[32]byte]int{nrToHash(9): 10}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

func Test_collectTgTreeNodesByOptimisticSpines_3_forks(t *testing.T) {

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	f.store.nodes = []*Node{

		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 1
		{slot: 4, root: nrToHash(4), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 2
		{slot: 6, root: nrToHash(6), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 3
		{slot: 8, root: nrToHash(8), parent: 5, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '6'}, {'a', '7'}, {'a', '8'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 9, root: nrToHash(9), parent: 8, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '7'}, {'a', '8'}, {'a', '9'}, {'a', '1', '0'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'a', '3'}, nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{nrToHash(0), {'a', '5'}, nrToHash(0), nrToHash(0)},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	wantLeafs := map[[32]byte]int{
		nrToHash(9): 6,
		nrToHash(7): 5,
		nrToHash(3): 4,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

func Test_collectTgTreeNodesByOptimisticSpines_1_forks(t *testing.T) {

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	f.store.nodes = []*Node{

		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 1
		{slot: 4, root: nrToHash(4), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 2
		{slot: 6, root: nrToHash(6), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 3
		{slot: 8, root: nrToHash(8), parent: 5, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '6'}, {'a', '7'}, {'a', '8'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 9, root: nrToHash(9), parent: 8, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '7'}, {'a', '8'}, {'a', '9'}, {'a', '1', '0'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'b', '3'}}, // <<< "b3"
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{{'a', '5'}},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
	}
	wantLeafs := map[[32]byte]int{
		nrToHash(2): 3,
		nrToHash(4): 3,
		nrToHash(3): 4,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

func TestGetParentByOptimisticSpines_TwoBranches(t *testing.T) {
	balances := []uint64{1, 1}
	justifiedRoot := nrToHash(0)
	finalizedRoot := nrToHash(0)
	var (
		r, hRoot          [32]byte
		err, hrErr        error
		nodesRootIndexMap map[[32]byte]uint64
	)

	f := New(0, 0)
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 0, nrToHash(0), params.BeaconConfig().ZeroHash, 0, 0, justifiedRoot[:], finalizedRoot[:], nil))

	r, err = f.Head(context.Background(), 0, nrToHash(0), balances, 0)
	require.NoError(t, err)
	assert.Equal(t, nrToHash(0), r, "Incorrect head with genesis")

	nodesRootIndexMap = map[[32]byte]uint64{nrToHash(0): 0}
	fcBase, _, diffNodes := getCompatibleFc(nodesRootIndexMap, f)
	hRoot, hrErr = calculateHeadRootByNodesIndexes(context.Background(), fcBase, diffNodes, nodesRootIndexMap, justifiedRoot)
	require.NoError(t, hrErr)
	assert.Equal(t, nrToHash(0), hRoot, "Incorrect head with justified epoch at 0")

	// Define the following tree:
	//                                0
	//                               / \
	//  justified: 0, finalization: 0 -> 1   2 <- justified: 0, finalization: 0
	//                              |   |
	//  justified: 1, finalization: 0 -> 3   4 <- justified: 0, finalization: 0
	//                              |   |
	//  justified: 1, finalization: 0 -> 5   6 <- justified: 0, finalization: 0
	//                              |   |
	//  justified: 1, finalization: 0 -> 7   8 <- justified: 1, finalization: 0
	//                              |   |
	//  justified: 2, finalization: 0 -> 9  10 <- justified: 2, finalization: 0
	// Left branch.
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 1, nrToHash(1), nrToHash(0), 0, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 2, nrToHash(3), nrToHash(1), 1, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 3, nrToHash(5), nrToHash(3), 1, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 4, nrToHash(7), nrToHash(5), 1, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 4, nrToHash(9), nrToHash(7), 2, 0, justifiedRoot[:], finalizedRoot[:], nil))
	// Right branch.
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 1, nrToHash(2), nrToHash(0), 0, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 2, nrToHash(4), nrToHash(2), 0, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 3, nrToHash(6), nrToHash(4), 0, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 4, nrToHash(8), nrToHash(6), 1, 0, justifiedRoot[:], finalizedRoot[:], nil))
	require.NoError(t, f.InsertOptimisticBlock(context.Background(), 4, nrToHash(10), nrToHash(8), 2, 0, justifiedRoot[:], finalizedRoot[:], nil))

	// With start at 0, the head should be 10:
	//           0  <-- start
	//          / \
	//         1   2
	//         |   |
	//         3   4
	//         |   |
	//         5   6
	//         |   |
	//         7   8
	//         |   |
	//         9  10 <-- head
	r, err = f.Head(context.Background(), 0, nrToHash(0), balances, 0)
	require.NoError(t, err)
	assert.Equal(t, nrToHash(10), r, "Incorrect head with justified epoch at 0")

	nodesRootIndexMap = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(3): 2,
		nrToHash(5): 3,
		nrToHash(7): 4,
		nrToHash(9): 5,

		nrToHash(2):  6,
		nrToHash(4):  7,
		nrToHash(6):  8,
		nrToHash(8):  9,
		nrToHash(10): 10,
	}

	fcBase, _, diffNodes = getCompatibleFc(nodesRootIndexMap, f)

	hRoot, hrErr = calculateHeadRootByNodesIndexes(context.Background(), fcBase, diffNodes, nodesRootIndexMap, justifiedRoot)
	require.NoError(t, hrErr)
	assert.Equal(t, nrToHash(10), hRoot, "Incorrect head with justified epoch at 0")

	// Add a vote to 1:
	//                 0
	//                / \
	//    +1 vote -> 1   2
	//               |   |
	//               3   4
	//               |   |
	//               5   6
	//               |   |
	//               7   8
	//               |   |
	//               9  10
	f.ProcessAttestation(context.Background(), []uint64{0}, nrToHash(1), 0)

	// With the additional vote to the left branch, the head should be 9:
	//           0  <-- start
	//          / \
	//         1   2
	//         |   |
	//         3   4
	//         |   |
	//         5   6
	//         |   |
	//         7   8
	//         |   |
	// head -> 9  10
	r, err = f.Head(context.Background(), 0, nrToHash(0), balances, 0)
	require.NoError(t, err)
	assert.Equal(t, nrToHash(9), r, "Incorrect head with justified epoch at 0")

	nodesRootIndexMap = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(3): 2,
		nrToHash(5): 3,
		nrToHash(7): 4,
		nrToHash(9): 5,

		//nrToHash(2):  6,
		//nrToHash(4):  7,
		//nrToHash(6):  8,
		//nrToHash(8):  9,
		//nrToHash(10): 10,
	}
	fcBase, _, diffNodes = getCompatibleFc(nodesRootIndexMap, f)
	hRoot, hrErr = calculateHeadRootByNodesIndexes(context.Background(), fcBase, diffNodes, nodesRootIndexMap, justifiedRoot)
	require.NoError(t, hrErr)
	assert.Equal(t, nrToHash(9), hRoot, "Incorrect head with justified epoch at 0")

	// Add a vote to 2:
	//                 0
	//                / \
	//               1   2 <- +1 vote
	//               |   |
	//               3   4
	//               |   |
	//               5   6
	//               |   |
	//               7   8
	//               |   |
	//               9  10
	f.ProcessAttestation(context.Background(), []uint64{1}, nrToHash(2), 0)

	// With the additional vote to the right branch, the head should be 10:
	//           0  <-- start
	//          / \
	//         1   2
	//         |   |
	//         3   4
	//         |   |
	//         5   6
	//         |   |
	//         7   8
	//         |   |
	//         9  10 <-- head
	r, err = f.Head(context.Background(), 0, nrToHash(0), balances, 0)
	require.NoError(t, err)
	assert.Equal(t, nrToHash(10), r, "Incorrect head with justified epoch at 0")

	nodesRootIndexMap = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(3): 2,
		nrToHash(5): 3,
		nrToHash(7): 4,
		nrToHash(9): 5,

		//nrToHash(2):  2,
		//nrToHash(4):  4,
		//nrToHash(6):  6,
		//nrToHash(8):  8,
		//nrToHash(10): 10,
	}
	fcBase, _, diffNodes = getCompatibleFc(nodesRootIndexMap, f)
	hRoot, hrErr = calculateHeadRootByNodesIndexes(context.Background(), fcBase, diffNodes, nodesRootIndexMap, justifiedRoot)
	require.NoError(t, hrErr)
	assert.Equal(t, nrToHash(9), hRoot, "Incorrect head with justified epoch at 0")

	r, err = f.Head(context.Background(), 1, nrToHash(1), balances, 0)
	require.NoError(t, err)
	assert.Equal(t, nrToHash(7), r, "Incorrect head with justified epoch at 0")
}

func Test_isSequenceMatchOptimisticSpines(t *testing.T) {
	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'b', '3'}}, // <<< "b3"
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{{'a', '5'}},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	spineSeq0 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		{'a', '4'},
		{'a', '5'},
		{'a', '6'},
		{'a', '7'},
		{'a', '8'},
		{'a', '9'},
		{'a', '1', '0'},
	}

	spineSeq1 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		{'a', '4'},
		//{'a', '5'},
		//{'a', '6'},
		{'a', '7'},
		{'a', '8'},
		{'a', '9'},
		{'a', '1', '0'},
	}

	//before fork FcTgTreeForkSlot
	actual := isSequenceMatchOptimisticSpines(spineSeq0, optSpines)
	require.Equal(t, true, actual)
	actual = isSequenceMatchOptimisticSpines(spineSeq1, optSpines)
	require.Equal(t, false, actual)
}

func Test_isPrefixMatchOptimisticSpines(t *testing.T) {
	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'b', '3'}}, // <<< "b3"
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{{'a', '5'}},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	spineSeq0 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		{'a', '4'},
		{'a', '5'},
		{'a', '6'},
		{'a', '7'},
		{'a', '8'},
		{'a', '9'},
		{'a', '1', '0'},
	}
	actual := isPrefixMatchOptimisticSpines(spineSeq0, optSpines)
	require.Equal(t, true, actual)

	spineSeq1 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		{'a', '4'},
		//{'a', '5'},
		//{'a', '6'},
		{'a', '7'},
		{'a', '8'},
		{'a', '9'},
		{'a', '1', '0'},
	}
	actual = isPrefixMatchOptimisticSpines(spineSeq1, optSpines)
	require.Equal(t, true, actual)

	//must be failed
	spineSeq2 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'f', 'f'}, //no in optSpines
		{'a', '4'},
	}
	actual = isPrefixMatchOptimisticSpines(spineSeq2, optSpines)
	require.Equal(t, false, actual)

	//must be true
	spineSeq3 := gwatCommon.HashArray{
		//{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		{'a', '4'},
	}
	actual = isPrefixMatchOptimisticSpines(spineSeq3, optSpines)
	require.Equal(t, true, actual)

	//must be failed
	spineSeq4 := gwatCommon.HashArray{
		{'a', '1'},
		{'a', '2'},
		{'b', '3'},
		//{'a', '4'}, // skipped
		//{'a', '5'}, // skipped
		{'a', '6'},
		{'a', '7'},
		{'a', '8'},
		{'a', '9'},
		{'a', '1', '0'},
		{'f', 'f', 'f'}, // no in opsSpines
	}
	actual = isPrefixMatchOptimisticSpines(spineSeq4, optSpines)
	require.Equal(t, false, actual)
}

func Test_collectTgTreeNodesByOptimisticSpines_1_forks_FcTgTreeForkSlot(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 1
		{slot: 4, root: nrToHash(4), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 2
		{slot: 6, root: nrToHash(6), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 3
		{slot: 8, root: nrToHash(8), parent: 5, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '6'}, {'a', '7'}, {'a', '8'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 9, root: nrToHash(9), parent: 8, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '7'}, {'a', '8'}, {'a', '9'}, {'a', '1', '0'}},
			prefix:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}},
			finalization: gwatCommon.HashArray{{'a', '1'}, {'a', '2'}, {'a', '3'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	optSpines := []gwatCommon.HashArray{
		{{'a', '1'}},
		{nrToHash(0), nrToHash(0), {'a', '2'}, nrToHash(0)},
		{{'b', '3'}}, // <<< insert new slot-block
		{nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), nrToHash(0), {'a', '4'}},
		{{'a', '5'}},
		{nrToHash(0), nrToHash(0), nrToHash(0), {'a', '6'}, nrToHash(0), nrToHash(0)},
		{{'a', '7'}},
		{{'a', '8'}, nrToHash(0)},
		{nrToHash(0), nrToHash(0), {'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
	}
	wantLeafs := map[[32]byte]int{
		nrToHash(2): 3,
		nrToHash(3): 4,
		nrToHash(4): 3,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

// NO cpFinalized terminal spine in optimistic spines
// Accepted by empty prefix condition
// `if isExtended || len(published) == 0 {`
func Test_collectTgTreeNodesByOptimisticSpines_no_cpFinalizedTermSpine(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}

	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}
	optSpines := []gwatCommon.HashArray{
		// no cpFinalized terminal spine
		{{'s', '0'}},
		{{'s', '1'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs := map[[32]byte]int{
		nrToHash(0): 1,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

// cpFinalized terminal spine IS in optimistic spines
// Accepted by empty prefix condition
// `if isExtended || len(published) == 0 {`
func Test_collectTgTreeNodesByOptimisticSpines_has_cpFinalizedTermSpine(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}

	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}
	optSpines := []gwatCommon.HashArray{
		{{'x', '0'}},      // must be ignored
		{{'x', '1'}},      // must be ignored
		{{'c', 'p', 'f'}}, // cpFinalized terminal spine
		{{'s', '0'}},
		{{'s', '1'}},
	}

	wantRootIndexMap := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs := map[[32]byte]int{
		nrToHash(0): 1,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap, rootIndexMap)
	require.DeepEqual(t, wantLeafs, leafs)
}

// tests of finalisation sequence.
func Test_collectTgTreeNodesByOptimisticSpines_finalization(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}

	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'f', '0'}, {'f', '1'}, {'f', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}

	//case: additional slot-blocks in optSpines
	optSpines_0 := []gwatCommon.HashArray{
		{{'x', 'x', '0'}}, // additional slot-blocks - must be ignored
		{{'f', '0'}},      // finalization
		{{'f', '0', '0'}}, // additional slot-blocks - must be ignored
		{{'f', '1'}},      // finalization
		{{'f', '1', '0'}}, // additional slot-blocks - must be ignored
		{{'f', '2'}},      // finalization
		{{'s', 's', 's'}},
	}

	wantRootIndexMap_0 := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs_0 := map[[32]byte]int{
		nrToHash(0): 1,
	}
	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines_0, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_0, rootIndexMap)
	require.DeepEqual(t, wantLeafs_0, leafs)

	//case: no spine of finalization in optSpines
	// No acceptable node
	optSpines_1 := []gwatCommon.HashArray{
		{{'f', '0'}}, // finalization
		{{'f', '1'}}, // finalization
		//{{'a', '3'}}, // no spine of finalization
		{{'s', 's', 's'}},
	}

	wantRootIndexMap_1 := map[[32]byte]uint64{}
	wantLeafs_1 := map[[32]byte]int{}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_1, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_1, rootIndexMap)
	require.DeepEqual(t, wantLeafs_1, leafs)
}

// tests of prefix sequence.
func Test_collectTgTreeNodesByOptimisticSpines_prefix(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}

	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{{'p', '0'}, {'p', '1'}, {'p', '2'}},
			finalization: gwatCommon.HashArray{{'f', '0'}, {'f', '1'}, {'f', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}

	//case: additional slot-blocks in optSpines
	optSpines_0 := []gwatCommon.HashArray{
		{{'f', '0'}},      // finalization
		{{'f', '1'}},      // finalization
		{{'f', '2'}},      // finalization
		{{'p', '0', '0'}}, // additional slot-blocks - must be ignored
		{{'p', '0'}},      // prefix
		{{'p', '1', '1'}}, // additional slot-blocks - must be ignored
		{{'p', '1'}},      // prefix
		{{'p', '2'}},      // prefix
		{{'s', 's', 's'}},
	}

	wantRootIndexMap_0 := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs_0 := map[[32]byte]int{
		nrToHash(0): 1,
	}
	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines_0, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_0, rootIndexMap)
	require.DeepEqual(t, wantLeafs_0, leafs)

	//case: no spine of prefix in optSpines
	// No acceptable node
	optSpines_1 := []gwatCommon.HashArray{
		{{'f', '0'}}, // finalization
		{{'f', '1'}}, // finalization
		{{'f', '2'}}, // finalization
		{{'p', '0'}}, // prefix
		{{'p', '1'}}, // prefix
		//{{'p', '2'}}, // no spine of prefix
		{{'s', 's', 's'}},
	}

	wantRootIndexMap_1 := map[[32]byte]uint64{}
	wantLeafs_1 := map[[32]byte]int{}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_1, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_1, rootIndexMap)
	require.DeepEqual(t, wantLeafs_1, leafs)

	//case: optSpines order mismatch to prefix
	// No acceptable node
	optSpines_2 := []gwatCommon.HashArray{
		{{'f', '0'}}, // finalization
		{{'f', '1'}}, // finalization
		{{'f', '2'}}, // finalization
		{{'p', '0'}}, // prefix
		{{'p', '2'}}, // bad order
		{{'p', '1'}}, // prefix
		{{'s', 's', 's'}},
	}

	wantRootIndexMap_2 := map[[32]byte]uint64{}
	wantLeafs_2 := map[[32]byte]int{}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_2, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_2, rootIndexMap)
	require.DeepEqual(t, wantLeafs_2, leafs)
}

// tests of candidates(spines) sequence.
func Test_collectTgTreeNodesByOptimisticSpines_candidates(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}

	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'s', '0'}, {'s', '1'}, {'s', '2'}},
			prefix:       gwatCommon.HashArray{{'p', '0'}, {'p', '1'}, {'p', '2'}},
			finalization: gwatCommon.HashArray{{'f', '0'}, {'f', '1'}, {'f', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}

	// case: matching of the first candidate and first optimistic spine
	optSpines_0 := []gwatCommon.HashArray{
		{{'f', '0'}},      // finalization
		{{'f', '1'}},      // finalization
		{{'f', '2'}},      // finalization
		{{'p', '0'}},      // prefix
		{{'p', '1'}},      // prefix
		{{'p', '2'}},      // prefix
		{{'s', '0'}},      // candidate[0]
		{{'s', '1', '1'}}, // new opt candidate
		{{'s', '2', '2'}}, // new opt candidate
	}

	wantRootIndexMap_0 := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs_0 := map[[32]byte]int{
		nrToHash(0): 1,
	}
	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines_0, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_0, rootIndexMap)
	require.DeepEqual(t, wantLeafs_0, leafs)

	// case: additional opt spine between prefix and candidates - fail
	optSpines_1 := []gwatCommon.HashArray{
		{{'f', '0'}},      // finalization
		{{'f', '1'}},      // finalization
		{{'f', '2'}},      // finalization
		{{'p', '0'}},      // prefix
		{{'p', '1'}},      // prefix
		{{'p', '2'}},      // prefix
		{{'x', '1', '1'}}, // additional opt spine
		{{'s', '0'}},      // candidate[0]
		{{'s', '1', '1'}}, // new opt candidate
		{{'s', '2', '2'}}, // new opt candidate
	}

	wantRootIndexMap_1 := map[[32]byte]uint64{}
	wantLeafs_1 := map[[32]byte]int{}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_1, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_1, rootIndexMap)
	require.DeepEqual(t, wantLeafs_1, leafs)

	// case: no candidates in optSpine - fail
	optSpines_2 := []gwatCommon.HashArray{
		{{'f', '0'}}, // finalization
		{{'f', '1'}}, // finalization
		{{'f', '2'}}, // finalization
		{{'p', '0'}}, // prefix
		{{'p', '1'}}, // prefix
		{{'p', '2'}}, // prefix
	}

	wantRootIndexMap_2 := map[[32]byte]uint64{}
	wantLeafs_2 := map[[32]byte]int{}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_2, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_2, rootIndexMap)
	require.DeepEqual(t, wantLeafs_2, leafs)

	// case: no candidates in spines and optSpine - ok
	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{{'p', '0'}, {'p', '1'}, {'p', '2'}},
			finalization: gwatCommon.HashArray{{'f', '0'}, {'f', '1'}, {'f', '2'}},
			cpFinalized:  gwatCommon.HashArray{{'c', 'p', 'f'}}, // terminal spine
		}},
	}

	optSpines_3 := []gwatCommon.HashArray{
		{{'f', '0'}}, // finalization
		{{'f', '1'}}, // finalization
		{{'f', '2'}}, // finalization
		{{'p', '0'}}, // prefix
		{{'p', '1'}}, // prefix
		{{'p', '2'}}, // prefix
	}

	wantRootIndexMap_3 := map[[32]byte]uint64{
		nrToHash(0): 0,
	}
	wantLeafs_3 := map[[32]byte]int{
		nrToHash(0): 1,
	}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_3, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_3, rootIndexMap)
	require.DeepEqual(t, wantLeafs_3, leafs)
}

// excluding forks with leafs before justified checkpoint.
func Test_collectTgTreeNodesByOptimisticSpines_jCpRoot(t *testing.T) {
	memoFcTgTreeForkSlot := params.BeaconConfig().FcTgTreeForkSlot
	params.BeaconConfig().FcTgTreeForkSlot = 0
	defer func() {
		params.BeaconConfig().FcTgTreeForkSlot = memoFcTgTreeForkSlot
	}()

	f := &ForkChoice{store: &Store{}}
	f.store.canonicalNodes = map[[32]byte]bool{}
	f.store.nodesIndices = map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
		nrToHash(8): 8,
		nrToHash(9): 9,
	}

	// Forks:
	//                 	0 << justified (case 0)
	//				   	|
	//					1 - 4 - 5 << fork 1
	//                  |
	//                  2 << justified (case 1)
	//                 / \
	//      fork 0 >> 3   6 - 7 << fork 2

	f.store.nodes = []*Node{
		{slot: 0, root: nrToHash(0), parent: NonExistentNode, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 1, root: nrToHash(1), parent: 0, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 2, root: nrToHash(2), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			prefix:       gwatCommon.HashArray{},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 3, root: nrToHash(3), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 1
		{slot: 4, root: nrToHash(4), parent: 1, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 5, root: nrToHash(5), parent: 4, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		//fork 2
		{slot: 6, root: nrToHash(6), parent: 2, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '4'}, {'a', '5'}, {'a', '6'}},
			prefix:       gwatCommon.HashArray{{'a', '2'}, {'a', '3'}, {'a', '4'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
		{slot: 7, root: nrToHash(7), parent: 6, spinesData: &SpinesData{
			spines:       gwatCommon.HashArray{{'a', '5'}, {'a', '6'}, {'a', '7'}},
			prefix:       gwatCommon.HashArray{{'a', '3'}, {'a', '4'}, {'a', '5'}},
			finalization: gwatCommon.HashArray{{'a', '1'}},
			cpFinalized:  gwatCommon.HashArray{{'x', 'x', 'x'}},
		}},
	}

	// case 0: include all forks
	optSpines_0 := []gwatCommon.HashArray{
		{{'a', '1'}},
		{{'a', '2'}},
		{{'a', '3'}},
		{{'a', '4'}},
		{{'a', '5'}},
		{{'a', '6'}},
		{{'a', '7'}},
		{{'a', '8'}},
		{{'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap_0 := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		nrToHash(4): 4,
		nrToHash(5): 5,
		nrToHash(6): 6,
		nrToHash(7): 7,
	}
	wantLeafs_0 := map[[32]byte]int{
		nrToHash(3): 4,
		nrToHash(5): 4,
		nrToHash(7): 5,
	}

	rootIndexMap, leafs := collectTgTreeNodesByOptimisticSpines(f, optSpines_0, nrToHash(0))
	require.DeepEqual(t, wantRootIndexMap_0, rootIndexMap)
	require.DeepEqual(t, wantLeafs_0, leafs)

	// case 1: include all forks
	optSpines_1 := []gwatCommon.HashArray{
		{{'a', '1'}},
		{{'a', '2'}},
		{{'a', '3'}},
		{{'a', '4'}},
		{{'a', '5'}},
		{{'a', '6'}},
		{{'a', '7'}},
		{{'a', '8'}},
		{{'a', '9'}},
		{{'a', '1', '0'}},
	}

	wantRootIndexMap_1 := map[[32]byte]uint64{
		nrToHash(0): 0,
		nrToHash(1): 1,
		nrToHash(2): 2,
		nrToHash(3): 3,
		//nrToHash(4): 4, // excluded node
		//nrToHash(5): 5, // excluded node
		nrToHash(6): 6,
		nrToHash(7): 7,
	}
	wantLeafs_1 := map[[32]byte]int{
		nrToHash(3): 4,
		//nrToHash(5): 4, // excluded fork
		nrToHash(7): 5,
	}

	rootIndexMap, leafs = collectTgTreeNodesByOptimisticSpines(f, optSpines_1, nrToHash(2))
	require.DeepEqual(t, wantRootIndexMap_1, rootIndexMap)
	require.DeepEqual(t, wantLeafs_1, leafs)
}
