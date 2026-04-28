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
	"context"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/forkchoice/protoarray"
	gwatCommon "gitlab.waterfall.network/waterfall/protocol/gwat/common"
	wfcommon "gitlab.waterfall.network/waterfall/protocol/wf-types/common"
	wfiface "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/iface"
	wftypes "gitlab.waterfall.network/waterfall/protocol/wf-types/coordinator/types"
)

var (
	_ wfiface.ForkChoice     = (*ForkChoiceAdapter)(nil)
	_ wfiface.ForkChoiceFork = (*ForkAdapter)(nil)
	_ wfiface.ForkChoiceNode = (*NodeAdapter)(nil)
)

// ForkChoiceAdapter adapts protoarray.ForkChoice to iface.ForkChoice.
type ForkChoiceAdapter struct {
	inner *protoarray.ForkChoice
}

// WrapForkChoice returns a ForkChoiceAdapter for the given protoarray ForkChoice.
func WrapForkChoice(fc *protoarray.ForkChoice) *ForkChoiceAdapter {
	return &ForkChoiceAdapter{inner: fc}
}

func (a *ForkChoiceAdapter) HasNode(root [32]byte) bool {
	return a.inner.HasNode(root)
}

func (a *ForkChoiceAdapter) GetForks() []wfiface.ForkChoiceFork {
	forks := a.inner.GetForks()
	res := make([]wfiface.ForkChoiceFork, len(forks))
	for i, f := range forks {
		res[i] = &ForkAdapter{inner: f}
	}
	return res
}

func (a *ForkChoiceAdapter) HeadBySubset(ctx context.Context, acceptableRoots map[[32]byte]struct{}, jCpRoot [32]byte) ([32]byte, error) {
	return a.inner.HeadBySubset(ctx, acceptableRoots, jCpRoot)
}

// ForkAdapter adapts protoarray.Fork to iface.ForkChoiceFork.
type ForkAdapter struct {
	inner *protoarray.Fork
}

func (a *ForkAdapter) Roots() [][32]byte {
	return a.inner.Roots()
}

func (a *ForkAdapter) NodeByRoot(root [32]byte) wfiface.ForkChoiceNode {
	n := a.inner.NodeByRoot(root)
	if n == nil {
		return nil
	}
	return &NodeAdapter{inner: n}
}

// NodeAdapter adapts protoarray.Node to iface.ForkChoiceNode.
type NodeAdapter struct {
	inner *protoarray.Node
}

func (a *NodeAdapter) Root() [32]byte {
	return a.inner.Root()
}

func (a *NodeAdapter) Slot() wftypes.Slot {
	return wftypes.Slot(a.inner.Slot())
}

func (a *NodeAdapter) SpinesFinalized() wfcommon.HashArray {
	sd := a.inner.SpinesData()
	if sd == nil {
		return nil
	}
	return gwatHashArrayToWF(sd.CpFinalized())
}

func (a *NodeAdapter) SpinesFinalization() wfcommon.HashArray {
	sd := a.inner.SpinesData()
	if sd == nil {
		return nil
	}
	return gwatHashArrayToWF(sd.Finalization())
}

func (a *NodeAdapter) SpinesPrefix() wfcommon.HashArray {
	sd := a.inner.SpinesData()
	if sd == nil {
		return nil
	}
	return gwatHashArrayToWF(sd.Prefix())
}

func (a *NodeAdapter) SpinesPublished() wfcommon.HashArray {
	sd := a.inner.SpinesData()
	if sd == nil {
		return nil
	}
	return gwatHashArrayToWF(sd.Spines())
}

func gwatHashArrayToWF(ha gwatCommon.HashArray) wfcommon.HashArray {
	if ha == nil {
		return nil
	}
	res := make(wfcommon.HashArray, len(ha))
	for i, h := range ha {
		res[i] = wfcommon.Hash(h)
	}
	return res
}
