package blockchain

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	types "github.com/prysmaticlabs/eth2-types"
	"github.com/sirupsen/logrus"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/core/helpers"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/beacon-chain/state"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/encoding/bytesutil"
	mathutil "gitlab.waterfall.network/waterfall/protocol/coordinator/math"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/monitoring/tracing"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1/block"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/time/slots"
	"go.opencensus.io/trace"
)

// CurrentSlot returns the current slot based on time.
func (s *Service) CurrentSlot() types.Slot {
	return slots.CurrentSlot(uint64(s.genesisTime.Unix()))
}

// getBlockPreState returns the pre state of an incoming block. It uses the parent root of the block
// to retrieve the state in DB. It verifies the pre state's validity and the incoming block
// is in the correct time window.
func (s *Service) getBlockPreState(ctx context.Context, b block.BeaconBlock) (state.BeaconState, error) {
	ctx, span := trace.StartSpan(ctx, "blockChain.getBlockPreState")
	defer span.End()

	// Verify incoming block has a valid pre state.
	if err := s.verifyBlkPreState(ctx, b); err != nil {
		return nil, err
	}

	preState, err := s.cfg.StateGen.StateByRoot(ctx, bytesutil.ToBytes32(b.ParentRoot()))
	if err != nil {
		return nil, errors.Wrapf(err, "could not get pre state for slot %d", b.Slot())
	}
	if preState == nil || preState.IsNil() {
		return nil, errors.Wrapf(err, "nil pre state for slot %d", b.Slot())
	}

	// Verify block slot time is not from the future.
	if err := slots.VerifyTime(uint64(s.genesisTime.Unix()), b.Slot(), params.BeaconNetworkConfig().MaximumGossipClockDisparity); err != nil {
		return nil, err
	}

	// Verify block is later than the finalized epoch slot.
	if err := s.verifyBlkFinalizedSlot(b); err != nil {
		return nil, err
	}

	return preState, nil
}

// verifyBlkPreState validates input block has a valid pre-state.
func (s *Service) verifyBlkPreState(ctx context.Context, b block.BeaconBlock) error {
	ctx, span := trace.StartSpan(ctx, "blockChain.verifyBlkPreState")
	defer span.End()

	parentRoot := bytesutil.ToBytes32(b.ParentRoot())
	// Loosen the check to HasBlock because state summary gets saved in batches
	// during initial syncing. There's no risk given a state summary object is just a
	// a subset of the block object.
	if !s.cfg.BeaconDB.HasStateSummary(ctx, parentRoot) && !s.cfg.BeaconDB.HasBlock(ctx, parentRoot) {
		return errors.New("could not reconstruct parent state")
	}

	if err := s.VerifyBlkDescendant(ctx, bytesutil.ToBytes32(b.ParentRoot())); err != nil {
		return err
	}

	has, err := s.cfg.StateGen.HasState(ctx, parentRoot)
	if err != nil {
		return err
	}
	if !has {
		if err := s.cfg.BeaconDB.SaveBlocks(ctx, s.getInitSyncBlocks()); err != nil {
			return errors.Wrap(err, "could not save initial sync blocks")
		}
		s.clearInitSyncBlocks()
	}
	return nil
}

// VerifyBlkDescendant validates input block root is a descendant of the
// current finalized block root.
func (s *Service) VerifyBlkDescendant(ctx context.Context, root [32]byte) error {
	ctx, span := trace.StartSpan(ctx, "blockChain.VerifyBlkDescendant")
	defer span.End()
	finalized := s.store.FinalizedCheckpt()
	if finalized == nil {
		return errNilFinalizedInStore
	}
	fRoot := s.ensureRootNotZeros(bytesutil.ToBytes32(finalized.Root))
	finalizedBlkSigned, err := s.cfg.BeaconDB.Block(ctx, fRoot)
	if err != nil {
		return err
	}
	if finalizedBlkSigned == nil || finalizedBlkSigned.IsNil() || finalizedBlkSigned.Block().IsNil() {
		return errors.New("nil finalized block")
	}
	finalizedBlk := finalizedBlkSigned.Block()
	bFinalizedRoot, err := s.ancestor(ctx, root[:], finalizedBlk.Slot())
	if err != nil {
		return errors.Wrap(err, "could not get finalized block root")
	}
	if bFinalizedRoot == nil {
		return fmt.Errorf("no finalized block known for block %#x", bytesutil.Trunc(root[:]))
	}

	if !bytes.Equal(bFinalizedRoot, fRoot[:]) {
		err := fmt.Errorf("block %#x is not a descendant of the current finalized block slot %d, %#x != %#x",
			bytesutil.Trunc(root[:]), finalizedBlk.Slot(), bytesutil.Trunc(bFinalizedRoot),
			bytesutil.Trunc(fRoot[:]))
		tracing.AnnotateError(span, err)
		return err
	}
	return nil
}

// verifyBlkFinalizedSlot validates input block is not less than or equal
// to current finalized slot.
func (s *Service) verifyBlkFinalizedSlot(b block.BeaconBlock) error {
	finalized := s.store.FinalizedCheckpt()
	if finalized == nil {
		return errNilFinalizedInStore
	}
	finalizedSlot, err := slots.EpochStart(finalized.Epoch)
	if err != nil {
		return err
	}
	if finalizedSlot >= b.Slot() {
		return fmt.Errorf("block is equal or earlier than finalized block, slot %d < slot %d", b.Slot(), finalizedSlot)
	}
	return nil
}

// shouldUpdateCurrentJustified prevents bouncing attack, by only update conflicting justified
// checkpoints in the fork choice if in the early slots of the epoch.
// Otherwise, delay incorporation of new justified checkpoint until next epoch boundary.
//
// Spec code:
// def should_update_justified_checkpoint(store: Store, new_justified_checkpoint: Checkpoint) -> bool:
//
//	"""
//	To address the bouncing attack, only update conflicting justified
//	checkpoints in the fork choice if in the early slots of the epoch.
//	Otherwise, delay incorporation of new justified checkpoint until next epoch boundary.
//
//	See https://ethresear.ch/t/prevention-of-bouncing-attack-on-ffg/6114 for more detailed analysis and discussion.
//	"""
//	if compute_slots_since_epoch_start(get_current_slot(store)) < SAFE_SLOTS_TO_UPDATE_JUSTIFIED:
//	    return True
//
//	justified_slot = compute_start_slot_at_epoch(store.justified_checkpoint.epoch)
//	if not get_ancestor(store, new_justified_checkpoint.root, justified_slot) == store.justified_checkpoint.root:
//	    return False
//
//	return True
func (s *Service) shouldUpdateCurrentJustified(ctx context.Context, newJustifiedCheckpt *ethpb.Checkpoint) (bool, error) {
	ctx, span := trace.StartSpan(ctx, "blockChain.shouldUpdateCurrentJustified")
	defer span.End()

	if slots.SinceEpochStarts(s.CurrentSlot()) < params.BeaconConfig().SafeSlotsToUpdateJustified {
		return true, nil
	}
	justified := s.store.JustifiedCheckpt()
	jSlot, err := slots.EpochStart(justified.Epoch)
	if err != nil {
		return false, err
	}
	justifiedRoot := s.ensureRootNotZeros(bytesutil.ToBytes32(newJustifiedCheckpt.Root))
	b, err := s.ancestor(ctx, justifiedRoot[:], jSlot)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(b, justified.Root) {
		return false, nil
	}

	return true, nil
}

func (s *Service) updateJustified(ctx context.Context, state state.ReadOnlyBeaconState) error {
	ctx, span := trace.StartSpan(ctx, "blockChain.updateJustified")
	defer span.End()

	cpt := state.CurrentJustifiedCheckpoint()
	bestJustified := s.store.BestJustifiedCheckpt()
	if bestJustified == nil {
		return errNilBestJustifiedInStore
	}
	if cpt.Epoch > bestJustified.Epoch {
		s.store.SetBestJustifiedCheckpt(cpt)
	}
	canUpdate, err := s.shouldUpdateCurrentJustified(ctx, cpt)
	if err != nil {
		return err
	}

	if canUpdate {
		justified := s.store.JustifiedCheckpt()
		if justified == nil {
			return errNilJustifiedInStore
		}
		s.store.SetPrevJustifiedCheckpt(justified)
		s.store.SetJustifiedCheckpt(cpt)
	}

	return nil
}

// This caches input checkpoint as justified for the service struct. It rotates current justified to previous justified,
// caches justified checkpoint balances for fork choice and save justified checkpoint in DB.
// This method does not have defense against fork choice bouncing attack, which is why it's only recommend to be used during initial syncing.
func (s *Service) updateJustifiedInitSync(ctx context.Context, cp *ethpb.Checkpoint) error {
	justified := s.store.JustifiedCheckpt()
	if justified == nil {
		return errNilJustifiedInStore
	}
	s.store.SetPrevJustifiedCheckpt(justified)

	if err := s.cfg.BeaconDB.SaveJustifiedCheckpoint(ctx, cp); err != nil {
		return err
	}
	s.store.SetJustifiedCheckpt(cp)

	return nil
}

func (s *Service) updateFinalized(ctx context.Context, cp *ethpb.Checkpoint) error {
	ctx, span := trace.StartSpan(ctx, "blockChain.updateFinalized")
	defer span.End()

	// Blocks need to be saved so that we can retrieve finalized block from
	// DB when migrating states.
	if err := s.cfg.BeaconDB.SaveBlocks(ctx, s.getInitSyncBlocks()); err != nil {
		return err
	}
	s.clearInitSyncBlocks()

	if err := s.cfg.BeaconDB.SaveFinalizedCheckpoint(ctx, cp); err != nil {
		return err
	}

	fRoot := bytesutil.ToBytes32(cp.Root)
	optimistic, err := s.cfg.ForkChoiceStore.IsOptimistic(fRoot)
	if err != nil {
		return err
	}
	if !optimistic {
		err = s.cfg.BeaconDB.SaveLastValidatedCheckpoint(ctx, cp)
		if err != nil {
			return err
		}
	}
	go func() {
		if err := s.cfg.StateGen.MigrateToCold(s.ctx, fRoot); err != nil {
			log.WithError(err).Error("could not migrate to cold")
		}
	}()
	return nil
}

// ancestor returns the block root of an ancestry block from the input block root.
//
// Spec pseudocode definition:
//
//	def get_ancestor(store: Store, root: Root, slot: Slot) -> Root:
//	 block = store.blocks[root]
//	 if block.slot > slot:
//	     return get_ancestor(store, block.parent_root, slot)
//	 elif block.slot == slot:
//	     return root
//	 else:
//	     # root is older than queried slot, thus a skip slot. Return most recent root prior to slot
//	     return root
func (s *Service) ancestor(ctx context.Context, root []byte, slot types.Slot) ([]byte, error) {
	ctx, span := trace.StartSpan(ctx, "blockChain.ancestor")
	defer span.End()

	r := bytesutil.ToBytes32(root)
	// Get ancestor root from fork choice store instead of recursively looking up blocks in DB.
	// This is most optimal outcome.
	ar, err := s.ancestorByForkChoiceStore(ctx, r, slot)
	if err != nil {
		// Try getting ancestor root from DB when failed to retrieve from fork choice store.
		// This is the second line of defense for retrieving ancestor root.
		ar, err = s.ancestorByDB(ctx, r, slot)
		if err != nil {
			return nil, err
		}
	}

	return ar, nil
}

// This retrieves an ancestor root using fork choice store. The look up is looping through the a flat array structure.
func (s *Service) ancestorByForkChoiceStore(ctx context.Context, r [32]byte, slot types.Slot) ([]byte, error) {
	ctx, span := trace.StartSpan(ctx, "blockChain.ancestorByForkChoiceStore")
	defer span.End()

	if !s.cfg.ForkChoiceStore.HasParent(r) {
		return nil, errors.New("could not find root in fork choice store")
	}
	return s.cfg.ForkChoiceStore.AncestorRoot(ctx, r, slot)
}

// This retrieves an ancestor root using DB. The look up is recursively looking up DB. Slower than `ancestorByForkChoiceStore`.
func (s *Service) ancestorByDB(ctx context.Context, r [32]byte, slot types.Slot) ([]byte, error) {
	ctx, span := trace.StartSpan(ctx, "blockChain.ancestorByDB")
	defer span.End()

	// Stop recursive ancestry lookup if context is cancelled.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	signed, err := s.getBlock(ctx, r)
	if err != nil {
		return nil, err
	}
	b := signed.Block()
	if b.Slot() == slot || b.Slot() < slot {
		return r[:], nil
	}

	return s.ancestorByDB(ctx, bytesutil.ToBytes32(b.ParentRoot()), slot)
}

// This retrieves missing blocks from DB (ie. the blocks that couldn't be received over sync) and inserts them to fork choice store.
// This is useful for block tree visualizer and additional vote accounting.
func (s *Service) fillInForkChoiceMissingBlocks(ctx context.Context, blk block.BeaconBlock,
	fCheckpoint, jCheckpoint *ethpb.Checkpoint) error {
	pendingNodes := make([]block.BeaconBlock, 0)
	pendingRoots := make([][32]byte, 0)

	parentRoot := bytesutil.ToBytes32(blk.ParentRoot())
	slot := blk.Slot()
	// Fork choice only matters from last finalized slot.
	finalized := s.store.FinalizedCheckpt()
	if finalized == nil {
		return errNilFinalizedInStore
	}
	fSlot, err := slots.EpochStart(finalized.Epoch)
	if err != nil {
		return err
	}
	higherThanFinalized := slot > fSlot
	// As long as parent node is not in fork choice store, and parent node is in DB.
	for !s.cfg.ForkChoiceStore.HasNode(parentRoot) && s.cfg.BeaconDB.HasBlock(ctx, parentRoot) && higherThanFinalized {
		b, err := s.cfg.BeaconDB.Block(ctx, parentRoot)
		if err != nil {
			return err
		}

		pendingNodes = append(pendingNodes, b.Block())
		copiedRoot := parentRoot
		pendingRoots = append(pendingRoots, copiedRoot)
		parentRoot = bytesutil.ToBytes32(b.Block().ParentRoot())
		slot = b.Block().Slot()
		higherThanFinalized = slot > fSlot
	}

	// Insert parent nodes to fork choice store in reverse order.
	// Lower slots should be at the end of the list.
	for i := len(pendingNodes) - 1; i >= 0; i-- {
		b := pendingNodes[i]
		r := pendingRoots[i]

		st, err := s.cfg.StateGen.StateByRoot(ctx, pendingRoots[i])
		if err != nil {
			return err
		}

		if err := s.cfg.ForkChoiceStore.InsertOptimisticBlock(ctx,
			b.Slot(), r, bytesutil.ToBytes32(b.ParentRoot()),
			jCheckpoint.Epoch,
			fCheckpoint.Epoch,
			jCheckpoint.Root,
			fCheckpoint.Root,
			st.SpineData(),
		); err != nil {
			return errors.Wrap(err, "could not process block for proto array fork choice")
		}
	}
	return nil
}

// inserts finalized deposits into our finalized deposit trie.
func (s *Service) insertFinalizedDeposits(ctx context.Context, fRoot [32]byte) error {
	ctx, span := trace.StartSpan(ctx, "blockChain.insertFinalizedDeposits")
	defer span.End()

	// Update deposit cache.
	finalizedState, err := s.cfg.StateGen.StateByRoot(ctx, fRoot)
	if err != nil {
		return errors.Wrap(err, "could not fetch finalized state")
	}
	// We update the cache up to the last deposit index in the finalized block's state.
	// We can be confident that these deposits will be included in some block
	// because the Eth1 follow distance makes such long-range reorgs extremely unlikely.
	eth1DepositIndex, err := mathutil.Int(finalizedState.Eth1DepositIndex())
	if err != nil {
		return errors.Wrap(err, "could not cast eth1 deposit index")
	}
	// The deposit index in the state is always the index of the next deposit
	// to be included(rather than the last one to be processed). This was most likely
	// done as the state cannot represent signed integers.
	eth1DepositIndex -= 1
	s.cfg.DepositCache.InsertFinalizedDeposits(ctx, int64(eth1DepositIndex))
	// Deposit proofs are only used during state transition and can be safely removed to save space.
	if err = s.cfg.DepositCache.PruneProofs(ctx, int64(eth1DepositIndex)); err != nil {
		return errors.Wrap(err, "could not prune deposit proofs")
	}
	return nil
}

// The deletes input attestations from the attestation pool, so proposers don't include them in a block for the future.
func (s *Service) deletePoolAtts(atts []*ethpb.Attestation) error {
	for _, att := range atts {
		if helpers.IsAggregated(att) {
			if err := s.cfg.AttPool.DeleteAggregatedAttestation(att); err != nil {
				return err
			}
		} else {
			if err := s.cfg.AttPool.DeleteUnaggregatedAttestation(att); err != nil {
				return err
			}
		}
	}

	return nil
}

// This ensures that the input root defaults to using genesis root instead of zero hashes. This is needed for handling
// fork choice justification routine.
func (s *Service) ensureRootNotZeros(root [32]byte) [32]byte {
	if root == params.BeaconConfig().ZeroHash {
		return s.originBlockRoot
	}
	return root
}

// verifyBlkSyncOps validates SyncOps included to block:
// 1. Validate by op pool: if is valide - break.
// 2. Validate by parent state: if parentState contains SyncOp - block is invalid.
// 3. Validate by leaves' states: if any state of leaves contains SyncOp - block is valid.
func (s *Service) verifyBlkSyncOps(ctx context.Context, block block.BeaconBlock, preState state.BeaconState) error {
	validate := !s.IsGwatSynchronizing() &&
		!s.isSynchronizing() &&
		params.BeaconConfig().IsDelegatingStakeSlot(block.Slot()) &&
		s.IsValOpPoolValid() &&
		params.BeaconConfig().IsValOpVerifyForkSlot(block.Slot())
	if !validate {
		log.WithFields(logrus.Fields{
			"slot":                  block.Slot(),
			"IsGwatSynchronizing":   s.IsGwatSynchronizing(),
			"isSynchronizing":       s.isSynchronizing(),
			"IsDelegatingStakeSlot": params.BeaconConfig().IsDelegatingStakeSlot(block.Slot()),
			"IsValOpPoolValid":      s.IsValOpPoolValid(),
			"IsValOpVerifyForkSlot": params.BeaconConfig().IsValOpVerifyForkSlot(block.Slot()),
		}).Info("onBlock: valSyncOp: skip validation")
		return nil
	}

	//1. Validate by op pool: if is valide - break.
	notFoundOpsWth, err := s.verifyWithdrawalsInPool(block)
	if err != nil {
		return err
	}
	notFoundOpsExit, err := s.verifyExitsInPool(block)
	if err != nil {
		return err
	}
	if len(notFoundOpsWth) == 0 && len(notFoundOpsExit) == 0 {
		return nil
	}

	// 2. Validate by parent state: if parentState contains SyncOp - block is invalid.
	err = s.checkAnyWithdrawalsInParentState(preState, notFoundOpsWth)
	if err != nil {
		return err
	}
	err = s.checkAnyExitsInParentState(preState, notFoundOpsExit)
	if err != nil {
		return err
	}

	//3. Validate by leaves' states: if any state of leaves contains SyncOp - block is valid.
	roots, slots := s.ForkChoicer().Tips()
	for i, root := range roots {
		if root == params.BeaconConfig().ZeroHash {
			continue
		}
		// skip parent state
		if bytes.Equal(root[:], block.ParentRoot()) {
			continue
		}
		//fetch state of leaf
		st, err := s.cfg.StateGen.StateByRoot(ctx, root)
		if err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"slot": slots[i],
				"root": fmt.Sprintf("%#x", root),
			}).Warn("onBlock: valSyncOp: fetch leaf state failed")
			continue
		}
		notFoundOpsWth, err = s.verifyWithdrawalsInLeafState(st, notFoundOpsWth)
		if err != nil {
			return err
		}
		notFoundOpsExit, err = s.verifyExitsInLeafState(st, notFoundOpsExit)
		if err != nil {
			return err
		}
		if len(notFoundOpsWth) == 0 && len(notFoundOpsExit) == 0 {
			return nil
		}
	}
	return fmt.Errorf("valSyncOp not found in leaves states")
}

func (s *Service) verifyWithdrawalsInPool(block block.BeaconBlock) (notFoundOps []*ethpb.Withdrawal, err error) {
	notFoundOps = make([]*ethpb.Withdrawal, 0, len(block.Body().Withdrawals()))
	if len(block.Body().Withdrawals()) == 0 {
		return notFoundOps, nil
	}
	for i, itm := range block.Body().Withdrawals() {
		if err = s.cfg.WithdrawalPool.Verify(itm); err != nil {
			if err.Error() == "not found" {
				log.WithError(err).WithFields(logrus.Fields{
					"i":              i,
					"slot":           block.Slot(),
					"Amount":         fmt.Sprintf("%d", itm.Amount),
					"Epoch":          fmt.Sprintf("%d", itm.Epoch),
					"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
					"PublicKey":      fmt.Sprintf("%#x", itm.PublicKey),
					"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
				}).Warn("onBlock: valSyncOp: withdrawal not found")
				notFoundOps = append(notFoundOps, itm)
				continue
			}
			log.WithError(err).WithFields(logrus.Fields{
				"i":              i,
				"slot":           block.Slot(),
				"Amount":         fmt.Sprintf("%d", itm.Amount),
				"Epoch":          fmt.Sprintf("%d", itm.Epoch),
				"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
				"PublicKey":      fmt.Sprintf("%#x", itm.PublicKey),
				"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
			}).Error("onBlock: valSyncOp: withdrawal is invalid")
			return nil, err
		}
		log.WithFields(logrus.Fields{
			"i":              i,
			"slot":           block.Slot(),
			"Amount":         fmt.Sprintf("%d", itm.Amount),
			"Epoch":          fmt.Sprintf("%d", itm.Epoch),
			"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
			"PublicKey":      fmt.Sprintf("%#x", itm.PublicKey),
			"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
		}).Info("onBlock: valSyncOp: withdrawal is valid")
	}
	return notFoundOps, nil
}

func (s *Service) verifyExitsInPool(block block.BeaconBlock) (notFoundOps []*ethpb.VoluntaryExit, err error) {
	notFoundOps = make([]*ethpb.VoluntaryExit, 0, len(block.Body().VoluntaryExits()))
	if len(block.Body().VoluntaryExits()) == 0 {
		return notFoundOps, nil
	}
	for i, itm := range block.Body().VoluntaryExits() {
		if err = s.cfg.ExitPool.Verify(itm); err != nil {
			if err.Error() == "not found" {
				log.WithError(err).WithFields(logrus.Fields{
					"i":              i,
					"slot":           block.Slot(),
					"Epoch":          fmt.Sprintf("%d", itm.Epoch),
					"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
					"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
				}).Warn("onBlock: valSyncOp: exit not found")
				notFoundOps = append(notFoundOps, itm)
				continue
			}
			log.WithError(err).WithFields(logrus.Fields{
				"i":              i,
				"slot":           block.Slot(),
				"Epoch":          fmt.Sprintf("%d", itm.Epoch),
				"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
				"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
			}).Error("onBlock: valSyncOp: exit is invalid")
			return nil, err
		}
		log.WithFields(logrus.Fields{
			"i":              i,
			"slot":           block.Slot(),
			"Epoch":          fmt.Sprintf("%d", itm.Epoch),
			"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
			"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
		}).Info("onBlock: valSyncOp: exit is valid")
	}
	return notFoundOps, nil
}

func (s *Service) checkAnyWithdrawalsInParentState(preState state.BeaconState, ops []*ethpb.Withdrawal) error {
	if len(ops) == 0 {
		return nil
	}
	valsNr := preState.NumValidators()
	for _, itm := range ops {
		if int(itm.ValidatorIndex) >= valsNr {
			continue
		}
		validator, err := preState.ValidatorAtIndex(itm.ValidatorIndex)
		if err != nil {
			return err
		}
		for _, wop := range validator.WithdrawalOps {
			if bytes.Equal(wop.Hash, itm.InitTxHash) {
				return fmt.Errorf("valSyncOp exists in parent state op=withdrawal initTx=%#x", itm.InitTxHash)
			}
		}
	}
	return nil
}

func (s *Service) checkAnyExitsInParentState(preState state.BeaconState, ops []*ethpb.VoluntaryExit) error {
	if len(ops) == 0 {
		return nil
	}
	valsNr := preState.NumValidators()
	for _, itm := range ops {
		if int(itm.ValidatorIndex) >= valsNr {
			continue
		}
		validator, err := preState.ValidatorAtIndex(itm.ValidatorIndex)
		if err != nil {
			return err
		}
		if bytesutil.ToBytes32(validator.ExitHash) != [32]byte{} {
			return fmt.Errorf("valSyncOp exists in parent state op=exit initTx=%#x", itm.InitTxHash)
		}
	}
	return nil
}

func (s *Service) verifyWithdrawalsInLeafState(leafSt state.BeaconState, ops []*ethpb.Withdrawal) (notFoundOps []*ethpb.Withdrawal, err error) {
	notFoundOps = make([]*ethpb.Withdrawal, 0, len(ops))
	if len(ops) == 0 {
		return notFoundOps, nil
	}
	valsNr := leafSt.NumValidators()
	for _, itm := range ops {
		if int(itm.ValidatorIndex) >= valsNr {
			notFoundOps = append(notFoundOps, itm)
			continue
		}
		isValid := false
		validator, err := leafSt.ValidatorAtIndex(itm.ValidatorIndex)
		if err != nil {
			return nil, err
		}
		for _, wop := range validator.WithdrawalOps {
			if bytes.Equal(wop.Hash, itm.InitTxHash) {
				//validate op data
				if !bytes.Equal(validator.PublicKey, itm.PublicKey) {
					log.WithFields(logrus.Fields{
						"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
						"stPublicKey":    fmt.Sprintf("%#x", validator.PublicKey),
						"opPublicKey":    fmt.Sprintf("%#x", itm.PublicKey),
						"opAmount":       fmt.Sprintf("%d", itm.Amount),
						"opEpoch":        fmt.Sprintf("%d", itm.Epoch),
						"opInitTxHash":   fmt.Sprintf("%#x", itm.InitTxHash),
					}).Error("onBlock: valSyncOp: withdrawal PublicKey mismatch with leaf state")
					return nil, fmt.Errorf("valSyncOp PublicKey missmatch with leaf state op=withdrawal initTx=%#x", itm.InitTxHash)
				}
				if wop.Amount != itm.Amount {
					log.WithFields(logrus.Fields{
						"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
						"stOpAmount":     fmt.Sprintf("%d", wop.Amount),
						"opAmount":       fmt.Sprintf("%d", itm.Amount),
						"opPublicKey":    fmt.Sprintf("%#x", itm.PublicKey),
						"opEpoch":        fmt.Sprintf("%d", itm.Epoch),
						"opInitTxHash":   fmt.Sprintf("%#x", itm.InitTxHash),
					}).Error("onBlock: valSyncOp: withdrawal Amount mismatch with leaf state")
					return nil, fmt.Errorf("valSyncOp Amount missmatch with leaf state op=withdrawal initTx=%#x", itm.InitTxHash)
				}
				log.WithFields(logrus.Fields{
					"stSlot":         leafSt.Slot(),
					"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
					"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
					"Amount":         fmt.Sprintf("%d", itm.Amount),
					"Epoch":          fmt.Sprintf("%d", itm.Epoch),
					"PublicKey":      fmt.Sprintf("%#x", itm.PublicKey),
				}).Info("onBlock: valSyncOp: withdrawal is valid (by leaf state)")
				isValid = true
				break
			}
		}
		if !isValid {
			notFoundOps = append(notFoundOps, itm)
		}
	}
	return notFoundOps, nil
}

func (s *Service) verifyExitsInLeafState(leafSt state.BeaconState, ops []*ethpb.VoluntaryExit) (notFoundOps []*ethpb.VoluntaryExit, err error) {
	notFoundOps = make([]*ethpb.VoluntaryExit, 0, len(ops))
	if len(ops) == 0 {
		return notFoundOps, nil
	}
	valsNr := leafSt.NumValidators()
	for _, itm := range ops {
		if int(itm.ValidatorIndex) >= valsNr {
			notFoundOps = append(notFoundOps, itm)
			continue
		}
		validator, err := leafSt.ValidatorAtIndex(itm.ValidatorIndex)
		if err != nil {
			return nil, err
		}
		//validate op data
		if bytes.Equal(validator.ExitHash, itm.InitTxHash) {
			log.WithFields(logrus.Fields{
				"stSlot":         leafSt.Slot(),
				"InitTxHash":     fmt.Sprintf("%#x", itm.InitTxHash),
				"Epoch":          fmt.Sprintf("%d", itm.Epoch),
				"ValidatorIndex": fmt.Sprintf("%d", itm.ValidatorIndex),
			}).Info("onBlock: valSyncOp: exit is valid (by leaf state)")
			continue
		}
		notFoundOps = append(notFoundOps, itm)
	}
	return notFoundOps, nil
}
