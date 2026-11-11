package slots

import (
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/pkg/errors"
	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	prysmTime "gitlab.waterfall.network/waterfall/protocol/coordinator/time"
	commonMath "gitlab.waterfall.network/waterfall/protocol/gwat/common/math"
)

// MaxSlotBuffer specifies the max buffer given to slots from
// incoming objects. (24 mins with mainnet spec)
const MaxSlotBuffer = uint64(1 << 7)

// upSecondsPerSlotForkElapsed returns the number of seconds between genesis and the start of
// the UpSecondsPerSlotForkSlot slot. The second result is false if the slot duration fork is
// disabled or unreachable, in that case the whole chain is timed with the base SecondsPerSlot.
//
// It is expressed relative to genesis rather than as an absolute timestamp so that the whole
// conversion works on elapsed seconds and never has to cast a genesis time to uint64.
func upSecondsPerSlotForkElapsed() (uint64, bool) {
	cfg := params.BeaconConfig()
	if cfg.UpSecondsPerSlot == 0 {
		return 0, false
	}
	overflows, elapsed := bits.Mul64(uint64(cfg.UpSecondsPerSlotForkSlot), cfg.SecondsPerSlot)
	if overflows > 0 {
		return 0, false
	}
	return elapsed, true
}

// ElapsedSinceGenesis returns the number of seconds between genesis and the start of the given
// slot. Slots before UpSecondsPerSlotForkSlot are counted with SecondsPerSlot, the rest are
// counted from the fork with UpSecondsPerSlot, so the offsets of the slots preceding the fork
// stay the same as they were without the fork.
//
// It is the single source of the slot to time conversion, SlotByElapsedSinceGenesis is its inverse.
func ElapsedSinceGenesis(slot types.Slot) (uint64, error) {
	cfg := params.BeaconConfig()
	baseElapsed, baseSlot, secondsPerSlot := uint64(0), types.Slot(0), cfg.SecondsPerSlot
	if forkElapsed, ok := upSecondsPerSlotForkElapsed(); ok && slot >= cfg.UpSecondsPerSlotForkSlot {
		baseElapsed, baseSlot, secondsPerSlot = forkElapsed, cfg.UpSecondsPerSlotForkSlot, cfg.UpSecondsPerSlot
	}
	overflows, sinceBase := bits.Mul64(uint64(slot-baseSlot), secondsPerSlot)
	if overflows > 0 {
		return 0, errors.New("multiplication overflows")
	}
	elapsed, carry := bits.Add64(sinceBase, baseElapsed, 0)
	if carry > 0 {
		return 0, errors.New("addition overflows")
	}
	return elapsed, nil
}

// SlotByElapsedSinceGenesis returns the slot containing the moment elapsed seconds after
// genesis. It is the inverse of ElapsedSinceGenesis and takes the slot duration fork
// into account.
func SlotByElapsedSinceGenesis(elapsed uint64) types.Slot {
	cfg := params.BeaconConfig()
	forkElapsed, ok := upSecondsPerSlotForkElapsed()
	if !ok || elapsed < forkElapsed {
		return types.Slot(elapsed / cfg.SecondsPerSlot)
	}
	slot, carry := bits.Add64(uint64(cfg.UpSecondsPerSlotForkSlot), (elapsed-forkElapsed)/cfg.UpSecondsPerSlot, 0)
	if carry > 0 {
		return types.Slot(math.MaxUint64)
	}
	return types.Slot(slot)
}

// SlotStartTimestamp takes the given slot and genesis time to determine the unix time of the
// slot start, taking the slot duration fork into account.
func SlotStartTimestamp(genesisTimeSec uint64, slot types.Slot) (uint64, error) {
	elapsed, err := ElapsedSinceGenesis(slot)
	if err != nil {
		return 0, err
	}
	sTime, carry := bits.Add64(elapsed, genesisTimeSec, 0)
	if carry > 0 {
		return 0, errors.New("addition overflows")
	}
	return sTime, nil
}

// SlotByTimestamp returns the slot containing the given unix timestamp on a chain with the
// given genesis time. It is the inverse of SlotStartTimestamp.
func SlotByTimestamp(genesisTimeSec, timestamp uint64) types.Slot {
	if timestamp <= genesisTimeSec {
		return 0
	}
	return SlotByElapsedSinceGenesis(timestamp - genesisTimeSec)
}

// SecondsPerSlot returns the duration of the given slot in seconds.
func SecondsPerSlot(slot types.Slot) uint64 {
	return params.BeaconConfig().GetSecondsPerSlot(slot)
}

// StartTime returns the start time in terms of its unix epoch
// value.
func StartTime(genesis uint64, slot types.Slot) time.Time {
	startTime, err := ToTime(genesis, slot)
	if err != nil {
		return time.Unix(0, 0)
	}
	return startTime
}

// SinceGenesis returns the number of slots since
// the provided genesis time.
func SinceGenesis(genesis time.Time) types.Slot {
	now := prysmTime.Now()
	if genesis.After(now) { // Genesis has not occurred yet.
		return 0
	}
	// The difference is taken on signed seconds so that a genesis time before the unix epoch
	// (the zero time.Time used by tests) behaves the same way as it did before the fork.
	return SlotByElapsedSinceGenesis(uint64(now.Unix() - genesis.Unix()))
}

// EpochsSinceGenesis returns the number of slots since
// the provided genesis time.
func EpochsSinceGenesis(genesis time.Time) types.Epoch {
	return types.Epoch(SinceGenesis(genesis) / params.BeaconConfig().SlotsPerEpoch)
}

// DivideSlotBy divides the SECONDS_PER_SLOT configuration
// parameter by a specified number. It returns a value of time.Duration
// in milliseconds, useful for dividing values such as 1 second into
// millisecond-based durations.
//
// It is based on the slot duration before the UpSecondsPerSlotForkSlot fork. Use
// DivideSlotByAt wherever the intra-slot offset has to follow the fork.
func DivideSlotBy(timesPerSlot int64) time.Duration {
	return time.Duration(int64(params.BeaconConfig().SecondsPerSlot*1000)/timesPerSlot) * time.Millisecond
}

// DivideSlotByAt divides the duration of the given slot by a specified number, taking the
// UpSecondsPerSlotForkSlot fork into account. It returns a value of time.Duration in
// milliseconds.
func DivideSlotByAt(slot types.Slot, timesPerSlot int64) time.Duration {
	return time.Duration(int64(SecondsPerSlot(slot)*1000)/timesPerSlot) * time.Millisecond
}

// MultiplySlotBy multiplies the SECONDS_PER_SLOT configuration
// parameter by a specified number. It returns a value of time.Duration
// in millisecond-based durations.
//
// It is based on the slot duration before the UpSecondsPerSlotForkSlot fork. Use
// MultiplySlotByAt wherever the resulting duration has to follow the fork.
func MultiplySlotBy(times int64) time.Duration {
	return time.Duration(int64(params.BeaconConfig().SecondsPerSlot)*times) * time.Second
}

// MultiplySlotByAt multiplies the duration of the given slot by a specified number, taking
// the UpSecondsPerSlotForkSlot fork into account.
func MultiplySlotByAt(slot types.Slot, times int64) time.Duration {
	return time.Duration(int64(SecondsPerSlot(slot))*times) * time.Second // lint:ignore uintcast -- Slot duration is a small config value and will not exceed int64.
}

// AbsoluteValueSlotDifference between two slots.
func AbsoluteValueSlotDifference(x, y types.Slot) uint64 {
	if x > y {
		return uint64(x.SubSlot(y))
	}
	return uint64(y.SubSlot(x))
}

// ToEpoch returns the epoch number of the input slot.
//
// Spec pseudocode definition:
//
//	def compute_epoch_at_slot(slot: Slot) -> Epoch:
//	  """
//	  Return the epoch number at ``slot``.
//	  """
//	  return Epoch(slot // SLOTS_PER_EPOCH)
func ToEpoch(slot types.Slot) types.Epoch {
	return types.Epoch(slot.DivSlot(params.BeaconConfig().SlotsPerEpoch))
}

// EpochStart returns the first slot number of the
// current epoch.
//
// Spec pseudocode definition:
//
//	def compute_start_slot_at_epoch(epoch: Epoch) -> Slot:
//	  """
//	  Return the start slot of ``epoch``.
//	  """
//	  return Slot(epoch * SLOTS_PER_EPOCH)
func EpochStart(epoch types.Epoch) (types.Slot, error) {
	slot, err := params.BeaconConfig().SlotsPerEpoch.SafeMul(uint64(epoch))
	if err != nil {
		return slot, errors.Errorf("start slot calculation overflows: %v", err)
	}
	return slot, nil
}

// EpochEnd returns the last slot number of the
// current epoch.
func EpochEnd(epoch types.Epoch) (types.Slot, error) {
	if epoch == math.MaxUint64 {
		return 0, errors.New("start slot calculation overflows")
	}
	slot, err := EpochStart(epoch + 1)
	if err != nil {
		return 0, err
	}
	return slot - 1, nil
}

// IsEpochStart returns true if the given slot number is an epoch starting slot
// number.
func IsEpochStart(slot types.Slot) bool {
	return slot%params.BeaconConfig().SlotsPerEpoch == 0
}

// IsEpochEnd returns true if the given slot number is an epoch ending slot
// number.
func IsEpochEnd(slot types.Slot) bool {
	return IsEpochStart(slot + 1)
}

// SinceEpochStarts returns number of slots since the start of the epoch.
func SinceEpochStarts(slot types.Slot) types.Slot {
	return slot % params.BeaconConfig().SlotsPerEpoch
}

// VerifyTime validates the input slot is not from the future.
func VerifyTime(genesisTime uint64, slot types.Slot, timeTolerance time.Duration) error {
	slotTime, err := ToTime(genesisTime, slot)
	if err != nil {
		return err
	}

	// Defensive check to ensure unreasonable slots are rejected
	// straight away.
	if err := ValidateClock(slot, genesisTime); err != nil {
		return err
	}

	currentTime := prysmTime.Now()
	diff := slotTime.Sub(currentTime)

	if diff > timeTolerance {
		return fmt.Errorf("could not process slot from the future, slot time %s > current time %s (timeTolerance=%s)", slotTime, currentTime, timeTolerance)
	}
	return nil
}

// ToTime takes the given slot and genesis time to determine the start time of the slot.
func ToTime(genesisTimeSec uint64, slot types.Slot) (time.Time, error) {
	sTime, err := SlotStartTimestamp(genesisTimeSec, slot)
	if err != nil {
		return time.Unix(0, 0), fmt.Errorf("slot (%d) is in the far distant future: %w", slot, err)
	}
	return time.Unix(int64(sTime), 0), nil // lint:ignore uintcast -- A timestamp will not exceed int64 in your lifetime.
}

// Since computes the number of time slots that have occurred since the given timestamp.
func Since(time time.Time) types.Slot {
	return CurrentSlot(uint64(time.Unix()))
}

// CurrentSlot returns the current slot as determined by the local clock and
// provided genesis time.
func CurrentSlot(genesisTimeSec uint64) types.Slot {
	now := prysmTime.Now().Unix()
	genesis := int64(genesisTimeSec) // lint:ignore uintcast -- Genesis timestamp will not exceed int64 in your lifetime.
	if now < genesis {
		return 0
	}
	return SlotByElapsedSinceGenesis(uint64(now - genesis))
}

// ValidateClock validates a provided slot against the local
// clock to ensure slots that are unreasonable are returned with
// an error.
func ValidateClock(slot types.Slot, genesisTimeSec uint64) error {
	maxPossibleSlot := CurrentSlot(genesisTimeSec).Add(MaxSlotBuffer)
	// Defensive check to ensure that we only process slots up to a hard limit
	// from our local clock.
	if slot > maxPossibleSlot {
		return fmt.Errorf("slot %d > %d which exceeds max allowed value relative to the local clock", slot, maxPossibleSlot)
	}
	return nil
}

// RoundUpToNearestEpoch rounds up the provided slot value to the nearest epoch.
func RoundUpToNearestEpoch(slot types.Slot) types.Slot {
	if slot%params.BeaconConfig().SlotsPerEpoch != 0 {
		slot -= slot % params.BeaconConfig().SlotsPerEpoch
		slot += params.BeaconConfig().SlotsPerEpoch
	}
	return slot
}

// VotingPeriodStartTime returns the current voting period's start time
// depending on the provided genesis and current slot.
func VotingPeriodStartTime(genesis uint64, slot types.Slot) uint64 {
	slots := params.BeaconConfig().SlotsPerEpoch.Mul(uint64(params.BeaconConfig().EpochsPerEth1VotingPeriod))
	startSlot := slot - slot.ModSlot(slots)
	startTime, err := SlotStartTimestamp(genesis, startSlot)
	if err != nil {
		log.WithError(err).WithField("slot", startSlot).Error("Voting period start time calculation failed")
		return genesis
	}
	return startTime
}

// PrevSlot returns previous slot, with an exception in slot 0 to prevent underflow.
func PrevSlot(slot types.Slot) types.Slot {
	if slot > 0 {
		return slot.Sub(1)
	}
	return 0
}

// SyncCommitteePeriod returns the sync committee period of input epoch `e`.
//
// Spec code:
// def compute_sync_committee_period(epoch: Epoch) -> uint64:
//
//	return epoch // EPOCHS_PER_SYNC_COMMITTEE_PERIOD
func SyncCommitteePeriod(e types.Epoch) uint64 {
	return uint64(e / params.BeaconConfig().EpochsPerSyncCommitteePeriod)
}

// SyncCommitteePeriodStartEpoch returns the start epoch of a sync committee period.
func SyncCommitteePeriodStartEpoch(e types.Epoch) (types.Epoch, error) {
	// Overflow is impossible here because of division of `EPOCHS_PER_SYNC_COMMITTEE_PERIOD`.
	startEpoch, overflow := commonMath.SafeMul(SyncCommitteePeriod(e), uint64(params.BeaconConfig().EpochsPerSyncCommitteePeriod))
	if overflow {
		return 0, errors.New("start epoch calculation overflow")
	}
	return types.Epoch(startEpoch), nil
}
