// Package slots includes ticker and timer-related functions for consensus.
package slots

import (
	"time"

	types "github.com/prysmaticlabs/eth2-types"
	prysmTime "gitlab.waterfall.network/waterfall/protocol/coordinator/time"
)

// The Ticker interface defines a type which can expose a
// receive-only channel firing slot events.
type Ticker interface {
	C() <-chan types.Slot
	Done()
}

// SlotTicker is a special ticker for the beacon chain block.
// The channel emits over the slot interval, and ensures that
// the ticks are in line with the genesis time. This means that
// the duration between the ticks and the genesis time are always a
// multiple of the slot duration.
// In addition, the channel returns the new slot number.
type SlotTicker struct {
	c    chan types.Slot
	done chan struct{}
}

// C returns the ticker channel. Call Cancel afterwards to ensure
// that the goroutine exits cleanly.
func (s *SlotTicker) C() <-chan types.Slot {
	return s.c
}

// Done should be called to clean up the ticker.
func (s *SlotTicker) Done() {
	go func() {
		s.done <- struct{}{}
	}()
}

// NewSlotTicker starts and returns a new SlotTicker instance.
func NewSlotTicker(genesisTime time.Time, secondsPerSlot uint64) *SlotTicker {
	if genesisTime.IsZero() {
		panic("zero genesis time")
	}
	ticker := &SlotTicker{
		c:    make(chan types.Slot),
		done: make(chan struct{}),
	}
	ticker.start(genesisTime, 0, secondsPerSlot, prysmTime.Since, prysmTime.Until, time.After)
	return ticker
}

// NewSlotTickerWithOffset starts and returns a SlotTicker instance that allows a offset of time from genesis,
// entering a offset greater than secondsPerSlot is not allowed.
func NewSlotTickerWithOffset(genesisTime time.Time, offset time.Duration, secondsPerSlot uint64) *SlotTicker {
	if genesisTime.Unix() == 0 {
		panic("zero genesis time")
	}
	if offset > time.Duration(secondsPerSlot)*time.Second {
		panic("invalid ticker offset")
	}
	ticker := &SlotTicker{
		c:    make(chan types.Slot),
		done: make(chan struct{}),
	}
	ticker.start(genesisTime, offset, secondsPerSlot, prysmTime.Since, prysmTime.Until, time.After)
	return ticker
}

// start emits slot numbers on the ticker channel at the start time of every slot, shifted by
// offset. Tick times are taken from ToTime rather than derived from a fixed slot duration, so
// the ticker follows the UpSecondsPerSlotForkSlot slot duration change without a restart.
// secondsPerSlot is only used to detect that the current time is still before genesis.
func (s *SlotTicker) start(
	genesisTime time.Time,
	offset time.Duration,
	secondsPerSlot uint64,
	since, until func(time.Time) time.Duration,
	after func(time.Duration) <-chan time.Time,
) {
	genesisSec := uint64(genesisTime.Unix()) // lint:ignore uintcast -- Genesis timestamp will not exceed int64 in your lifetime.

	go func() {
		var (
			nextTickTime time.Time
			slot         types.Slot
		)
		sinceGenesis := since(genesisTime.Add(offset))
		if sinceGenesis < time.Duration(secondsPerSlot)*time.Second {
			// Handle when the current time is before the genesis time.
			nextTickTime = genesisTime.Add(offset)
			slot = 0
		} else {
			// Tick on the slot following the one the current time belongs to.
			slot = SlotByTimestamp(genesisSec, uint64(genesisTime.Add(sinceGenesis).Unix())) + 1
			startTime, err := ToTime(genesisSec, slot)
			if err != nil {
				log.WithError(err).WithField("slot", slot).Error("Slot ticker: start slot time failed")
				return
			}
			nextTickTime = startTime.Add(offset)
		}

		for {
			waitTime := until(nextTickTime)
			select {
			case <-after(waitTime):
				s.c <- slot
				slot++
				startTime, err := ToTime(genesisSec, slot)
				if err != nil {
					log.WithError(err).WithField("slot", slot).Error("Slot ticker: start slot time failed")
					return
				}
				nextTickTime = startTime.Add(offset)
			case <-s.done:
				return
			}
		}
	}()
}
