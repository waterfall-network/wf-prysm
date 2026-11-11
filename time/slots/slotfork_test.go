package slots

import (
	"math"
	"testing"
	"time"

	types "github.com/prysmaticlabs/eth2-types"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/assert"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	prysmTime "gitlab.waterfall.network/waterfall/protocol/coordinator/time"
)

const (
	// forkTestGenesis is an arbitrary genesis time used by the slot duration fork tests.
	forkTestGenesis = uint64(1_700_000_000)
	// forkTestSlot is the slot the slot duration changes from 6 to 3 seconds at,
	// which mirrors the mainnet UpSecondsPerSlotForkSlot setup.
	forkTestSlot = types.Slot(100)
)

// useForkedConfig installs a config with the slot duration fork enabled at forkTestSlot.
func useForkedConfig(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 6
	cfg.SlotsPerEpoch = 32
	cfg.UpSecondsPerSlotForkSlot = forkTestSlot
	cfg.UpSecondsPerSlot = 3
	params.OverrideBeaconConfig(cfg)
}

// useUnforkedConfig installs the same config with the slot duration fork disabled.
func useUnforkedConfig(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 6
	cfg.SlotsPerEpoch = 32
	cfg.UpSecondsPerSlotForkSlot = math.MaxUint64
	cfg.UpSecondsPerSlot = 0
	params.OverrideBeaconConfig(cfg)
}

func TestUpSecondsPerSlotFork_SecondsPerSlot(t *testing.T) {
	useForkedConfig(t)
	tests := []struct {
		slot types.Slot
		want uint64
	}{
		{slot: 0, want: 6},
		{slot: forkTestSlot - 1, want: 6},
		{slot: forkTestSlot, want: 3},
		{slot: forkTestSlot + 1, want: 3},
		{slot: math.MaxUint64, want: 3},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, SecondsPerSlot(tt.slot), "slot %d", tt.slot)
	}
}

// TestUpSecondsPerSlotFork_KeepsHistory is the regression guard of the whole fork: every slot
// and every timestamp before the fork must resolve exactly as it does with the fork disabled.
func TestUpSecondsPerSlotFork_KeepsHistory(t *testing.T) {
	forkTime := forkTestGenesis + uint64(forkTestSlot)*6

	unforked := make(map[types.Slot]uint64, forkTestSlot)
	unforkedSlots := make(map[uint64]types.Slot)
	func() {
		useUnforkedConfig(t)
		for slot := types.Slot(0); slot < forkTestSlot; slot++ {
			ts, err := SlotStartTimestamp(forkTestGenesis, slot)
			require.NoError(t, err)
			unforked[slot] = ts
		}
		for ts := forkTestGenesis; ts < forkTime; ts++ {
			unforkedSlots[ts] = SlotByTimestamp(forkTestGenesis, ts)
		}
	}()

	useForkedConfig(t)
	for slot := types.Slot(0); slot < forkTestSlot; slot++ {
		ts, err := SlotStartTimestamp(forkTestGenesis, slot)
		require.NoError(t, err)
		assert.Equal(t, unforked[slot], ts, "start time of pre-fork slot %d shifted", slot)
	}
	for ts := forkTestGenesis; ts < forkTime; ts++ {
		assert.Equal(t, unforkedSlots[ts], SlotByTimestamp(forkTestGenesis, ts), "slot of pre-fork timestamp %d shifted", ts)
	}
}

func TestUpSecondsPerSlotFork_StartTimeBoundaries(t *testing.T) {
	useForkedConfig(t)
	forkTime := forkTestGenesis + uint64(forkTestSlot)*6

	tests := []struct {
		name string
		slot types.Slot
		want uint64
	}{
		{name: "genesis", slot: 0, want: forkTestGenesis},
		{name: "first slot", slot: 1, want: forkTestGenesis + 6},
		{name: "last slot before fork", slot: forkTestSlot - 1, want: forkTime - 6},
		{name: "fork slot", slot: forkTestSlot, want: forkTime},
		{name: "first slot after fork", slot: forkTestSlot + 1, want: forkTime + 3},
		{name: "tenth slot after fork", slot: forkTestSlot + 10, want: forkTime + 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SlotStartTimestamp(forkTestGenesis, tt.slot)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUpSecondsPerSlotFork_SlotByTimestampBoundaries(t *testing.T) {
	useForkedConfig(t)
	forkTime := forkTestGenesis + uint64(forkTestSlot)*6

	tests := []struct {
		name      string
		timestamp uint64
		want      types.Slot
	}{
		{name: "before genesis", timestamp: forkTestGenesis - 1, want: 0},
		{name: "genesis", timestamp: forkTestGenesis, want: 0},
		{name: "last second of slot 0", timestamp: forkTestGenesis + 5, want: 0},
		{name: "last second before fork", timestamp: forkTime - 1, want: forkTestSlot - 1},
		{name: "fork time", timestamp: forkTime, want: forkTestSlot},
		{name: "last second of fork slot", timestamp: forkTime + 2, want: forkTestSlot},
		{name: "first second of next slot", timestamp: forkTime + 3, want: forkTestSlot + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SlotByTimestamp(forkTestGenesis, tt.timestamp))
		})
	}
}

// TestUpSecondsPerSlotFork_RoundTrip walks a range of slots on both sides of the fork and
// checks that every slot lasts exactly the expected number of seconds and that the inverse
// function maps the first and the last second of a slot back onto it — no gaps, no overlaps.
func TestUpSecondsPerSlotFork_RoundTrip(t *testing.T) {
	useForkedConfig(t)

	for slot := types.Slot(0); slot <= forkTestSlot+50; slot++ {
		start, err := SlotStartTimestamp(forkTestGenesis, slot)
		require.NoError(t, err)
		next, err := SlotStartTimestamp(forkTestGenesis, slot+1)
		require.NoError(t, err)

		assert.Equal(t, SecondsPerSlot(slot), next-start, "unexpected duration of slot %d", slot)
		assert.Equal(t, slot, SlotByTimestamp(forkTestGenesis, start), "start of slot %d maps back to a different slot", slot)
		assert.Equal(t, slot, SlotByTimestamp(forkTestGenesis, next-1), "last second of slot %d maps back to a different slot", slot)
	}
}

func TestUpSecondsPerSlotFork_DisabledByMaxSlot(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 6
	// The fork slot is unreachable but UpSecondsPerSlot is set, as it is on mainnet today.
	cfg.UpSecondsPerSlotForkSlot = math.MaxUint64
	cfg.UpSecondsPerSlot = 3
	params.OverrideBeaconConfig(cfg)

	for _, slot := range []types.Slot{0, 1, 100, 1_000_000} {
		got, err := SlotStartTimestamp(forkTestGenesis, slot)
		require.NoError(t, err)
		assert.Equal(t, forkTestGenesis+uint64(slot)*6, got, "slot %d", slot)
		assert.Equal(t, slot, SlotByTimestamp(forkTestGenesis, got), "slot %d", slot)
	}
	// The timeline must not shift and the fork slot multiplication must not overflow.
	assert.Equal(t, uint64(6), SecondsPerSlot(math.MaxUint64-1))
}

func TestUpSecondsPerSlotFork_AtGenesis(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 6
	cfg.UpSecondsPerSlotForkSlot = 0
	cfg.UpSecondsPerSlot = 3
	params.OverrideBeaconConfig(cfg)

	for _, slot := range []types.Slot{0, 1, 2, 100} {
		got, err := SlotStartTimestamp(forkTestGenesis, slot)
		require.NoError(t, err)
		assert.Equal(t, forkTestGenesis+uint64(slot)*3, got, "slot %d", slot)
		assert.Equal(t, slot, SlotByTimestamp(forkTestGenesis, got), "slot %d", slot)
	}
}

func TestUpSecondsPerSlotFork_StartTimeOverflow(t *testing.T) {
	useForkedConfig(t)
	_, err := SlotStartTimestamp(forkTestGenesis, math.MaxUint64)
	require.ErrorContains(t, "overflows", err)
}

// TestUpSecondsPerSlotFork_CurrentSlot checks the current slot for a genesis far in the past,
// with the fork already behind.
func TestUpSecondsPerSlotFork_CurrentSlot(t *testing.T) {
	useForkedConfig(t)

	// 600 seconds of pre-fork slots (100 * 6) followed by 300 seconds of post-fork slots.
	genesis := uint64(prysmTime.Now().Unix()) - 900
	assert.Equal(t, forkTestSlot+100, CurrentSlot(genesis))
	assert.Equal(t, forkTestSlot+100, SinceGenesis(time.Unix(int64(genesis), 0)))

	// Genesis in the future yields slot 0.
	assert.Equal(t, types.Slot(0), CurrentSlot(uint64(prysmTime.Now().Unix())+3600))
	assert.Equal(t, types.Slot(0), SinceGenesis(prysmTime.Now().Add(time.Hour)))
}

// TestUpSecondsPerSlotFork_TickerCrossesFork checks that the ticker emits tick times taken
// from the fork-aware slot start, so it switches to the new slot duration on its own.
func TestUpSecondsPerSlotFork_TickerCrossesFork(t *testing.T) {
	useForkedConfig(t)

	genesisTime := time.Unix(int64(forkTestGenesis), 0)
	forkTime := forkTestGenesis + uint64(forkTestSlot)*6

	ticker := &SlotTicker{
		c:    make(chan types.Slot),
		done: make(chan struct{}),
	}
	defer ticker.Done()

	// Pretend the current time is three seconds into the slot preceding the fork slot.
	sinceGenesis := time.Duration(forkTime-6-forkTestGenesis+3) * time.Second
	since := func(time.Time) time.Duration { return sinceGenesis }

	// Capture the time the ticker intends to wait for on every iteration.
	var tickTimes []time.Time
	until := func(t time.Time) time.Duration {
		tickTimes = append(tickTimes, t)
		return 0
	}
	tick := make(chan time.Time, 1)
	after := func(time.Duration) <-chan time.Time { return tick }

	ticker.start(genesisTime, 0, params.BeaconConfig().SecondsPerSlot, since, until, after)

	want := []struct {
		slot types.Slot
		time uint64
	}{
		{slot: forkTestSlot, time: forkTime},
		{slot: forkTestSlot + 1, time: forkTime + 3},
		{slot: forkTestSlot + 2, time: forkTime + 6},
	}
	for i, w := range want {
		tick <- time.Now()
		assert.Equal(t, w.slot, <-ticker.C(), "unexpected slot at tick %d", i)
	}
	require.Equal(t, true, len(tickTimes) >= len(want))
	for i, w := range want {
		assert.Equal(t, w.time, uint64(tickTimes[i].Unix()), "unexpected tick time at tick %d", i)
	}
}

func TestUpSecondsPerSlotFork_DivideAndMultiplySlotByAt(t *testing.T) {
	useForkedConfig(t)

	assert.Equal(t, 2*time.Second, DivideSlotByAt(forkTestSlot-1, 3))
	assert.Equal(t, time.Second, DivideSlotByAt(forkTestSlot, 3))
	assert.Equal(t, 12*time.Second, MultiplySlotByAt(forkTestSlot-1, 2))
	assert.Equal(t, 6*time.Second, MultiplySlotByAt(forkTestSlot, 2))

	// The fork-unaware helpers stay on the base slot duration.
	assert.Equal(t, 2*time.Second, DivideSlotBy(3))
	assert.Equal(t, 12*time.Second, MultiplySlotBy(2))
}
