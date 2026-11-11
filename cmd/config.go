package cmd

import (
	types "github.com/prysmaticlabs/eth2-types"
	"github.com/urfave/cli/v2"
	fieldparams "gitlab.waterfall.network/waterfall/protocol/coordinator/config/fieldparams"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
)

// Flags is a struct to represent which features the client will perform on runtime.
type Flags struct {
	// Configuration related flags.
	MinimalConfig  bool // MinimalConfig as defined in the spec.
	E2EConfig      bool // E2EConfig made specifically for testing, do not use except in E2E.
	MaxRPCPageSize int  // MaxRPCPageSize is used for a cap of page sizes in RPC requests.
}

var sharedConfig *Flags

// Get retrieves feature config.
func Get() *Flags {
	if sharedConfig == nil {
		return &Flags{
			MaxRPCPageSize: params.BeaconConfig().DefaultPageSize,
		}
	}
	return sharedConfig
}

// Init sets the global config equal to the config that is passed in.
func Init(c *Flags) {
	sharedConfig = c
}

// InitWithReset sets the global config and returns function that is used to reset configuration.
func InitWithReset(c *Flags) func() {
	resetFunc := func() {
		Init(nil)
	}
	Init(c)
	return resetFunc
}

// ConfigureBeaconChain sets the global config based
// on what flags are enabled for the beacon-chain client.
func ConfigureBeaconChain(ctx *cli.Context) {
	cfg := newConfig(ctx)
	if ctx.IsSet(RPCMaxPageSizeFlag.Name) {
		cfg.MaxRPCPageSize = ctx.Int(RPCMaxPageSizeFlag.Name)
		log.Warnf("Starting beacon chain with max RPC page size of %d", cfg.MaxRPCPageSize)
	}
	Init(cfg)
}

// ConfigureValidator sets the global config based
// on what flags are enabled for the validator client.
func ConfigureValidator(ctx *cli.Context) {
	cfg := newConfig(ctx)
	Init(cfg)
}

// ConfigureForkSlots overrides the params of the slot duration fork by the cli flags.
// They must be kept in sync with the gwat ones, otherwise the nodes calculate different
// slots for the same time.
func ConfigureForkSlots(cliCtx *cli.Context) {
	forkSlotIsSet := cliCtx.IsSet(UpSecondsPerSlotForkSlot.Name)
	secondsIsSet := cliCtx.IsSet(UpSecondsPerSlot.Name)
	if !forkSlotIsSet && !secondsIsSet {
		return
	}
	c := params.BeaconConfig()
	if forkSlotIsSet {
		c.UpSecondsPerSlotForkSlot = types.Slot(cliCtx.Uint64(UpSecondsPerSlotForkSlot.Name))
	}
	if secondsIsSet {
		c.UpSecondsPerSlot = cliCtx.Uint64(UpSecondsPerSlot.Name)
	}
	params.OverrideBeaconConfig(c)
	if c.UpSecondsPerSlot == 0 {
		log.Warnf("Slot duration fork is disabled by zero %s: slot duration stays %d seconds",
			UpSecondsPerSlot.Name, c.SecondsPerSlot)
		return
	}
	log.Warnf("Slot duration fork is set to slot %d with %d seconds per slot: the same values must be set for gwat",
		c.UpSecondsPerSlotForkSlot, c.UpSecondsPerSlot)
}

func newConfig(ctx *cli.Context) *Flags {
	cfg := Get()
	if ctx.Bool(MinimalConfigFlag.Name) {
		log.Warn("Using minimal config")
		cfg.MinimalConfig = true
		params.UseMinimalConfig()
	}
	if ctx.Bool(E2EConfigFlag.Name) {
		log.Warn("Using end-to-end testing config")
		switch fieldparams.Preset {
		case "mainnet":
			params.UseE2EMainnetConfig()
		case "minimal":
			params.UseE2EConfig()
		default:
			log.Fatalf("Unrecognized preset being used: %s", fieldparams.Preset)
		}
	}
	return cfg
}
