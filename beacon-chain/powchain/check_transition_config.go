package powchain

import (
	"errors"
)

var (
	configMismatchLog = "Configuration mismatch between your execution client and Prysm. " +
		"Please check your execution client and restart it with the proper configuration. If this is not done, " +
		"your node will not be able to complete the proof-of-stake transition"
)

// We check if there is a configuration mismatch error between the execution client
// and the Prysm beacon node. If so, we need to log errors in the node as it cannot successfully
// complete the merge transition for the Bellatrix hard fork.
func (s *Service) handleExchangeConfigurationError(err error) {
	if err == nil {
		// If there is no error in checking the exchange configuration error, we clear
		// the run error of the service if we had previously set it to ErrConfigMismatch.
		if errors.Is(s.runError, ErrConfigMismatch) {
			s.runError = nil
		}
		return
	}
	// If the error is a configuration mismatch, we set a runtime error in the service.
	if errors.Is(err, ErrConfigMismatch) {
		s.runError = err
		log.WithError(err).Error(configMismatchLog)
		return
	}
	log.WithError(err).Error("Could not check configuration values between execution and consensus client")
}
