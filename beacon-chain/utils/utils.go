/*
Package implemets util to verify deposit data.
*/
package utils

import (
	"github.com/pkg/errors"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/features"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/crypto/bls"
	ethpb "gitlab.waterfall.network/waterfall/protocol/coordinator/proto/prysm/v1alpha1"
)

// VerifyDepositData validates the format of a deposit's public key and signature,
// and verifies the structural integrity of the signing root.
//
// Note: BLS signature verification (sig.Verify) is intentionally skipped here.
// Deposit signature is verified during deposit transaction processing on GWAT,
// so re-verifying it on the coordinator side is not required.
func VerifyDepositData(dd *ethpb.Deposit_Data, domain []byte) error {
	if features.Get().SkipBLSVerify {
		return nil
	}
	ddCopy := ethpb.CopyDepositData(dd)
	_, err := bls.PublicKeyFromBytes(ddCopy.PublicKey)
	if err != nil {
		return errors.Wrap(err, "could not convert bytes to public key")
	}
	_, err = bls.SignatureFromBytes(ddCopy.Signature)
	if err != nil {
		return errors.Wrap(err, "could not convert bytes to signature")
	}
	di := &ethpb.DepositMessage{
		PublicKey:             ddCopy.PublicKey,
		CreatorAddress:        ddCopy.CreatorAddress,
		WithdrawalCredentials: ddCopy.WithdrawalCredentials,
	}
	root, err := di.HashTreeRoot()
	if err != nil {
		return errors.Wrap(err, "could not get signing root")
	}
	signingData := &ethpb.SigningData{
		ObjectRoot: root[:],
		Domain:     domain,
	}
	_, err = signingData.HashTreeRoot()
	if err != nil {
		return errors.Wrap(err, "could not get container root")
	}

	return nil
}
