package p2p

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"net"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/config/params"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/assert"
	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
	gethCrypto "gitlab.waterfall.network/waterfall/protocol/gwat/crypto"
	"gitlab.waterfall.network/waterfall/protocol/gwat/p2p/enode"
	"gitlab.waterfall.network/waterfall/protocol/gwat/p2p/enr"
)

func TestPrivateKeyLoading(t *testing.T) {
	file, err := ioutil.TempFile(t.TempDir(), "key")
	require.NoError(t, err)
	key, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
	require.NoError(t, err, "Could not generate key")
	raw, err := key.Raw()
	if err != nil {
		panic(err)
	}
	out := hex.EncodeToString(raw)

	err = ioutil.WriteFile(file.Name(), []byte(out), params.BeaconIoConfig().ReadWritePermissions)
	require.NoError(t, err, "Could not write key to file")
	log.WithField("file", file.Name()).WithField("key", out).Info("Wrote key to file")
	cfg := &Config{
		PrivateKey: file.Name(),
	}
	pKey, err := privKey(cfg)
	require.NoError(t, err, "Could not apply option")
	newPkey, err := convertToInterfacePrivkey(pKey)
	require.NoError(t, err)
	rawBytes, err := key.Raw()
	require.NoError(t, err)
	newRaw, err := newPkey.Raw()
	require.NoError(t, err)
	assert.DeepEqual(t, rawBytes, newRaw, "Private keys do not match")
}

// TestConvertToInterfacePrivkey_ShortScalar guards the leading-zero case: big.Int.Bytes()
// drops the most significant zero bytes, so roughly one generated key in 256 marshals to
// fewer than 32 bytes and used to be rejected with "expected secp256k1 data size to be 32".
func TestConvertToInterfacePrivkey_ShortScalar(t *testing.T) {
	tests := []struct {
		name string
		d    []byte
	}{
		{name: "one leading zero byte", d: append([]byte{0x00}, bytes.Repeat([]byte{0xAB}, 31)...)},
		{name: "two leading zero bytes", d: append([]byte{0x00, 0x00}, bytes.Repeat([]byte{0xCD}, 30)...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			privKey, err := gethCrypto.ToECDSA(tt.d)
			require.NoError(t, err)
			require.Equal(t, true, len(privKey.D.Bytes()) < 32, "test key must have a short scalar")

			ifaceKey, err := convertToInterfacePrivkey(privKey)
			require.NoError(t, err)

			raw, err := ifaceKey.Raw()
			require.NoError(t, err)
			assert.DeepEqual(t, tt.d, raw, "Private key must round-trip with its leading zero bytes")

			// The nil dereference reported from buildOptions happened right here.
			_, err = peer.IDFromPublicKey(ifaceKey.GetPublic())
			require.NoError(t, err)
		})
	}
}

func TestIPV6Support(t *testing.T) {
	key, err := gethCrypto.GenerateKey()
	require.NoError(t, err)
	db, err := enode.OpenDB("")
	if err != nil {
		log.Error("could not open node's peer database")
	}
	lNode := enode.NewLocalNode(db, key)
	mockIPV6 := net.IP{0xff, 0x02, 0xAA, 0, 0x1F, 0, 0x2E, 0, 0, 0x36, 0x45, 0, 0, 0, 0, 0x02}
	lNode.Set(enr.IP(mockIPV6))
	ma, err := convertToSingleMultiAddr(lNode.Node())
	if err != nil {
		t.Fatal(err)
	}
	ipv6Exists := false
	for _, p := range ma.Protocols() {
		if p.Name == "ip4" {
			t.Error("Got ip4 address instead of ip6")
		}
		if p.Name == "ip6" {
			ipv6Exists = true
		}
	}
	if !ipv6Exists {
		t.Error("Multiaddress did not have ipv6 protocol")
	}
}
