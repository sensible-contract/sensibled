package script

import (
	"encoding/hex"
	"encoding/json"
)

const (
	CodeType_NONE   uint32 = 0
	CodeType_FT     uint32 = 1
	CodeType_UNIQUE uint32 = 2
	CodeType_NFT    uint32 = 3

	CodeType_P2PK   uint32 = 4
	CodeType_P2PKH  uint32 = 5
	CodeType_P2SH   uint32 = 6
	CodeType_P2WPKH uint32 = 7
	CodeType_P2WSH  uint32 = 8
	CodeType_P2TR   uint32 = 9

	CodeType_SENSIBLE uint32 = 65536
)

var CodeTypeName []string = []string{
	"NONE",
	"FT",
	"UNIQUE",
	"NFT",

	"P2PK",
	"P2PKH",
	"P2SH",
	"P2WPKH",
	"P2WSH",
	"P2TR",
}

type AddressData struct {
	HasAddress bool
	CodeType   uint32
	AddressPkh [20]byte
}

func (u *AddressData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		CodeType   uint32
		HasAddress bool
		AddressPkh string
	}{
		CodeType:   u.CodeType,
		HasAddress: u.HasAddress,
		AddressPkh: hex.EncodeToString(u.AddressPkh[:]),
	})
}

type NFTData struct {
	Invalid     bool
	ContentType []byte
	ContentBody []byte
}
