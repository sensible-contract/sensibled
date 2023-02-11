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

	CodeType_P2PK  uint32 = 4
	CodeType_P2PKH uint32 = 5
	CodeType_P2SH  uint32 = 6
	CodeType_P2WKH uint32 = 7
	CodeType_P2WSH uint32 = 8
	CodeType_P2TR  uint32 = 9

	CodeType_SENSIBLE uint32 = 65536
	CodeType_NFT_SELL uint32 = 65536 + 1

	CodeType_NFT_AUCTION uint32 = 65536 + 4

	CodeType_NFT_SELL_V2 uint32 = 65536 + 6
)

var CodeTypeName []string = []string{
	"NONE",
	"FT",
	"UNIQUE",
	"NFT",

	"P2PK",
	"P2PKH",
	"P2SH",
	"P2WKH",
	"P2WSH",
	"P2TR",
}

// nft
type NFTData struct {
	SensibleId []byte // GenesisTx outpoint

	MetaTxId        [32]byte // nft metatxid
	MetaOutputIndex uint32
	TokenIndex      uint64 // nft tokenIndex
	TokenSupply     uint64 // nft tokenSupply
}

func (u *NFTData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		SensibleId      string // GenesisTx outpoint
		MetaTxId        string // nft metatxid
		MetaOutputIndex uint32
		TokenIndex      uint64 // nft tokenIndex
		TokenSupply     uint64 // nft tokenSupply

	}{
		SensibleId:      hex.EncodeToString(u.SensibleId),
		MetaTxId:        hex.EncodeToString(u.MetaTxId[:]),
		MetaOutputIndex: u.MetaOutputIndex,
		TokenIndex:      u.TokenIndex,
		TokenSupply:     u.TokenSupply,
	})
}

type TxoData struct {
	CodeType   uint32
	AddressPkh [20]byte
	NFT        *NFTData
	HasAddress bool
}

func (u *TxoData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		CodeType   uint32
		HasAddress bool
		AddressPkh string
		NFT        *NFTData
	}{
		CodeType:   u.CodeType,
		HasAddress: u.HasAddress,
		AddressPkh: hex.EncodeToString(u.AddressPkh[:]),
		NFT:        u.NFT,
	})
}
