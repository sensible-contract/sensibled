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
	HasAddress bool
	AddressPkh [20]byte
	NFT        *NFTData
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
