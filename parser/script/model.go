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

// swap
// 64/84 bytes
type SwapData struct {
	// fetchTokenContractHash + lpTokenID + lpTokenScriptCodeHash + Token1Amount + Token2Amount + lpAmount
	Token1Amount uint64
	Token2Amount uint64
	LpAmount     uint64
}

// ft
type FTData struct {
	SensibleId []byte // GenesisTx outpoint

	Name    string // ft name
	Symbol  string // ft symbol
	Amount  uint64 // ft amount
	Decimal uint8  // ft decimal
}

func (u *FTData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		SensibleId string // GenesisTx outpoint
		Name       string // ft name
		Symbol     string // ft symbol
		Amount     uint64 // ft amount
		Decimal    uint8  // ft decimal
	}{
		SensibleId: hex.EncodeToString(u.SensibleId[:]),
		Name:       u.Name,
		Symbol:     u.Symbol,
		Amount:     u.Amount,
		Decimal:    u.Decimal,
	})
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

// nft sell
type NFTSellData struct {
	TokenIndex uint64 // nft tokenIndex
	Price      uint64 // nft price
}

func (u *NFTSellData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TokenIndex uint64
		Price      uint64
	}{
		TokenIndex: u.TokenIndex,
		Price:      u.Price,
	})
}

// nft sell
type NFTSellV2Data struct {
	TokenIndex       uint64
	Price            uint64
	FeeAddressPkh    [20]byte
	FeeRate          byte
	SellerAddressPkh [20]byte
	NFTID            [20]byte
}

func (u *NFTSellV2Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TokenIndex       uint64
		Price            uint64
		FeeAddressPkh    string
		FeeRate          byte
		SellerAddressPkh string
		NFTID            string
	}{
		TokenIndex:       u.TokenIndex,
		Price:            u.Price,
		FeeAddressPkh:    hex.EncodeToString(u.FeeAddressPkh[:]),
		FeeRate:          u.FeeRate,
		SellerAddressPkh: hex.EncodeToString(u.SellerAddressPkh[:]),
		NFTID:            hex.EncodeToString(u.NFTID[:]),
	})
}

// nft auction
// <nft auction data> = <rabinPubkeyHashArrayHash>(20bytes) + <timeRabinPubkeyHash>(20byte) +
type NFTAuctionData struct {
	SensibleId       [36]byte // Auction GenesisTx outpoint
	NFTCodeHash      [20]byte
	NFTID            [20]byte
	FeeAmount        uint64 // v1
	FeeRate          byte   // v2
	FeeAddressPkh    [20]byte
	StartBsvPrice    uint64
	SenderAddressPkh [20]byte
	EndTimestamp     uint64
	BidTimestamp     uint64
	BidBsvPrice      uint64
	BidderAddressPkh [20]byte
}

func (u *NFTAuctionData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		SensibleId       string
		NFTCodeHash      string
		NFTID            string
		FeeAmount        uint64 // v1
		FeeRate          byte   // v2
		FeeAddressPkh    string
		StartBsvPrice    uint64
		SenderAddressPkh string
		EndTimestamp     uint64
		BidTimestamp     uint64
		BidBsvPrice      uint64
		BidderAddressPkh string
	}{
		SensibleId:       hex.EncodeToString(u.SensibleId[:]),
		NFTCodeHash:      hex.EncodeToString(u.NFTCodeHash[:]),
		NFTID:            hex.EncodeToString(u.NFTID[:]),
		FeeAmount:        u.FeeAmount,
		FeeRate:          u.FeeRate,
		FeeAddressPkh:    hex.EncodeToString(u.FeeAddressPkh[:]),
		StartBsvPrice:    u.StartBsvPrice,
		SenderAddressPkh: hex.EncodeToString(u.SenderAddressPkh[:]),
		EndTimestamp:     u.EndTimestamp,
		BidTimestamp:     u.BidTimestamp,
		BidBsvPrice:      u.BidBsvPrice,
		BidderAddressPkh: hex.EncodeToString(u.BidderAddressPkh[:]),
	})
}

// unique
type UniqueData struct {
	SensibleId []byte // GenesisTx outpoint
	CustomData []byte // unique data
	Swap       *SwapData
}

func (u *UniqueData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		SensibleId string // GenesisTx outpoint
		CustomData string // unique data
		Swap       *SwapData
	}{
		SensibleId: hex.EncodeToString(u.SensibleId),
		CustomData: hex.EncodeToString(u.CustomData),
		Swap:       u.Swap,
	})
}

type SensibleData struct {
	CodeHash     [20]byte
	GenesisId    [40]byte // for search: codehash + genesis
	GenesisIdLen uint8
	NFT          *NFTData
	FT           *FTData
	Uniq         *UniqueData
	NFTSell      *NFTSellData
	NFTSellV2    *NFTSellV2Data
	NFTAuction   *NFTAuctionData
}

type AddressData struct {
	CodeType   uint32
	HasAddress bool
	AddressPkh [20]byte

	SensibleData *SensibleData
}

func (u *AddressData) MarshalJSON() ([]byte, error) {
	sData := &SensibleData{}
	if u.SensibleData != nil {
		sData = u.SensibleData
	}

	return json.Marshal(&struct {
		CodeType     uint32
		CodeHash     string
		GenesisId    string // for search: codehash + genesis
		GenesisIdLen uint8
		HasAddress   bool
		AddressPkh   string
		NFT          *NFTData
		FT           *FTData
		Uniq         *UniqueData
		NFTSell      *NFTSellData
		NFTSellV2    *NFTSellV2Data
		NFTAuction   *NFTAuctionData
	}{
		CodeType:     u.CodeType,
		CodeHash:     hex.EncodeToString(sData.CodeHash[:]),
		GenesisId:    hex.EncodeToString(sData.GenesisId[:]),
		GenesisIdLen: sData.GenesisIdLen,
		HasAddress:   u.HasAddress,
		AddressPkh:   hex.EncodeToString(u.AddressPkh[:]),
		NFT:          sData.NFT,
		FT:           sData.FT,
		Uniq:         sData.Uniq,
		NFTSell:      sData.NFTSell,
		NFTSellV2:    sData.NFTSellV2,
		NFTAuction:   sData.NFTAuction,
	})
}
