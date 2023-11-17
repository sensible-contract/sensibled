package script

import (
	"encoding/binary"
)

// nft sell
func decodeNFTSellV2(scriptLen int, pkScript []byte, txo *AddressData) bool {
	// dataLen := 0
	if pkScript[scriptLen-133-1-1-1] == OP_RETURN &&
		pkScript[scriptLen-133-1-1] == 0x4c &&
		pkScript[scriptLen-133-1] == 133 {
		// nft sell v2
		// NFT 的codehash + NFT的genesis + NFT index + 商家手续费地址 + 商家费率 + 卖家地址 + 价格 + nftID + 1 + 0x00010006 即 65542
		// <nft sell v2 data> =
		//<codehash(20 bytes)> +
		//<genesis(20 bytes)> +
		//<tokenIndex(8 bytes)> +
		//<feeAddress(20 bytes)> +
		//<feeRate(1 bytes)> +
		//<sellerAddress(20 bytes)> +
		//<satoshisPrice(8 bytes)> +
		//<nftID(20 bytes)> +
		//<proto_version(4 bytes)> +
		//<proto_type(4 bytes)> +
		//<'sensible'(8 bytes)>

		// dataLen = 1 + 1 + 20 + 20 + 8 + 20 + 1 + 20 + 8 + 20 + 4 + 4 + 8 // 0x4c + pushdata + data
	} else {
		return false
	}

	protoVersionOffset := scriptLen - 8 - 4 - 4
	nftIdOffset := protoVersionOffset - 20
	priceOffset := nftIdOffset - 8
	addressOffset := priceOffset - 20
	feeRateOffset := addressOffset - 1
	feeAddressOffset := feeRateOffset - 20
	tokenIndexOffset := feeAddressOffset - 8
	genesisOffset := tokenIndexOffset - 20
	codehashOffset := genesisOffset - 20

	txo.CodeType = CodeType_NFT_SELL_V2

	nftsell := &NFTSellV2Data{
		TokenIndex: binary.LittleEndian.Uint64(pkScript[tokenIndexOffset : tokenIndexOffset+8]),
		Price:      binary.LittleEndian.Uint64(pkScript[priceOffset : priceOffset+8]),
		FeeRate:    pkScript[feeRateOffset],
	}
	txo.NFTSellV2 = nftsell
	// txo.CodeHash = GetHash160(pkScript[:scriptLen-dataLen])

	txo.GenesisIdLen = 20
	copy(txo.CodeHash[:], pkScript[codehashOffset:codehashOffset+20])
	copy(txo.GenesisId[:], pkScript[genesisOffset:genesisOffset+20])
	copy(nftsell.FeeAddressPkh[:], pkScript[feeAddressOffset:feeAddressOffset+20])
	copy(nftsell.NFTID[:], pkScript[nftIdOffset:nftIdOffset+20])
	txo.HasAddress = true
	copy(nftsell.SellerAddressPkh[:], pkScript[addressOffset:addressOffset+20])
	copy(txo.AddressPkh[:], pkScript[addressOffset:addressOffset+20]) // seller
	return true
}
