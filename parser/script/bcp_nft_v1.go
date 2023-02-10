package script

import (
	"encoding/binary"
)

func decodeNFTIssue(scriptLen int, pkScript []byte, txo *TxoData) bool {
	// nft issue
	txo.CodeType = CodeType_NFT
	genesisIdLen := 40
	genesisOffset := scriptLen - 37 - 1 - genesisIdLen
	tokenSupplyOffset := scriptLen - 1 - 8
	tokenIndexOffset := tokenSupplyOffset - 8
	addressOffset := tokenIndexOffset - 20

	dataLen := 1 + 1 + genesisIdLen + 1 + 37 // opreturn + pushdata + pushdata + data

	nft := &NFTData{
		TokenSupply: binary.LittleEndian.Uint64(pkScript[tokenSupplyOffset : tokenSupplyOffset+8]),
		TokenIndex:  binary.LittleEndian.Uint64(pkScript[tokenIndexOffset : tokenIndexOffset+8]),
	}
	nft.TokenIndex = nft.TokenSupply
	txo.NFT = nft

	txo.GenesisIdLen = uint8(genesisIdLen)
	copy(txo.CodeHash[:], GetHash160(pkScript[:scriptLen-dataLen]))
	copy(txo.GenesisId[:], pkScript[genesisOffset:genesisOffset+genesisIdLen])
	copy(txo.AddressPkh[:], pkScript[addressOffset:addressOffset+20])
	txo.HasAddress = true
	return true
}

func decodeNFTTransfer(scriptLen int, pkScript []byte, txo *TxoData) bool {
	// nft transfer
	txo.CodeType = CodeType_NFT
	genesisIdLen := 40
	genesisOffset := scriptLen - 61 - 1 - genesisIdLen
	metaTxIdOffset := scriptLen - 1 - 32
	tokenIndexOffset := metaTxIdOffset - 8
	addressOffset := tokenIndexOffset - 20

	dataLen := 1 + 1 + genesisIdLen + 1 + 61 // opreturn + pushdata + pushdata + data

	nft := &NFTData{
		TokenIndex: binary.LittleEndian.Uint64(pkScript[tokenIndexOffset : tokenIndexOffset+8]),
	}
	txo.NFT = nft
	txo.GenesisIdLen = uint8(genesisIdLen)
	copy(txo.CodeHash[:], GetHash160(pkScript[:scriptLen-dataLen]))
	copy(txo.GenesisId[:], pkScript[genesisOffset:genesisOffset+genesisIdLen])
	copy(nft.MetaTxId[:], pkScript[metaTxIdOffset:metaTxIdOffset+32])
	copy(txo.AddressPkh[:], pkScript[addressOffset:addressOffset+20])
	txo.HasAddress = true
	return true
}
