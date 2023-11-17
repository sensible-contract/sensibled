package script

import (
	"encoding/binary"
)

// nft
func decodeNFT(scriptLen int, pkScript []byte, txo *AddressData) bool {
	dataLen := 0
	protoVersionLen := 0
	genesisIdLen := 76 // nft v2
	sensibleIdLen := 36
	useTokenIdHash := false
	if pkScript[scriptLen-89-76-1-1-1] == OP_RETURN &&
		pkScript[scriptLen-89-76-1-1] == 0x4c &&
		pkScript[scriptLen-89-76-1] == 165 {
		// nft v3
		// <nft data> = <metaid_outpoint(36 bytes)> + <is_genesis(1 byte)> + <address(20 bytes)> + <totalSupply(8 bytes) + <tokenIndex(8 bytes)> + <genesisHash<20 bytes>) + <RABIN_PUBKEY_HASH_ARRAY_HASH(20 bytes)> + <sensibleID(36 bytes)> + <protoVersion(4 bytes)> + <protoType(4 bytes)> + <protoFlag(8 bytes)>
		dataLen = 1 + 1 + 36 + 1 + 20 + 8 + 8 + 20 + 20 + 36 + 4 + 4 + 8 // 0x4c + pushdata + data
		protoVersionLen = 4
		useTokenIdHash = true
	} else if pkScript[scriptLen-85-76-1-1-1] == OP_RETURN &&
		pkScript[scriptLen-85-76-1-1] == 0x4c &&
		pkScript[scriptLen-85-76-1] == 161 {
		// nft v2
		// <nft data> = <metaid_outpoint(36 bytes)> + <is_genesis(1 byte)> + <address(20 bytes)> + <totalSupply(8 bytes) + <tokenIndex(8 bytes)> + <genesisHash<20 bytes>) + <RABIN_PUBKEY_HASH_ARRAY_HASH(20 bytes)> + <sensibleID(36 bytes)> + <protoType(4 bytes)> + <protoFlag(8 bytes)>
		dataLen = 1 + 1 + 1 + 36 + 1 + 20 + 8 + 8 + 20 + 20 + 36 + 4 + 8 // opreturn + 0x4c + pushdata + data
		protoVersionLen = 0
		useTokenIdHash = false
	} else {
		return false
	}

	protoTypeOffset := scriptLen - 8 - 4
	sensibleOffset := protoTypeOffset - protoVersionLen - sensibleIdLen

	genesisOffset := protoTypeOffset - protoVersionLen - genesisIdLen
	tokenIndexOffset := genesisOffset - 8
	tokenSupplyOffset := tokenIndexOffset - 8
	addressOffset := tokenSupplyOffset - 20
	isGenesisOffset := addressOffset - 1
	metaOutputIndexOffset := isGenesisOffset - 4
	metaTxIdOffset := metaOutputIndexOffset - 32

	txo.CodeType = CodeType_NFT

	nft := &NFTData{
		SensibleId:  make([]byte, sensibleIdLen),
		TokenSupply: binary.LittleEndian.Uint64(pkScript[tokenSupplyOffset : tokenSupplyOffset+8]),
		TokenIndex:  binary.LittleEndian.Uint64(pkScript[tokenIndexOffset : tokenIndexOffset+8]),
	}
	txo.SensibleData.NFT = nft

	copy(txo.SensibleData.CodeHash[:], GetHash160(pkScript[:scriptLen-dataLen]))
	copy(nft.SensibleId, pkScript[sensibleOffset:sensibleOffset+sensibleIdLen])

	if useTokenIdHash {
		// GenesisId is tokenIdHash
		txo.SensibleData.GenesisIdLen = 20
		copy(txo.SensibleData.GenesisId[:], GetHash160(pkScript[genesisOffset:genesisOffset+genesisIdLen]))
	} else {
		// for search: codehash + genesis
		txo.SensibleData.GenesisIdLen = uint8(sensibleIdLen)
		copy(txo.SensibleData.GenesisId[:], nft.SensibleId)
	}

	nft.MetaOutputIndex = binary.LittleEndian.Uint32(pkScript[metaOutputIndexOffset : metaOutputIndexOffset+4])
	copy(nft.MetaTxId[:], pkScript[metaTxIdOffset:metaTxIdOffset+32])
	ReverseBytesInPlace(nft.MetaTxId[:])

	if pkScript[isGenesisOffset] == 1 {
		nft.TokenIndex = nft.TokenSupply
	}

	txo.HasAddress = true
	copy(txo.AddressPkh[:], pkScript[addressOffset:addressOffset+20])
	return true
}
