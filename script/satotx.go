package script

import (
	"blkparser/utils"
	"bytes"
	"encoding/binary"
)

var empty = make([]byte, 1)

func ExtractPkScriptGenesisIdAndAddressPkh(pkscript []byte) (isNFT bool, codeHash, genesisId, addressPkh []byte, value uint64) {
	scriptLen := len(pkscript)
	if scriptLen < 2048 {
		return false, empty, empty, empty, 0
	}
	dataLen := 72
	genesisIdLen := 0
	genesisOffset := scriptLen - 8 - 4
	valueOffset := scriptLen - 8 - 4 - 8
	addressOffset := scriptLen - 8 - 4 - 8 - 20

	if bytes.HasSuffix(pkscript, []byte("sensible")) && pkscript[scriptLen-8-4] == 1 { // PROTO_TYPE == 1
		// new ft
		genesisIdLen = 36
		genesisOffset -= genesisIdLen
		valueOffset -= genesisIdLen
		addressOffset -= genesisIdLen

	} else if bytes.HasSuffix(pkscript, []byte("oraclesv")) && pkscript[scriptLen-8-4] == 1 { // PROTO_TYPE == 1
		// old ft
		genesisIdLen = 20
		genesisOffset -= genesisIdLen
		valueOffset -= genesisIdLen
		addressOffset -= genesisIdLen

	} else if pkscript[scriptLen-1] < 2 && pkscript[scriptLen-37-1] == 37 && pkscript[scriptLen-37-1-40-1] == 40 && pkscript[scriptLen-37-1-40-1-1] == OP_RETURN {
		// nft issue
		isNFT = true
		genesisIdLen = 40
		genesisOffset = scriptLen - 37 - 1 - genesisIdLen
		valueOffset = scriptLen - 1 - 8
		addressOffset = scriptLen - 1 - 8 - 8 - 20

		dataLen = 1 + 1 + 1 + 37 // opreturn+data
	} else if pkscript[scriptLen-1] == 1 && pkscript[scriptLen-61-1] == 61 && pkscript[scriptLen-61-1-40-1] == 40 && pkscript[scriptLen-61-1-40-1-1] == OP_RETURN {
		// nft transfer
		isNFT = true
		genesisIdLen = 40
		genesisOffset = scriptLen - 61 - 1 - genesisIdLen
		valueOffset = scriptLen - 1 - 32 - 8
		addressOffset = scriptLen - 1 - 32 - 8 - 20

		dataLen = 1 + 1 + 1 + 61 // opreturn+data
	} else {
		return false, empty, empty, empty, 0
	}

	genesisId = make([]byte, genesisIdLen)
	addressPkh = make([]byte, 20)
	copy(genesisId, pkscript[genesisOffset:genesisOffset+genesisIdLen])
	copy(addressPkh, pkscript[addressOffset:addressOffset+20])

	value = binary.LittleEndian.Uint64(pkscript[valueOffset : valueOffset+8])

	codeHash = utils.GetHash160(pkscript[:scriptLen-genesisIdLen-dataLen])

	// logger.Log.Info("sensible",
	// 	zap.String("script", hex.EncodeToString(pkscript)),
	// 	zap.String("hash", hex.EncodeToString(codeHash)),
	// 	zap.String("genesis", hex.EncodeToString(genesisId)),
	// 	zap.String("address", hex.EncodeToString(addressPkh)),
	// 	zap.Bool("nft", isNFT),
	// 	zap.Uint64("v", value),
	// 	zap.Uint8("type", pkscript[scriptLen-1]),
	// )

	return isNFT, codeHash, genesisId, addressPkh, value
}
