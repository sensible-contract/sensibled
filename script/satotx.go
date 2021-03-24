package script

import (
	"blkparser/utils"
	"bytes"
	"encoding/binary"
)

var empty = make([]byte, 1)

func ExtractPkScriptGenesisIdAndAddressPkh(pkscript []byte) (codeHash, genesisId, addressPkh []byte, amount uint64) {
	scriptLen := len(pkscript)
	if scriptLen < 1024 {
		return empty, empty, empty, 0
	}

	genesisIdLen := 0
	if bytes.HasSuffix(pkscript, []byte("sensible")) {
		genesisIdLen = 36
	} else if bytes.HasSuffix(pkscript, []byte("oraclesv")) {
		genesisIdLen = 20
	} else {
		return empty, empty, empty, 0
	}

	genesisOffset := scriptLen - 8 - 4 - genesisIdLen
	amountOffset := scriptLen - 8 - 4 - genesisIdLen - 8
	addressOffset := scriptLen - 8 - 4 - genesisIdLen - 8 - 20

	// PROTO_TYPE == 1
	if pkscript[scriptLen-8-4] != 1 {
		return empty, empty, empty, 0
	}

	genesisId = make([]byte, genesisIdLen)
	addressPkh = make([]byte, 20)
	copy(genesisId, pkscript[genesisOffset:genesisOffset+genesisIdLen])
	copy(addressPkh, pkscript[addressOffset:addressOffset+20])

	amount = binary.LittleEndian.Uint64(pkscript[amountOffset : amountOffset+8])

	codeHash = utils.GetHash160(pkscript[:scriptLen-genesisIdLen-72])

	return genesisId, codeHash, addressPkh, amount
}
