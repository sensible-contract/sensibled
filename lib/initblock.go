package blkparser

import (
	"encoding/binary"
)

func initTx(tx *Tx) {
	key := make([]byte, 36)
	copy(key, tx.Hash)
	for idx, output := range tx.TxOuts {
		if output.Value == 0 {
			continue
		}

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)

		// test locking script
		output.LockingScriptMatch = true

		// if isLockingScriptOnlyEqual(output.Pkscript) {
		// 	output.LockingScriptMatch = true
		// }
	}
}

func isLockingScriptOnlyEqual(pkscript []byte) bool {
	// test locking script
	// "0b 3c4b616e7965323032303e 87"

	length := len(pkscript)
	if length == 0 {
		return true
	}
	if pkscript[length-1] != 0x87 {
		return false
	}
	cnt, cntsize := SafeDecodeVariableLengthInteger(pkscript)
	if length == int(cnt+cntsize+1) {
		return true
	}
	return false
}
