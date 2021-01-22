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

		// "0b 3c4b616e7965323032303e 87"
		length := len(output.Pkscript)
		if length == 0 {
			output.ScriptIsOnlyEqual = true
			continue
		}
		if output.Pkscript[length-1] != 0x87 {
			continue
		}
		cnt, cntsize := SafeDecodeVariableLengthInteger(output.Pkscript)
		if length == int(cnt+cntsize+1) {
			output.ScriptIsOnlyEqual = true
		}
	}
}
