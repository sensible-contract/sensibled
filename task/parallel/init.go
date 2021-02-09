package parallel

import (
	"blkparser/model"
	"blkparser/script"
	"encoding/binary"
	"encoding/hex"
)

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxFirst(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {

	for idx, input := range tx.TxIns {
		key := make([]byte, 36)
		copy(key, tx.Hash)
		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		input.InputPoint = key
	}

	for idx, output := range tx.TxOuts {
		// if output.Value == 0 {
		// 	continue
		// }

		key := make([]byte, 36)
		copy(key, tx.Hash)

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)
		output.Outpoint = key

		output.LockingScriptType = script.GetLockingScriptType(output.Pkscript)
		output.LockingScriptTypeHex = hex.EncodeToString(output.LockingScriptType)

		// address
		output.GenesisId, output.AddressPkh = script.ExtractPkScriptAddressPkh(output.Pkscript, output.LockingScriptType)

		if output.AddressPkh == nil {
			output.GenesisId, output.AddressPkh = script.ExtractPkScriptGenesisIdAndAddressPkh(output.Pkscript)
		}
		// test locking script
		output.LockingScriptMatch = true

		// if utils.IsLockingScriptOnlyEqual(output.Pkscript) {
		// 	output.LockingScriptMatch = true
		// }
	}
}
