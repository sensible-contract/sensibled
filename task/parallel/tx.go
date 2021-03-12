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
		// output.LockingScriptMatch = true

		if !script.IsOpreturn(output.LockingScriptType) {
			output.LockingScriptMatch = true
		}
	}
}

// ParseTxoSpendByTxParallel utxo被使用
func ParseTxoSpendByTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	if isCoinbase {
		return
	}
	for _, input := range tx.TxIns {
		block.SpentUtxoKeysMap[input.InputOutpointKey] = true

		// if _, ok := block.UtxoMap[input.InputOutpointKey]; !ok {
		// 	block.UtxoMissingMap[input.InputOutpointKey] = true
		// } else {
		// 	delete(block.UtxoMap, input.InputOutpointKey)
		// }
	}
}

// ParseUtxoParallel utxo 信息
func ParseUtxoParallel(txIdx int, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.Value == 0 || !output.LockingScriptMatch {
			continue
		}

		block.NewUtxoDataMap[output.OutpointKey] = model.CalcData{
			BlockHeight: block.Height,
			TxIdx:       uint64(txIdx),
			AddressPkh:  output.AddressPkh,
			GenesisId:   output.GenesisId,
			Value:       output.Value,
			ScriptType:  output.LockingScriptType,
			Script:      output.Pkscript,
		}

		// if _, ok := block.UtxoMissingMap[output.OutpointKey]; ok {
		// 	delete(block.UtxoMissingMap, output.OutpointKey)
		// } else {
		// 	block.UtxoMap[output.OutpointKey] = model.CalcData{
		// 		BlockHeight: block.Height,
		// 		Value:       output.Value,
		// 		ScriptType:  output.LockingScriptType,
		// 		AddressPkh:  output.AddressPkh,
		// 		GenesisId:   output.GenesisId,
		// 	}
		// }
	}
}
