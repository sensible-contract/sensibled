package parallel

import (
	"blkparser/model"
	"blkparser/script"
	"encoding/binary"
	"encoding/hex"
	"strconv"
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
		// if output.Satoshi == 0 {
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
		output.IsNFT, output.CodeHash, output.GenesisId, output.AddressPkh, output.DataValue = script.ExtractPkScriptForTxo(output.Pkscript, output.LockingScriptType)

		// test locking script
		// output.LockingScriptMatch = true

		// token summary
		if len(output.CodeHash) == 20 && len(output.GenesisId) > 32 {
			NFTIdx := uint64(0)
			key := string(output.CodeHash) + string(output.GenesisId)
			if output.IsNFT {
				key += strconv.Itoa(int(output.DataValue))
				NFTIdx = output.DataValue
			}
			tokenSummary, ok := block.TokenSummaryMap[key]
			if !ok {
				tokenSummary = &model.TokenData{
					IsNFT:     output.IsNFT,
					NFTIdx:    NFTIdx,
					CodeHash:  output.CodeHash,
					GenesisId: output.GenesisId,
				}
				block.TokenSummaryMap[key] = tokenSummary
			}

			tokenSummary.OutSatoshi += output.Satoshi
			if output.IsNFT {
				tokenSummary.OutDataValue += 1
			} else {
				tokenSummary.OutDataValue += output.DataValue
			}
		}

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

// ParseNewUtxoInTxParallel utxo 信息
func ParseNewUtxoInTxParallel(txIdx int, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.Satoshi == 0 || !output.LockingScriptMatch {
			continue
		}

		d := model.TxoDataPool.Get().(*model.TxoData)
		d.BlockHeight = block.Height
		d.TxIdx = uint64(txIdx)
		d.AddressPkh = output.AddressPkh
		d.CodeHash = output.CodeHash
		d.GenesisId = output.GenesisId
		d.DataValue = output.DataValue
		d.Satoshi = output.Satoshi
		d.ScriptType = output.LockingScriptType
		d.Script = output.Pkscript

		block.NewUtxoDataMap[output.OutpointKey] = d

		// if _, ok := block.UtxoMissingMap[output.OutpointKey]; ok {
		// 	delete(block.UtxoMissingMap, output.OutpointKey)
		// } else {
		// 	block.UtxoMap[output.OutpointKey] = model.TxoData{
		// 		BlockHeight: block.Height,
		// 		Satoshi:     output.Satoshi,
		// 		ScriptType:  output.LockingScriptType,
		// 		AddressPkh:  output.AddressPkh,
		// 		GenesisId:   output.GenesisId,
		// 	}
		// }
	}
}
