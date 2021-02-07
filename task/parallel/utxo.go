package parallel

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// ParseUtxoParallel utxo 信息
func ParseUtxoParallel(tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.Value == 0 || !output.LockingScriptMatch {
			continue
		}

		if _, ok := block.UtxoMissingMap[output.OutpointKey]; ok {
			delete(block.UtxoMissingMap, output.OutpointKey)
		} else {
			block.UtxoMap[output.OutpointKey] = model.CalcData{
				Value:       output.Value,
				ScriptType:  output.LockingScriptTypeHex,
				BlockHeight: block.Height,
			}
		}

		utils.Log.Info("tx-output-info",
			zap.Object("utxoPoint", output.Outpoint),
			zap.Object("address", output.AddressPkh), // 20 byte
			zap.Object("genesis", output.GenesisId),  // 20 byte
			zap.Uint64("value", output.Value),
			zap.Object("scriptType", output.LockingScriptType),
			zap.Object("script", output.Pkscript),
		)
	}
}

// ParseTxoSpendByTxParallel utxo被使用
func ParseTxoSpendByTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	for _, input := range tx.TxIns {
		utils.Log.Info("tx-input-info",
			zap.Object("txidIdx", input.InputPoint),
			zap.Object("utxoPoint", input.InputOutpoint),
			zap.Object("script", input.ScriptSig),
		)
	}
	if isCoinbase {
		return
	}
	for _, input := range tx.TxIns {
		if _, ok := block.UtxoMap[input.InputOutpointKey]; !ok {
			block.UtxoMissingMap[input.InputOutpointKey] = true
		} else {
			delete(block.UtxoMap, input.InputOutpointKey)
		}

		utils.Log.Info("tx-output-info",
			zap.Object("utxoPoint", input.InputOutpoint),
			zap.Object("spendByTxidIdx", input.InputPoint),
			zap.Bool("utxo", false), // spent
		)
	}
}
