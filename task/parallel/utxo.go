package parallel

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// parseUtxoParallel utxo 信息
func parseUtxoParallel(tx *model.Tx, block *model.ProcessBlock) {
	for idx, output := range tx.TxOuts {
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

		utils.Log.Debug("utxo",
			zap.String("tx", tx.HashHex),
			zap.Int("vout", idx),
			zap.Uint64("v", output.Value),
			zap.String("type", output.LockingScriptTypeHex),
		)
	}
}

// parseTxoSpendByTxParallel utxo被使用
func parseTxoSpendByTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	if isCoinbase {
		return
	}
	for idx, input := range tx.TxIns {
		if _, ok := block.UtxoMap[input.InputOutpointKey]; !ok {
			block.UtxoMissingMap[input.InputOutpointKey] = true
		} else {
			delete(block.UtxoMap, input.InputOutpointKey)
		}

		utils.Log.Debug("spend",
			zap.String("tx", input.InputHashHex),
			zap.Uint32("vout", input.InputVout),
			zap.Int("idx", idx),
		)
	}
	utils.Log.Debug("by",
		zap.String("tx", tx.HashHex),
	)
}
