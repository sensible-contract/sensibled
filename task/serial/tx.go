package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

var (
	lastUtxoMapAddCount    int
	lastUtxoMapRemoveCount int
)

// ParseUtxoSerial utxo 信息
func ParseUtxoSerial(block *model.ProcessBlock) {
	insideTxo := make([]string, len(block.UtxoMissingMap))
	for key := range block.UtxoMissingMap {
		if _, ok := block.UtxoMap[key]; !ok {
			continue
		}
		insideTxo = append(insideTxo, key)
	}
	for _, key := range insideTxo {
		delete(block.UtxoMap, key)
		delete(block.UtxoMissingMap, key)
	}

	lastUtxoMapAddCount += len(block.UtxoMap)
	lastUtxoMapRemoveCount += len(block.UtxoMissingMap)

	for key, data := range block.UtxoMap {
		utxoMap[key] = data
	}
	for key := range block.UtxoMissingMap {
		delete(utxoMap, key)
	}
}

// DumpLockingScriptType  信息
func DumpLockingScriptType(block *model.Block) {
	for _, tx := range block.Txs {
		for idx, output := range tx.TxOuts {
			if output.Value == 0 || !output.LockingScriptMatch {
				continue
			}

			key := string(output.LockingScriptType)

			if data, ok := calcMap[key]; ok {
				data.Value += 1
				calcMap[key] = data
			} else {
				calcMap[key] = model.CalcData{Value: 1}
			}

			utils.Log.Info("pkscript",
				zap.String("tx", tx.HashHex),
				zap.Int("vout", idx),
				zap.Uint64("v", output.Value),
				zap.String("type", output.LockingScriptTypeHex),
			)
		}
	}
}
