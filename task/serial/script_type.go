package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// dumpLockingScriptType  信息
func dumpLockingScriptType(block *model.Block) {
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
