package serial

import (
	"sensibled/logger"
	"sensibled/model"
	"sensibled/store"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// SyncBlockTxContract all tx in block height
func SyncBlockTxContract(block *model.Block) {
	for txIdx, tx := range block.Txs {
		if txIdx == 0 { // skip coinbase
			continue
		}

		var swapIn *scriptDecoder.TxoData
		var swapOut *scriptDecoder.TxoData
		for _, input := range tx.TxIns {
			objData, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]
			if !ok {
				continue
			}
			if objData.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
				if objData.Data.Uniq.Swap != nil {
					swapIn = objData.Data
					break
				}
			}
		}
		if swapIn == nil {
			continue
		}

		for _, output := range tx.TxOuts {
			if output.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
				if output.Data.Uniq.Swap != nil {
					swapOut = output.Data
					break
				}
			}
		}
		if swapOut == nil {
			continue
		}

		operation := 0 // 0: sell, 1: buy, 2: add, 3: remove
		if swapIn.Uniq.Swap.Token1Amount < swapOut.Uniq.Swap.Token1Amount {
			if swapIn.Uniq.Swap.Token2Amount < swapOut.Uniq.Swap.Token2Amount {
				operation = 2 // add
			} else {
				operation = 1 // buy
			}
		} else {
			if swapIn.Uniq.Swap.Token2Amount < swapOut.Uniq.Swap.Token2Amount {
				operation = 0 // sell
			} else {
				operation = 3 // remove
			}
		}

		if _, err := store.SyncStmtTxContract.Exec(
			uint32(block.Height),
			block.BlockTime,
			string(swapOut.CodeHash[:]),
			string(swapOut.GenesisId[:swapOut.GenesisIdLen]),
			swapOut.CodeType,
			uint32(operation),
			swapIn.Uniq.Swap.Token1Amount,
			swapIn.Uniq.Swap.Token2Amount,
			swapIn.Uniq.Swap.LpAmount,
			swapOut.Uniq.Swap.Token1Amount,
			swapOut.Uniq.Swap.Token2Amount,
			swapOut.Uniq.Swap.LpAmount,
			string(block.Hash),
			uint64(txIdx),
			string(tx.TxId),
		); err != nil {
			logger.Log.Info("sync-tx-contract-err",
				zap.String("txid", tx.TxIdHex),
				zap.String("err", err.Error()),
			)
		}
	}
}
