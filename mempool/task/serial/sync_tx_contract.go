package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"

	"go.uber.org/zap"
)

// SyncBlockTxContract all tx in block height
func SyncBlockTxContract(startIdx int, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData) {
	for txIdx, tx := range txs {
		var swapIn *scriptDecoder.AddressData
		var swapOut *scriptDecoder.AddressData
		for _, input := range tx.TxIns {
			var objData *model.TxoData
			if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				continue
			}
			if objData.AddressData.CodeType == scriptDecoder.CodeType_UNIQUE {
				if objData.AddressData.SensibleData.Uniq.Swap != nil {
					swapIn = objData.AddressData
					break
				}
			}
		}
		if swapIn == nil {
			continue
		}

		for _, output := range tx.TxOuts {
			if output.AddressData.CodeType == scriptDecoder.CodeType_UNIQUE {
				if output.AddressData.SensibleData.Uniq.Swap != nil {
					swapOut = output.AddressData
					break
				}
			}
		}
		if swapOut == nil {
			continue
		}

		operation := 0 // 0: sell, 1: buy, 2: add, 3: remove
		if swapIn.SensibleData.Uniq.Swap.Token1Amount < swapOut.SensibleData.Uniq.Swap.Token1Amount {
			if swapIn.SensibleData.Uniq.Swap.Token2Amount < swapOut.SensibleData.Uniq.Swap.Token2Amount {
				operation = 2 // add
			} else {
				operation = 1 // buy
			}
		} else {
			if swapIn.SensibleData.Uniq.Swap.Token2Amount < swapOut.SensibleData.Uniq.Swap.Token2Amount {
				operation = 0 // sell
			} else {
				operation = 3 // remove
			}
		}

		if _, err := store.SyncStmtTxContract.Exec(
			model.MEMPOOL_HEIGHT, // uint32(block.Height),
			0,                    // block.BlockTime,
			string(swapOut.SensibleData.CodeHash[:]),
			string(swapOut.SensibleData.GenesisId[:swapOut.SensibleData.GenesisIdLen]),
			swapOut.CodeType,
			uint32(operation),
			swapIn.SensibleData.Uniq.Swap.Token1Amount,
			swapIn.SensibleData.Uniq.Swap.Token2Amount,
			swapIn.SensibleData.Uniq.Swap.LpAmount,
			swapOut.SensibleData.Uniq.Swap.Token1Amount,
			swapOut.SensibleData.Uniq.Swap.Token2Amount,
			swapOut.SensibleData.Uniq.Swap.LpAmount,
			"", //string(block.Hash),
			uint64(startIdx+txIdx),
			string(tx.TxId),
		); err != nil {
			logger.Log.Info("sync-tx-contract-err",
				zap.String("txid", tx.TxIdHex),
				zap.String("err", err.Error()),
			)
		}
	}
}
