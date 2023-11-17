package serial

import (
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/store"

	"go.uber.org/zap"
)

// SyncBlockTxContract all tx in block height
func SyncBlockTxContract(block *model.Block) {
	for txIdx, tx := range block.Txs {
		if txIdx == 0 { // skip coinbase
			continue
		}

		var swapIn *scriptDecoder.AddressData
		var swapOut *scriptDecoder.AddressData
		for _, input := range tx.TxIns {
			objData, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]
			if !ok {
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
			uint32(block.Height),
			block.BlockTime,
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
