package serial

import (
	"sensibled/logger"
	"sensibled/model"
	"sensibled/store"

	"go.uber.org/zap"
)

// SyncBlockTx all tx in block height
func SyncBlockTx(block *model.Block) {
	for txIdx, tx := range block.Txs {
		// keep sensible rawtx only
		// prune txraw
		txraw := ""
		if !isTxrawPrune || tx.IsSensible {
			txraw = string(tx.Raw)
		}
		if _, err := store.SyncStmtTx.Exec(
			string(tx.TxId),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			txraw,
			uint32(block.Height),
			uint64(txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("txid", tx.TxIdHex),
				zap.String("err", err.Error()),
			)
		}
	}
}

// MarkConfirmedBlockTx all tx in block height
func MarkConfirmedBlockTx(block *model.Block) {
	model.GlobalConfirmedBlkMap[block.HashHex] = struct{}{}
	model.GlobalConfirmedBlkMap[block.ParentHex] = struct{}{}
	for _, tx := range block.Txs {
		model.GlobalConfirmedTxMap[tx.TxIdHex] = struct{}{}
	}
}
