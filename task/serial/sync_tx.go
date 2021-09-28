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
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			txraw, // string(tx.Raw)
			uint32(block.Height),
			string(block.Hash),
			uint64(txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("txid", tx.HashHex),
				zap.String("err", err.Error()),
			)
		}
	}
}

// MarkConfirmedBlockTx all tx in block height
func MarkConfirmedBlockTx(block *model.Block) {
	for _, tx := range block.Txs {
		model.GlobalConfirmedTxMap[tx.HashHex] = true
	}
}
