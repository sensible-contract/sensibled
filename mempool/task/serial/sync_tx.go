package serial

import (
	"satoblock/logger"
	"satoblock/mempool/store"
	"satoblock/model"

	"go.uber.org/zap"
)

// SyncBlockTx all tx in block height
func SyncBlockTx(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		if _, err := store.SyncStmtTx.Exec(
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			string(tx.Raw),
			model.MEMPOOL_HEIGHT, // uint32(block.Height),
			"",                   // string(block.Hash),
			uint64(startIdx+txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("sync", "tx err"),
				zap.String("txid", tx.HashHex),
				zap.String("err", err.Error()),
			)
		}
	}
}
