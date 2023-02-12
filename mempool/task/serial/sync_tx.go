package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"

	"go.uber.org/zap"
)

// SyncBlockTx all tx in block height
func SyncBlockTx(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		if _, err := store.SyncStmtTx.Exec(
			string(tx.TxId),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.WitOffset,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			string(tx.Raw),
			model.MEMPOOL_HEIGHT, // uint32(block.Height),
			uint64(startIdx+txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("sync", "tx err"),
				zap.String("txid", tx.TxIdHex),
				zap.String("err", err.Error()),
			)
		}
	}
}
