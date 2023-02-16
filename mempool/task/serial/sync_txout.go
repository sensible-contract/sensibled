package serial

import (
	"unisatd/logger"
	"unisatd/mempool/store"
	"unisatd/model"

	"go.uber.org/zap"
)

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			address := ""
			if output.AddressData.HasAddress {
				address = string(output.AddressData.AddressPkh[:]) // 20 bytes
			}

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.TxId),
				uint32(vout),
				address,
				uint32(output.AddressData.CodeType),
				output.Satoshi,
				string(output.ScriptType),
				string(output.PkScript),
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
			); err != nil {
				logger.Log.Info("sync-txout-err",
					zap.String("sync", "txout err"),
					zap.String("utxid", tx.TxIdHex),
					zap.Uint32("vout", uint32(vout)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
