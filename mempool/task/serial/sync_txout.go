package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"

	"go.uber.org/zap"
)

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			address := ""
			if output.Data.HasAddress {
				address = string(output.Data.AddressPkh[:]) // 20 bytes
			}

			var dataValue uint64
			if output.Data.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.Data.NFT.TokenIndex
			}

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.TxId),
				uint32(vout),
				address,
				uint32(output.Data.CodeType),
				dataValue,
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
