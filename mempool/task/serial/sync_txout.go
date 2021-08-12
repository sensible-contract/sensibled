package serial

import (
	"satoblock/logger"
	"satoblock/mempool/store"
	"satoblock/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			address := ""
			codehash := ""
			genesis := ""
			if output.Data.HasAddress {
				address = string(output.Data.AddressPkh[:]) // 20 bytes
			}
			if output.Data.CodeType != scriptDecoder.CodeType_NONE && output.Data.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(output.Data.CodeHash[:])                         // 20 bytes
				genesis = string(output.Data.GenesisId[:output.Data.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			if output.Data.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.Data.NFT.TokenIndex
			} else if output.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
				dataValue = output.Data.NFTSell.TokenIndex
			} else if output.Data.CodeType == scriptDecoder.CodeType_FT {
				dataValue = output.Data.FT.Amount
			}

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.Hash),
				uint32(vout),
				address,
				codehash,
				genesis,
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
					zap.String("utxid", tx.HashHex),
					zap.Uint32("vout", uint32(vout)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
