package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"

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
			if output.AddressData.HasAddress {
				address = string(output.AddressData.AddressPkh[:]) // 20 bytes
			}
			if output.AddressData.CodeType != scriptDecoder.CodeType_NONE && output.AddressData.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(output.AddressData.CodeHash[:])                                // 20 bytes
				genesis = string(output.AddressData.GenesisId[:output.AddressData.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			if output.AddressData.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.AddressData.NFT.TokenIndex
			} else if output.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
				dataValue = output.AddressData.NFTSell.TokenIndex
			} else if output.AddressData.CodeType == scriptDecoder.CodeType_FT {
				dataValue = output.AddressData.FT.Amount
			}

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.TxId),
				uint32(vout),
				address,
				codehash,
				genesis,
				uint32(output.AddressData.CodeType),
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
