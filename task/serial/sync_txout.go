package serial

import (
	"sensibled/logger"
	"sensibled/model"
	"sensibled/prune"
	"sensibled/store"
	"time"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(block *model.Block) {
	for txIdx, tx := range block.Txs {
		for model.NeedPause {
			logger.Log.Info("SyncBlockTxOutputInfo pause ...")
			time.Sleep(5 * time.Second)
		}

		for _, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi
			// set sensible flag
			if output.Data.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}
		}

		for vout, output := range tx.TxOuts {
			// prune false opreturn output
			if prune.IsOpReturnPrune && !tx.IsSensible && scriptDecoder.IsFalseOpreturn(output.ScriptType) {
				continue
			}

			// prune string(output.Pkscript),
			pkscript := ""
			if !prune.IsPkScriptPrune || tx.IsSensible || output.Data.HasAddress {
				pkscript = string(output.PkScript)
			}

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
				string(tx.TxId),
				uint32(vout),
				address,
				codehash,
				genesis,
				uint32(output.Data.CodeType),
				dataValue,
				output.Satoshi,
				string(output.ScriptType),
				pkscript,
				uint32(block.Height),
				uint64(txIdx),
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
