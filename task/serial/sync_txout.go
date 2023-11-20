package serial

import (
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/prune"
	"sensibled/store"

	"go.uber.org/zap"
)

// UpdateBlockTxOutputInfo all tx output info
func UpdateBlockTxOutputInfo(block *model.Block) {
	for _, tx := range block.Txs {
		for _, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi
			// set sensible flag
			if output.AddressData.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}
		}
	}
}

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(block *model.Block) {
	for txIdx, tx := range block.Txs {
		for vout, output := range tx.TxOuts {
			// prune string(output.Pkscript),
			pkscript := ""
			if !prune.IsPkScriptPrune || tx.IsSensible || output.AddressData.HasAddress {
				pkscript = string(output.PkScript)
			}

			// prune false opreturn output
			if prune.IsOpReturnPrune && !tx.IsSensible && scriptDecoder.IsFalseOpreturn(output.ScriptType) {
				pkscript = string(model.FALSE_OP_RETURN)
			}

			address := ""
			codehash := ""
			genesis := ""
			if output.AddressData.HasAddress {
				address = string(output.AddressData.AddressPkh[:]) // 20 bytes
			}
			if output.AddressData.CodeType != scriptDecoder.CodeType_NONE && output.AddressData.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(output.AddressData.SensibleData.CodeHash[:])                                             // 20 bytes
				genesis = string(output.AddressData.SensibleData.GenesisId[:output.AddressData.SensibleData.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			if output.AddressData.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.AddressData.SensibleData.NFT.TokenIndex
			} else if output.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
				dataValue = output.AddressData.SensibleData.NFTSell.TokenIndex
			} else if output.AddressData.CodeType == scriptDecoder.CodeType_FT {
				dataValue = output.AddressData.SensibleData.FT.Amount
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
