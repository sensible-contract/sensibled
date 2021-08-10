package serial

import (
	"satoblock/logger"
	"satoblock/mempool/store"
	"satoblock/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

var (
	SyncTxFullCount     int
	SyncTxCodeHashCount int
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

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(startIdx int, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData) {
	var commonObjData *model.TxoData = &model.TxoData{}

	for txIdx, tx := range txs {
		for vin, input := range tx.TxIns {
			objData := commonObjData
			if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				logger.Log.Info("tx-input-err",
					zap.String("txin", "input missing utxo"),
					zap.String("txid", tx.HashHex),
					zap.Int("vin", vin),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
			}
			tx.InputsValue += objData.Satoshi

			address := ""
			codehash := ""
			genesis := ""
			if objData.Data.HasAddress {
				address = string(objData.Data.AddressPkh[:]) // 20 bytes
			}
			if objData.Data.CodeType != scriptDecoder.CodeType_NONE && objData.Data.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(objData.Data.CodeHash[:])                          // 20 bytes
				genesis = string(objData.Data.GenesisId[:objData.Data.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			if objData.Data.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = objData.Data.NFT.TokenIndex
			} else if objData.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
				dataValue = objData.Data.NFTSell.TokenIndex
			} else if objData.Data.CodeType == scriptDecoder.CodeType_FT {
				dataValue = objData.Data.FT.Amount
			}

			SyncTxFullCount++
			if _, err := store.SyncStmtTxIn.Exec(
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
				string(tx.Hash),
				uint32(vin),
				string(input.ScriptSig),
				uint32(input.Sequence),

				uint32(objData.BlockHeight),
				uint64(objData.TxIdx),
				string(input.InputHash),
				input.InputVout,
				address,
				codehash,
				genesis,
				uint32(objData.Data.CodeType),
				dataValue,
				objData.Satoshi,
				string(objData.ScriptType),
				string(objData.PkScript),
			); err != nil {
				logger.Log.Info("sync-txin-full-err",
					zap.String("sync", "txin full err"),
					zap.String("txid", tx.HashHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
