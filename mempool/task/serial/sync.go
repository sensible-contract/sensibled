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

			var dataValue uint64
			if output.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.TokenIndex
			} else if output.CodeType == scriptDecoder.CodeType_FT {
				dataValue = output.Amount
			}

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.Hash),
				uint32(vout),
				string(output.AddressPkh), // 20 bytes
				string(output.CodeHash),   // 20 bytes
				string(output.GenesisId),  // 20/36/40 bytes
				uint32(output.CodeType),
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
	var commonObjData *model.TxoData = &model.TxoData{
		CodeHash:   make([]byte, 1),
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
		Satoshi:    0,
	}

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
			var dataValue uint64
			if objData.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = objData.TokenIndex
			} else if objData.CodeType == scriptDecoder.CodeType_FT {
				dataValue = objData.Amount
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
				string(objData.AddressPkh), // 20 byte
				string(objData.CodeHash),   // 20 byte
				string(objData.GenesisId),  // 20 byte
				uint32(objData.CodeType),
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
