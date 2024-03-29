package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(startIdx int, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData, addrPkhInTxMap map[string][]int) {
	var commonObjData *model.TxoData = &model.TxoData{
		Data: &scriptDecoder.TxoData{},
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
					zap.String("txid", tx.TxIdHex),
					zap.Int("vin", vin),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
			}
			tx.InputsValue += objData.Satoshi

			address := ""
			if objData.Data.HasAddress {
				address = string(objData.Data.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if objData.Data.HasAddress {
				addrPkhInTxMap[address] = append(addrPkhInTxMap[address], startIdx+txIdx)
			}

			codehash := ""
			genesis := ""
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

			if _, err := store.SyncStmtTxIn.Exec(
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
				string(tx.TxId),
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
					zap.String("txid", tx.TxIdHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
