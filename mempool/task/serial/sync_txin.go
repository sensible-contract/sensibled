package serial

import (
	"sensibled/logger"
	"sensibled/mempool/store"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"

	"go.uber.org/zap"
)

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(startIdx int, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData, addrPkhInTxMap map[string][]int) {
	var commonObjData *model.TxoData = &model.TxoData{
		AddressData: &scriptDecoder.AddressData{},
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
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if objData.AddressData.HasAddress {
				addrPkhInTxMap[address] = append(addrPkhInTxMap[address], startIdx+txIdx)
			}

			codehash := ""
			genesis := ""
			if objData.AddressData.CodeType != scriptDecoder.CodeType_NONE && objData.AddressData.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(objData.AddressData.CodeHash[:])                                 // 20 bytes
				genesis = string(objData.AddressData.GenesisId[:objData.AddressData.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			if objData.AddressData.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = objData.AddressData.NFT.TokenIndex
			} else if objData.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
				dataValue = objData.AddressData.NFTSell.TokenIndex
			} else if objData.AddressData.CodeType == scriptDecoder.CodeType_FT {
				dataValue = objData.AddressData.FT.Amount
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
				uint32(objData.AddressData.CodeType),
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
