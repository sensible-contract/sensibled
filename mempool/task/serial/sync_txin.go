package serial

import (
	"unisatd/logger"
	"unisatd/mempool/store"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"

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
			tx.NFTInputsCnt += uint64(len(input.CreatePointOfNFTs))

			address := ""
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if objData.AddressData.HasAddress {
				addrPkhInTxMap[address] = append(addrPkhInTxMap[address], startIdx+txIdx)
			}

			nftPointsBuf := make([]byte, len(input.CreatePointOfNFTs)*3*8)
			model.DumpNFTCreatePoints(nftPointsBuf, input.CreatePointOfNFTs)

			if _, err := store.SyncStmtTxIn.Exec(
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
				string(tx.TxId),
				uint32(vin),
				string(input.ScriptSig),
				string(input.ScriptWitness),
				uint32(input.Sequence),

				uint64(len(input.CreatePointOfNewNFTs)), // new nft count

				uint32(objData.BlockHeight),
				uint64(objData.TxIdx),
				string(input.InputHash),
				input.InputVout,
				address,
				uint32(objData.AddressData.CodeType),
				objData.Satoshi,
				string(objData.ScriptType),
				string(objData.PkScript),
				// nft
				uint64(len(input.CreatePointOfNFTs)),
				string(nftPointsBuf),
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
