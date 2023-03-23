package serial

import (
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/prune"
	"unisatd/store"
	"unisatd/utils"

	"go.uber.org/zap"
)

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(block *model.Block) {
	var commonObjData *model.TxoData = &model.TxoData{
		Satoshi:     utils.CalcBlockSubsidy(block.Height),
		AddressData: &scriptDecoder.AddressData{},
	}

	for txIdx, tx := range block.Txs {
		isCoinbase := (txIdx == 0)

		for vin, input := range tx.TxIns {
			objData := commonObjData
			if !isCoinbase {
				objData.Satoshi = 0
				if obj, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]; ok {
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
			}
			tx.InputsValue += objData.Satoshi
			tx.NFTInputsCnt += uint64(len(input.CreatePointOfNFTs))

			address := ""
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if !prune.IsHistoryPrune && objData.AddressData.HasAddress {
				block.ParseData.AddrPkhInTxMap[address] = append(block.ParseData.AddrPkhInTxMap[address], txIdx)
			}

			// 解锁脚本一般可安全清理
			scriptsig := ""
			if !prune.IsScriptSigPrune {
				scriptsig = string(input.ScriptSig)
			}
			scriptwits := ""
			if !prune.IsScriptSigPrune {
				scriptwits = string(input.ScriptWitness)
			}

			// 清理非sensible且无地址的锁定脚本
			pkscript := ""
			if !prune.IsPkScriptPrune || tx.GenesisNewNFT || objData.AddressData.HasAddress {
				pkscript = string(objData.PkScript)
			}

			nftPointsBuf := make([]byte, len(input.CreatePointOfNFTs)*3*8)
			model.DumpNFTCreatePoints(nftPointsBuf, input.CreatePointOfNFTs)

			if _, err := store.SyncStmtTxIn.Exec(
				uint32(block.Height),
				uint64(txIdx),
				string(tx.TxId),
				uint32(vin),
				scriptsig, // prune string(input.ScriptSig),
				scriptwits,
				uint32(input.Sequence),

				uint64(input.CreatePointCountOfNewNFTs), // new nft count

				uint32(objData.BlockHeight),
				uint64(objData.TxIdx),
				string(input.InputHash),
				input.InputVout,
				address,
				uint32(objData.AddressData.CodeType),
				objData.Satoshi,
				string(objData.ScriptType),
				pkscript,

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
