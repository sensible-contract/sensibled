package serial

import (
	"encoding/binary"
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/prune"
	"sensibled/store"
	"sensibled/utils"

	"go.uber.org/zap"
)

// UpdateBlockTxInputDetail all tx input info
func UpdateBlockTxInputDetail(block *model.Block) {
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

			address := ""
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if !prune.IsHistoryPrune && objData.AddressData.HasAddress {
				block.ParseData.AddrPkhInTxMap[address] = append(block.ParseData.AddrPkhInTxMap[address], txIdx)
			}

			// set sensible flag
			if objData.AddressData.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}
		}
	}
}

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

			address := ""
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// 解锁脚本一般可安全清理
			scriptsig := ""
			if !prune.IsScriptSigPrune {
				scriptsig = string(input.ScriptSig)
			}

			// 清理非sensible且无地址的锁定脚本
			pkscript := ""
			if !prune.IsPkScriptPrune || tx.IsSensible || objData.AddressData.HasAddress {
				pkscript = string(objData.PkScript)
			}

			codehash := ""
			genesis := ""
			if objData.AddressData.CodeType != scriptDecoder.CodeType_NONE && objData.AddressData.CodeType != scriptDecoder.CodeType_SENSIBLE {
				codehash = string(objData.AddressData.SensibleData.CodeHash[:])                                              // 20 bytes
				genesis = string(objData.AddressData.SensibleData.GenesisId[:objData.AddressData.SensibleData.GenesisIdLen]) // 20/36/40 bytes
			}

			var dataValue uint64
			var tokenIndex uint64
			var decimal uint8
			// token summary
			if objData.AddressData.CodeType != scriptDecoder.CodeType_NONE && objData.AddressData.CodeType != scriptDecoder.CodeType_SENSIBLE {
				buf := make([]byte, 12, 12+20+40)
				binary.LittleEndian.PutUint32(buf, objData.AddressData.CodeType)

				if objData.AddressData.CodeType == scriptDecoder.CodeType_NFT {
					binary.LittleEndian.PutUint64(buf[4:], objData.AddressData.SensibleData.NFT.TokenIndex)
					tokenIndex = objData.AddressData.SensibleData.NFT.TokenIndex
					dataValue = tokenIndex
				} else if objData.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
					binary.LittleEndian.PutUint64(buf[4:], objData.AddressData.SensibleData.NFTSell.TokenIndex)
					tokenIndex = objData.AddressData.SensibleData.NFTSell.TokenIndex
					dataValue = tokenIndex
				} else if objData.AddressData.CodeType == scriptDecoder.CodeType_FT {
					decimal = objData.AddressData.SensibleData.FT.Decimal
					dataValue = objData.AddressData.SensibleData.FT.Amount
				}

				buf = append(buf, objData.AddressData.SensibleData.CodeHash[:]...)
				buf = append(buf, objData.AddressData.SensibleData.GenesisId[:objData.AddressData.SensibleData.GenesisIdLen]...)

				tokenKey := string(buf)
				// skip if no db write
				tokenSummary, ok := block.ParseData.TokenSummaryMap[tokenKey]
				if !ok {
					tokenSummary = &model.TokenData{
						CodeType:  objData.AddressData.CodeType,
						NFTIdx:    tokenIndex,
						Decimal:   decimal,
						CodeHash:  objData.AddressData.SensibleData.CodeHash[:],
						GenesisId: objData.AddressData.SensibleData.GenesisId[:objData.AddressData.SensibleData.GenesisIdLen],
					}
					block.ParseData.TokenSummaryMap[tokenKey] = tokenSummary
				}

				tokenSummary.InSatoshi += objData.Satoshi
				tokenSummary.InDataValue += 1
			}

			if _, err := store.SyncStmtTxIn.Exec(
				uint32(block.Height),
				uint64(txIdx),
				string(tx.TxId),
				uint32(vin),
				scriptsig, // prune string(input.ScriptSig),
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
				pkscript,
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
