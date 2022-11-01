package serial

import (
	"encoding/binary"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/store"
	"sensibled/utils"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(block *model.Block) {
	var commonObjData *model.TxoData = &model.TxoData{
		Satoshi: utils.CalcBlockSubsidy(block.Height),
		Data:    &scriptDecoder.TxoData{},
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

			// address tx历史记录
			if objData.Data.HasAddress {
				block.ParseData.AddrPkhInTxMap[txIdx][string(objData.Data.AddressPkh[:])] = struct{}{}
			}

			// set sensible flag
			if objData.Data.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}

			// 解锁脚本一般可安全清理
			scriptsig := ""
			if !isScriptSigPrune {
				scriptsig = string(input.ScriptSig)
			}

			// 清理非sensible且无地址的锁定脚本
			pkscript := ""
			if !isPkScriptPrune || tx.IsSensible || objData.Data.HasAddress {
				pkscript = string(objData.PkScript)
			}

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
			var tokenIndex uint64
			var decimal uint8
			// token summary
			if objData.Data.CodeType != scriptDecoder.CodeType_NONE && objData.Data.CodeType != scriptDecoder.CodeType_SENSIBLE {
				buf := make([]byte, 12, 12+20+40)
				binary.LittleEndian.PutUint32(buf, objData.Data.CodeType)

				if objData.Data.CodeType == scriptDecoder.CodeType_NFT {
					binary.LittleEndian.PutUint64(buf[4:], objData.Data.NFT.TokenIndex)
					tokenIndex = objData.Data.NFT.TokenIndex
					dataValue = tokenIndex
				} else if objData.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
					binary.LittleEndian.PutUint64(buf[4:], objData.Data.NFTSell.TokenIndex)
					tokenIndex = objData.Data.NFTSell.TokenIndex
					dataValue = tokenIndex
				} else if objData.Data.CodeType == scriptDecoder.CodeType_FT {
					decimal = objData.Data.FT.Decimal
					dataValue = objData.Data.FT.Amount
				}

				buf = append(buf, objData.Data.CodeHash[:]...)
				buf = append(buf, objData.Data.GenesisId[:objData.Data.GenesisIdLen]...)

				tokenKey := string(buf)

				tokenSummary, ok := block.ParseData.TokenSummaryMap[tokenKey]
				if !ok {
					tokenSummary = &model.TokenData{
						CodeType:  objData.Data.CodeType,
						NFTIdx:    tokenIndex,
						Decimal:   decimal,
						CodeHash:  objData.Data.CodeHash[:],
						GenesisId: objData.Data.GenesisId[:objData.Data.GenesisIdLen],
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
				uint32(objData.Data.CodeType),
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
