package serial

import (
	"encoding/binary"
	"fmt"
	"satoblock/logger"
	"satoblock/model"
	"satoblock/store"
	"satoblock/utils"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	SyncTxFullCount     int
	SyncTxCodeHashCount int
	isTxrawPrune        bool
	isPkScriptPrune     bool
	isScriptSigPrune    bool
)

func init() {
	viper.SetConfigFile("conf/prune.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	// 清理非sensible的txraw
	isTxrawPrune = viper.GetBool("txraw")

	// 清理非sensible的锁定脚本
	// 清理无地址的锁定脚本，若要保留，可以设置为20位空地址
	isPkScriptPrune = viper.GetBool("pkscript")

	// 清理所有的解锁脚本
	isScriptSigPrune = viper.GetBool("scriptsig")
}

// SyncBlock block id
func SyncBlock(block *model.Block) {
	coinbase := block.Txs[0]
	coinbaseOut := coinbase.OutputsValue

	txInputsValue := uint64(0)
	txOutputsValue := uint64(0)

	for _, tx := range block.Txs[1:] {
		txInputsValue += tx.InputsValue
		txOutputsValue += tx.OutputsValue
	}

	for _, tokenSummary := range block.ParseData.TokenSummaryMap {
		if _, err := store.SyncStmtBlkCodeHash.Exec(
			uint32(block.Height),
			string(tokenSummary.CodeHash),
			string(tokenSummary.GenesisId),
			uint32(tokenSummary.CodeType),
			tokenSummary.NFTIdx,
			tokenSummary.InDataValue,
			tokenSummary.OutDataValue,
			tokenSummary.InSatoshi,
			tokenSummary.OutSatoshi,
			string(block.Hash),
		); err != nil {
			logger.Log.Info("sync-block-codehash-err",
				zap.String("blkid", block.HashHex),
				zap.String("err", err.Error()),
			)
		}
		SyncTxCodeHashCount++
	}

	if _, err := store.SyncStmtBlk.Exec(
		uint32(block.Height),
		string(block.Hash),
		string(block.Parent),
		string(block.MerkleRoot),
		block.TxCnt,
		txInputsValue,
		txOutputsValue,
		coinbaseOut,
		block.BlockTime,
		block.Bits,
		block.Size,
	); err != nil {
		logger.Log.Info("sync-block-err",
			zap.String("blkid", block.HashHex),
			zap.String("err", err.Error()),
		)
	}
}

// SyncBlockTx all tx in block height
func SyncBlockTx(block *model.Block) {
	for txIdx, tx := range block.Txs {
		// keep sensible rawtx only
		// prune txraw
		txraw := ""
		if !isTxrawPrune || tx.IsSensible {
			txraw = string(tx.Raw)
		}
		if _, err := store.SyncStmtTx.Exec(
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			txraw, // string(tx.Raw)
			uint32(block.Height),
			string(block.Hash),
			uint64(txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("txid", tx.HashHex),
				zap.String("err", err.Error()),
			)
		}
	}
}

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(block *model.Block) {
	for txIdx, tx := range block.Txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			// set sensible flag
			if output.Data.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}

			// prune string(output.Pkscript),
			pkscript := ""
			if !isPkScriptPrune || tx.IsSensible || output.Data.HasAddress {
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
				string(tx.Hash),
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
					zap.String("utxid", tx.HashHex),
					zap.Uint32("vout", uint32(vout)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(block *model.Block) {
	var commonObjData *model.TxoData = &model.TxoData{
		Satoshi: utils.CalcBlockSubsidy(block.Height),
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
						zap.String("txid", tx.HashHex),
						zap.Int("vin", vin),

						zap.String("utxid", input.InputHashHex),
						zap.Uint32("vout", input.InputVout),
					)
				}
			}
			tx.InputsValue += objData.Satoshi

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

			SyncTxFullCount++

			if _, err := store.SyncStmtTxIn.Exec(
				uint32(block.Height),
				uint64(txIdx),
				string(tx.Hash),
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
					zap.String("txid", tx.HashHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
