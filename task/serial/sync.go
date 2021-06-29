package serial

import (
	"satoblock/logger"
	"satoblock/model"
	"satoblock/store"
	"satoblock/utils"
	"strconv"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

var (
	SyncTxFullCount     int
	SyncTxCodeHashCount int
)

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
				zap.String("sync", "block codehash err"),
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
		uint64(block.TxCnt),
		txInputsValue,
		txOutputsValue,
		coinbaseOut,
		block.BlockTime,
		block.Bits,
		block.Size,
	); err != nil {
		logger.Log.Info("sync-block-err",
			zap.String("sync", "block err"),
			zap.String("blkid", block.HashHex),
			zap.String("err", err.Error()),
		)
	}
}

// SyncBlockTx all tx in block height
func SyncBlockTx(block *model.Block) {
	for txIdx, tx := range block.Txs {
		if _, err := store.SyncStmtTx.Exec(
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			string(tx.Raw),
			uint32(block.Height),
			string(block.Hash),
			uint64(txIdx),
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
func SyncBlockTxOutputInfo(block *model.Block) {
	for txIdx, tx := range block.Txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			var dataValue uint64
			if output.CodeType == scriptDecoder.CodeType_NFT {
				dataValue = output.TokenIdx
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
				string(output.LockingScriptType),
				string(output.Pkscript),
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
		CodeHash:   make([]byte, 1),
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
		Satoshi:    utils.CalcBlockSubsidy(block.Height),
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

			var dataValue uint64
			// token summary
			if len(objData.CodeHash) == 20 && len(objData.GenesisId) >= 20 {
				key := string(objData.CodeHash) + string(objData.GenesisId)

				if objData.CodeType == scriptDecoder.CodeType_NFT {
					key += strconv.Itoa(int(objData.TokenIdx))
					dataValue = objData.TokenIdx
				} else if objData.CodeType == scriptDecoder.CodeType_FT {
					dataValue = objData.Amount
				}

				tokenSummary, ok := block.ParseData.TokenSummaryMap[key]
				if !ok {
					tokenSummary = &model.TokenData{
						CodeType:  objData.CodeType,
						NFTIdx:    objData.TokenIdx,
						CodeHash:  objData.CodeHash,
						GenesisId: objData.GenesisId,
					}
					block.ParseData.TokenSummaryMap[key] = tokenSummary
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
				string(objData.Script),
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
