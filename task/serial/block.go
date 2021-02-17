package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// DumpBlock block id
func DumpBlock(block *model.Block) {
	utils.LogBlk.Info("blk-list",
		zap.Uint32("height", uint32(block.Height)),
		zap.Binary("blkid", block.Hash),

		zap.Binary("previd", block.Parent),
		zap.Uint64("ntx", uint64(block.TxCnt)),
	)
}

// DumpBlockTx all tx in block height
func DumpBlockTx(block *model.Block) {
	for idx, tx := range block.Txs {
		utils.LogTx.Info("tx-list",
			zap.Binary("txid", tx.Hash),
			zap.Uint32("nTxIn", tx.TxInCnt),
			zap.Uint32("nTxOut", tx.TxOutCnt),

			zap.Uint32("height", uint32(block.Height)),
			zap.Binary("blkid", block.Hash),
			zap.Uint64("idx", uint64(idx)),
		)
	}
}

// DumpBlockTxInputInfo all tx input info
func DumpBlockTxInputInfo(block *model.Block) {
	var commonObjData *model.CalcData = &model.CalcData{
		GenesisId:  make(model.Bytes, 20),
		AddressPkh: make(model.Bytes, 20),
	}

	for idx, tx := range block.Txs {
		isCoinbase := (idx == 0)

		for inputIndex, input := range tx.TxIns {
			objData := commonObjData
			if !isCoinbase {
				if obj, ok := block.ParseData.UtxoMap[input.InputOutpointKey]; ok {
					objData = &obj
				} else if obj, ok := utxoMap[input.InputOutpointKey]; ok {
					objData = &obj
				} else {
					utils.Log.Info("tx-input-err",
						zap.String("txin", "input missing utxo"),
						zap.String("txid", tx.HashHex),
						zap.Int("idx", inputIndex),

						zap.String("utxid", input.InputHashHex),
						zap.Uint32("vout", input.InputVout),
					)
				}
			}

			utils.LogTxIn.Info("tx-input",
				zap.Uint32("height", uint32(block.Height)),
				zap.Binary("txidIdx", input.InputPoint),
				zap.ByteString("script", input.ScriptSig),

				zap.Uint32("height_out", uint32(objData.BlockHeight)),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.Binary("address", objData.AddressPkh), // 20 byte
				zap.Binary("genesis", objData.GenesisId),  // 20 byte
				zap.Uint64("value", objData.Value),
				zap.ByteString("scriptType", objData.ScriptType),
			)
		}
	}
}

// DumpBlockTxOutputInfo all tx output info
func DumpBlockTxOutputInfo(block *model.Block) {
	for _, tx := range block.Txs {
		for _, output := range tx.TxOuts {
			// if output.Value == 0 || !output.LockingScriptMatch {
			// 	continue
			// }

			utils.LogTxOut.Info("tx-utxo",
				zap.Binary("utxoPoint", output.Outpoint), // 36 byte
				zap.Binary("address", output.AddressPkh), // 20 byte
				zap.Binary("genesis", output.GenesisId),  // 20 byte
				zap.Uint64("value", output.Value),
				zap.ByteString("scriptType", output.LockingScriptType),
				zap.ByteString("script", output.Pkscript),
				zap.Uint32("height", uint32(block.Height)),
			)
		}
	}
}
