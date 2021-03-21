package serial

import (
	"blkparser/logger"
	"blkparser/model"

	"go.uber.org/zap"
)

var (
	DumpTxFullCount int
)

// DumpBlock block id
func DumpBlock(block *model.Block) {
	logger.LogBlk.Info("blk-list",
		zap.Uint32("height", uint32(block.Height)),
		zap.Binary("blkid", block.Hash),
		zap.Binary("previd", block.Parent),
		zap.Binary("merkle", block.MerkleRoot),
		zap.Uint64("ntx", uint64(block.TxCnt)),
		zap.Uint32("time", uint32(block.BlockTime)),
		zap.Uint32("bits", uint32(block.Bits)),
		zap.Uint32("size", uint32(block.Size)),
	)
}

// DumpBlockTx all tx in block height
func DumpBlockTx(block *model.Block) {
	for idx, tx := range block.Txs {
		logger.LogTx.Info("tx-list",
			zap.Binary("txid", tx.Hash),
			zap.Uint32("nTxIn", tx.TxInCnt),
			zap.Uint32("nTxOut", tx.TxOutCnt),
			zap.Uint32("size", tx.Size),
			zap.Uint32("locktime", tx.LockTime),

			zap.Uint32("height", uint32(block.Height)),
			zap.Binary("blkid", block.Hash),
			zap.Uint64("idx", uint64(idx)),
		)
	}
}

// DumpBlockTxOutputInfo all tx output info
func DumpBlockTxOutputInfo(block *model.Block) {
	for _, tx := range block.Txs {
		for _, output := range tx.TxOuts {
			// if output.Value == 0 || !output.LockingScriptMatch {
			// 	continue
			// }

			logger.LogTxOut.Info("tx-txo",
				zap.Binary("utxoPoint", output.Outpoint),     // 36 byte
				zap.ByteString("address", output.AddressPkh), // 20 byte
				zap.ByteString("genesis", output.GenesisId),  // 20 byte
				zap.Uint64("value", output.Value),
				zap.ByteString("scriptType", output.LockingScriptType),
				zap.ByteString("script", output.Pkscript),
				zap.Uint32("height", uint32(block.Height)),
			)
		}
	}
}

// DumpBlockTxInputInfo all tx input info
func DumpBlockTxInputInfo(block *model.Block) {
	for _, tx := range block.Txs {
		for _, input := range tx.TxIns {
			logger.LogTxIn.Info("tx-input",
				zap.Binary("txidIdx", input.InputPoint),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.ByteString("scriptSig", input.ScriptSig),
				zap.Uint32("sequence", uint32(input.Sequence)),
				zap.Uint32("height", uint32(block.Height)),
			)
		}
	}
}

// DumpBlockTxInputDetail all tx input info
func DumpBlockTxInputDetail(block *model.Block) {
	var commonObjData *model.CalcData = &model.CalcData{
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
	}

	for idx, tx := range block.Txs {
		isCoinbase := (idx == 0)

		for inputIndex, input := range tx.TxIns {
			objData := commonObjData
			if !isCoinbase {
				if obj, ok := block.ParseData.NewUtxoDataMap[input.InputOutpointKey]; ok {
					objData = obj
				} else if obj, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]; ok {
					objData = obj
				} else {
					logger.Log.Info("tx-input-err",
						zap.String("txin", "input missing utxo"),
						zap.String("txid", tx.HashHex),
						zap.Int("idx", inputIndex),

						zap.String("utxid", input.InputHashHex),
						zap.Uint32("vout", input.InputVout),
					)
				}
			}
			DumpTxFullCount++
			logger.LogTxIn.Info("tx-input-detail",
				zap.Uint32("height", uint32(block.Height)),
				zap.Binary("txidIdx", input.InputPoint),
				zap.ByteString("scriptSig", input.ScriptSig),
				zap.Uint32("sequence", uint32(input.Sequence)),

				zap.Uint32("height_out", uint32(objData.BlockHeight)),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.ByteString("address", objData.AddressPkh), // 20 byte
				zap.ByteString("genesis", objData.GenesisId),  // 20 byte
				zap.Uint64("value", objData.Value),
				zap.ByteString("scriptType", objData.ScriptType),
				zap.ByteString("script", objData.Script),
			)
		}
	}
}
