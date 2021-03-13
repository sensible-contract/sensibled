package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// SyncBlock block id
func SyncBlock(block *model.Block) {
	if _, err := utils.SyncStmtBlk.Exec(
		uint32(block.Height),
		string(block.Hash),
		string(block.Parent),
		string(block.MerkleRoot),
		uint64(block.TxCnt),
		block.BlockTime,
		block.Bits,
		block.Size,
	); err != nil {
		utils.Log.Info("sync-block-err",
			zap.String("sync", "block err"),
			zap.String("txid", block.HashHex),
			zap.String("err", err.Error()),
		)
	}
}

// SyncBlockTx all tx in block height
func SyncBlockTx(block *model.Block) {
	for txIdx, tx := range block.Txs {
		if _, err := utils.SyncStmtTx.Exec(
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			uint32(block.Height),
			string(block.Hash),
			uint64(txIdx),
		); err != nil {
			utils.Log.Info("sync-tx-err",
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
			// if output.Value == 0 || !output.LockingScriptMatch {
			// 	continue
			// }

			if _, err := utils.SyncStmtTxOut.Exec(
				string(tx.Hash),
				uint32(vout),
				string(output.AddressPkh), // 20 byte
				string(output.GenesisId),  // 20 byte
				output.Value,
				string(output.LockingScriptType),
				string(output.Pkscript),
				uint32(block.Height),
				uint64(txIdx),
			); err != nil {
				utils.Log.Info("sync-txout-err",
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
	var commonObjData *model.CalcData = &model.CalcData{
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
	}

	for txIdx, tx := range block.Txs {
		isCoinbase := (txIdx == 0)

		for vin, input := range tx.TxIns {
			objData := commonObjData
			if !isCoinbase {
				if obj, ok := block.ParseData.NewUtxoDataMap[input.InputOutpointKey]; ok {
					objData = obj
				} else if obj, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]; ok {
					objData = obj
				} else {
					utils.Log.Info("tx-input-err",
						zap.String("txin", "input missing utxo"),
						zap.String("txid", tx.HashHex),
						zap.Int("vin", vin),

						zap.String("utxid", input.InputHashHex),
						zap.Uint32("vout", input.InputVout),
					)
				}
			}
			DumpTxFullCount++
			if _, err := utils.SyncStmtTxInFull.Exec(
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
				string(objData.GenesisId),  // 20 byte
				objData.Value,
				string(objData.ScriptType),
				string(objData.Script),
			); err != nil {
				utils.Log.Info("sync-txin-full-err",
					zap.String("sync", "txin full err"),
					zap.String("txid", tx.HashHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
