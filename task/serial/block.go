package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// dumpBlock block id
func dumpBlock(block *model.Block) {
	utils.LogBlk.Info("blk-list",
		zap.Uint32("height", uint32(block.Height)),
		zap.Binary("blkid", block.Hash),

		zap.Binary("previd", block.Parent),
		zap.Uint64("ntx", uint64(block.TxCnt)),
	)
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *model.Block) {
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

// dumpBlockTxInputInfo all tx input info
func dumpBlockTxInputInfo(block *model.Block) {
	for idx, tx := range block.Txs {
		for _, input := range tx.TxIns {
			utils.LogTxIn.Info("tx-input",
				zap.Binary("txidIdx", input.InputPoint),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.ByteString("script", input.ScriptSig),
			)
		}

		isCoinbase := (idx == 0)
		if isCoinbase {
			continue
		}

		for _, input := range tx.TxIns {
			utils.LogTxOutSpent.Info("tx-utxo-spent",
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.Binary("spendByTxidIdx", input.InputPoint),
				zap.Bool("utxo", false), // spent
			)
		}
	}
}

// dumpBlockTxOutputInfo all tx output info
func dumpBlockTxOutputInfo(block *model.Block) {
	for _, tx := range block.Txs {
		for _, output := range tx.TxOuts {
			utils.LogTxOut.Info("tx-utxo",
				zap.Binary("utxoPoint", output.Outpoint), // 36 byte
				zap.Binary("address", output.AddressPkh), // 20 byte
				zap.Binary("genesis", output.GenesisId),  // 20 byte
				zap.Uint64("value", output.Value),
				zap.ByteString("scriptType", output.LockingScriptType),
				zap.ByteString("script", output.Pkscript),
				zap.Bool("utxo", true), // unspent
			)
		}
	}
}

// dumpBlockTxInfo all tx info
func dumpBlockTxInfo(block *model.Block) {
	for _, tx := range block.Txs {
		utils.Log.Info("tx-info",
			zap.Binary("_id", tx.Hash),
			// zap.String("t", tx.HashHex),
			zap.Uint32("i", tx.TxInCnt),
			zap.Uint32("o", tx.TxOutCnt),
			zap.Array("in", tx.TxIns),
			zap.Array("out", tx.TxOuts),
			// zap.Int("h", block.Height),
		)
	}
}
