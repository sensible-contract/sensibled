package serial

import (
	"blkparser/model"
	"blkparser/utils"

	"go.uber.org/zap"
)

// dumpBlock block id
func dumpBlock(block *model.Block) {
	utils.Log.Info("blk-list",
		zap.String("b", block.HashHex),
		zap.Int("h", block.Height),
	)
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *model.Block) {
	for _, tx := range block.Txs {
		utils.Log.Info("tx-list",
			zap.String("t", tx.HashHex),
			zap.String("b", block.HashHex),
			// zap.Int("h", block.Height),
		)
	}
}

// dumpBlockTxInfo all tx info
func dumpBlockTxInfo(block *model.Block) {
	for _, tx := range block.Txs {
		utils.Log.Info("tx-info",
			zap.String("t", tx.HashHex),
			zap.Uint32("i", tx.TxInCnt),
			zap.Uint32("o", tx.TxOutCnt),
			zap.Array("in", tx.TxIns),
			zap.Array("out", tx.TxOuts),
			// zap.Int("h", block.Height),
		)
	}
}
