package serial

import (
	"blkparser/model"
	"blkparser/utils"
	"encoding/binary"
	"encoding/hex"

	"go.uber.org/zap"
)

var (
	lastUtxoMapAddCount    int
	lastUtxoMapRemoveCount int
)

func parseEndDumpUtxo(log *zap.Logger) {
	for keyStr, data := range utxoMap {
		key := []byte(keyStr)

		log.Info("utxo",
			zap.Int("h", data.BlockHeight),
			zap.String("tx", utils.HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.String("type", data.ScriptType),
			zap.Int("n", len(data.ScriptType)),
		)
	}
}

func parseEndDumpScriptType(log *zap.Logger) {
	for keyStr, data := range calcMap {
		key := []byte(keyStr)

		log.Info("script type",
			zap.String("s", hex.EncodeToString(key)),
			zap.Int("n", len(keyStr)),
			zap.Uint64("num", data.Value),
		)
	}
}

func parseBlockCount(block *model.Block) {
	txs := block.Txs

	// 检查一些统计项
	countInsideTx := CheckTxsOrder(txs)
	countWitTx := CountWitTxsInBlock(txs)
	countValueTx := CountValueOfTxsInBlock(txs)
	countZeroValueTx := CountZeroValueOfTxsInBlock(txs)

	utils.Log.Info("parsing",
		zap.String("log", "block"),
		zap.Int("height", block.Height),
		zap.Uint32("timestamp", block.BlockTime),
		zap.String("blk", block.HashHex),
		zap.Uint32("size", block.Size),
		zap.Int("nTx", len(txs)),
		zap.Int("inside", countInsideTx),
		zap.Int("wit", countWitTx),
		zap.Uint64("zero", countZeroValueTx),
		zap.Uint64("v", countValueTx),
	)
}

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

// parseUtxoSerial utxo 信息
func parseUtxoSerial(block *model.ProcessBlock) {
	lastUtxoMapAddCount += len(block.UtxoMap)
	lastUtxoMapRemoveCount += len(block.UtxoMissingMap)

	for key, data := range block.UtxoMap {
		utxoMap[key] = data
	}
	for key := range block.UtxoMissingMap {
		delete(utxoMap, key)
	}
}
