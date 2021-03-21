package utils

import (
	"blkparser/logger"
	"blkparser/model"
	"log"

	"go.uber.org/zap"
)

func ParseBlockCount(block *model.Block) {
	txs := block.Txs

	// 检查一些统计项
	countInsideTx := checkTxsOrder(txs)
	countWitTx := countWitTxsInBlock(txs)
	countValueTx := countValueOfTxsInBlock(txs)
	countZeroValueTx := countZeroValueOfTxsInBlock(txs)

	logger.Log.Info("parsing",
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

func countWitTxsInBlock(txs []*model.Tx) int {
	count := 0
	for _, tx := range txs {
		if tx.WitOffset > 0 {
			count++
		}
	}
	return count
}

// countValueOfTxsInBlock 统计tx中所有
func countValueOfTxsInBlock(txs []*model.Tx) uint64 {
	allValue := uint64(0)
	for _, tx := range txs {
		for _, output := range tx.TxOuts {
			allValue += output.Value
		}
	}
	return allValue
}

// countZeroValueOfTxsInBlock 统计存在0输出的tx数量
func countZeroValueOfTxsInBlock(txs []*model.Tx) uint64 {
	zeroCount := uint64(0)
	for _, tx := range txs {
		hasZero := false
		for _, output := range tx.TxOuts {
			if output.Value == 0 {
				hasZero = true
				break
			}
		}
		if hasZero {
			zeroCount++
		}
	}
	return zeroCount
}

func checkTxsOrder(txs []*model.Tx) int {
	allTx := make(map[string]bool)
	parsedTx := make(map[string]bool)
	for _, tx := range txs {
		allTx[tx.HashHex] = true
	}
	count := 0
	for _, tx := range txs {
		hasInside := false
		for _, input := range tx.TxIns {
			inTxHashHex := input.InputHashHex
			if _, ok := allTx[inTxHashHex]; ok {
				hasInside = true
				// log.Printf("inside block Tx input: %s", HashString(input.InputHash))
				if _, ok := parsedTx[inTxHashHex]; !ok {
					log.Printf("reverse Tx input: %s", input.InputHashHex)
				}
			}
		}
		if hasInside {
			count++
		}
		parsedTx[tx.HashHex] = true
	}
	return count
}
