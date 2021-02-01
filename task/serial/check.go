package serial

import (
	"blkparser/model"
	"log"
)

func CountWitTxsInBlock(txs []*model.Tx) int {
	count := 0
	for _, tx := range txs {
		if tx.WitOffset > 0 {
			count++
		}
	}
	return count
}

// CountValueOfTxsInBlock 统计tx中所有
func CountValueOfTxsInBlock(txs []*model.Tx) uint64 {
	allValue := uint64(0)
	for _, tx := range txs {
		for _, output := range tx.TxOuts {
			allValue += output.Value
		}
	}
	return allValue
}

// CountZeroValueOfTxsInBlock 统计存在0输出的tx数量
func CountZeroValueOfTxsInBlock(txs []*model.Tx) uint64 {
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

func CheckTxsOrder(txs []*model.Tx) int {
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
