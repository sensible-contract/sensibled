package parser

import (
	"math"
	"sensibled/model"
)

func isTxFinal(tx *model.Tx) bool {
	if tx.LockTime == 0 {
		return true
	}

	isFinal := true
	for _, input := range tx.TxIns {
		if input.Sequence != math.MaxUint32 {
			isFinal = false
		}
	}
	return isFinal
}

func IsTxNonFinal(tx *model.Tx, nonFinalTxs map[string]struct{}) bool {
	if !isTxFinal(tx) {
		return true
	}

	for _, input := range tx.TxIns {
		if _, ok := nonFinalTxs[input.InputHashHex]; ok {
			return true
		}
	}
	return false
}
