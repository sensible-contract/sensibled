package parser

import (
	"math"
	"unisatd/model"
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

func isTxOptInReplaceByFee(tx *model.Tx) bool {
	for _, input := range tx.TxIns {
		if input.Sequence < math.MaxUint32-1 {
			return true
		}
	}
	return false
}

func IsTxOptInReplaceByFee(tx *model.Tx, optInReplaceByFeeTxs map[string]struct{}) bool {
	if isTxOptInReplaceByFee(tx) {
		return true
	}

	for _, input := range tx.TxIns {
		if _, ok := optInReplaceByFeeTxs[input.InputHashHex]; ok {
			return true
		}
	}
	return false
}
