package parallel

import "blkparser/model"

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTx(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	// parseTxoSpendByTxParallel(tx, isCoinbase, block)
	// parseUtxoParallel(tx, block)
}
