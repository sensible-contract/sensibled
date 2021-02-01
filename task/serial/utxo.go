package serial

import "blkparser/model"

var (
	lastUtxoMapAddCount    int
	lastUtxoMapRemoveCount int
)

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
