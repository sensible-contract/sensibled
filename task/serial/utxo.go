package serial

import "blkparser/model"

var (
	lastUtxoMapAddCount    int
	lastUtxoMapRemoveCount int
)

// parseUtxoSerial utxo 信息
func parseUtxoSerial(block *model.ProcessBlock) {
	insideTxo := make([]string, len(block.UtxoMissingMap))
	for key := range block.UtxoMissingMap {
		if _, ok := block.UtxoMap[key]; !ok {
			continue
		}
		insideTxo = append(insideTxo, key)
	}
	for _, key := range insideTxo {
		delete(block.UtxoMap, key)
		delete(block.UtxoMissingMap, key)
	}

	lastUtxoMapAddCount += len(block.UtxoMap)
	lastUtxoMapRemoveCount += len(block.UtxoMissingMap)

	for key, data := range block.UtxoMap {
		utxoMap[key] = data
	}
	for key := range block.UtxoMissingMap {
		delete(utxoMap, key)
	}
}
