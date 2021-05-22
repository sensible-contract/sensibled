package serial

import (
	"runtime"
	"satoblock/model"
)

var (
	GlobalSpentUtxoDataMap map[string]*model.TxoData
	GlobalNewUtxoDataMap   map[string]*model.TxoData
)

func init() {
	CleanUtxoMap()
}

// 清空本地map内存
func CleanUtxoMap() {
	GlobalNewUtxoDataMap = nil
	GlobalSpentUtxoDataMap = nil
	runtime.GC()

	GlobalNewUtxoDataMap = make(map[string]*model.TxoData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*model.TxoData, 0)
}
