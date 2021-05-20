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

func CleanUtxoMap() {
	GlobalNewUtxoDataMap = make(map[string]*model.TxoData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*model.TxoData, 0)

	runtime.GC()
}
