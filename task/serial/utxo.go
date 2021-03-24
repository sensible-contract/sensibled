package serial

import (
	"blkparser/model"
	"runtime"
	"sync"
)

var (
	calcMap   map[string]int
	calcMutex sync.Mutex

	GlobalSpentUtxoDataMap map[string]*model.TxoData
	GlobalNewUtxoDataMap   map[string]*model.TxoData
)

func init() {
	CleanUtxoMap()
	// loadUtxoFromGobFile()
}

func CleanUtxoMap() {
	calcMap = make(map[string]int, 0)
	GlobalNewUtxoDataMap = make(map[string]*model.TxoData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*model.TxoData, 0)

	runtime.GC()
}
