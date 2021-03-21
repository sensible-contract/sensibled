package serial

import (
	"blkparser/model"
	"runtime"
	"sync"
)

var (
	calcMap   map[string]*model.CalcData
	calcMutex sync.Mutex

	GlobalSpentUtxoDataMap map[string]*model.CalcData
	GlobalNewUtxoDataMap   map[string]*model.CalcData
)

func init() {
	CleanUtxoMap()
	// loadUtxoFromGobFile()
}

func CleanUtxoMap() {
	calcMap = make(map[string]*model.CalcData, 0)
	GlobalNewUtxoDataMap = make(map[string]*model.CalcData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*model.CalcData, 0)

	runtime.GC()
}
