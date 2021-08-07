package model

import (
	"runtime"
)

var (
	GlobalNewUtxoDataMap   map[string]*TxoData
	GlobalSpentUtxoDataMap map[string]*TxoData

	GlobalMempoolNewUtxoDataMap map[string]*TxoData
)

func init() {
	CleanUtxoMap()
}

// 清空本地map内存
func CleanUtxoMap() {
	GlobalNewUtxoDataMap = nil
	GlobalSpentUtxoDataMap = nil
	runtime.GC()

	GlobalNewUtxoDataMap = make(map[string]*TxoData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*TxoData, 0)
}

// 清空本地map内存
func CleanMempoolUtxoMap() {
	GlobalMempoolNewUtxoDataMap = nil
	runtime.GC()

	GlobalMempoolNewUtxoDataMap = make(map[string]*TxoData, 0)
}
