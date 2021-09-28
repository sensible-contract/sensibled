package model

import (
	"runtime"
)

var (
	GlobalConfirmedTxMap   map[string]bool
	GlobalNewUtxoDataMap   map[string]*TxoData
	GlobalSpentUtxoDataMap map[string]*TxoData

	GlobalMempoolNewUtxoDataMap map[string]*TxoData
)

func init() {
	CleanUtxoMap()
}

// 清空本地tx map内存
func CleanConfirmedTxMap() {
	GlobalConfirmedTxMap = nil
	runtime.GC()

	GlobalConfirmedTxMap = make(map[string]bool, 0)
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
