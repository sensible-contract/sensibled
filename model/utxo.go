package model

import (
	"runtime"
)

var (
	cleanTimes              = 0
	GlobalConfirmedBlkMap   map[string]struct{}
	GlobalConfirmedTxMap    map[string]struct{}
	GlobalConfirmedTxOldMap map[string]struct{}

	GlobalAddrPkhInTxMap map[string][]TxLocation

	GlobalNewUtxoDataMap   map[string]*TxoData
	GlobalSpentUtxoDataMap map[string]*TxoData

	GlobalMempoolNewUtxoDataMap map[string]*TxoData

	GlobalNewInscriptions        []*NewInscriptionInfo
	GlobalMempoolNewInscriptions []*NewInscriptionInfo
)

func init() {
	CleanUtxoMap()
	CleanConfirmedTxMap(true)
	GlobalConfirmedBlkMap = make(map[string]struct{}, 0)
}

// 清空本地已确认tx map内存
// 因为zmq在区块确认和区块重组时，会将区块内所有tx推送一遍。
// 但这些推送的tx不属于内存池的部分，需要全部识别并丢弃。
// 目前在扫区块文件时，记录已经扫描的最近10个块的所有tx，用于识别过滤zmq重复推送。
// 另一种解决方法是关闭节点区块tx推送
func CleanConfirmedTxMap(force bool) {
	if force {
		GlobalConfirmedTxMap = nil
		GlobalConfirmedTxMap = make(map[string]struct{}, 0)
	} else if cleanTimes < 10 {
		cleanTimes++
		return
	}
	cleanTimes = 0

	GlobalConfirmedTxOldMap = nil
	runtime.GC()
	GlobalConfirmedTxOldMap = GlobalConfirmedTxMap
	GlobalConfirmedTxMap = make(map[string]struct{}, 0)
}

// 清空本地map内存
func CleanUtxoMap() {
	GlobalAddrPkhInTxMap = nil

	GlobalNewUtxoDataMap = nil
	GlobalSpentUtxoDataMap = nil
	GlobalNewInscriptions = nil
	runtime.GC()

	GlobalAddrPkhInTxMap = make(map[string][]TxLocation, 0)

	GlobalNewUtxoDataMap = make(map[string]*TxoData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*TxoData, 0)
	GlobalNewInscriptions = make([]*NewInscriptionInfo, 0)
}

// 清空本地map内存
func CleanMempoolUtxoMap() {
	GlobalMempoolNewUtxoDataMap = nil
	GlobalMempoolNewInscriptions = nil

	runtime.GC()

	GlobalMempoolNewUtxoDataMap = make(map[string]*TxoData, 0)
	GlobalMempoolNewInscriptions = make([]*NewInscriptionInfo, 0)
}
