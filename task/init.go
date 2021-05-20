package task

import (
	"satoblock/logger"
	"satoblock/model"
	"satoblock/store"
	"satoblock/task/parallel"
	"satoblock/task/serial"
	"satoblock/task/utils"
)

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for txIdx, tx := range block.Txs {
		isCoinbase := txIdx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// 准备utxo花费关系数据
		parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
		parallel.ParseNewUtxoInTxParallel(txIdx, tx, block.ParseData)
	}

	serial.SyncBlockTxOutputInfo(block)
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, maxBlockHeight int) {
	utils.ParseBlockSpeed(len(block.Txs), len(serial.GlobalNewUtxoDataMap), len(serial.GlobalSpentUtxoDataMap), block.Height, maxBlockHeight)

	serial.ParseGetSpentUtxoDataFromRedisSerial(block.ParseData)
	serial.SyncBlockTxInputDetail(block)

	serial.SyncBlock(block)
	serial.SyncBlockTx(block)

	// for txin dump
	serial.UpdateUtxoInMapSerial(block.ParseData)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd(isFull bool) {
	defer logger.SyncLog()

	serial.UpdateUtxoInRedis(serial.GlobalNewUtxoDataMap, serial.GlobalSpentUtxoDataMap)
	serial.CleanUtxoMap()

	store.CommitSyncCk()
	store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
	store.CommitCodeHashSyncCk(serial.SyncTxCodeHashCount > 0)
	if isFull {
		store.ProcessAllSyncCk()
	} else {
		store.ProcessPartSyncCk()
	}
}
