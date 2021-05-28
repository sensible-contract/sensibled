package task

import (
	"satoblock/logger"
	"satoblock/model"
	"satoblock/store"
	"satoblock/task/parallel"
	"satoblock/task/serial"
)

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for txIdx, tx := range block.Txs {
		isCoinbase := txIdx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// 准备utxo花费关系数据
		// 所有txin使用的utxo记录
		parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
		// 所有txout产生的utxo记录
		parallel.ParseNewUtxoInTxParallel(txIdx, tx, block.ParseData)
	}

	// DB更新txout，比较独立，可以并行更新
	serial.SyncBlockTxOutputInfo(block)
}

// ParseBlockSerialStart 再串行处理区块
func ParseBlockSerialStart(block *model.Block) {
	// 从redis中补全查询当前block内所有Tx花费的utxo信息来使用
	serial.ParseGetSpentUtxoDataFromRedisSerial(block.ParseData)

	// DB更新txin，需要前序和当前区块的txout处理完毕，且依赖从redis查来的utxo。
	serial.SyncBlockTxInputDetail(block)

	// 需要串行，更新当前区块的utxo信息变化到程序内存缓存
	serial.UpdateUtxoInMapSerial(block.ParseData)
}

// ParseBlockParallelEnd 再并行处理区块
func ParseBlockParallelEnd(block *model.Block) {
	// DB更新block, 需要依赖txout、txin执行完毕，以统计区块Fee
	serial.SyncBlock(block)
	// DB更新tx, 需要依赖txout、txin执行完毕，以统计Tx Fee
	serial.SyncBlockTx(block)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd(isFull bool) {
	defer logger.SyncLog()

	// 批量更新redis utxo
	serial.UpdateUtxoInRedis(serial.GlobalNewUtxoDataMap, serial.GlobalSpentUtxoDataMap)

	// 清空本地map内存
	serial.CleanUtxoMap()

	// 提交DB
	store.CommitSyncCk()
	store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
	store.CommitCodeHashSyncCk(serial.SyncTxCodeHashCount > 0)

	// 执行DB数据额外更新
	if isFull {
		store.ProcessAllSyncCk()
	} else {
		store.ProcessPartSyncCk()
	}
}
