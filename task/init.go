package task

import (
	"blkparser/logger"
	"blkparser/model"
	"blkparser/store"
	"blkparser/task/parallel"
	"blkparser/task/serial"
	"blkparser/task/utils"
)

var (
	MaxBlockHeightParallel int

	IsSync   bool
	IsDump   bool
	WithUtxo bool
	IsFull   bool
	UseMap   bool
)

func init() {
	// serial.LoadUtxoFromGobFile()
}

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for txIdx, tx := range block.Txs {
		isCoinbase := txIdx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		if WithUtxo {
			// 准备utxo花费关系数据
			parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
			parallel.ParseNewUtxoInTxParallel(txIdx, tx, block.ParseData)
		}
	}

	if IsSync {
		serial.SyncBlockTxOutputInfo(block)
	} else if IsDump {
		serial.DumpBlock(block)
		serial.DumpBlockTx(block)
		serial.DumpBlockTxOutputInfo(block)
		serial.DumpBlockTxInputInfo(block)
	}
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, maxBlockHeight int) {
	utils.ParseBlockSpeed(len(block.Txs), len(serial.GlobalNewUtxoDataMap), len(serial.GlobalSpentUtxoDataMap), block.Height, maxBlockHeight)

	if WithUtxo {
		if IsSync {
			serial.ParseGetSpentUtxoDataFromRedisSerial(block.ParseData, UseMap)
			serial.SyncBlockTxInputDetail(block)

			serial.SyncBlock(block)
			serial.SyncBlockTx(block)
		} else if IsDump {
			serial.DumpBlockTxInputDetail(block)
		}

		// for txin dump
		if UseMap {
			serial.UpdateUtxoInMapSerial(block.ParseData)
		} else {
			serial.UpdateUtxoInRedisSerial(block.ParseData)
		}
	}

	// serial.DumpBlockTxInfo(block)
	// serial.DumpLockingScriptType(block)

	// ParseBlock
	// serial.ParseBlockCount(block)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd() {
	defer logger.SyncLog()

	if WithUtxo {
		if UseMap {
			serial.UpdateUtxoInRedis(serial.GlobalNewUtxoDataMap, serial.GlobalSpentUtxoDataMap)
		}
		serial.CleanUtxoMap()
	}

	if IsSync {
		store.CommitSyncCk()
		store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
		store.CommitCodeHashSyncCk(serial.SyncTxCodeHashCount > 0)
		if IsFull {
			store.ProcessAllSyncCk()
		} else {
			store.ProcessPartSyncCk()
		}
	}

	// loggerMap, _ := zap.Config{
	// 	Encoding:    "console",                                // 配置编码方式（json 或 console）
	// 	Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
	// 	OutputPaths: []string{"/data/calcMap.log"},            // 输出目的地
	// }.Build()
	// defer loggerMap.Sync()

	// serial.DumpUtxoToGobFile()
	// serial.ParseEndDumpUtxo(loggerMap)
	// serial.ParseEndDumpScriptType(loggerMap)
}
