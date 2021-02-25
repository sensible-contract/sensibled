package task

import (
	"blkparser/model"
	"blkparser/task/parallel"
	"blkparser/task/serial"
	"blkparser/utils"
)

var (
	MaxBlockHeightParallel int

	IsSync bool
	IsFull bool
)

func init() {
	// serial.LoadUtxoFromGobFile()
}

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for idx, tx := range block.Txs {
		isCoinbase := idx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// for txin full dump
		if IsFull {
			parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
			parallel.ParseUtxoParallel(tx, block.ParseData)
		}
	}

	// DumpBlockData
	if IsSync {
		serial.SyncBlock(block)
		serial.SyncBlockTx(block)
		serial.SyncBlockTxOutputInfo(block)
		serial.SyncBlockTxInputInfo(block)
	} else {
		serial.DumpBlock(block)
		serial.DumpBlockTx(block)
		serial.DumpBlockTxOutputInfo(block)
		serial.DumpBlockTxInputInfo(block)
	}
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, blockCountInBuffer, maxBlockHeight int) {
	serial.ParseBlockSpeed(len(block.Txs), block.Height, blockCountInBuffer, MaxBlockHeightParallel, maxBlockHeight)

	// DumpBlockData
	if IsFull {
		if IsSync {
			serial.SyncBlockTxInputDetail(block)
		} else {
			serial.DumpBlockTxInputDetail(block)
		}

		// for txin full dump
		serial.ParseUtxoSerial(block.ParseData)
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
	defer utils.SyncLog()

	serial.CleanUtxoMap()

	if IsSync {
		utils.CommitSyncCk()
		if IsFull {
			utils.CommitFullSyncCk(serial.DumpTxFullCount > 0)
			utils.ProcessAllSyncCk()
		} else {
			utils.ProcessPartSyncCk()
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
