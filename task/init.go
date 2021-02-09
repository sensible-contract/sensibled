package task

import (
	"blkparser/model"
	"blkparser/task/parallel"
	"blkparser/task/serial"
	"blkparser/utils"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 先并行分析交易tx，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for idx, tx := range block.Txs {
		isCoinbase := idx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
		// parallel.ParseUtxoParallel(tx, block.ParseData)
	}

	serial.DumpBlockData(block)
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, maxBlockHeight int) {
	serial.ParseBlockSpeed(len(block.Txs), block.Height, maxBlockHeight)

	// serial.ParseBlock(block)
	// serial.DumpBlockData(block)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd() {
	defer utils.SyncLog()

	loggerMap, _ := zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"/data/calcMap.log"},            // 输出目的地
	}.Build()
	defer loggerMap.Sync()

	serial.End(loggerMap)
}
