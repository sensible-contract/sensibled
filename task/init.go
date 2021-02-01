package task

import (
	"blkparser/model"
	"blkparser/task/parallel"
	"blkparser/task/serial"
	"blkparser/utils"
	"encoding/binary"
	"encoding/hex"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	key := make([]byte, 36)
	copy(key, tx.Hash)
	for idx, output := range tx.TxOuts {
		if output.Value == 0 {
			continue
		}

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)

		output.LockingScriptType = parallel.GetLockingScriptType(output.Pkscript)
		output.LockingScriptTypeHex = hex.EncodeToString(output.LockingScriptType)

		// test locking script
		output.LockingScriptMatch = true

		// if isLockingScriptOnlyEqual(output.Pkscript) {
		// 	output.LockingScriptMatch = true
		// }
	}

	// dumpLockingScriptType(tx)
	// parseTxoSpendByTxParallel(tx, isCoinbase, block)
	// parseUtxoParallel(tx, block)
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, maxBlockHeight int) {
	serial.ParseBlockSpeed(len(block.Txs), block.Height, maxBlockHeight)
	// parseBlockCount(block)

	// parseUtxoSerial(block.ParseData)

	serial.Parse(block)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd() {
	defer utils.Log.Sync()
	defer utils.LogErr.Sync()

	// dumpUtxoToGobFile()

	loggerMap, _ := zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"/data/calcMap.log"},            // 输出目的地
	}.Build()
	defer loggerMap.Sync()

	// parseEndDumpUtxo(loggerMap)
	// parseEndDumpScriptType(loggerMap)
}
