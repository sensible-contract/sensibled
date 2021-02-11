package serial

import (
	"blkparser/model"
	"sync"

	"go.uber.org/zap"
)

var (
	calcMap   map[string]model.CalcData
	calcMutex sync.Mutex

	utxoMap map[string]model.CalcData
)

func init() {
	calcMap = make(map[string]model.CalcData, 0)
	utxoMap = make(map[string]model.CalcData, 0)

	// loadUtxoFromGobFile()
}

func DumpBlockData(block *model.Block) {
	// dumpBlock(block)
	// dumpBlockTx(block)
	// dumpBlockTxOutputInfo(block)
	dumpBlockTxInputInfo(block)

	// dumpBlockTxInfo(block)
	// dumpLockingScriptType(block)
}

// ParseBlock 再串行分析区块
func ParseBlock(block *model.Block) {
	// parseBlockCount(block)

	parseUtxoSerial(block.ParseData)
}

// End 最后分析执行
func End(log *zap.Logger) {
	// dumpUtxoToGobFile()

	// parseEndDumpUtxo(loggerMap)
	// parseEndDumpScriptType(loggerMap)
}
