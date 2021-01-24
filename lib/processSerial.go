package blkparser

import (
	"encoding/binary"
	"encoding/hex"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	lastLogTime      time.Time
	lastBlockHeight  int
	lastBlockTxCount int
)

func ParseBlockSerial(block *Block, maxBlockHeight int) {
	ParseBlockSpeed(len(block.Txs), block.Height, maxBlockHeight)
	// ParseBlockCount(block)

	// dumpBlock(block)
	// dumpBlockTx(block)

	block.Txs = nil
}

func ParseEnd() {
	logger.Sync()
	loggerErr.Sync()

	loggerMap, _ := zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"/data/calcMap.log"},            // 输出目的地
	}.Build()
	defer loggerMap.Sync()

	// logger.Info("end",
	// 	zap.Int("dataMap", len(calcMap)),
	// )

	ParseEndDumpUtxo(loggerMap)
	// ParseEndDumpScriptType(loggerMap)
}

func ParseEndDumpUtxo(log *zap.Logger) {
	utxoMap.Range(func(keyStr, value interface{}) bool {
		if _, ok := utxoMissingMap.Load(keyStr); ok {
			return true
		}
		key := []byte(keyStr.(string))
		data := value.(*CalcData)

		log.Info("utxo",
			zap.String("tx", HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.String("type", data.ScriptType),
			zap.Int("n", len(data.ScriptType)),
		)
		return true
	})
}

func ParseEndDumpScriptType(log *zap.Logger) {
	for keyStr, data := range calcMap {
		key := []byte(keyStr)

		log.Info("script type",
			zap.String("s", hex.EncodeToString(key)),
			zap.Int("n", len(keyStr)),
			zap.Uint64("num", data.Value),
		)
	}
}

func ParseBlockSpeed(nTx int, nextBlockHeight, maxBlockHeight int) {
	lastBlockTxCount += nTx

	if time.Since(lastLogTime) > time.Second {
		if nextBlockHeight < lastBlockHeight {
			lastBlockHeight = 0
		}

		lastLogTime = time.Now()

		timeLeft := 0
		if maxBlockHeight > 0 && (nextBlockHeight-lastBlockHeight) != 0 {
			timeLeft = (maxBlockHeight - nextBlockHeight) / (nextBlockHeight - lastBlockHeight)
		}

		loggerErr.Info("parsing",
			zap.String("log", "speed"),
			zap.Int("height", nextBlockHeight),
			zap.Int("bps", nextBlockHeight-lastBlockHeight),
			zap.Int("tps", lastBlockTxCount),
			zap.Int("time", timeLeft),
			zap.Int("dataMap", len(calcMap)),
			// zap.Duration("backoff", time.Second),
		)

		lastBlockHeight = nextBlockHeight
		lastBlockTxCount = 0
	}
}

func ParseBlockCount(block *Block) {
	txs := block.Txs

	// 检查一些统计项
	countInsideTx := CheckTxsOrder(txs)
	countWitTx := CountWitTxsInBlock(txs)
	countValueTx := CountValueOfTxsInBlock(txs)
	countZeroValueTx := CountZeroValueOfTxsInBlock(txs)

	logger.Info("parsing",
		zap.String("log", "block"),
		zap.Int("height", block.Height),
		zap.Uint32("timestamp", block.BlockTime),
		zap.String("blk", block.HashHex),
		zap.Uint32("size", block.Size),
		zap.Int("nTx", len(txs)),
		zap.Int("inside", countInsideTx),
		zap.Int("wit", countWitTx),
		zap.Uint64("zero", countZeroValueTx),
		zap.Uint64("v", countValueTx),
		// zap.Duration("backoff", time.Second),
	)
}

// dumpBlock block id
func dumpBlock(block *Block) {

	logger.Info("blkid",
		zap.String("id", block.HashHex),
		zap.Int("height", block.Height),
	)
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *Block) {
	for _, tx := range block.Txs {
		logger.Info("tx-of-block",
			zap.String("tx", tx.HashHex),
			zap.Int("height", block.Height),
		)
	}
}
