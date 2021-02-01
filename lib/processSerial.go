package blkparser

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	lastLogTime            time.Time
	lastBlockHeight        int
	lastBlockTxCount       int
	lastUtxoMapAddCount    int
	lastUtxoMapRemoveCount int
)

func ParseBlockSerial(block *Block, maxBlockHeight int) {
	ParseBlockSpeed(len(block.Txs), block.Height, maxBlockHeight)
	// ParseBlockCount(block)

	// parseUtxoSerial(block.ParseData)

	dumpBlock(block)
	dumpBlockTx(block)
	dumpBlockTxInfo(block)

	block.ParseData = nil
	block.Txs = nil
}

func ParseEnd() {
	defer logger.Sync()
	defer loggerErr.Sync()

	// dumpUtxoToGobFile()

	loggerMap, _ := zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"/data/calcMap.log"},            // 输出目的地
	}.Build()
	defer loggerMap.Sync()

	// ParseEndDumpUtxo(loggerMap)
	// ParseEndDumpScriptType(loggerMap)
}

func dumpUtxoToGobFile() {
	utxoFile, err := os.OpenFile("/data/utxo.gob", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		loggerErr.Info("dump utxo",
			zap.String("log", "dump utxo file failed"),
		)
		return
	}
	defer utxoFile.Close()

	enc := gob.NewEncoder(utxoFile)
	if err := enc.Encode(utxoMap); err != nil {
		loggerErr.Info("dump utxo",
			zap.String("log", "dump utxo failed"),
		)
	}
	loggerErr.Info("dump utxo",
		zap.String("log", "dump utxo ok"),
	)
}

func ParseEndDumpUtxo(log *zap.Logger) {
	for keyStr, data := range utxoMap {
		key := []byte(keyStr)

		log.Info("utxo",
			zap.Int("h", data.BlockHeight),
			zap.String("tx", HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.String("type", data.ScriptType),
			zap.Int("n", len(data.ScriptType)),
		)
	}
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

	if nextBlockHeight != maxBlockHeight-1 && time.Since(lastLogTime) < time.Second {
		return
	}

	if nextBlockHeight < lastBlockHeight {
		lastBlockHeight = 0
	}

	lastLogTime = time.Now()

	timeLeft := 0
	if maxBlockHeight > 0 && (nextBlockHeight-lastBlockHeight) != 0 {
		timeLeft = (maxBlockHeight - nextBlockHeight) / (nextBlockHeight - lastBlockHeight)
	}

	loggerErr.Info("parsing",
		zap.Int("height", nextBlockHeight),
		zap.Int("blk", nextBlockHeight-lastBlockHeight),
		zap.Int("tx", lastBlockTxCount),
		zap.Int("+u", lastUtxoMapAddCount),
		zap.Int("-u", lastUtxoMapRemoveCount),
		zap.Int("=u", lastUtxoMapAddCount-lastUtxoMapRemoveCount),
		zap.Int("utxo", len(utxoMap)),
		zap.Int("calc", len(calcMap)),
		zap.Int("time", timeLeft),
	)

	lastBlockHeight = nextBlockHeight
	lastBlockTxCount = 0
	lastUtxoMapAddCount = 0
	lastUtxoMapRemoveCount = 0
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
	)
}

// dumpBlock block id
func dumpBlock(block *Block) {
	logger.Info("blk-list",
		zap.String("b", block.HashHex),
		zap.Int("h", block.Height),
	)
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *Block) {
	for _, tx := range block.Txs {
		logger.Info("tx-list",
			zap.String("t", tx.HashHex),
			zap.String("b", block.HashHex),
			// zap.Int("h", block.Height),
		)
	}
}

// dumpBlockTxInfo all tx info
func dumpBlockTxInfo(block *Block) {
	for _, tx := range block.Txs {
		logger.Info("tx-info",
			zap.String("t", tx.HashHex),
			zap.Uint32("i", tx.TxInCnt),
			zap.Uint32("o", tx.TxOutCnt),
			zap.Array("in", tx.TxIns),
			zap.Array("out", tx.TxOuts),
			// zap.Int("h", block.Height),
		)
	}
}

// parseUtxoSerial utxo 信息
func parseUtxoSerial(block *ProcessBlock) {
	lastUtxoMapAddCount += len(block.UtxoMap)
	lastUtxoMapRemoveCount += len(block.UtxoMissingMap)

	for key, data := range block.UtxoMap {
		utxoMap[key] = data
	}
	for key := range block.UtxoMissingMap {
		delete(utxoMap, key)
	}
}
