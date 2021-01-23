package blkparser

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	lastLogTime      time.Time
	lastBlockHeight  int
	lastBlockTxCount int

	calcMap map[string]int

	logger    *zap.Logger
	loggerErr *zap.Logger
)

func init() {
	calcMap = make(map[string]int, 0)

	// logger, _ = zap.NewProduction()
	logger, _ = zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"/data/output.log"},             // 输出目的地
	}.Build()

	loggerErr, _ = zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"stderr"},                       // 输出目的地
	}.Build()
}

func ParseBlock(block *Block, maxBlockHeight int) {
	ParseBlockSpeed(len(block.Txs), block.Height, maxBlockHeight)
	// ParseBlockCount(block)

	// dumpBlock(block)
	// dumpBlockTx(block)

	// dumpLockingScriptType(block)
	dumpUtxo(block)
	dumpTxoSpendBy(block)

	block.Txs = nil
}

func ParseEnd() {
	filePathUTXO := "/data/calcMap.bsv"
	fileUTXO, err := os.OpenFile(filePathUTXO, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	defer fileUTXO.Close()

	write := bufio.NewWriter(fileUTXO)

	logger.Info("end",
		zap.Int("dataMap", len(calcMap)),
	)
	// log.Printf("len calcMap: %d", len(calcMap))
	for keyStr, value := range calcMap {
		key := []byte(keyStr)

		// write.WriteString(fmt.Sprintf("%s %d %d\n",
		// 	HashString(key[:32]),
		// 	binary.LittleEndian.Uint32(key[32:]), value))

		write.WriteString(fmt.Sprintf("%s %d\n",
			hex.EncodeToString(key),
			value))

	}

	write.Flush()

	logger.Sync()
	loggerErr.Sync()
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
		// log.Printf("%d, speed: %d bps, tx: %d tps, time: %d s, dataMap: %d",
		// 	nextBlockHeight,
		// 	nextBlockHeight-lastBlockHeight
		// 	lastBlockTxCount,
		// 	timeLeft,
		// 	len(calcMap),
		// )
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

	// log.Printf("%d Time: %d blk: %s size: %d nTx: %d %d %d %d value: %d",
	// 	block.Height,
	// 	block.BlockTime,
	// 	block.HashHex,
	// 	block.Size, len(txs),
	// 	countInsideTx, countWitTx, countZeroValueTx,
	// 	countValueTx,
	// )

}

// dumpBlock block id
func dumpBlock(block *Block) {

	logger.Info("blkid",
		zap.String("id", block.HashHex),
		zap.Int("height", block.Height),
	)

	// fmt.Println("blkid "+block.HashHex,
	// 	block.Height,
	// )
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *Block) {
	for _, tx := range block.Txs {
		logger.Info("tx-of-block",
			zap.String("tx", tx.HashHex),
			zap.Int("height", block.Height),
		)

		// fmt.Println("tx-of-block "+tx.HashHex,
		// 	block.Height,
		// )
	}
}

// dumpLockingScriptType  信息
func dumpLockingScriptType(block *Block) {
	txs := block.Txs

	for _, tx := range txs {
		// fmt.Println("tx", tx.HashHex)
		for idx, output := range tx.TxOuts {
			if output.Value == 0 || !output.LockingScriptMatch {
				continue
			}

			key := string(output.LockingScriptType)
			if _, ok := calcMap[key]; ok {
				calcMap[key] += 1
			} else {
				calcMap[key] = 1
			}

			logger.Info("pkscript",
				zap.String("tx", tx.HashHex),
				zap.Int("vout", idx),
				zap.Uint64("v", output.Value),
				zap.String("type", output.LockingScriptTypeHex),
			)
			// fmt.Println("pkscript vout",
			// 	idx,
			// 	output.Value,
			// 	output.LockingScriptTypeHex,
			// )
		}
	}
}

// dumpUtxo utxo 信息
func dumpUtxo(block *Block) {
	txs := block.Txs

	for _, tx := range txs {
		// fmt.Println("utxo", tx.HashHex)
		for idx, output := range tx.TxOuts {
			if output.Value == 0 || !output.LockingScriptMatch {
				continue
			}

			calcMap[output.OutpointKey] = int(output.Value)

			logger.Info("utxo",
				zap.String("tx", tx.HashHex),
				zap.Int("vout", idx),
				zap.Uint64("v", output.Value),
			)

			// fmt.Println("utxo vout",
			// 	idx,
			// 	output.Value,
			// )
		}
	}
}

// dumpTxoSpendBy utxo被使用
func dumpTxoSpendBy(block *Block) {
	txs := block.Txs
	for _, tx := range txs[1:] {
		for idx, input := range tx.TxIns {
			if _, ok := calcMap[input.InputOutpointKey]; !ok {
				continue
			}
			delete(calcMap, input.InputOutpointKey)

			logger.Info("spend",
				zap.String("tx", input.InputHashHex),
				zap.Uint32("vout", input.InputVout),
				zap.Int("idx", idx),
			)

			// fmt.Println("spend "+input.InputHashHex,
			// 	input.InputVout,
			// 	idx,
			// )
		}
		logger.Info("by",
			zap.String("tx", tx.HashHex),
		)
		// fmt.Println("by", tx.HashHex)
	}
}
