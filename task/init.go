package task

import (
	"blkparser/model"
	"blkparser/task/parallel"
	"blkparser/task/serial"
	"blkparser/utils"
	"fmt"

	"github.com/spf13/viper"
)

var (
	MaxBlockHeightParallel int
	DumpBlock              bool = true
	DumpTx                 bool
	DumpTxin               bool
	DumpTxout              bool
	DumpTxinFull           bool
)

func init() {
	viper.SetConfigFile("conf/task.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	} else {
		DumpBlock = viper.GetBool("block")
		DumpTx = viper.GetBool("tx")
		DumpTxin = viper.GetBool("txin")
		DumpTxout = viper.GetBool("txout")
		DumpTxinFull = viper.GetBool("txin_full")
	}

	// serial.LoadUtxoFromGobFile()
}

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for idx, tx := range block.Txs {
		isCoinbase := idx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// for txin full dump
		if DumpTxinFull {
			parallel.ParseTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
			parallel.ParseUtxoParallel(tx, block.ParseData)
		}
	}

	// DumpBlockData
	if DumpBlock {
		serial.DumpBlock(block)
	}
	if DumpTx {
		serial.DumpBlockTx(block)
	}
	if DumpTxout {
		serial.DumpBlockTxOutputInfo(block)
	}
	if DumpTxin {
		serial.DumpBlockTxInputInfo(block)
	}
}

// ParseBlockSerial 再串行分析区块
func ParseBlockSerial(block *model.Block, blockCountInBuffer, maxBlockHeight int) {
	serial.ParseBlockSpeed(len(block.Txs), block.Height, blockCountInBuffer, MaxBlockHeightParallel, maxBlockHeight)

	// DumpBlockData
	if DumpTxinFull {
		serial.DumpBlockTxInputDetail(block)
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
