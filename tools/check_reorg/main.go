// go build -v tools/check_reorg
// ./check_reorg -block 000000000000000008e031f99c8545487ac10fe8c9a09cdfbfef3688a53e7dba -end 694790

package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"sensibled/logger"
	"sensibled/parser"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	currentBlockId string
	endBlockHeight int
	blocksPath     string
	blockMagic     string
)

func init() {
	// 当前区块Id
	flag.StringVar(&currentBlockId, "block", "", "current block id")
	// 同步截止区块高度
	flag.IntVar(&endBlockHeight, "end", 100, "end block height")
	flag.Parse()

	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	blocksPath = viper.GetString("blocks")
	blockMagic = viper.GetString("magic")
}

func main() {
	// 初始化区块
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		logger.Log.Error("init chain error", zap.Error(err))
		return
	}
	// 初始化载入block header
	blockchain.InitLongestChainHeader()

	////////////////////////////////////////////////////////////////
	if endBlockHeight < 0 || endBlockHeight > len(blockchain.BlocksOfChainById) {
		endBlockHeight = len(blockchain.BlocksOfChainById)
	}
	blockIdHex := currentBlockId
	orphanCount := 0
	for {
		block, ok := blockchain.Blocks[blockIdHex]
		if !ok {
			logger.Log.Error("blockId not found",
				zap.String("blkId", blockIdHex))
			break
		}

		if _, ok := blockchain.BlocksOfChainById[blockIdHex]; ok {
			newblock := endBlockHeight - int(block.Height) - 1
			logger.Log.Info("shoud sync block",
				zap.Int("lastHeight", block.Height),
				zap.Int("nOrphan", orphanCount),
				zap.Int("nBlkNew", newblock))
			break
		}

		blockIdHex = block.ParentHex
		orphanCount++
	}

	logger.Log.Info("stoped")
}
