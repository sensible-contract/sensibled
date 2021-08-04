// go build -v tools/block_graph.go
// ./block_graph -end 695441  > tools/branch.dot
// dot branch.dot -T svg -o branch.svg

package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"satoblock/logger"
	"satoblock/model"
	"satoblock/parser"
	"satoblock/utils"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	BLK_OMIT_COUNT    = 3
	HASH_SHORT_LENGTH = 8
)

var (
	endBlockHeight int
	blocksPath     string
	blockMagic     string
)

func init() {
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

	logger.Log.Info("count", zap.Int("BlocksOfChainById", len(blockchain.BlocksOfChainById)),
		zap.Int("Blocks", len(blockchain.Blocks)),
	)

	if endBlockHeight < 0 || endBlockHeight > len(blockchain.BlocksOfChainById) {
		endBlockHeight = len(blockchain.BlocksOfChainById) - 1
	}

	heightBlocks := make([][]*model.Block, len(blockchain.BlocksOfChainById))

	for _, blk := range blockchain.Blocks {
		blks := heightBlocks[blk.Height]
		blks = append(blks, blk)

		heightBlocks[blk.Height] = blks
	}

	fmt.Println(`#@startdot
digraph demo {
    node [shape="record", height=0]
    "0000000000000000000000000000000000000000000000000000000000000000" [label="<f0> - | <f1> 0000000000000000000000000000000000000000000000000000000000000000"]`)

	showCount := BLK_OMIT_COUNT

	var lastBlk *model.Block
	var prevBlkHash string
	for height, blks := range heightBlocks {
		if height > endBlockHeight {
			logger.Log.Info("the end", zap.Int("height", height))
			break
		}
		if len(blks) == 0 {
			logger.Log.Info("empty height", zap.Int("height", height))
			break
		}

		if len(blks) > 1 || height == endBlockHeight {
			showCount = BLK_OMIT_COUNT
			if lastBlk != nil {
				if utils.HashString(lastBlk.Hash) != prevBlkHash {
					fmt.Printf(`"%s" [label="<f0> %d | <f1> ...%s"]
`, utils.HashString(lastBlk.Hash), lastBlk.Height, utils.HashString(lastBlk.Hash)[64-HASH_SHORT_LENGTH:])
					fmt.Printf(`"%s":f0 -> "%s":f0 [style="dotted"]
`, utils.HashString(lastBlk.Hash), prevBlkHash)
				}
				lastBlk = nil
			}
		} else {
			lastBlk = blks[0]
		}

		if showCount < 0 {
			continue
		}
		showCount--
		for _, blk := range blks {
			fmt.Printf(`"%s" [label="<f0> %d | <f1> ...%s"]
`, utils.HashString(blk.Hash), blk.Height, utils.HashString(blk.Hash)[64-HASH_SHORT_LENGTH:])

			fmt.Printf(`"%s":f0 -> "%s":f0
`, utils.HashString(blk.Hash), utils.HashString(blk.Parent))
		}
		if len(blks) == 1 && lastBlk != nil {
			prevBlkHash = utils.HashString(lastBlk.Hash)
		}
	}
	fmt.Println(`}

#@enddot`)

	logger.Log.Info("stoped")
}
