package main

import (
	"blkparser/parser"
	"blkparser/task"
	"blkparser/utils"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/spf13/viper"
)

var (
	startBlockHeight int
	endBlockHeight   int
	blocksPath       string
	blockMagic       string
)

func init() {
	flag.BoolVar(&task.IsSync, "sync", false, "sync into db")
	flag.BoolVar(&task.IsFull, "full", false, "full dump")

	flag.IntVar(&startBlockHeight, "start", 0, "start block height")
	flag.IntVar(&endBlockHeight, "end", -1, "end block height")
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
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	server := &http.Server{Addr: "0.0.0.0:8080", Handler: nil}
	go func() {

		// 初始化载入block header
		blockchain.InitLongestChainHeader()

		if task.IsFull {
			log.Printf("full")
			startBlockHeight = 0
			if task.IsSync {
				log.Printf("sync")
				utils.CreateAllSyncCk()
				utils.PrepareFullSyncCk()
			}
		} else {
			log.Printf("part")
			if task.IsSync {
				log.Printf("sync")
				// 从clickhouse读取现有同步区块，判断同步位置
				commonHeigth := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
				startBlockHeight = commonHeigth + 1

				utils.CreatePartSyncCk()
				utils.PreparePartSyncCk()
			}
		}

		blockchain.ParseLongestChain(startBlockHeight, endBlockHeight)
		log.Printf("finished")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	server.ListenAndServe()
}
