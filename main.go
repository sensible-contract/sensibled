package main

import (
	"blkparser/loader"
	"blkparser/parser"
	"blkparser/task"
	"blkparser/task/serial"
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

	newBlockNotify := make(chan string)
	go func() {
		for {
			// 初始化载入block header
			blockchain.InitLongestChainHeader()

			for {
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
						commonHeigth, orphanCount, newblock := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
						startBlockHeight = commonHeigth + 1
						if newblock == 0 {
							break
						}
						if orphanCount > 0 {
							log.Printf("orphan")
							// 在更新之前，如果有上次已导入但是当前被孤立的块，需要先删除这些块的数据。
							// 获取需要补回的utxo
							utxoToRestore, err := loader.GetSpentUTXOAfterBlockHeight(commonHeigth)
							if err != nil {
								log.Printf("get utxo to restore failed: %v", err)
								break
							}
							utxoToRemove, err := loader.GetNewUTXOAfterBlockHeight(commonHeigth)
							if err != nil {
								log.Printf("get utxo to remove failed: %v", err)
								break
							}

							if err := serial.RestoreUtxoFromRedis(utxoToRestore, utxoToRemove); err != nil {
								log.Printf("restore/remove utxo from redis failed: %v", err)
								break
							}

							// 直接从公有块高度（COMMON_HEIGHT）往上删除就可以了。
							utils.RemoveOrphanPartSyncCk(commonHeigth)
						}

						utils.CreatePartSyncCk()
						utils.PreparePartSyncCk()
					}
				}

				blockchain.ParseLongestChain(startBlockHeight, endBlockHeight)
				log.Printf("finished")
				break
			}
			if task.IsSync && endBlockHeight < 0 {
				task.IsFull = false
				<-newBlockNotify
				log.Printf("new block")
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				server.Shutdown(ctx)
			}
		}
	}()
	go func() {
		loader.ZmqNotify(newBlockNotify)
	}()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	server.ListenAndServe()
}
