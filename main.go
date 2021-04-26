package main

import (
	"blkparser/loader"
	"blkparser/parser"
	"blkparser/store"
	"blkparser/task"
	"blkparser/task/serial"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/spf13/viper"
)

var (
	startBlockHeight int
	endBlockHeight   int
	zmqEndpoint      string
	blocksPath       string
	blockMagic       string
	listen_address   = os.Getenv("LISTEN")
)

func init() {
	flag.BoolVar(&task.IsDump, "dump", false, "dump to file")
	flag.BoolVar(&task.IsSync, "sync", false, "sync to db")
	flag.BoolVar(&task.IsFull, "full", false, "start from genesis")
	flag.BoolVar(&task.WithUtxo, "utxo", true, "with utxo")
	flag.BoolVar(&task.UseMap, "map", false, "use map, instead of redis")

	flag.IntVar(&startBlockHeight, "start", -1, "start block height")
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

	zmqEndpoint = viper.GetString("zmq")
	blocksPath = viper.GetString("blocks")
	blockMagic = viper.GetString("magic")
}

func main() {
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	// "0.0.0.0:8080"
	server := &http.Server{Addr: listen_address, Handler: nil}

	newBlockNotify := make(chan string)

	// 扫描区块
	go func() {
		for {
			// 初始化载入block header
			blockchain.InitLongestChainHeader()

			for {
				if task.IsFull {
					startBlockHeight = 0
					// 重新全量扫描
					if task.IsSync {
						// 初始化同步数据库表
						store.CreateAllSyncCk()
						store.PrepareFullSyncCk()
					}
				} else {
					// 现有追加扫描
					if task.IsSync {
						needRemove := false
						if startBlockHeight < 0 {
							// 从clickhouse读取现有同步区块，判断同步位置
							commonHeigth, orphanCount, newblock := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
							// 从公有块高度（COMMON_HEIGHT）下一个开始扫描
							startBlockHeight = commonHeigth + 1
							if orphanCount > 0 {
								needRemove = true
							}
							if newblock == 0 {
								break
							}
						} else {
							needRemove = true
						}

						if needRemove {
							log.Printf("remove")
							if task.WithUtxo {
								// 在更新之前，如果有上次已导入但是当前被孤立的块，需要先删除这些块的数据。
								// 获取需要补回的utxo
								utxoToRestore, err := loader.GetSpentUTXOAfterBlockHeight(startBlockHeight)
								if err != nil {
									log.Printf("get utxo to restore failed: %v", err)
									break
								}
								utxoToRemove, err := loader.GetNewUTXOAfterBlockHeight(startBlockHeight)
								if err != nil {
									log.Printf("get utxo to remove failed: %v", err)
									break
								}

								if err := serial.UpdateUtxoInRedis(utxoToRestore, utxoToRemove); err != nil {
									log.Printf("restore/remove utxo from redis failed: %v", err)
									break
								}
							}
							store.RemoveOrphanPartSyncCk(startBlockHeight)
						}

						// 初始化同步数据库表
						store.CreatePartSyncCk()
						store.PreparePartSyncCk()
					}
				}

				// 开始扫描区块，包括start，不包括end
				blockchain.ParseLongestChain(startBlockHeight, endBlockHeight)
				log.Printf("finished")
				// 扫描完毕
				break
			}

			if task.IsSync && endBlockHeight < 0 {
				// 等待新块出现，再重新追加扫描
				task.IsFull = false
				startBlockHeight = -1
				log.Printf("waiting new block...")
				<-newBlockNotify
			} else {
				// 结束
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				server.Shutdown(ctx)
			}
		}
	}()

	// 监听新块确认
	go func() {
		// zmqEndpoint := "tcp://192.168.31.236:16331"
		loader.ZmqNotify(zmqEndpoint, newBlockNotify)
	}()

	go func() {
		if !task.UseMap {
			return
		}
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	if err := server.ListenAndServe(); err != nil {
		log.Printf("profile listen failed: %v", err)
	}
}
