package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"satoblock/loader"
	"satoblock/parser"
	"satoblock/store"
	"satoblock/task/serial"
	"syscall"
	"time"

	"github.com/spf13/viper"
)

var (
	startBlockHeight int
	endBlockHeight   int
	zmqEndpoint      string
	blocksPath       string
	blockMagic       string
	isFull           bool
)

func init() {
	flag.BoolVar(&isFull, "full", false, "start from genesis")

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
	// 监听新块确认
	newBlockNotify := make(chan string)
	go func() {
		// 启动
		newBlockNotify <- ""
		loader.ZmqNotify(zmqEndpoint, newBlockNotify)
	}()

	// GC
	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	//创建监听退出
	var needStop bool
	sigCtrl := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(sigCtrl, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range sigCtrl {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				log.Println("program exit...", s)
				needStop = true
				newBlockNotify <- ""
			default:
				fmt.Println("other signal", s)
			}
		}
	}()

	// 初始化区块
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}
	// 扫描区块
	for {
		// 等待新块出现，再重新追加扫描
		log.Println("waiting new block...")
		<-newBlockNotify
		if needStop {
			// 结束
			break
		}

		// 初始化载入block header
		blockchain.InitLongestChainHeader()

		if !isFull {
			// 现有追加扫描
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
					// 无新区块，开始等待
					continue
				}
			} else {
				needRemove = true
			}

			if needRemove {
				log.Println("remove")
				// 在更新之前，如果有上次已导入但是当前被孤立的块，需要先删除这些块的数据。
				// 获取需要补回的utxo
				utxoToRestore, err := loader.GetSpentUTXOAfterBlockHeight(startBlockHeight)
				if err != nil {
					log.Printf("get utxo to restore failed: %v", err)
					return
				}
				utxoToRemove, err := loader.GetNewUTXOAfterBlockHeight(startBlockHeight)
				if err != nil {
					log.Printf("get utxo to remove failed: %v", err)
					return
				}

				if err := serial.UpdateUtxoInRedis(utxoToRestore, utxoToRemove, true); err != nil {
					log.Printf("restore/remove utxo from redis failed: %v", err)
					return
				}
				store.RemoveOrphanPartSyncCk(startBlockHeight)
			}
		}

		if isFull {
			// 重新全量扫描
			startBlockHeight = 0

			// 清空redis
			serial.FlushdbInRedis()

			// 初始化同步数据库表
			store.CreateAllSyncCk()
			store.PrepareFullSyncCk()
		} else {
			// 初始化同步数据库表
			store.CreatePartSyncCk()
			store.PreparePartSyncCk()
		}

		// 开始扫描区块，包括start，不包括end
		blockchain.ParseLongestChain(startBlockHeight, endBlockHeight, isFull)
		log.Println("finished")

		isFull = false
		startBlockHeight = -1
		serial.PublishBlockSyncFinished()

		// 扫描完毕
		if endBlockHeight > 0 || needStop {
			// 结束
			break
		}
	}
	log.Println("stoped")
}
