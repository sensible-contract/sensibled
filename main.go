package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"satoblock/loader"
	"satoblock/logger"
	"satoblock/parser"
	"satoblock/store"
	"satoblock/task/serial"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	startBlockHeight int
	endBlockHeight   int
	batchBlockCount  int
	zmqEndpoint      string
	blocksPath       string
	blockMagic       string
	isFull           bool

	cpuProfile   string
	memProfile   string
	traceProfile string
)

func init() {
	flag.StringVar(&cpuProfile, "cpu", "", "write cpu profile to file")
	flag.StringVar(&memProfile, "mem", "", "write mem profile to file")
	flag.StringVar(&traceProfile, "trace", "", "write trace profile to file")

	flag.BoolVar(&isFull, "full", false, "start from genesis")
	flag.IntVar(&startBlockHeight, "start", -1, "start block height")
	flag.IntVar(&endBlockHeight, "end", -1, "end block height")
	flag.IntVar(&batchBlockCount, "batch", 0, "batch block count")

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
	//采样cpu运行状态
	if cpuProfile != "" {
		cpuf, err := os.Create(cpuProfile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(cpuf)
		defer pprof.StopCPUProfile()
	}
	// 采样goroutine
	if traceProfile != "" {
		tracef, err := os.Create(traceProfile)
		if err != nil {
			panic(err)
		}
		trace.Start(tracef)
		defer tracef.Close()
		defer trace.Stop()
	}

	// 监听新块确认
	newBlockNotify := make(chan string)
	go func() {
		loader.ZmqNotify(zmqEndpoint, newBlockNotify)
	}()

	// GC
	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	// 初始化区块
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		logger.Log.Error("init chain error", zap.Error(err))
		return
	}

	//创建监听退出
	sigCtrl := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(sigCtrl, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range sigCtrl {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				logger.Log.Info("program exit...")
				blockchain.NeedStop = true
				newBlockNotify <- ""
			default:
				fmt.Println("other signal", s)
				logger.Log.Info("other signal", zap.String("sig", s.String()))
			}
		}
	}()
	needOneMoreSync := true
	// 扫描区块
	for {
		// 等待新块出现，再重新追加扫描
		logger.Log.Info("waiting new block...")
		if !needOneMoreSync {
			<-newBlockNotify
		}
		needOneMoreSync = false

		// 初始化载入block header
		blockchain.InitLongestChainHeader()

		if blockchain.NeedStop {
			// 结束
			break
		}

		if !isFull {
			// 现有追加扫描
			needRemove := false
			if startBlockHeight < 0 {
				// 从clickhouse读取现有同步区块，判断同步位置
				commonHeigth, orphanCount, newblock := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
				if orphanCount > 0 {
					needRemove = true
				}
				if newblock == 0 {
					// 无新区块，开始等待
					continue
				}
				// 从公有块高度（COMMON_HEIGHT）下一个开始扫描
				startBlockHeight = commonHeigth + 1
			} else {
				needRemove = true
			}

			if needRemove {
				logger.Log.Info("remove...")
				// 在更新之前，如果有上次已导入但是当前被孤立的块，需要先删除这些块的数据。
				// 获取需要补回的utxo
				utxoToRestore, err := loader.GetSpentUTXOAfterBlockHeight(startBlockHeight)
				if err != nil {
					logger.Log.Error("get utxo to restore failed", zap.Error(err))
					break
				}
				utxoToRemove, err := loader.GetNewUTXOAfterBlockHeight(startBlockHeight)
				if err != nil {
					logger.Log.Error("get utxo to remove failed", zap.Error(err))
					break
				}

				if err := serial.UpdateUtxoInRedis(utxoToRestore, utxoToRemove, true); err != nil {
					logger.Log.Error("restore/remove utxo from redis failed", zap.Error(err))
					break
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

		// 按批次处理区块
		stageBlockHeight := endBlockHeight
		if batchBlockCount > 0 {
			needOneMoreSync = true
			if endBlockHeight < 0 || (endBlockHeight-startBlockHeight > batchBlockCount) {
				stageBlockHeight = startBlockHeight + batchBlockCount
			}
		}
		logger.Log.Info("range", zap.Int("start", startBlockHeight), zap.Int("end", stageBlockHeight))
		// 开始扫描区块，包括start，不包括end
		blockchain.ParseLongestChain(startBlockHeight, stageBlockHeight, isFull)
		logger.Log.Info("finished")

		isFull = false
		startBlockHeight = -1
		serial.PublishBlockSyncFinished()

		// 扫描完毕
		if (stageBlockHeight == endBlockHeight && endBlockHeight > 0) || blockchain.NeedStop {
			// 结束
			break
		}
	}

	loader.DumpToGobFile("./cmd/headers-list.gob", blockchain.Blocks)
	logger.Log.Info("stoped")

	////////////////
	//采样memory状态
	if memProfile != "" {
		memf, err := os.Create(memProfile)
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(memf)
		memf.Close()
	}

	if blockchain.NeedStop {
		os.Exit(1)
	}
}
