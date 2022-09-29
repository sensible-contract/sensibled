package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sensibled/loader/clickhouse"
	"sensibled/logger"
	memLoader "sensibled/mempool/loader"
	memTask "sensibled/mempool/task"
	memSerial "sensibled/mempool/task/serial"
	"sensibled/model"
	"sensibled/parser"
	"sensibled/rdb"
	"sensibled/store"
	"sensibled/task"
	"sensibled/task/serial"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	ctx = context.Background()

	startBlockHeight int
	endBlockHeight   int
	batchTxCount     int
	blocksPath       string
	blockMagic       string
	blockStrip       bool
	isFull           bool
	syncOnce         bool
	gobFlushFrom     int

	cpuProfile   string
	memProfile   string
	traceProfile string
)

func init() {
	flag.StringVar(&cpuProfile, "cpu", "", "write cpu profile to file")
	flag.StringVar(&memProfile, "mem", "", "write mem profile to file")
	flag.StringVar(&traceProfile, "trace", "", "write trace profile to file")

	flag.BoolVar(&blockStrip, "strip", false, "load blocks from striped files")
	flag.BoolVar(&syncOnce, "once", false, "sync 1 block then stop")
	flag.BoolVar(&isFull, "full", false, "start from genesis")
	flag.IntVar(&startBlockHeight, "start", -1, "start block height")
	flag.IntVar(&endBlockHeight, "end", -1, "end block height")
	flag.IntVar(&batchTxCount, "batch", 0, "batch tx count")

	flag.IntVar(&gobFlushFrom, "gob", -1, "gob flush block header cache after fileIdx")

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

	rdb.RedisClient = rdb.Init("conf/redis.yaml")
	rdb.PikaClient = rdb.Init("conf/pika.yaml")
	clickhouse.Init()
	serial.Init()
}

func syncBlock() {
	blockchain, err := parser.NewBlockchain(blockStrip, blocksPath, blockMagic) // 初始化区块
	if err != nil {
		logger.Log.Error("init blockchain error", zap.Error(err))
		return
	}

	// 重新扫区块头缓存
	if gobFlushFrom > 0 {
		blockchain.LastFileIdx = gobFlushFrom
	}

	var onceRpc sync.Once
	var onceZmq sync.Once

	// 扫描区块
	for {
		ok := blockchain.InitLongestChainHeader() // 读取新的block header
		if !ok || model.NeedStop {                // 主动触发了结束，则终止
			break
		}

		needSaveBlock := false
		stageBlockHeight := 0
		if !isFull {
			// 现有追加扫描
			needRemove := false
			if startBlockHeight < 0 {
				// 从clickhouse读取已同步的区块，判断新的同步位置
				commonHeigth, orphanCount, newblock := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
				if orphanCount > 0 {
					needRemove = true
				}
				if newblock == 0 {
					stageBlockHeight = commonHeigth
					goto WAIT_BLOCK // 无新区块，开始等待
				}
				startBlockHeight = commonHeigth + 1 // 从公有块高度（COMMON_HEIGHT）下一个开始扫描
			} else {
				// 手动指定同步位置
				needRemove = true
			}
			if needRemove {
				if ok := task.RemoveBlocksForReorg(startBlockHeight); !ok {
					break
				}
			}

			store.CreatePartSyncCk() // 初始化同步数据库表
			store.PreparePartSyncCk()
		} else {
			startBlockHeight = 0    // 重新全量扫描
			rdb.FlushdbInRedis()    // 清空redis
			store.CreateAllSyncCk() // 初始化同步数据库表
			store.PrepareFullSyncCk()
		}

		needSaveBlock = true
		// model.CleanConfirmedTxMap(false)
		model.CleanConfirmedTxMap(true) // 注意暂时不保存10个块的txid，而是要求节点zmq通知中去掉确认区块tx

		// 开始扫描区块，包括start，不包括end，满batchTxCount后终止
		stageBlockHeight = blockchain.ParseLongestChain(startBlockHeight, endBlockHeight, batchTxCount)
		// 按批次处理区块
		logger.Log.Info("range", zap.Int("start", startBlockHeight), zap.Int("end", stageBlockHeight))

		// 无需同步内存池
		if stageBlockHeight < len(blockchain.BlocksOfChainById)-1 ||
			(endBlockHeight > 0 && stageBlockHeight == endBlockHeight-1) || model.NeedStop {
			needSaveBlock = false

			task.SubmitBlocksWithoutMempool(isFull, stageBlockHeight)

			isFull = false // 准备继续同步
			startBlockHeight = -1
			logger.Log.Info("block finished")
		}
		if stageBlockHeight < len(blockchain.BlocksOfChainById)-1 {
			continue
		}

	WAIT_BLOCK:
		// 扫描完毕，结束
		if (endBlockHeight > 0 && stageBlockHeight == endBlockHeight-1) || model.NeedStop {
			break
		}

		// 等待新块出现，再重新追加扫描
		logger.Log.Info("waiting new block...")

		// 同步内存池
		startIdx := 0
		initSyncMempool := true

		mempool, err := memTask.NewMempool() // 准备内存池
		if err != nil {
			logger.Log.Info("init mempool error: %v", zap.Error(err))
			return
		}
		onceRpc.Do(memLoader.InitRpc)
		onceZmq.Do(memLoader.InitZmq)
		for {
			needSaveMempool := mempool.Process(initSyncMempool, stageBlockHeight, startIdx)
			if !needSaveMempool {
				break
			}

			memSerial.UpdateUtxoInLocalMapSerial(mempool.SpentUtxoKeysMap,
				mempool.NewUtxoDataMap,
				mempool.RemoveUtxoDataMap)

			if needSaveBlock {
				task.SubmitBlocksWithMempool(isFull, stageBlockHeight, mempool, initSyncMempool)
				needSaveBlock = false
			} else {
				mempool.SubmitMempoolWithoutBlocks(initSyncMempool)
			}

			initSyncMempool = false
			startIdx += len(mempool.BatchTxs) // 同步完毕
			logger.Log.Info("mempool finished", zap.Int("idx", startIdx), zap.Int("nNewTx", len(mempool.BatchTxs)))
		}

		// 未完成同步内存池 且未同步区块
		if needSaveBlock {
			task.SubmitBlocksWithoutMempool(isFull, stageBlockHeight)
		}
		isFull = false // 准备继续同步
		startBlockHeight = -1
		logger.Log.Info("block finished")
		if model.NeedStop { // 主动触发了结束，则终止
			break
		}

		if syncOnce { // 终止
			logger.Log.Info("sync once")
			break
		}
	}
	logger.Log.Info("stoped")
}

func main() {
	// pprof
	go func() {
		http.ListenAndServe("0.0.0.0:8000", nil)
	}()

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

	//创建监听退出
	sigCtrl := make(chan os.Signal, 1)
	//监听指定信号 ctrl+c kill
	signal.Notify(sigCtrl, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range sigCtrl {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				triggerStop()
			default:
				fmt.Println("other signal", s)
				logger.Log.Info("other signal", zap.String("sig", s.String()))
			}
		}
	}()

	// GC
	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	syncBlock()
	logger.SyncLog()

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

	if model.NeedStop {
		os.Exit(1)
	}
}

func triggerStop() {
	logger.Log.Info("program exit...")
	model.NeedStop = true
	select {
	case memLoader.NewBlockNotify <- "":
	default:
	}
}
