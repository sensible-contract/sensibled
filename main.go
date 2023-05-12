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
	"strconv"
	"sync"
	"syscall"
	"time"
	"unisatd/loader/clickhouse"
	"unisatd/logger"
	memLoader "unisatd/mempool/loader"
	memTask "unisatd/mempool/task"
	memSerial "unisatd/mempool/task/serial"
	"unisatd/model"
	"unisatd/parser"
	"unisatd/prune"
	"unisatd/rdb"
	"unisatd/store"
	"unisatd/task"
	"unisatd/task/serial"
	"unisatd/utils"

	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type processInfo struct {
	Start           int64 // 启动开始
	Header          int64 // 读取索引完成
	Block           int64 // 新区块头读取完成，block读取开始
	Mempool         int64 // block读取结束，mempool 同步开始，
	ZmqFirst        int64 // mempool读取完成，zmq读取开始
	ZmqLast         int64 // zmq最后一条消息
	Stop            int64 // 新区块到来，退出
	Height          int
	ConfirmedTx     int
	MempoolFirstIdx int
	MempoolLastIdx  int
	NeedStop        bool
}

func (info *processInfo) String() string {
	stopType := ""
	if info.NeedStop {
		stopType = "!TRIGGER!"
	}
	return fmt.Sprintf("%d StartH:%d idx:%d hdr:%d blk:%d mem:%d zmq:%d nTxInMempool:%d-%d=%d nTxInBlk:%d %s stop:%d",
		info.Start,
		info.Height,

		info.Header,
		info.Block,
		info.Mempool,
		info.ZmqFirst,
		info.ZmqLast,

		info.MempoolLastIdx,
		info.MempoolFirstIdx,
		info.MempoolLastIdx-info.MempoolFirstIdx,
		info.ConfirmedTx,

		stopType,
		info.Stop,
	)
}

var (
	ctx = context.Background()

	selfLabel  = os.Getenv("SELF_LABEL")
	otherLabel = os.Getenv("OTHER_LABEL")

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

	rdb.CacheClient = rdb.InitClient("conf/cache.yaml")

	rdb.RdbBalanceClient = rdb.Init("conf/rdb_balance.yaml")
	rdb.RdbUtxoClient = rdb.Init("conf/rdb_utxo.yaml")
	rdb.RdbAddrTxClient = rdb.Init("conf/rdb_address.yaml")
	clickhouse.Init()
	prune.Init()
}

func logProcessInfo(info processInfo) {
	content := fmt.Sprintf("%s", info.String())
	member := &redis.Z{Score: float64(info.Start), Member: content}

	rdb.RdbBalanceClient.ZRemRangeByScore(ctx, "s:log"+selfLabel, strconv.Itoa(int(info.Start)), strconv.Itoa(int(info.Start)))
	rdb.RdbBalanceClient.ZAdd(ctx, "s:log"+selfLabel, member)
}

func isPrimary() bool {
	label, err := rdb.RdbBalanceClient.Get(ctx, "s:primary").Result()
	if err != nil {
		return false
	}
	return label == selfLabel
}

func switchToSecondary() {
	rdb.RdbBalanceClient.Set(ctx, "s:switch", "false", 0)
	rdb.RdbBalanceClient.Set(ctx, "s:primary", otherLabel, 0)
}

func needToSwitchToSecondary() bool {
	label, err := rdb.RdbBalanceClient.Get(ctx, "s:switch").Result()
	if err != nil {
		return false
	}
	return label == "true"
}

func syncBlock() {
	var info processInfo
	info.Start = time.Now().Unix()
	logProcessInfo(info)

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
		if info.Header == 0 {
			info.Header = time.Now().Unix() - info.Start
			logProcessInfo(info)
		}
		ok := blockchain.InitLongestChainHeader() // 读取新的block header
		if !ok || model.NeedStop {                // 主动触发了结束，则终止
			break
		}

		if selfLabel != "" && !isPrimary() {
			logger.Log.Info("secondary, waiting...")
			time.Sleep(time.Second * 5)
			continue
		}
		if needToSwitchToSecondary() {
			switchToSecondary()
			logger.Log.Info("switch to secondary, quit.")
			break
		}

		var stageBlockID []byte
		needSaveBlock := false
		stageBlockHeight := 0
		txCount := 0
		nftStartNumber := uint64(0)

		if !isFull {
			// 现有追加扫描
			needRemove := false
			if startBlockHeight < 0 {
				// 从clickhouse读取已同步的区块，判断新的同步位置
				commonHeigth, orphanCount, newblock := blockchain.GetBlockSyncCommonBlockHeight(endBlockHeight)
				if orphanCount > 0 {
					needRemove = true
				}
				if commonHeigth < 0 {
					// 节点区块落后于db高度
					logger.Log.Error("less blocks in /node/blocks/")
					time.Sleep(time.Second * 5)
					break
				}
				if newblock == 0 {
					stageBlockHeight = commonHeigth
					nftStartNumber = serial.CountNewNFTInRedisBeforeBlockHeight(commonHeigth + 1)

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

		if info.Block == 0 {
			info.Block = time.Now().Unix() - info.Start
			logProcessInfo(info)
		}

		nftStartNumber = serial.CountNewNFTInRedisBeforeBlockHeight(startBlockHeight)

		// 开始扫描区块，包括start，不包括end，满batchTxCount后终止
		stageBlockID, stageBlockHeight, txCount = blockchain.ParseLongestChain(startBlockHeight, endBlockHeight, batchTxCount, nftStartNumber)
		// 按批次处理区块
		logger.Log.Info("range", zap.Int("start", startBlockHeight), zap.Int("end", stageBlockHeight+1))

		nftStartNumber += uint64(len(model.GlobalNewInscriptions))

		if info.Height == 0 {
			info.Height = stageBlockHeight
			info.ConfirmedTx = txCount
			logProcessInfo(info)
		}

		// 无需同步内存池
		if stageBlockHeight < len(blockchain.BlocksOfChainById)-1 ||
			(endBlockHeight > 0 && stageBlockHeight == endBlockHeight-1) || model.NeedStop {
			needSaveBlock = false

			task.SubmitBlocksWithoutMempool(isFull, stageBlockHeight)

			if len(stageBlockID) == 32 {
				rdb.RdbBalanceClient.HSet(ctx, "info",
					"block", utils.HashString(stageBlockID),
				)
			}
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

		if info.Mempool == 0 {
			info.Mempool = time.Now().Unix() - info.Start
			logProcessInfo(info)
		}
		for {
			needSaveMempool := mempool.Process(initSyncMempool, stageBlockHeight, startIdx, nftStartNumber)
			if !needSaveMempool {
				break
			}
			nftStartNumber += uint64(len(mempool.NewInscriptions))

			memSerial.UpdateUtxoInLocalMapSerial(mempool.SpentUtxoKeysMap,
				mempool.NewUtxoDataMap,
				mempool.RemoveUtxoDataMap)

			if needSaveBlock {
				task.SubmitBlocksWithMempool(isFull, stageBlockHeight, mempool)
				if len(stageBlockID) == 32 {
					rdb.RdbBalanceClient.HSet(ctx, "info",
						"block", utils.HashString(stageBlockID),
					)
				}
				needSaveBlock = false
				logger.Log.Info("block finished")
			} else {
				mempool.SubmitMempoolWithoutBlocks(initSyncMempool)
			}

			initSyncMempool = false
			startIdx += len(mempool.BatchTxs) // 同步完毕
			logger.Log.Info("mempool finished", zap.Int("idx", startIdx), zap.Int("nNewTx", len(mempool.BatchTxs)))

			if info.ZmqFirst == 0 {
				info.ZmqFirst = time.Now().Unix() - info.Start
				info.MempoolFirstIdx = startIdx
			}
			info.ZmqLast = time.Now().Unix() - info.Start
			info.MempoolLastIdx = startIdx
			logProcessInfo(info)

			if needToSwitchToSecondary() {
				break
			}
		}

		// 未完成同步内存池 且未同步区块
		if needSaveBlock {
			task.SubmitBlocksWithoutMempool(isFull, stageBlockHeight)
			if len(stageBlockID) == 32 {
				rdb.RdbBalanceClient.HSet(ctx, "info",
					"block", utils.HashString(stageBlockID),
				)
			}
			logger.Log.Info("block finished")
		}
		isFull = false // 准备继续同步
		startBlockHeight = -1

		if needToSwitchToSecondary() {
			switchToSecondary()
			logger.Log.Info("switch to secondary, quit.")
			break
		}

		if model.NeedStop { // 主动触发了结束，则终止
			break
		}

		if syncOnce { // 终止
			logger.Log.Info("sync once")
			break
		}
	}
	logger.Log.Info("stoped")
	info.NeedStop = model.NeedStop
	info.Stop = time.Now().Unix() - info.Start
	logProcessInfo(info)
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
