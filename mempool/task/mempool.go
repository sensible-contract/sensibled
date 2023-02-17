package task

import (
	"bytes"
	"context"
	"encoding/hex"
	"sync"
	"time"
	"unisatd/logger"
	"unisatd/mempool/loader"
	"unisatd/mempool/parser"
	"unisatd/mempool/store"
	"unisatd/mempool/task/parallel"
	"unisatd/mempool/task/serial"
	"unisatd/model"
	"unisatd/rdb"
	"unisatd/utils"

	"go.uber.org/zap"
)

type Mempool struct {
	Txs     map[string]struct{} // 所有Tx
	SkipTxs map[string]struct{} // 需要跳过的Tx

	BatchTxs               []*model.Tx // 当前同步批次的Tx
	AddrPkhInTxMap         map[string][]int
	SpentUtxoKeysMap       map[string]struct{}       // 在当前同步批次中被花费的所有utxo集合
	SpentUtxoDataMap       map[string]*model.TxoData // 当前同步批次中花费的已确认的utxo集合
	NewUtxoDataMap         map[string]*model.TxoData // 当前同步批次中新产生的utxo集合
	RemoveUtxoDataMap      map[string]*model.TxoData // 当前同步批次中花费的未确认的utxo集合，且属于前批次产生的utxo
	NFTsCreateIndexToNFTID []*model.InscriptionID    // order in block/mempool  nft: nftpoint/nftid

	m sync.Mutex
}

func NewMempool() (mp *Mempool, err error) {
	mp = new(Mempool)
	return
}

func (mp *Mempool) Init() {
	mp.BatchTxs = make([]*model.Tx, 0)
	mp.SpentUtxoKeysMap = make(map[string]struct{}, 1)
	mp.SpentUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.NewUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.RemoveUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.NFTsCreateIndexToNFTID = make([]*model.InscriptionID, 0) // order in block/mempool  nft: nftpoint/nftid
}

func (mp *Mempool) LoadFromMempool() bool {
	// 清空
	mp.Txs = make(map[string]struct{}, 0)
	mp.SkipTxs = make(map[string]struct{}, 0)

	for i := 0; i < 1000; i++ {
		select {
		case <-loader.RawTxNotify:
		default:
			// skip
		}
	}

	rawtxs := loader.GetRawMemPoolRPC()
	if rawtxs == nil {
		return false
	}

	logger.Log.Info("start load all tx in mempool from rpc", zap.Int("count", len(rawtxs)))

	for _, rawtxHex := range rawtxs {
		rawtx, err := hex.DecodeString(rawtxHex.(string))
		if err != nil {
			logger.Log.Info("skip bad rawtxHex")
			continue
		}

		// parser tx
		tx, txoffset := parser.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			logger.Log.Info("skip bad rawtx")
			continue
		}
		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		if tx.WitOffset > 0 {
			tx.TxId = utils.GetWitnessHash256(tx.Raw, tx.WitOffset)
		} else {
			tx.TxId = utils.GetHash256(tx.Raw)
		}
		tx.TxIdHex = utils.HashString(tx.TxId)

		// maybe impossible dup here
		if _, ok := mp.Txs[tx.TxIdHex]; ok {
			logger.Log.Info("init skip dup")
			continue
		}

		if parser.IsTxNonFinal(tx, mp.SkipTxs) {
			logger.Log.Info("skip non final tx",
				zap.String("txid", tx.TxIdHex),
			)
			mp.SkipTxs[tx.TxIdHex] = struct{}{}
			continue
		}

		mp.Txs[tx.TxIdHex] = struct{}{}
		mp.BatchTxs = append(mp.BatchTxs, tx)
	}
	return true
}

// SyncMempoolFromZmq 从zmq同步tx
func (mp *Mempool) SyncMempoolFromZmq() (blockReady bool) {
	COINBASE_TX_PREFIX, _ := hex.DecodeString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff")

	start := time.Now()
	firstGot := false
	rawtx := make([]byte, 0)
	for {
		timeout := false
		select {
		case rawtx = <-loader.RawTxNotify:
			if !firstGot {
				start = time.Now()
			}
			firstGot = true

		case blockIdHex := <-loader.NewBlockNotify:
			logger.Log.Info("block sync subcribe")
			if _, ok := model.GlobalConfirmedBlkMap[blockIdHex]; ok {
				logger.Log.Info("skip confirmed block",
					zap.String("blockid", blockIdHex),
				)
				continue
			}

			blockReady = true
		case <-time.After(time.Second):
			timeout = true
		}

		if timeout {
			if firstGot {
				return false
			} else {
				continue
			}
		}
		if blockReady {
			return true
		}

		// parser tx
		tx, txoffset := parser.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			logger.Log.Info("skip bad rawtx")
			continue
		}
		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		if tx.WitOffset > 0 {
			tx.TxId = utils.GetWitnessHash256(tx.Raw, tx.WitOffset)
		} else {
			tx.TxId = utils.GetHash256(tx.Raw)
		}
		tx.TxIdHex = utils.HashString(tx.TxId)

		// ignore non final tx
		if parser.IsTxNonFinal(tx, mp.SkipTxs) {
			logger.Log.Info("skip non final tx",
				zap.String("txid", tx.TxIdHex),
			)
			mp.SkipTxs[tx.TxIdHex] = struct{}{}
			continue
		}

		// 在内存池重复出现，说明区块已确认，但还未收到zmq hashblock通知。
		if _, ok := mp.Txs[tx.TxIdHex]; ok {
			logger.Log.Info("skip dup",
				zap.String("txid", tx.TxIdHex),
			)
			continue
		}
		// 被区块确认
		if _, ok := model.GlobalConfirmedTxMap[tx.TxIdHex]; ok {
			logger.Log.Info("skip confirmed tx",
				zap.String("txid", tx.TxIdHex),
			)
			continue
		}
		if _, ok := model.GlobalConfirmedTxOldMap[tx.TxIdHex]; ok {
			logger.Log.Info("skip confirmed tx",
				zap.String("txid", tx.TxIdHex),
			)
			continue
		}

		// 首次遇到coinbase，说明有区块确认
		if bytes.HasPrefix(rawtx, COINBASE_TX_PREFIX) {
			blockReady = true
			return true
		}

		logger.Log.Info("tx: " + tx.TxIdHex)
		mp.Txs[tx.TxIdHex] = struct{}{}
		mp.BatchTxs = append(mp.BatchTxs, tx)

		if time.Since(start) > 200*time.Millisecond {
			return false
		}
	}
}

// ParseMempool 开始串行同步mempool
func (mp *Mempool) ParseMempool(startIdx int) {

	mp.AddrPkhInTxMap = make(map[string][]int, len(mp.BatchTxs))
	// first
	for txIdx, tx := range mp.BatchTxs {
		// no dep, 准备utxo花费关系数据
		parallel.ParseUpdateTxoSpendByTxParallel(tx, mp.SpentUtxoKeysMap)

		// 0
		parallel.ParseTxFirst(tx)

		// 1 dep 0
		// NewUtxoDataMap w
		parallel.ParseUpdateNewUtxoInTxParallel(uint64(startIdx+txIdx), tx, mp.NewUtxoDataMap)

		// 按address追踪tx历史
		parallel.ParseUpdateAddressInTxParallel(uint64(startIdx+txIdx), tx, mp.AddrPkhInTxMap)
	}

	// 2 dep 0
	serial.SyncBlockTxOutputInfo(startIdx, mp.BatchTxs)

	// 3 dep 1
	// SpentUtxoDataMap w
	serial.ParseGetSpentUtxoDataFromRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

	// 4 dep 3
	// SpentUtxoDataMap r
	serial.SyncBlockTxInputDetail(startIdx, mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap, mp.AddrPkhInTxMap)

	// 5 dep 2 4
	serial.SyncBlockTx(startIdx, mp.BatchTxs)
}

// ParseEnd 最后分析执行
func ParseEnd() bool {
	// 7 dep 5
	if ok := store.CommitSyncCk(); !ok {
		return false
	}
	return store.ProcessPartSyncCk()
}

func (mp *Mempool) Process(initSyncMempool bool, stageBlockHeight, startIdx int) bool {
	mp.Init()
	if initSyncMempool {
		logger.Log.Info("init sync mempool...")
		model.CleanMempoolUtxoMap()
		if ok := mp.LoadFromMempool(); !ok { // 重新全量同步
			logger.Log.Info("LoadFromMempool failed")
			return false
		}

		latestBlockHeight := loader.GetBlockCountRPC()
		if stageBlockHeight < latestBlockHeight-1 {
			// 有新区块，不同步内存池
			return false
		}

		store.ProcessAllSyncCk() // 从db删除mempool数据
	} else {
		// 现有追加同步
		isNewBlockReady := mp.SyncMempoolFromZmq()
		if isNewBlockReady {
			return false
		}
		if model.NeedStop { // 主动触发了结束，则终止
			return false
		}
	}
	store.CreatePartSyncCk()  // 初始化同步数据库表
	store.PreparePartSyncCk() // 准备同步db，todo: 可能初始化失败
	mp.ParseMempool(startIdx) // 开始同步mempool
	return true
}

// SubmitMempoolWithoutBlocks
func (mp *Mempool) SubmitMempoolWithoutBlocks(initSyncMempool bool) {
	var wg sync.WaitGroup

	// address history
	wg.Add(1)
	go func() {
		defer wg.Done()
		return

		// Pika更新addr tx历史
		if ok := serial.SaveAddressTxHistoryIntoPika(initSyncMempool, mp.AddrPkhInTxMap); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("history done")
	}()

	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()
		// ParseEnd 最后分析执行
		// 7 dep 5
		if ok := ParseEnd(); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("ck done")
	}()

	if len(mp.NewUtxoDataMap)+len(mp.RemoveUtxoDataMap) > 0 {
		// pika
		wg.Add(1)
		go func() {
			defer wg.Done()

			// 批量更新redis utxo
			// for txin dump
			// 6 dep 2 4
			if ok := serial.UpdateUtxoInPika(mp.NewUtxoDataMap, mp.RemoveUtxoDataMap); !ok {
				model.NeedStop = true
				return
			}
			logger.Log.Info("pika done")
		}()
	}

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()

		rdsPipe := rdb.RdbBalanceClient.TxPipeline()
		// for txin dump
		// 6 dep 2 4
		serial.UpdateUtxoInRedis(rdsPipe, initSyncMempool,
			mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

		ctx := context.Background()
		if _, err := rdsPipe.Exec(ctx); err != nil {
			logger.Log.Error("redis exec failed", zap.Error(err))
			model.NeedStop = true
		}
		logger.Log.Info("redis done")
	}()
	wg.Wait()

}
