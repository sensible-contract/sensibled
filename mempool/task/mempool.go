package task

import (
	"satoblock/logger"
	"satoblock/mempool/loader"
	"satoblock/mempool/parser"
	"satoblock/mempool/store"
	"satoblock/mempool/task/parallel"
	"satoblock/mempool/task/serial"
	"satoblock/model"
	"satoblock/utils"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Mempool struct {
	BatchTxs []*model.Tx         // 所有Tx
	Txs      map[string]struct{} // 所有Tx
	SkipTxs  map[string]struct{} // 需要跳过的Tx

	SpentUtxoKeysMap  map[string]struct{}
	SpentUtxoDataMap  map[string]*model.TxoData
	NewUtxoDataMap    map[string]*model.TxoData
	RemoveUtxoDataMap map[string]*model.TxoData

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
}

func (mp *Mempool) LoadFromMempool() bool {
	// 清空
	mp.Txs = make(map[string]struct{}, 0)
	mp.SkipTxs = make(map[string]struct{}, 0)

	for i := 0; i < 1000; i++ {
		select {
		case <-loader.RawTxNotify:
		default:
		}
	}

	txids := loader.GetRawMemPoolRPC()
	if txids == nil {
		return false
	}
	for _, txid := range txids {
		rawtx := loader.GetRawTxRPC(txid)
		if rawtx == nil {
			continue
		}

		tx, txoffset := parser.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			logger.Log.Info("skip bad rawtx")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if parser.IsTxNonFinal(tx, mp.SkipTxs) {
			logger.Log.Info("skip non final tx",
				zap.String("txid", tx.HashHex),
			)
			mp.SkipTxs[tx.HashHex] = struct{}{}
			continue
		}

		if _, ok := mp.Txs[tx.HashHex]; ok {
			logger.Log.Info("skip dup")
			continue
		}
		mp.Txs[tx.HashHex] = struct{}{}
		mp.BatchTxs = append(mp.BatchTxs, tx)
	}
	return true
}

// SyncMempoolFromZmq 从zmq同步tx
func (mp *Mempool) SyncMempoolFromZmq() (blockReady bool) {
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
		case <-loader.NewBlockNotify:
			logger.Log.Info("block sync subcribe")
			blockReady = true
		case <-time.After(time.Second):
			timeout = true
		}

		if blockReady {
			return true
		}
		if timeout {
			if firstGot {
				return false
			} else {
				continue
			}
		}

		tx, txoffset := parser.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			logger.Log.Info("skip bad rawtx")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if parser.IsTxNonFinal(tx, mp.SkipTxs) {
			logger.Log.Info("skip non final tx",
				zap.String("txid", tx.HashHex),
			)
			mp.SkipTxs[tx.HashHex] = struct{}{}
			continue
		}

		if _, ok := mp.Txs[tx.HashHex]; ok {
			logger.Log.Info("skip dup")
			continue
		}
		mp.Txs[tx.HashHex] = struct{}{}
		mp.BatchTxs = append(mp.BatchTxs, tx)

		if time.Since(start) > 200*time.Millisecond {
			return false
		}
	}
}

// ParseMempool 先并行分析区块，不同区块并行，同区块内串行
func (mp *Mempool) ParseMempool(startIdx int) {
	// first
	for txIdx, tx := range mp.BatchTxs {
		// no dep, 准备utxo花费关系数据
		parallel.ParseTxoSpendByTxParallel(tx, mp.SpentUtxoKeysMap)

		// 0
		parallel.ParseTxFirst(tx)

		// 1 dep 0
		parallel.ParseNewUtxoInTxParallel(startIdx+txIdx, tx, mp.NewUtxoDataMap)
	}

	// 2 dep 0
	serial.SyncBlockTxOutputInfo(startIdx, mp.BatchTxs)

	// 3 dep 1
	serial.ParseGetSpentUtxoDataFromRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)
	// 4 dep 3
	serial.SyncBlockTxInputDetail(startIdx, mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

	// 5 dep 2 4
	serial.SyncBlockTx(startIdx, mp.BatchTxs)
}

// ParseEnd 最后分析执行
func ParseEnd() {
	// 7 dep 5
	store.CommitSyncCk()
	store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
	store.ProcessPartSyncCk()
}
