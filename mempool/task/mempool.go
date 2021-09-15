package task

import (
	blkLoader "sensibled/loader"
	"sensibled/logger"
	"sensibled/mempool/loader"
	"sensibled/mempool/parser"
	"sensibled/mempool/store"
	"sensibled/mempool/task/parallel"
	"sensibled/mempool/task/serial"
	"sensibled/model"
	"sensibled/utils"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Mempool struct {
	Txs     map[string]struct{} // 所有Tx
	SkipTxs map[string]struct{} // 需要跳过的Tx

	BatchTxs          []*model.Tx               // 当前同步批次的Tx
	SpentUtxoKeysMap  map[string]struct{}       // 在当前同步批次中被花费的所有utxo集合
	SpentUtxoDataMap  map[string]*model.TxoData // 当前同步批次中花费的已确认的utxo集合
	NewUtxoDataMap    map[string]*model.TxoData // 当前同步批次中新产生的utxo集合
	RemoveUtxoDataMap map[string]*model.TxoData // 当前同步批次中花费的未确认的utxo集合，且属于前批次产生的utxo

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
		select {
		case <-loader.NewTxNotify:
		default:
		}
	}

	txids := loader.GetRawMemPoolRPC()
	if txids == nil {
		return false
	}
	for _, txid := range txids {
		rawtx, err := blkLoader.GetRawTxByIdFromMempool(txid.(string))
		if err != nil {
			rawtx = loader.GetRawTxRPC(txid)
			if rawtx == nil {
				// fixme, may all fail, mey need to break
				continue
			}
			logger.Log.Info("init tx in mempool from rpc",
				zap.Any("txid", txid),
			)
		} else {
			logger.Log.Info("init tx in mempool from db",
				zap.Any("txid", txid),
			)
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
		case txid := <-loader.NewTxNotify:
			if _, ok := mp.Txs[txid]; ok {
				logger.Log.Info("skip dup",
					zap.String("txid", txid),
				)
				continue
			}

			rawtx = loader.GetRawTxRPC(txid)
			if rawtx == nil {
				// fixme, may all fail, mey need to break
				continue
			}

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

		logger.Log.Info("new tx in mempool",
			zap.String("txid", tx.HashHex),
		)
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
	// 8 dep 3
	serial.SyncBlockTxContract(startIdx, mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

	// 5 dep 2 4
	serial.SyncBlockTx(startIdx, mp.BatchTxs)
}

// ParseEnd 最后分析执行
func ParseEnd() {
	// 7 dep 5
	store.CommitSyncCk()
	store.ProcessPartSyncCk()
}
