package task

import (
	"bytes"
	"encoding/hex"
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

	allRawtxs := make(map[string]*model.TxData, 1)
	for i := 0; i < 1000; i++ {
		select {
		case rawtx := <-loader.RawTxNotify:
			txid := utils.GetHash256(rawtx)
			allRawtxs[utils.HashString(txid)] = &model.TxData{
				Raw:  rawtx,
				Hash: txid,
			}
		default:
		}
	}

	txids := loader.GetRawMemPoolRPC()
	if txids == nil {
		return false
	}

	logger.Log.Info("start load all tx in mempool from db",
		zap.Int("zmq get count", len(allRawtxs)),
	)
	if err := blkLoader.GetAllMempoolRawTx(allRawtxs); err != nil {
		logger.Log.Info("load all tx in mempool from db failed", zap.Error(err))
	}
	for _, txid := range txids {
		var rawtx []byte
		if txData, ok := allRawtxs[txid.(string)]; ok {
			rawtx = txData.Raw
			logger.Log.Info("init tx in mempool from db",
				zap.Any("txid", txid),
			)
		} else {
			rawtx = loader.GetRawTxRPC(txid)
			if rawtx == nil {
				// fixme, may all fail, mey need to break
				continue
			}
			logger.Log.Info("init tx in mempool from rpc",
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

		// maybe impossible dup here
		if _, ok := mp.Txs[tx.HashHex]; ok {
			logger.Log.Info("init skip dup")
			continue
		}

		if parser.IsTxNonFinal(tx, mp.SkipTxs) {
			logger.Log.Info("skip non final tx",
				zap.String("txid", tx.HashHex),
			)
			mp.SkipTxs[tx.HashHex] = struct{}{}
			continue
		}

		mp.Txs[tx.HashHex] = struct{}{}
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

		txHash := utils.GetHash256(rawtx)
		txHashHex := utils.HashString(txHash)
		// 在内存池重复出现，说明区块已确认，但还未收到zmq hashblock通知。
		if _, ok := mp.Txs[txHashHex]; ok {
			logger.Log.Info("skip dup",
				zap.String("txid", txHashHex),
			)
			continue
		}
		// 被区块确认
		if _, ok := model.GlobalConfirmedTxMap[txHashHex]; ok {
			logger.Log.Info("skip confirmed tx",
				zap.String("txid", txHashHex),
			)
			continue
		}
		if _, ok := model.GlobalConfirmedTxOldMap[txHashHex]; ok {
			logger.Log.Info("skip confirmed tx",
				zap.String("txid", txHashHex),
			)
			continue
		}

		// 首次遇到coinbase，说明有区块确认
		if bytes.HasPrefix(rawtx, COINBASE_TX_PREFIX) {
			blockReady = true
			return true
		}

		tx, txoffset := parser.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			logger.Log.Info("skip bad rawtx")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = txHash
		tx.HashHex = txHashHex

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
