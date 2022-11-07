package task

import (
	"context"
	"sensibled/loader"
	"sensibled/logger"
	memTask "sensibled/mempool/task"
	memSerial "sensibled/mempool/task/serial"
	"sensibled/model"
	"sensibled/rdb"
	"sensibled/store"
	"sensibled/task/parallel"
	"sensibled/task/serial"
	"sync"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var ctx = context.Background()

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseBlockParallel(block *model.Block) {
	for txIdx, tx := range block.Txs {
		isCoinbase := txIdx == 0
		parallel.ParseTxFirst(tx, isCoinbase, block.ParseData)

		// 准备utxo花费关系数据
		// 所有txin使用的utxo记录
		parallel.ParseUpdateTxoSpendByTxParallel(tx, isCoinbase, block.ParseData)
		// 所有txout产生的utxo记录
		parallel.ParseUpdateNewUtxoInTxParallel(uint64(txIdx), tx, block.ParseData)

		// 按address追踪tx历史
		parallel.ParseUpdateAddressInTxParallel(uint64(txIdx), tx, block.ParseData)
	}

	// DB更新txout，比较独立，可以并行更新
	serial.SyncBlockTxOutputInfo(block)
}

// ParseBlockSerialStart 再串行处理区块
func ParseBlockSerialStart(withMempool bool, block *model.Block) {
	if withMempool {
		serial.MarkConfirmedBlockTx(block)
	}
	// 从redis中补全查询当前block内所有Tx花费的utxo信息来使用
	serial.ParseGetSpentUtxoDataFromRedisSerial(block.ParseData)

	// DB更新txin，需要前序和当前区块的txout处理完毕，且依赖从redis查来的utxo。
	serial.SyncBlockTxInputDetail(block)

	// 需要串行，更新当前区块的utxo信息变化到程序内存缓存
	serial.UpdateUtxoInMapSerial(block.ParseData)

	// UpdateAddrPkhInTxMapSerial 顺序更新当前区块的address tx history信息变化到程序全局缓存，需要依赖txout、txin执行完毕
	serial.UpdateAddrPkhInTxMapSerial(block.ParseData)
}

// ParseBlockParallelEnd 再并行处理区块
func ParseBlockParallelEnd(block *model.Block) {
	// DB更新block, 需要依赖txout、txin执行完毕，以统计区块Fee
	serial.SyncBlock(block)
	// DB更新tx, 需要依赖txout、txin执行完毕，以统计Tx Fee
	serial.SyncBlockTx(block)
	serial.SyncBlockTxContract(block)

	block.Txs = nil
	block.ParseData = nil
}

// ParseEnd 最后分析执行
func ParseEnd(isFull bool) bool {
	// 提交DB
	if ok := store.CommitSyncCk(); !ok {
		return false
	}

	// 执行DB数据额外更新
	if isFull {
		return store.ProcessAllSyncCk()
	}
	return store.ProcessPartSyncCk()
}

// RemoveBlocksForReorg
func RemoveBlocksForReorg(startBlockHeight int) bool {
	// 在更新之前，如果有上次已导入但是当前被孤立的块，需要先删除这些块的数据。
	logger.Log.Info("remove...")
	utxoToRestore, err := loader.GetSpentUTXOAfterBlockHeight(startBlockHeight, 0) // 已花费的utxo需要回滚
	if err != nil {
		logger.Log.Error("get utxo to restore failed", zap.Error(err))
		return false
	}
	utxoToRemove, err := loader.GetNewUTXOAfterBlockHeight(startBlockHeight, 0) // 新产生的utxo需要删除
	if err != nil {
		logger.Log.Error("get utxo to remove failed", zap.Error(err))
		return false
	}

	var wg sync.WaitGroup
	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 清除db
		store.RemoveOrphanPartSyncCk(startBlockHeight)
		model.CleanConfirmedTxMap(true)

		logger.Log.Info("ck done")
	}()

	// pika addr history
	wg.Add(1)
	go func() {
		defer wg.Done()
		serial.RemoveAddressTxHistoryFromPikaForReorg(startBlockHeight, utxoToRestore, utxoToRemove)
		logger.Log.Info("pika address done")
	}()

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		if ok := memSerial.UpdateUtxoInPika(utxoToRestore, utxoToRemove); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("pika done")
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 更新redis
		rdsPipe := rdb.RdbBalanceClient.TxPipeline()
		addressBalanceCmds := make(map[string]*redis.IntCmd, 0)
		serial.UpdateUtxoInRedis(rdsPipe, startBlockHeight, addressBalanceCmds, utxoToRestore, utxoToRemove, true)
		if _, err = rdsPipe.Exec(ctx); err != nil {
			logger.Log.Error("redis exec failed", zap.Error(err))
			model.NeedStop = true
		} else {
			if ok := serial.DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds); !ok {
				logger.Log.Error("redis clean zero balance failed")
				model.NeedStop = true
			}
		}
		logger.Log.Info("redis done")
	}()
	wg.Wait()

	if model.NeedStop {
		return false
	}
	return true
}

// SubmitBlocksWithoutMempool
func SubmitBlocksWithoutMempool(isFull bool, stageBlockHeight int) {
	var wg sync.WaitGroup

	// address history
	wg.Add(1)
	go func() {
		defer wg.Done()

		if ok := serial.SaveGlobalAddressTxHistoryIntoPika(); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("history done")
	}()

	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 最后分析执行
		if ok := ParseEnd(isFull); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("ck done")
	}()

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		if ok := memSerial.UpdateUtxoInPika(model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap); !ok {
			model.NeedStop = true
			return
		}
		logger.Log.Info("pika done")
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()
		rdsPipe := rdb.RdbBalanceClient.TxPipeline()
		addressBalanceCmds := make(map[string]*redis.IntCmd, 0)
		// 批量更新redis utxo
		serial.UpdateUtxoInRedis(rdsPipe, stageBlockHeight, addressBalanceCmds,
			model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap, false)
		if _, err := rdsPipe.Exec(ctx); err != nil {
			logger.Log.Error("redis exec failed", zap.Error(err))
			model.NeedStop = true
		} else {
			if ok := serial.DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds); !ok {
				logger.Log.Error("redis clean zero balance failed")
				model.NeedStop = true
			}
		}
		logger.Log.Info("redis done")
	}()
	wg.Wait()

	// 清空本地map内存
	model.CleanUtxoMap()
}

// SubmitBlocksWithMempool
func SubmitBlocksWithMempool(isFull bool, stageBlockHeight int, mempool *memTask.Mempool) {
	needSaveBlock := true
	needSaveMempool := true

	var wg sync.WaitGroup

	// address history
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Pika更新addr tx历史
		if needSaveBlock {
			if ok := serial.SaveGlobalAddressTxHistoryIntoPika(); !ok {
				model.NeedStop = true
				return
			}
		}

		if needSaveMempool {
			startIdx := 0
			if ok := memSerial.SaveAddressTxHistoryIntoPika(uint64(startIdx), mempool.AddrPkhInTxMap); !ok {
				model.NeedStop = true
				return
			}
		}

		logger.Log.Info("history done")
	}()

	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()
		// ParseEnd 最后分析执行
		if needSaveBlock {
			if ok := ParseEnd(isFull); !ok {
				model.NeedStop = true
				return
			}
		}
		// 7 dep 5
		if needSaveMempool {
			if ok := memTask.ParseEnd(); !ok {
				model.NeedStop = true
				return
			}
		}
		logger.Log.Info("ck done")
	}()

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 批量更新redis utxo
		if needSaveBlock {
			if ok := memSerial.UpdateUtxoInPika(model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap); !ok {
				model.NeedStop = true
				return
			}
		}
		// for txin dump
		// 6 dep 2 4
		if needSaveMempool {
			if ok := memSerial.UpdateUtxoInPika(mempool.NewUtxoDataMap, mempool.RemoveUtxoDataMap); !ok {
				model.NeedStop = true
				return
			}
		}
		logger.Log.Info("pika done")
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()
		rdsPipe := rdb.RdbBalanceClient.TxPipeline()
		addressBalanceCmds := make(map[string]*redis.IntCmd, 0)
		if needSaveBlock {
			// 批量更新redis utxo
			serial.UpdateUtxoInRedis(rdsPipe, stageBlockHeight, addressBalanceCmds,
				model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap, false)

		}
		// for txin dump
		// 6 dep 2 4
		initSyncMempool := true
		if needSaveMempool {
			memSerial.UpdateUtxoInRedis(rdsPipe, initSyncMempool,
				mempool.NewUtxoDataMap, mempool.RemoveUtxoDataMap, mempool.SpentUtxoDataMap)
		}
		if _, err := rdsPipe.Exec(ctx); err != nil {
			logger.Log.Error("redis exec failed", zap.Error(err))
			model.NeedStop = true
		} else {
			// should after rdsPipe.exec finished
			if ok := serial.DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds); !ok {
				logger.Log.Error("redis clean zero balance failed")
				model.NeedStop = true
			}
		}
		logger.Log.Info("redis done")
	}()
	wg.Wait()

	if needSaveBlock {
		// 清空本地map内存
		model.CleanUtxoMap()
	}
}
