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
	"time"

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
		parallel.ParseUpdateNewUtxoInTxParallel(txIdx, tx, block.ParseData)
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
}

// ParseBlockParallelEnd 再并行处理区块
func ParseBlockParallelEnd(block *model.Block) {
	// DB更新block, 需要依赖txout、txin执行完毕，以统计区块Fee
	serial.SyncBlock(block)
	// DB更新tx, 需要依赖txout、txin执行完毕，以统计Tx Fee
	serial.SyncBlockTx(block)

	serial.SyncBlockTxContract(block)

	block.ParseData = nil
	block.Txs = nil
}

// ParseEnd 最后分析执行
func ParseEnd(isFull bool) {
	// 提交DB
	store.CommitSyncCk()

	// 执行DB数据额外更新
	if isFull {
		store.ProcessAllSyncCk()
	} else {
		store.ProcessPartSyncCk()
	}
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

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		pikaPipe := rdb.PikaClient.Pipeline()
		serial.UpdateUtxoInPika(pikaPipe, utxoToRestore, utxoToRemove)
		if _, err = pikaPipe.Exec(ctx); err != nil {
			logger.Log.Error("pika exec failed", zap.Error(err))
			model.NeedStop = true
		}

		logger.Log.Info("pika done")
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 更新redis
		rdsPipe := rdb.RedisClient.Pipeline()
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
	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 最后分析执行
		ParseEnd(isFull)
		logger.Log.Info("ck done")
	}()

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		for idx := 0; idx < 3; idx++ {
			pikaPipe := rdb.PikaClient.Pipeline()
			serial.UpdateUtxoInPika(pikaPipe,
				model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap)

			if _, err := pikaPipe.Exec(ctx); err != nil {
				logger.Log.Error("pika exec failed", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}
			logger.Log.Info("pika done")
			return
		}
		model.NeedStop = true
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()
		rdsPipe := rdb.RedisClient.Pipeline()
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
func SubmitBlocksWithMempool(isFull bool, stageBlockHeight int, mempool *memTask.Mempool, initSyncMempool bool) {
	needSaveBlock := true
	needSaveMempool := true

	var wg sync.WaitGroup
	// ck
	wg.Add(1)
	go func() {
		defer wg.Done()
		// ParseEnd 最后分析执行
		if needSaveBlock {
			ParseEnd(isFull)
		}
		// 7 dep 5
		if needSaveMempool {
			memTask.ParseEnd()
		}
	}()

	// pika
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 批量更新redis utxo
		pikaPipe := rdb.PikaClient.Pipeline()
		if needSaveBlock {
			serial.UpdateUtxoInPika(pikaPipe, model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap)
		}
		// for txin dump
		// 6 dep 2 4
		if needSaveMempool {
			memSerial.UpdateUtxoInPika(pikaPipe, mempool.NewUtxoDataMap, mempool.RemoveUtxoDataMap)
		}
		if _, err := pikaPipe.Exec(ctx); err != nil {
			logger.Log.Error("pika exec failed", zap.Error(err))
			model.NeedStop = true
		}
	}()

	// redis
	wg.Add(1)
	go func() {
		defer wg.Done()

		rdsPipe := rdb.RedisClient.TxPipeline()
		addressBalanceCmds := make(map[string]*redis.IntCmd, 0)
		if needSaveBlock {
			// 批量更新redis utxo
			serial.UpdateUtxoInRedis(rdsPipe, stageBlockHeight, addressBalanceCmds,
				model.GlobalNewUtxoDataMap, model.GlobalSpentUtxoDataMap, false)

		}
		// for txin dump
		// 6 dep 2 4
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
	}()
	wg.Wait()

	if needSaveBlock {
		// 清空本地map内存
		model.CleanUtxoMap()
	}
}
