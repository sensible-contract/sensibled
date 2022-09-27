// go build -v sensibled/tools/rewrite_utxo_balance

package main

import (
	"context"
	_ "net/http/pprof"
	"runtime"
	"sensibled/loader"
	"sensibled/loader/clickhouse"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/rdb"
	"sensibled/task/serial"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var (
	ctx              = context.Background()
	startBlockHeight int
)

func init() {
	rdb.RedisClient = rdb.Init("conf/redis.yaml")
	rdb.PikaClient = rdb.Init("conf/pika.yaml")

	clickhouse.Init()
}

func main() {
	// 修复redis
	bestHeightFromRedis, err := loader.GetBestBlockHeightFromRedis()
	if err != nil {
		panic("sync check by GetBestBlockHeightFromRedis, but failed.")
	}
	lastBlock, err := loader.GetLatestBlockFromDB()
	if err != nil {
		panic("sync check by GetLatestBlocksFromDB, but failed.")
	}

	startFixHeight := bestHeightFromRedis + 1
	for startFixHeight < int(lastBlock.Height)+1 {
		endFixHeight := startFixHeight + 128
		if endFixHeight > int(lastBlock.Height)+1 {
			endFixHeight = int(lastBlock.Height) + 1
		}

		logger.Log.Info("fixing redis...",
			zap.Int("start", startFixHeight), zap.Int("end", endFixHeight))

		utxoToRestore, err := loader.GetNewUTXOAfterBlockHeight(startFixHeight, endFixHeight) // 新产生的utxo需要增加
		if err != nil {
			logger.Log.Error("get utxo to restore failed", zap.Error(err))
			break
		}
		utxoToRemove, err := loader.GetSpentUTXOAfterBlockHeight(startFixHeight, endFixHeight) // 已花费的utxo需要删除
		if err != nil {
			logger.Log.Error("get utxo to remove failed", zap.Error(err))
			break
		}

		utxosMapCommon := make(map[string]bool, len(utxoToRemove))
		for key := range utxoToRemove {
			if _, ok := utxoToRestore[key]; ok {
				utxosMapCommon[key] = true
			}
		}
		for key := range utxosMapCommon {
			delete(utxoToRemove, key)
			delete(utxoToRestore, key)
		}
		utxosMapCommon = nil
		runtime.GC()

		startFixHeight = endFixHeight

		// 更新redis
		rdsPipe := rdb.RedisClient.Pipeline()
		addressBalanceCmds := make(map[string]*redis.IntCmd, 0)
		serial.UpdateUtxoInRedis(rdsPipe, endFixHeight-1, addressBalanceCmds, utxoToRestore, utxoToRemove, true)
		if _, err = rdsPipe.Exec(ctx); err != nil {
			logger.Log.Error("restore/remove utxo from redis failed", zap.Error(err))
			panic(err)
		} else {
			if ok := serial.DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds); !ok {
				logger.Log.Error("redis clean zero balance failed")
				model.NeedStop = true
			}
		}

		pikaPipe := rdb.PikaClient.Pipeline()
		serial.UpdateUtxoInPika(pikaPipe, utxoToRestore, utxoToRemove)
		if _, err = pikaPipe.Exec(ctx); err != nil {
			logger.Log.Error("restore/remove utxo from pika failed", zap.Error(err))
			panic(err)
		}

		if model.NeedStop {
			break
		}
	}

	logger.Log.Info("stoped")
	logger.SyncLog()
}
