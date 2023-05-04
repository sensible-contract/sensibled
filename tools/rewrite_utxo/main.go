// go build -v unisatd/tools/rewrite_utxo

package main

import (
	"context"
	"flag"
	_ "net/http/pprof"
	"runtime"
	"unisatd/loader"
	"unisatd/loader/clickhouse"
	"unisatd/logger"
	memSerial "unisatd/mempool/task/serial"
	"unisatd/model"
	"unisatd/rdb"

	"go.uber.org/zap"
)

var (
	ctx              = context.Background()
	startBlockHeight int
)

func init() {
	flag.IntVar(&startBlockHeight, "start", -1, "start block height")
	flag.Parse()

	rdb.RdbUtxoClient = rdb.Init("conf/rdb_utxo.yaml")

	clickhouse.Init()
}

func main() {
	// 修复utxo
	lastBlock, err := loader.GetLatestBlockFromDB()
	if err != nil {
		panic("sync check by GetLatestBlocksFromDB, but failed.")
	}

	startFixHeight := startBlockHeight
	for startFixHeight < int(lastBlock.Height)+1 {
		endFixHeight := startFixHeight + 128
		if endFixHeight > int(lastBlock.Height)+1 {
			endFixHeight = int(lastBlock.Height) + 1
		}

		logger.Log.Info("fixing pika...",
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
		if ok := memSerial.UpdateUtxoInPika(utxoToRestore, utxoToRemove); !ok {
			logger.Log.Error("restore/remove utxo from pika failed")
			break
		}

		if model.NeedStop {
			break
		}
	}

	logger.Log.Info("stoped")
	logger.SyncLog()
}
