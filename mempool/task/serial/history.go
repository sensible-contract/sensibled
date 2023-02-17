package serial

import (
	"context"
	"fmt"
	"sort"
	"unisatd/logger"
	"unisatd/model"
	"unisatd/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// SaveAddressTxHistoryIntoPika Pika更新addr tx历史
func SaveAddressTxHistoryIntoPika(needReset bool, addrPkhInTxMap map[string][]int) bool {
	ctx := context.Background()
	// 清除内存池数据
	if needReset {
		logger.Log.Info("reset pika mempool start")
		addrs, err := rdb.RdbBalanceClient.SMembers(ctx, "mp:addresses").Result()
		if err != nil {
			logger.Log.Error("redis get mempool reset addresses failed", zap.Error(err))
			model.NeedStop = true
			return false
		}
		logger.Log.Info("reset pika mempool done", zap.Int("nAddrs", len(addrs)))

		strHeight := fmt.Sprintf("%d000000000", model.MEMPOOL_HEIGHT)

		pipe := rdb.RdbAddrTxClient.Pipeline()
		for _, strAddressPkh := range addrs {
			pipe.ZRemRangeByScore(ctx, "{ah"+strAddressPkh+"}", strHeight, "+inf") // 有序address tx history数据添加
		}
		if _, err := pipe.Exec(ctx); err != nil {
			logger.Log.Error("pika remove mempool address exec failed", zap.Error(err))
			model.NeedStop = true
			return false
		}

		// 清除地址追踪
		rdb.RdbBalanceClient.Del(ctx, "mp:addresses")
		logger.Log.Info("mempool FlushdbInPika address finish")
	}

	if len(addrPkhInTxMap) == 0 {
		return true
	}

	// 写入地址的交易历史
	pipe := rdb.RdbAddrTxClient.Pipeline()
	for strAddressPkh, listTxid := range addrPkhInTxMap {
		sort.Ints(listTxid)
		lastTxIdx := -1
		for _, txIdx := range listTxid {
			if lastTxIdx == txIdx {
				continue
			}
			lastTxIdx = txIdx

			key := fmt.Sprintf("%d:%d", model.MEMPOOL_HEIGHT, txIdx)
			score := float64(model.MEMPOOL_HEIGHT)*1000000000 + float64(txIdx)
			// redis有序utxo数据成员
			member := &redis.Z{Score: score, Member: key}
			pipe.ZAdd(ctx, "{ah"+strAddressPkh+"}", member) // 有序address tx history数据添加
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logger.Log.Error("mempool pika address exec failed", zap.Error(err))
		model.NeedStop = true
	}

	// 记录哪些地址在内存池中更新了交易历史
	rdsPipe := rdb.RdbBalanceClient.TxPipeline()
	for strAddressPkh := range addrPkhInTxMap {
		rdsPipe.SAdd(ctx, "mp:addresses", strAddressPkh)
	}
	if _, err := rdsPipe.Exec(ctx); err != nil {
		logger.Log.Error("mempool add address in redis exec failed", zap.Error(err))
		model.NeedStop = true
		return false
	}
	return true
}
