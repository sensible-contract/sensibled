package serial

import (
	"context"
	"fmt"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// SaveGlobalAddressTxHistoryIntoPika Pika更新address tx历史
func SaveGlobalAddressTxHistoryIntoPika() bool {
	type Item struct {
		Member *redis.Z
		Addr   string
	}
	items := make([]*Item, 0)

	for strAddressPkh, listTxPosition := range model.GlobalAddrPkhInTxMap {
		for _, txPosition := range listTxPosition {
			key := fmt.Sprintf("%d:%d", txPosition.BlockHeight, txPosition.TxIdx)
			score := float64(uint64(txPosition.BlockHeight)*1000000000 + txPosition.TxIdx)
			member := &redis.Z{Score: score, Member: key}
			items = append(items, &Item{
				Member: member,
				Addr:   strAddressPkh,
			})
		}
	}

	if len(items) == 0 {
		return true
	}

	ctx := context.Background()
	sliceLen := 100000
	for idx := 0; idx < (len(items)-1)/sliceLen+1; idx++ {

		pikaPipe := rdb.RdbAddrTxClient.Pipeline()
		n := 0
		for _, item := range items[idx*sliceLen:] {
			if n == sliceLen {
				break
			}

			// 有序address tx history数据添加
			pikaPipe.ZAdd(ctx, "{ah"+item.Addr+"}", item.Member)
			n++
		}
		if _, err := pikaPipe.Exec(ctx); err != nil && err != redis.Nil {
			logger.Log.Error("pika address exec failed", zap.Error(err))
			model.NeedStop = true
			return false
		}
	}

	return true
}

// RemoveAddressTxHistoryFromPikaForReorg 清理被重组区块内的address tx历史
func RemoveAddressTxHistoryFromPikaForReorg(height int, utxoToRestore, utxoToRemove map[string]*model.TxoData) {
	addressMap := make(map[string]struct{})
	for _, data := range utxoToRemove {
		scriptType := scriptDecoder.GetLockingScriptType(data.PkScript)
		dData := scriptDecoder.ExtractPkScriptForTxo(data.PkScript, scriptType)
		if dData.HasAddress {
			addressMap[string(dData.AddressPkh[:])] = struct{}{}
		}
	}
	for _, data := range utxoToRestore {
		scriptType := scriptDecoder.GetLockingScriptType(data.PkScript)
		dData := scriptDecoder.ExtractPkScriptForTxo(data.PkScript, scriptType)
		if dData.HasAddress {
			addressMap[string(dData.AddressPkh[:])] = struct{}{}
		}
	}

	logger.Log.Info("RemoveAddressTxHistoryFromPikaForReorg",
		zap.Int("nAddr", len(addressMap)))

	strHeight := fmt.Sprintf("%d000000000", height)

	ctx := context.Background()
	pipe := rdb.RdbAddrTxClient.Pipeline()
	for strAddressPkh := range addressMap {
		pipe.ZRemRangeByScore(ctx, "{ah"+strAddressPkh+"}", strHeight, "+inf") // 有序address tx history数据添加
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Log.Error("pika exec failed", zap.Error(err))
		model.NeedStop = true
	}
}
