package serial

import (
	"context"
	"fmt"
	"sort"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// SaveGlobalAddressTxHistoryIntoPika Pika更新address tx历史
func SaveGlobalAddressTxHistoryIntoPika() bool {
	if len(model.GlobalAddrPkhInTxMap) == 0 {
		return true
	}

	maxSize := 1000000
	type Item struct {
		Members []*redis.Z
		Addr    string
	}
	items := make([]*Item, 0)
	for strAddressPkh, listTxPosition := range model.GlobalAddrPkhInTxMap {
		txZSetMembers := make([]*redis.Z, len(listTxPosition))
		for idx, txPosition := range listTxPosition {
			key := fmt.Sprintf("%d:%d", txPosition.BlockHeight, txPosition.TxIdx)
			score := float64(uint64(txPosition.BlockHeight)*1000000000 + txPosition.TxIdx)
			member := &redis.Z{Score: score, Member: key}
			txZSetMembers[idx] = member
		}
		if len(txZSetMembers) < maxSize/32 {
			items = append(items, &Item{
				Addr:    strAddressPkh,
				Members: txZSetMembers,
			})
			continue
		}

		for idx := 0; idx < len(txZSetMembers); idx += maxSize / 32 {
			end := idx + maxSize/32
			if end > len(txZSetMembers) {
				end = len(txZSetMembers)
			}
			items = append(items, &Item{
				Addr:    strAddressPkh,
				Members: txZSetMembers[idx:end],
			})
		}
	}

	ctx := context.Background()
	for idx := 0; idx < len(items); {

		pikaPipe := rdb.RdbAddrTxClient.Pipeline()
		size := 0
		for ; size < maxSize && idx < len(items); idx++ {
			// 有序address tx history数据添加
			pikaPipe.ZAdd(ctx, "{ah"+items[idx].Addr+"}", items[idx].Members...)
			size += 32 + len(items[idx].Members)*32
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
		pipe.ZRemRangeByScore(ctx, "{ah"+strAddressPkh+"}", strHeight, "+inf") // 有序address tx history数据清理
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Log.Error("pika exec failed", zap.Error(err))
		model.NeedStop = true
	}
}

// UpdateAddrPkhInTxMapSerial 顺序更新当前区块的address tx history信息变化到程序全局缓存
func UpdateAddrPkhInTxMapSerial(block *model.ProcessBlock) {
	for strAddressPkh, listTxid := range block.AddrPkhInTxMap {

		sort.Ints(listTxid)
		lastTxIdx := -1
		for _, txIdx := range listTxid {
			if lastTxIdx == txIdx {
				continue
			}
			lastTxIdx = txIdx

			model.GlobalAddrPkhInTxMap[strAddressPkh] = append(model.GlobalAddrPkhInTxMap[strAddressPkh],
				model.TxLocation{
					BlockHeight: block.Height,
					TxIdx:       uint64(txIdx),
				})
		}
	}
}
