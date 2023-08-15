package serial

import (
	"context"
	"fmt"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/rdb"
	"sort"
	"time"

	redis "github.com/go-redis/redis/v8"
	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

// UpdateAddrPkhInTxMapSerial 顺序更新当前区块的address tx历史
func UpdateAddrPkhInTxMapSerial(blockHeight uint32, addrPkhInTxMap map[string][]int) bool {
	if len(addrPkhInTxMap) == 0 {
		return true
	}

	nTxId := 0
	for _, listTxidx := range addrPkhInTxMap {
		nTxId += len(listTxidx)
	}
	logger.Log.Info("UpdateAddrPkhInTxMapSerial",
		zap.Uint32("height", blockHeight),
		zap.Int("nAddr", len(addrPkhInTxMap)),
		zap.Int("nTxId", nTxId),
	)

	maxSize := 1000000
	type Item struct {
		Members []*redis.Z
		Addr    string
	}
	items := make([]*Item, 0)
	for strAddressPkh, listTxidx := range addrPkhInTxMap {
		for model.NeedPauseStage < 5 {
			logger.Log.Info("UpdateAddrPkhInTxMapSerial(1/2) pause ...")
			time.Sleep(5 * time.Second)
		}

		sort.Ints(listTxidx)
		txZSetMembers := make([]*redis.Z, 0)

		lastTxIdx := -1
		for _, txIdx := range listTxidx {
			if lastTxIdx == txIdx {
				continue
			}
			lastTxIdx = txIdx

			key := fmt.Sprintf("%d:%d", blockHeight, txIdx)
			score := float64(uint64(blockHeight)*1000000000 + uint64(txIdx))
			member := &redis.Z{Score: score, Member: key}
			txZSetMembers = append(txZSetMembers, member)
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
		for model.NeedPauseStage < 5 {
			logger.Log.Info("UpdateAddrPkhInTxMapSerial(2/2) pause ...")
			time.Sleep(5 * time.Second)
		}

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
		pipe.ZRemRangeByScore(ctx, "{ah"+strAddressPkh+"}", strHeight, "+inf") // 有序address tx history数据添加
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Log.Error("pika exec failed", zap.Error(err))
		model.NeedStop = true
	}
}
