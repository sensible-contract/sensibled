package serial

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用
// 部分utxo信息在程序内存，missing的utxo将从redis查询
// 区块同步结束时会批量更新缓存的utxo到redis
func ParseGetSpentUtxoDataFromRedisSerial(block *model.ProcessBlock) {
	pipe := rdb.RdbUtxoClient.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	ctx := context.Background()
	for outpointKey := range block.SpentUtxoKeysMap {
		// 检查是否是区块内自产自花
		if data, ok := block.NewUtxoDataMap[outpointKey]; ok {
			block.SpentUtxoDataMap[outpointKey] = data
			delete(block.NewUtxoDataMap, outpointKey)
			continue
		}
		// 检查是否在本地全局缓存
		if data, ok := model.GlobalNewUtxoDataMap[outpointKey]; ok {
			block.SpentUtxoDataMap[outpointKey] = data
			delete(model.GlobalNewUtxoDataMap, outpointKey)
			continue
		}
		// 剩余utxo需要查询redis
		needExec = true
		m[outpointKey] = pipe.Get(ctx, "u"+outpointKey)
	}

	if !needExec {
		return
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		panic(err)
	}
	for outpointKey, v := range m {
		res, err := v.Result()
		if err == redis.Nil {
			logger.Log.Error("parse block, but missing utxo from redis",
				zap.String("outpoint", hex.EncodeToString([]byte(outpointKey))))
			continue
		} else if err != nil {
			panic(err)
		}
		d := &model.TxoData{}
		d.Unmarshal([]byte(res))

		// 从redis获取utxo的script，解码以备程序使用
		d.ScriptType = scriptDecoder.GetLockingScriptType(d.PkScript)
		d.AddressData = scriptDecoder.ExtractPkScriptForTxo(d.PkScript, d.ScriptType)

		block.SpentUtxoDataMap[outpointKey] = d
		model.GlobalSpentUtxoDataMap[outpointKey] = d
	}
}

// UpdateUtxoInMapSerial 顺序更新当前区块的utxo信息变化到程序全局缓存
func UpdateUtxoInMapSerial(block *model.ProcessBlock) {
	// 更新到本地新utxo存储
	for outpointKey, data := range block.NewUtxoDataMap {
		model.GlobalNewUtxoDataMap[outpointKey] = data
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

func UpdateNewNFTInRedis(pipe redis.Pipeliner) {
	logger.Log.Info("UpdateNewNFTInRedis",
		zap.Int("new", len(model.GlobalNewInscriptions)),
	)
	ctx := context.Background()

	for _, nftData := range model.GlobalNewInscriptions {
		strInscriptionID := fmt.Sprintf("%s%d", string(nftData.TxId[:]), nftData.IdxInTx)

		// redis有序utxo数据成员
		member := &redis.Z{
			Score:  float64(nftData.CreatePoint.Height)*1000000000 + float64(nftData.CreatePoint.IdxInBlock),
			Member: strInscriptionID}
		pipe.ZAdd(ctx, "nfts", member) // 有序new nft数据添加
	}
}

// RemoveNewNFTInRedisStartFromBlockHeight 清理被重组区块内的新创建nft
func RemoveNewNFTInRedisStartFromBlockHeight(pipe redis.Pipeliner, height int) {
	logger.Log.Info("RemoveNewNFTInRedisAfterBlockHeight",
		zap.Int("height", height),
	)
	ctx := context.Background()
	strHeight := fmt.Sprintf("%d000000000", height)
	pipe.ZRemRangeByScore(ctx, "nfts", strHeight, "+inf") // 有序nft数据清理
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(pipe redis.Pipeliner, blocksTotal int, addressBalanceCmds map[string]*redis.IntCmd, utxoToRestore, utxoToRemove map[string]*model.TxoData, isReorg bool) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

	ctx := context.Background()
	pipe.HSet(ctx, "info",
		"blocks_total", blocksTotal,
	)
	pipe.HIncrBy(ctx, "info",
		"utxo_total", int64(len(utxoToRestore)-len(utxoToRemove)),
	)

	// addrToRemove := make(map[string]struct{}, 1)
	for outpointKey, data := range utxoToRemove {
		// remove nft point to utxo point
		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := fmt.Sprintf("np:%d:%d", nftpoint.Height, nftpoint.IdxInBlock)
			pipe.Del(ctx, nftPointKey)
		}

		strAddressPkh := string(data.AddressData.AddressPkh[:])
		if !data.AddressData.HasAddress {
			continue
		}
		// 识别地址，只记录utxo和balance
		pipe.ZRem(ctx, "{au"+strAddressPkh+"}", outpointKey)                                               // 有序address utxo数据清除
		addressBalanceCmds["bl"+strAddressPkh] = pipe.DecrBy(ctx, "bl"+strAddressPkh, int64(data.Satoshi)) // balance of address

		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := fmt.Sprintf("%d:%d", nftpoint.Height, nftpoint.IdxInBlock)
			pipe.ZRem(ctx, "{an"+strAddressPkh+"}", nftPointKey) // 有序address nft数据清除
		}
	}

	for outpointKey, data := range utxoToRestore {
		// remove nft point to utxo point
		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := fmt.Sprintf("np:%d:%d", nftpoint.Height, nftpoint.IdxInBlock)
			var offset [8]byte
			binary.LittleEndian.PutUint64(offset[:], nftpoint.Offset)
			pipe.Set(ctx, nftPointKey, outpointKey+string(offset[:]), 0)
		}

		strAddressPkh := string(data.AddressData.AddressPkh[:])

		// redis有序utxo数据成员
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}

		// 非合约信息记录
		if !data.AddressData.HasAddress {
			// 无法识别地址，暂不记录utxo
			// pipe.ZAdd(ctx, "utxo", member)
			continue
		}

		// 识别地址，只记录utxo和balance
		pipe.ZAdd(ctx, "{au"+strAddressPkh+"}", member)           // 有序address utxo数据添加
		pipe.IncrBy(ctx, "bl"+strAddressPkh, int64(data.Satoshi)) // balance of address

		//更新nft的createIdx到current utxo映射记录
		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := fmt.Sprintf("%d:%d", nftpoint.Height, nftpoint.IdxInBlock)
			member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: nftPointKey}
			pipe.ZAdd(ctx, "{an"+strAddressPkh+"}", member) // 有序address nft数据清除
		}
	}

	logger.Log.Info("UpdateUtxoInRedis finished")
}
