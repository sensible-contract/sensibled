package serial

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(
	spentUtxoKeysMap map[string]struct{},
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	ctx := context.Background()
	pipe := rdb.RdbUtxoClient.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	for outpointKey := range spentUtxoKeysMap {
		if _, ok := newUtxoDataMap[outpointKey]; ok {
			continue
		}

		if data, ok := model.GlobalMempoolNewUtxoDataMap[outpointKey]; ok {
			removeUtxoDataMap[outpointKey] = data
			continue
		}

		// 检查是否在区块缓存
		if data, ok := model.GlobalNewUtxoDataMap[outpointKey]; ok {
			spentUtxoDataMap[outpointKey] = data
			// delete(model.GlobalNewUtxoDataMap, outpointKey)
			continue
		}

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
			logger.Log.Error("parse mempool, but missing utxo from redis",
				zap.String("outpoint", hex.EncodeToString([]byte(outpointKey))))
			continue
		} else if err != nil {
			panic(err)
		}
		d := &model.TxoData{}
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = scriptDecoder.GetLockingScriptType(d.PkScript)
		d.AddressData = scriptDecoder.ExtractPkScriptForTxo(d.PkScript, d.ScriptType)

		spentUtxoDataMap[outpointKey] = d
	}
}

// UpdateUtxoInLocalMapSerial 顺序更新当前处理的一批内存池交易的utxo信息变化，删除产生又立即花费的utxo
func UpdateUtxoInLocalMapSerial(spentUtxoKeysMap map[string]struct{},
	newUtxoDataMap, removeUtxoDataMap map[string]*model.TxoData) {

	insideTxo := make([]string, len(spentUtxoKeysMap))
	for outpointKey := range spentUtxoKeysMap {
		if _, ok := newUtxoDataMap[outpointKey]; !ok {
			continue
		}
		insideTxo = append(insideTxo, outpointKey)
	}
	for _, outpointKey := range insideTxo {
		delete(newUtxoDataMap, outpointKey)
	}

	for outpointKey, data := range newUtxoDataMap {
		model.GlobalMempoolNewUtxoDataMap[outpointKey] = data
	}

	for outpointKey := range removeUtxoDataMap {
		delete(model.GlobalMempoolNewUtxoDataMap, outpointKey)
	}
}

// UpdateUtxoInPika 批量更新redis utxo
func UpdateUtxoInPika(utxoToRestore, utxoToRemove map[string]*model.TxoData) bool {
	logger.Log.Info("UpdateUtxoInPika",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

	ctx := context.Background()
	// delete batch
	outpointKeys := make([]string, len(utxoToRemove))
	idx := 0
	for outpointKey := range utxoToRemove {
		outpointKeys[idx] = outpointKey
		idx++
	}
	if len(utxoToRemove) > 0 {
		sliceLen := 2500000
		for idx := 0; idx < (len(outpointKeys)-1)/sliceLen+1; idx++ {

			pikaPipe := rdb.RdbUtxoClient.Pipeline()
			n := 0
			for _, outpointKey := range outpointKeys[idx*sliceLen:] {
				if n == sliceLen {
					break
				}
				// redis全局utxo数据清除
				pikaPipe.Del(ctx, "u"+outpointKey)
				n++
			}
			if _, err := pikaPipe.Exec(ctx); err != nil && err != redis.Nil {
				logger.Log.Error("pika delete exec failed", zap.Error(err))
				return false
			}
		}
	}

	// add batch
	idx = 0
	utxoBufToRestore := make([][]byte, len(utxoToRestore))
	for outpointKey, data := range utxoToRestore {
		buf := make([]byte, 36+20+
			len(data.PkScript)+1+
			len(data.CreatePointOfNFTs)*32)
		length := data.Marshal(buf)

		buf = append(buf[:length], []byte(outpointKey)...)
		utxoBufToRestore[idx] = buf[:length+36]
		idx++
	}
	if len(utxoToRestore) > 0 {
		sliceLen := 10000
		for idx := 0; idx < (len(utxoBufToRestore)-1)/sliceLen+1; idx++ {

			pikaPipe := rdb.RdbUtxoClient.Pipeline()
			n := 0
			for _, utxoBuf := range utxoBufToRestore[idx*sliceLen:] {
				if n == sliceLen {
					break
				}
				length := len(utxoBuf)
				// redis全局utxo数据添加，以便关联后续花费的input，无论是否识别地址都需要记录
				pikaPipe.Set(ctx, "u"+string(utxoBuf[length-36:]), utxoBuf[:length-36], 0)
				n++
			}
			if _, err := pikaPipe.Exec(ctx); err != nil && err != redis.Nil {
				logger.Log.Error("pika store exec failed", zap.Error(err))
				return false
			}
		}
	}

	logger.Log.Info("UpdateUtxoInPika finished")
	return true

	// 注意: redis全局utxo数据不能在这里清除，必须留给区块确认时去做
	// for outpointKey := range utxoToSpend {
	// 	pikaPipe.Del(ctx, "u"+outpointKey)
	// }
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(pipe redis.Pipeliner, needReset bool, utxoToRestore, utxoToRemove, utxoToSpend map[string]*model.TxoData) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("nStore", len(utxoToRestore)),
		zap.Int("nRemove", len(utxoToRemove)),
		zap.Int("nSpend", len(utxoToSpend)))

	ctx := context.Background()
	// 清除内存池数据
	if needReset {
		// remove nft
		RemoveNewNFTInRedisStartFromBlockHeight(pipe, model.MEMPOOL_HEIGHT)

		logger.Log.Info("reset redis mempool start")
		keys, err := rdb.RdbBalanceClient.SMembers(ctx, "mp:keys").Result()
		if err != nil {
			logger.Log.Info("reset redis mempool failed", zap.Error(err))
			panic(err)
		}
		logger.Log.Info("reset redis mempool done", zap.Int("nKeys", len(keys)))

		for _, key := range keys {
			pipe.Del(ctx, key)
		}
		if len(keys) > 0 {
			pipe.Del(ctx, "mp:keys")
		}

		pipe.HSet(ctx, "info",
			"utxo_total_mempool", 0,
		)

		logger.Log.Info("mempool FlushdbInRedis finish")
	}

	pipe.HIncrBy(ctx, "info",
		"utxo_total_mempool", int64(len(utxoToRestore)-len(utxoToRemove)-len(utxoToSpend)),
	)

	// 更新内存池数据
	mpkeys := make([]string, 0)

	for outpointKey, data := range utxoToRemove {
		// remove nft point to utxo point
		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := "np" + nftpoint.GetCreateIdxKey()
			pipe.Del(ctx, nftPointKey)
		}

		// redis有序utxo数据清除
		if !data.AddressData.HasAddress {
			continue
		}

		strAddressPkh := string(data.AddressData.AddressPkh[:])

		// 单独记录非nft余额
		var mpkeyAU, mpkeyBL string
		if len(data.CreatePointOfNFTs) == 0 {
			mpkeyAU = "mp:{au" + strAddressPkh + "}"
			mpkeyBL = "mp:bl" + strAddressPkh
		} else {
			mpkeyAU = "mp:{aU" + strAddressPkh + "}"
			mpkeyBL = "mp:bL" + strAddressPkh
		}
		// redis有序address utxo数据清除
		pipe.ZRem(ctx, mpkeyAU, outpointKey)
		// balance of address
		pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))

		mpkeyAN := "mp:{an" + strAddressPkh + "}"
		for _, nftpoint := range data.CreatePointOfNFTs {
			pipe.ZRem(ctx, mpkeyAN, nftpoint.GetCreateIdxKey()) // 有序address nft数据清除
		}
	}

	for outpointKey, data := range utxoToSpend {
		// 注意: redis全局nft数据不能在这里清除，必须留给区块确认时去做
		// remove nft point to utxo point
		// for _, nftpoint := range data.CreatePointOfNFTs {
		//  nftPointKey := "np" + nftpoint.GetCreateIdxKey()
		// 	pipe.Del(ctx, nftPointKey)
		// }

		if !data.AddressData.HasAddress {
			continue
		}

		strAddressPkh := string(data.AddressData.AddressPkh[:])

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*model.HEIGHT_MUTIPLY + float64(data.TxIdx), Member: outpointKey}

		// 单独记录非nft余额
		var mpkeyAU, mpkeyBL string
		if len(data.CreatePointOfNFTs) == 0 {
			mpkeyAU = "mp:s:{au" + strAddressPkh + "}"
			mpkeyBL = "mp:bl" + strAddressPkh
		} else {
			mpkeyAU = "mp:s:{aU" + strAddressPkh + "}"
			mpkeyBL = "mp:bL" + strAddressPkh
		}
		// 不是合约tx，则记录address utxo
		// redis有序address utxo数据添加
		pipe.ZAdd(ctx, mpkeyAU, member)
		// balance of address
		pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))

		//更新nft的createIdx到current utxo映射记录
		mpkeyAN := "mp:s:{an" + strAddressPkh + "}"
		for _, nftpoint := range data.CreatePointOfNFTs {
			member := &redis.Z{Score: float64(data.BlockHeight)*model.HEIGHT_MUTIPLY + float64(data.TxIdx), Member: nftpoint.GetCreateIdxKey()}
			pipe.ZAdd(ctx, mpkeyAN, member) // 有序address nft数据清除
		}

		mpkeys = append(mpkeys, mpkeyAU, mpkeyBL, mpkeyAN)
	}

	for outpointKey, data := range utxoToRestore {
		// add nft point to utxo point
		for _, nftpoint := range data.CreatePointOfNFTs {
			nftPointKey := "np" + nftpoint.GetCreateIdxKey()
			var offset [8]byte
			binary.LittleEndian.PutUint64(offset[:], nftpoint.Offset)
			pipe.Set(ctx, nftPointKey, outpointKey+string(offset[:]), 0)
		}

		if !data.AddressData.HasAddress {
			continue
		}

		strAddressPkh := string(data.AddressData.AddressPkh[:])

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*model.HEIGHT_MUTIPLY + float64(data.TxIdx), Member: outpointKey}

		// 单独记录非nft余额
		var mpkeyAU, mpkeyBL string
		if len(data.CreatePointOfNFTs) == 0 {
			mpkeyAU = "mp:{au" + strAddressPkh + "}"
			mpkeyBL = "mp:bl" + strAddressPkh
		} else {
			mpkeyAU = "mp:{aU" + strAddressPkh + "}"
			mpkeyBL = "mp:bL" + strAddressPkh
		}
		// 不是合约tx，则记录address utxo
		// redis有序address utxo数据添加
		pipe.ZAdd(ctx, mpkeyAU, member)
		// balance of address
		pipe.IncrBy(ctx, mpkeyBL, int64(data.Satoshi))

		//更新nft的createIdx到current utxo映射记录
		mpkeyAN := "mp:{an" + strAddressPkh + "}"
		for _, nftpoint := range data.CreatePointOfNFTs {
			member := &redis.Z{Score: float64(data.BlockHeight)*model.HEIGHT_MUTIPLY + float64(data.TxIdx), Member: nftpoint.GetCreateIdxKey()}
			pipe.ZAdd(ctx, mpkeyAN, member) // 有序address nft数据清除
		}

		mpkeys = append(mpkeys, mpkeyAU, mpkeyBL, mpkeyAN)
	}

	// 记录所有的mp:keys，以备区块确认后直接删除重来
	for _, mpkey := range mpkeys {
		pipe.SAdd(ctx, "mp:keys", mpkey)
	}
}
