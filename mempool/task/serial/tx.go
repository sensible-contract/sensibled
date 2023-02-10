package serial

import (
	"context"
	"encoding/hex"
	"fmt"
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/rdb"
	"sort"
	"strconv"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var (
	ctx = context.Background()
)

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(
	spentUtxoKeysMap map[string]struct{},
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

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
		d.Data = scriptDecoder.ExtractPkScriptForTxo(d.PkScript, d.ScriptType)

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

// SaveAddressTxHistoryIntoPika Pika更新addr tx历史
func SaveAddressTxHistoryIntoPika(needReset bool, addrPkhInTxMap map[string][]int) bool {
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

// UpdateUtxoInPika 批量更新redis utxo
func UpdateUtxoInPika(utxoToRestore, utxoToRemove map[string]*model.TxoData) bool {
	logger.Log.Info("UpdateUtxoInPika",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

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
		buf := make([]byte, 36+20+len(data.PkScript))
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

	// for outpointKey := range utxoToSpend {
	// 	// redis全局utxo数据不能在这里清除，留给区块确认时去做
	// 	pikaPipe.Del(ctx, "u"+outpointKey)
	// }
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(pipe redis.Pipeliner, needReset bool, utxoToRestore, utxoToRemove, utxoToSpend map[string]*model.TxoData) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("nStore", len(utxoToRestore)),
		zap.Int("nRemove", len(utxoToRemove)),
		zap.Int("nSpend", len(utxoToSpend)))

	// 清除内存池数据
	if needReset {
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
	mpkeys := make([]string, 5*(len(utxoToRestore)+len(utxoToRemove)+len(utxoToSpend)))
	for outpointKey, data := range utxoToRestore {
		strAddressPkh := string(data.Data.AddressPkh[:])
		strCodeHash := string(data.Data.CodeHash[:])
		strGenesisId := string(data.Data.GenesisId[:data.Data.GenesisIdLen])

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}

		if data.Data.CodeType == scriptDecoder.CodeType_NONE {
			if !data.Data.HasAddress {
				// 无法识别地址，暂不记录utxo
				// logger.Log.Info("ignore mp:utxo", zap.String("key", hex.EncodeToString([]byte(outpointKey))),
				// 	zap.Float64("score", member.Score))
				// pipe.ZAdd(ctx, "mp:utxo", member)
				continue
			}

			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			// logger.Log.Info("ZAdd mp:au",
			// 	zap.String("addrHex", hex.EncodeToString(data.Data.AddressPkh[:])),
			// 	zap.String("key", hex.EncodeToString([]byte(outpointKey))),
			// 	zap.Float64("score", member.Score))
			mpkeyAU := "mp:{au" + strAddressPkh + "}"
			pipe.ZAdd(ctx, mpkeyAU, member)

			// balance of address
			// logger.Log.Info("IncrBy mp:bl",
			// 	zap.String("addrHex", hex.EncodeToString(data.Data.AddressPkh[:])),
			// 	zap.Uint64("satoshi", data.Satoshi))
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.IncrBy(ctx, mpkeyBL, int64(data.Satoshi))

			mpkeys = append(mpkeys, mpkeyAU, mpkeyBL)
			continue
		}

		// contract balance of address
		// logger.Log.Info("IncrBy mp:cb",
		// 	zap.String("addrHex", hex.EncodeToString(data.Data.AddressPkh[:])),
		// 	zap.Uint64("satoshi", data.Satoshi))
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.IncrBy(ctx, mpkeyCB, int64(data.Satoshi))
		mpkeys = append(mpkeys, mpkeyCB)

		// redis有序genesis utxo数据添加
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"
			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)

			member.Score = float64(data.Data.NFT.TokenIndex)
			pipe.ZAdd(ctx, mpkeyNU, member)                         // nft:utxo
			pipe.ZAdd(ctx, mpkeyND, member)                         // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, 1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, 1, strCodeHash+strGenesisId) // nft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_AUCTION {
			mpkeyNAU := "mp:{nau" + strAddressPkh + "}" + strCodeHash
			mpkeyNAD := "mp:nad" + strCodeHash + strGenesisId
			mpkeyNAS := "mp:{nas" + strAddressPkh + "}"
			mpkeys = append(mpkeys, mpkeyNAU, mpkeyNAD, mpkeyNAS)

			pipe.ZAdd(ctx, mpkeyNAU, member)            // nft:auction:utxo
			pipe.ZAdd(ctx, mpkeyNAD, member)            // nft:auction:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNAS, 1, strCodeHash) // nft:auction:sender-summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			mpkeySUT := "mp:{sut}"
			mpkeySUTA := "mp:{suta" + strAddressPkh + "}"
			mpkeySUTC := "mp:{sutc" + strGenesisId + strCodeHash + "}"
			mpkeySUP := "mp:{sup}"
			mpkeySUPA := "mp:{supa" + strAddressPkh + "}"
			mpkeySUPC := "mp:{supc" + strGenesisId + strCodeHash + "}"
			mpkeySUI := "mp:{sui}"
			mpkeySUIA := "mp:{suia" + strAddressPkh + "}"
			mpkeySUIC := "mp:{suic" + strGenesisId + strCodeHash + "}"

			mpkeys = append(mpkeys, mpkeySUT, mpkeySUTA, mpkeySUTC, mpkeySUP, mpkeySUPA, mpkeySUPC, mpkeySUI, mpkeySUIA, mpkeySUIC)

			pipe.ZAdd(ctx, mpkeySUT, member)  // nft:sell:all:utxo, sort by time
			pipe.ZAdd(ctx, mpkeySUTA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUTC, member) // nft:sell

			member.Score = float64(data.Data.NFTSell.Price)
			pipe.ZAdd(ctx, mpkeySUP, member)  // nft:sell:all:utxo, sort by price
			pipe.ZAdd(ctx, mpkeySUPA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUPC, member) // nft:sell

			member.Score = float64(data.Data.NFTSell.TokenIndex)
			pipe.ZAdd(ctx, mpkeySUI, member)  // nft:sell:all:utxo, sort by token index
			pipe.ZAdd(ctx, mpkeySUIA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUIC, member) // nft:sell

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"
			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)

			pipe.ZAdd(ctx, mpkeyFU, member)                                                    // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, float64(data.Data.FT.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, float64(data.Data.FT.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeys = append(mpkeys, mpkeyFU)

			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

		}

		// update token info
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			pipe.HSet(ctx, "nI"+strCodeHash+strGenesisId+strconv.Itoa(int(data.Data.NFT.TokenIndex)),
				"metatxid", data.Data.NFT.MetaTxId[:],
				"metavout", data.Data.NFT.MetaOutputIndex,
				"supply", data.Data.NFT.TokenSupply,
				"sensibleid", data.Data.NFT.SensibleId,
			)
			pipe.HSet(ctx, "ni"+strCodeHash+strGenesisId,
				"supply", data.Data.NFT.TokenSupply,
				"sensibleid", data.Data.NFT.SensibleId,
			)

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			pipe.HSet(ctx, "fi"+strCodeHash+strGenesisId,
				"decimal", data.Data.FT.Decimal,
				"name", data.Data.FT.Name,
				"symbol", data.Data.FT.Symbol,
				"sensibleid", data.Data.FT.SensibleId,
			)

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			pipe.HSet(ctx, "fi"+strCodeHash+strGenesisId,
				"sensibleid", data.Data.Uniq.SensibleId,
			)
		}

	}

	addrToRemove := make(map[string]struct{}, 1)
	tokenToRemove := make(map[string]struct{}, 1)
	for outpointKey, data := range utxoToRemove {
		strAddressPkh := string(data.Data.AddressPkh[:])
		strCodeHash := string(data.Data.CodeHash[:])
		strGenesisId := string(data.Data.GenesisId[:data.Data.GenesisIdLen])

		if data.Data.CodeType == scriptDecoder.CodeType_NONE {
			// redis有序utxo数据清除
			if !data.Data.HasAddress {
				// 无法识别地址，暂不记录utxo
				// pipe.ZRem(ctx, "mp:utxo", outpointKey)
				continue
			}

			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据清除
			mpkeyAU := "mp:{au" + strAddressPkh + "}"
			pipe.ZRem(ctx, mpkeyAU, outpointKey)

			// balance of address
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))
			continue
		}

		// contract balance of address
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.DecrBy(ctx, mpkeyCB, int64(data.Satoshi))

		// redis有序genesis utxo数据清除
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"

			pipe.ZRem(ctx, mpkeyNU, outpointKey)                     // nft:utxo
			pipe.ZRem(ctx, mpkeyND, outpointKey)                     // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_AUCTION {
			mpkeyNAU := "mp:{nau" + strAddressPkh + "}" + strCodeHash
			mpkeyNAD := "mp:nad" + strCodeHash + strGenesisId
			mpkeyNAS := "mp:{nas" + strAddressPkh + "}"

			pipe.ZRem(ctx, mpkeyNAU, outpointKey)        // nft:auction:utxo
			pipe.ZRem(ctx, mpkeyNAD, outpointKey)        // nft:auction:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNAS, -1, strCodeHash) // nft:auction:sender-summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			mpkeySUT := "mp:{sut}"
			mpkeySUTA := "mp:{suta" + strAddressPkh + "}"
			mpkeySUTC := "mp:{sutc" + strGenesisId + strCodeHash + "}"
			mpkeySUP := "mp:{sup}"
			mpkeySUPA := "mp:{supa" + strAddressPkh + "}"
			mpkeySUPC := "mp:{supc" + strGenesisId + strCodeHash + "}"
			mpkeySUI := "mp:{sui}"
			mpkeySUIA := "mp:{suia" + strAddressPkh + "}"
			mpkeySUIC := "mp:{suic" + strGenesisId + strCodeHash + "}"

			pipe.ZRem(ctx, mpkeySUT, outpointKey)  // nft:sell:all:utxo, sort by time
			pipe.ZRem(ctx, mpkeySUTA, outpointKey) // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, mpkeySUTC, outpointKey) // nft:sell

			pipe.ZRem(ctx, mpkeySUP, outpointKey)  // nft:sell:all:utxo, sort by price
			pipe.ZRem(ctx, mpkeySUPA, outpointKey) // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, mpkeySUPC, outpointKey) // nft:sell

			pipe.ZRem(ctx, mpkeySUI, outpointKey)  // nft:sell:all:utxo, sort by token index
			pipe.ZRem(ctx, mpkeySUIA, outpointKey) // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, mpkeySUIC, outpointKey) // nft:sell

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"

			pipe.ZRem(ctx, mpkeyFU, outpointKey)                                                // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Data.FT.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Data.FT.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyFU, outpointKey) // ft:utxo
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = struct{}{}
		addrToRemove[strAddressPkh] = struct{}{}
	}

	for outpointKey, data := range utxoToSpend {
		strAddressPkh := string(data.Data.AddressPkh[:])
		strCodeHash := string(data.Data.CodeHash[:])
		strGenesisId := string(data.Data.GenesisId[:data.Data.GenesisIdLen])

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}

		if data.Data.CodeType == scriptDecoder.CodeType_NONE {
			if !data.Data.HasAddress {
				// 无法识别地址，暂不记录utxo
				// pipe.ZAdd(ctx, "mp:s:utxo", member)
				continue
			}

			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			mpkeyAU := "mp:s:{au" + strAddressPkh + "}"
			pipe.ZAdd(ctx, mpkeyAU, member)

			// balance of address
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))

			mpkeys = append(mpkeys, mpkeyAU, mpkeyBL)
			continue
		}

		// contract balance of address
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.DecrBy(ctx, mpkeyCB, int64(data.Satoshi))
		mpkeys = append(mpkeys, mpkeyCB)

		// redis有序genesis utxo数据添加
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			member.Score = float64(data.Data.NFT.TokenIndex)

			mpkeyNU := "mp:s:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:s:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"

			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)

			pipe.ZAdd(ctx, mpkeyNU, member)                          // nft:utxo
			pipe.ZAdd(ctx, mpkeyND, member)                          // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_AUCTION {
			mpkeyNAU := "mp:s:{nau" + strAddressPkh + "}" + strCodeHash
			mpkeyNAD := "mp:s:nad" + strCodeHash + strGenesisId
			mpkeyNAS := "mp:{nas" + strAddressPkh + "}"

			mpkeys = append(mpkeys, mpkeyNAU, mpkeyNAD, mpkeyNAS)

			pipe.ZAdd(ctx, mpkeyNAU, member)             // nft:auction:utxo
			pipe.ZAdd(ctx, mpkeyNAD, member)             // nft:auction:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNAS, -1, strCodeHash) // nft:auction:sender-summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			mpkeySUT := "mp:s:{sut}"
			mpkeySUTA := "mp:s:{suta" + strAddressPkh + "}"
			mpkeySUTC := "mp:s:{sutc" + strGenesisId + strCodeHash + "}"
			mpkeySUP := "mp:s:{sup}"
			mpkeySUPA := "mp:s:{supa" + strAddressPkh + "}"
			mpkeySUPC := "mp:s:{supc" + strGenesisId + strCodeHash + "}"
			mpkeySUI := "mp:s:{sui}"
			mpkeySUIA := "mp:s:{suia" + strAddressPkh + "}"
			mpkeySUIC := "mp:s:{suic" + strGenesisId + strCodeHash + "}"

			mpkeys = append(mpkeys, mpkeySUT, mpkeySUTA, mpkeySUTC, mpkeySUP, mpkeySUPA, mpkeySUPC, mpkeySUI, mpkeySUIA, mpkeySUIC)

			pipe.ZAdd(ctx, mpkeySUT, member)  // nft:sell:all:utxo, sort by time
			pipe.ZAdd(ctx, mpkeySUTA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUTC, member) // nft:sell

			member.Score = float64(data.Data.NFTSell.Price)
			pipe.ZAdd(ctx, mpkeySUP, member)  // nft:sell:all:utxo, sort by price
			pipe.ZAdd(ctx, mpkeySUPA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUPC, member) // nft:sell

			member.Score = float64(data.Data.NFTSell.TokenIndex)
			pipe.ZAdd(ctx, mpkeySUI, member)  // nft:sell:all:utxo, sort by token index
			pipe.ZAdd(ctx, mpkeySUIA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUIC, member) // nft:sell

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"

			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)

			pipe.ZAdd(ctx, mpkeyFU, member)                                                     // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Data.FT.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Data.FT.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

			mpkeys = append(mpkeys, mpkeyFU)
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = struct{}{}
		addrToRemove[strAddressPkh] = struct{}{}
	}

	// 删除summary 为0的记录
	for codeKey := range tokenToRemove {
		pipe.ZRemRangeByScore(ctx, "mp:{no"+codeKey+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "mp:{fb"+codeKey+"}", "0", "0")
	}
	// 删除balance 为0的记录
	for addr := range addrToRemove {
		pipe.ZRemRangeByScore(ctx, "mp:{ns"+addr+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "mp:{fs"+addr+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "mp:{nas"+addr+"}", "0", "0")
	}

	// 记录所有的mp:keys，以备区块确认后直接删除重来
	for _, mpkey := range mpkeys {
		pipe.SAdd(ctx, "mp:keys", mpkey)
	}
}
