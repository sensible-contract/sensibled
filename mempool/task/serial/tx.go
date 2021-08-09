package serial

import (
	"context"
	"encoding/hex"
	"satoblock/logger"
	"satoblock/model"
	"satoblock/rdb"

	redis "github.com/go-redis/redis/v8"
	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

var (
	ctx = context.Background()
)

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	pipe := rdb.Client.Pipeline()
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
			continue
		} else if err != nil {
			panic(err)
		}
		d := model.TxoDataPool.Get().(*model.TxoData)
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = scriptDecoder.GetLockingScriptType(d.PkScript)
		txo := scriptDecoder.ExtractPkScriptForTxo(d.PkScript, d.ScriptType)

		d.CodeType = txo.CodeType
		d.CodeHash = txo.CodeHash
		d.GenesisId = txo.GenesisId
		d.SensibleId = txo.SensibleId
		d.AddressPkh = txo.AddressPkh

		// nft
		d.MetaTxId = txo.MetaTxId
		d.MetaOutputIndex = txo.MetaOutputIndex
		d.TokenIndex = txo.TokenIndex
		d.TokenSupply = txo.TokenSupply

		// ft
		d.Name = txo.Name
		d.Symbol = txo.Symbol
		d.Amount = txo.Amount
		d.Decimal = txo.Decimal

		spentUtxoDataMap[outpointKey] = d
	}
}

// UpdateUtxoInRedisSerial 顺序更新当前区块的utxo信息变化到redis
func UpdateUtxoInRedisSerial(pipe redis.Pipeliner, needReset bool,
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	insideTxo := make([]string, len(spentUtxoKeysMap))
	for outpointKey := range spentUtxoKeysMap {
		if data, ok := newUtxoDataMap[outpointKey]; !ok {
			continue
		} else {
			model.TxoDataPool.Put(data)
		}
		insideTxo = append(insideTxo, outpointKey)
	}
	for _, outpointKey := range insideTxo {
		delete(newUtxoDataMap, outpointKey)
	}

	for outpointKey, data := range newUtxoDataMap {
		model.GlobalMempoolNewUtxoDataMap[outpointKey] = data
	}

	for outpointKey, data := range removeUtxoDataMap {
		model.TxoDataPool.Put(data)
		delete(model.GlobalMempoolNewUtxoDataMap, outpointKey)
	}

	UpdateUtxoInRedis(pipe, needReset, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap)
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(pipe redis.Pipeliner, needReset bool, utxoToRestore, utxoToRemove, utxoToSpend map[string]*model.TxoData) (err error) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("nStore", len(utxoToRestore)),
		zap.Int("nRemove", len(utxoToRemove)),
		zap.Int("nSpend", len(utxoToSpend)))

	// 清除内存池数据
	if needReset {
		logger.Log.Info("reset redis mempool start")
		keys, err := rdb.Client.SMembers(ctx, "mp:keys").Result()
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
		logger.Log.Info("FlushdbInRedis finish")
	}

	// 更新内存池数据
	mpkeys := make([]string, 5*(len(utxoToRestore)+len(utxoToRemove)+len(utxoToSpend)))
	for outpointKey, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.PkScript))
		data.Marshal(buf)
		// redis全局utxo数据添加
		// fixme: 会覆盖satoblock？
		pipe.SetNX(ctx, "u"+outpointKey, buf, 0)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			logger.Log.Info("ignore mp:utxo", zap.String("key", hex.EncodeToString([]byte(outpointKey))), zap.Float64("score", member.Score))
			// pipe.ZAdd(ctx, "mp:utxo", member)
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			logger.Log.Info("ZAdd mp:au",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.String("key", hex.EncodeToString([]byte(outpointKey))),
				zap.Float64("score", member.Score))
			mpkeyAU := "mp:{au" + strAddressPkh + "}"
			pipe.ZAdd(ctx, mpkeyAU, member)

			// balance of address
			logger.Log.Info("IncrBy mp:bl",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.Uint64("satoshi", data.Satoshi))
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.IncrBy(ctx, mpkeyBL, int64(data.Satoshi))

			mpkeys = append(mpkeys, mpkeyAU, mpkeyBL)
			continue
		}

		// contract balance of address
		logger.Log.Info("IncrBy mp:cb",
			zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
			zap.Uint64("satoshi", data.Satoshi))
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.IncrBy(ctx, mpkeyCB, int64(data.Satoshi))
		mpkeys = append(mpkeys, mpkeyCB)

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"
			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)

			member.Score = float64(data.TokenIndex)
			pipe.ZAdd(ctx, mpkeyNU, member)                         // nft:utxo
			pipe.ZAdd(ctx, mpkeyND, member)                         // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, 1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, 1, strCodeHash+strGenesisId) // nft:summary

		} else if data.CodeType == scriptDecoder.CodeType_NFT_SELL {
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

			member.Score = float64(data.Amount)
			pipe.ZAdd(ctx, mpkeySUP, member)  // nft:sell:all:utxo, sort by price
			pipe.ZAdd(ctx, mpkeySUPA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUPC, member) // nft:sell

			member.Score = float64(data.TokenIndex)
			pipe.ZAdd(ctx, mpkeySUI, member)  // nft:sell:all:utxo, sort by token index
			pipe.ZAdd(ctx, mpkeySUIA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUIC, member) // nft:sell

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"
			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)

			pipe.ZAdd(ctx, mpkeyFU, member)                                            // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, float64(data.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeys = append(mpkeys, mpkeyFU)

			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

		}

		// update token info
		if data.CodeType == scriptDecoder.CodeType_NFT {
			pipe.HSet(ctx, "ni"+strCodeHash+strGenesisId,
				"metatxid", data.MetaTxId,
				"metavout", data.MetaOutputIndex,
				"supply", data.TokenSupply,
				"sensibleid", data.SensibleId,
			)
		} else if data.CodeType == scriptDecoder.CodeType_FT || data.CodeType == scriptDecoder.CodeType_UNIQUE {
			pipe.HSet(ctx, "fi"+strCodeHash+strGenesisId,
				"decimal", data.Decimal,
				"name", data.Name,
				"symbol", data.Symbol,
				"sensibleid", data.SensibleId,
			)
		}

	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for outpointKey, data := range utxoToRemove {
		// redis全局utxo数据清除
		// 暂时不清除
		// pipe.Del(ctx, "u"+outpointKey)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据清除
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// pipe.ZRem(ctx, "mp:utxo", outpointKey)
			continue
		}

		if len(data.GenesisId) < 20 {
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
		if data.CodeType == scriptDecoder.CodeType_NFT {
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"

			pipe.ZRem(ctx, mpkeyNU, outpointKey)                     // nft:utxo
			pipe.ZRem(ctx, mpkeyND, outpointKey)                     // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

		} else if data.CodeType == scriptDecoder.CodeType_NFT_SELL {
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

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"

			pipe.ZRem(ctx, mpkeyFU, outpointKey)                                        // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyFU, outpointKey) // ft:utxo
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = true
		addrToRemove[strAddressPkh] = true
	}

	for outpointKey, data := range utxoToSpend {
		// redis全局utxo数据不能在这里清除，留给区块确认时去做
		// pipe.Del(ctx, "u"+key)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// pipe.ZAdd(ctx, "mp:s:utxo", member)
			continue
		}

		if len(data.GenesisId) < 20 {
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
		if data.CodeType == scriptDecoder.CodeType_NFT {
			member.Score = float64(data.TokenIndex)

			mpkeyNU := "mp:s:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyND := "mp:s:nd" + strCodeHash + strGenesisId
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"

			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)

			pipe.ZAdd(ctx, mpkeyNU, member)                          // nft:utxo
			pipe.ZAdd(ctx, mpkeyND, member)                          // nft:utxo-detail
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh)            // nft:owners
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

		} else if data.CodeType == scriptDecoder.CodeType_NFT_SELL {
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

			member.Score = float64(data.Amount)
			pipe.ZAdd(ctx, mpkeySUP, member)  // nft:sell:all:utxo, sort by price
			pipe.ZAdd(ctx, mpkeySUPA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUPC, member) // nft:sell

			member.Score = float64(data.TokenIndex)
			pipe.ZAdd(ctx, mpkeySUI, member)  // nft:sell:all:utxo, sort by token index
			pipe.ZAdd(ctx, mpkeySUIA, member) // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, mpkeySUIC, member) // nft:sell

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"

			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)

			pipe.ZAdd(ctx, mpkeyFU, member)                                             // ft:utxo
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Amount), strAddressPkh)            // ft:balance
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

			mpkeys = append(mpkeys, mpkeyFU)
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = true
		addrToRemove[strAddressPkh] = true
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
	}

	// 记录所有的mp:keys，以备区块确认后直接删除重来
	for _, mpkey := range mpkeys {
		pipe.SAdd(ctx, "mp:keys", mpkey)
	}

	return nil
}
