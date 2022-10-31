package serial

import (
	"context"
	"encoding/hex"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/rdb"
	"strconv"

	redis "github.com/go-redis/redis/v8"
	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/zap"
)

var (
	ctx = context.Background()
)

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用
// 部分utxo信息在程序内存，missing的utxo将从redis查询
// 区块同步结束时会批量更新缓存的utxo到redis
func ParseGetSpentUtxoDataFromRedisSerial(block *model.ProcessBlock) {
	pipe := rdb.PikaClient.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
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
		d.Data = scriptDecoder.ExtractPkScriptForTxo(d.PkScript, d.ScriptType)

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

// UpdateUtxoInPika 批量更新redis utxo
func UpdateUtxoInPika(pikaPipe redis.Pipeliner, utxoToRestore, utxoToRemove map[string]*model.TxoData) {
	logger.Log.Info("UpdateUtxoInPika",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

	for outpointKey := range utxoToRemove {
		// redis全局utxo数据清除
		pikaPipe.Del(ctx, "u"+outpointKey)
	}

	for outpointKey, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.PkScript))
		length := data.Marshal(buf)
		// redis全局utxo数据添加，以便关联后续花费的input，无论是否识别地址都需要记录
		pikaPipe.Set(ctx, "u"+outpointKey, buf[:length], 0)
		logger.Log.Info("save new utxo",
			zap.String("outpoint", hex.EncodeToString([]byte(outpointKey))),
			zap.Int("size", length))
	}

	logger.Log.Info("UpdateUtxoInPika finished")
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(pipe redis.Pipeliner, blocksTotal int, addressBalanceCmds map[string]*redis.IntCmd, utxoToRestore, utxoToRemove map[string]*model.TxoData, isReorg bool) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

	pipe.HSet(ctx, "info",
		"blocks_total", blocksTotal,
	)
	pipe.HIncrBy(ctx, "info",
		"utxo_total", int64(len(utxoToRestore)-len(utxoToRemove)),
	)

	for outpointKey, data := range utxoToRestore {
		strAddressPkh := string(data.Data.AddressPkh[:])
		strCodeHash := string(data.Data.CodeHash[:])
		strGenesisId := string(data.Data.GenesisId[:data.Data.GenesisIdLen])

		// redis有序utxo数据成员
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}

		// 非合约信息记录
		if data.Data.CodeType == scriptDecoder.CodeType_NONE {
			if !data.Data.HasAddress {
				// 无法识别地址，暂不记录utxo
				// pipe.ZAdd(ctx, "utxo", member)
				continue
			}
			// 识别地址，只记录utxo和balance
			pipe.ZAdd(ctx, "{au"+strAddressPkh+"}", member)           // 有序address utxo数据添加
			pipe.IncrBy(ctx, "bl"+strAddressPkh, int64(data.Satoshi)) // balance of address
			continue
		}

		// 合约信息记录
		// contract satoshi balance of address
		pipe.IncrBy(ctx, "cb"+strAddressPkh, int64(data.Satoshi))

		// 有序genesis utxo数据添加
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			// nftIdx as score
			member.Score = float64(data.Data.NFT.TokenIndex)
			pipe.ZAdd(ctx, "{nu"+strAddressPkh+"}"+strCodeHash+strGenesisId, member) // nft:utxo
			pipe.ZAdd(ctx, "nd"+strCodeHash+strGenesisId, member)                    // nft:utxo-detail
			pipe.ZIncrBy(ctx, "{no"+strGenesisId+strCodeHash+"}", 1, strAddressPkh)  // nft:owners
			pipe.ZIncrBy(ctx, "{ns"+strAddressPkh+"}", 1, strCodeHash+strGenesisId)  // nft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_AUCTION {
			pipe.ZAdd(ctx, "{nau"+strAddressPkh+"}"+strCodeHash, member) // nft:auction:utxo
			pipe.ZAdd(ctx, "nad"+strCodeHash+strGenesisId, member)       // nft:auction:utxo-detail
			pipe.ZIncrBy(ctx, "{nas"+strAddressPkh+"}", 1, strCodeHash)  // nft:auction:sender-summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			pipe.ZAdd(ctx, "{sut}", member)                              // nft:sell:all:utxo, sort by time
			pipe.ZAdd(ctx, "{suta"+strAddressPkh+"}", member)            // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, "{sutc"+strGenesisId+strCodeHash+"}", member) // nft:sell

			member.Score = float64(data.Data.NFTSell.Price)
			pipe.ZAdd(ctx, "{sup}", member)                              // nft:sell:all:utxo, sort by price
			pipe.ZAdd(ctx, "{supa"+strAddressPkh+"}", member)            // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, "{supc"+strGenesisId+strCodeHash+"}", member) // nft:sell

			member.Score = float64(data.Data.NFTSell.TokenIndex)
			pipe.ZAdd(ctx, "{sui}", member)                              // nft:sell:all:utxo, sort by token index
			pipe.ZAdd(ctx, "{suia"+strAddressPkh+"}", member)            // nft:sell:seller-address:utxo
			pipe.ZAdd(ctx, "{suic"+strGenesisId+strCodeHash+"}", member) // nft:sell

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			pipe.ZAdd(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, member)                           // ft:utxo
			pipe.ZIncrBy(ctx, "{fb"+strGenesisId+strCodeHash+"}", float64(data.Data.FT.Amount), strAddressPkh) // ft:balance
			pipe.ZIncrBy(ctx, "{fs"+strAddressPkh+"}", float64(data.Data.FT.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			pipe.ZAdd(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, member) // uniq:utxo
		}

		// skip if reorg
		if isReorg {
			continue
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

		// 非合约信息清理
		if data.Data.CodeType == scriptDecoder.CodeType_NONE {
			// redis有序utxo数据清除
			if !data.Data.HasAddress {
				// 无法识别地址，暂不记录utxo
				// pipe.ZRem(ctx, "utxo", outpointKey)
				continue
			}
			// 识别地址，只记录utxo和balance
			pipe.ZRem(ctx, "{au"+strAddressPkh+"}", outpointKey)                                               // 有序address utxo数据清除
			addressBalanceCmds["bl"+strAddressPkh] = pipe.DecrBy(ctx, "bl"+strAddressPkh, int64(data.Satoshi)) // balance of address
			continue
		}

		// 非合约信息清理
		// contract satoshi balance of address
		addressBalanceCmds["cb"+strAddressPkh] = pipe.DecrBy(ctx, "cb"+strAddressPkh, int64(data.Satoshi))

		// redis有序genesis utxo数据清除
		if data.Data.CodeType == scriptDecoder.CodeType_NFT {
			pipe.ZRem(ctx, "{nu"+strAddressPkh+"}"+strCodeHash+strGenesisId, outpointKey) // nft:utxo
			pipe.ZRem(ctx, "nd"+strCodeHash+strGenesisId, outpointKey)                    // nft:utxo-detail
			pipe.ZIncrBy(ctx, "{no"+strGenesisId+strCodeHash+"}", -1, strAddressPkh)      // nft:owners
			pipe.ZIncrBy(ctx, "{ns"+strAddressPkh+"}", -1, strCodeHash+strGenesisId)      // nft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_AUCTION {
			pipe.ZRem(ctx, "{nau"+strAddressPkh+"}"+strCodeHash, outpointKey) // nft:auction:utxo
			pipe.ZRem(ctx, "nad"+strCodeHash+strGenesisId, outpointKey)       // nft:auction:utxo-detail
			pipe.ZIncrBy(ctx, "{nas"+strAddressPkh+"}", -1, strCodeHash)      // nft:auction:sender-summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			pipe.ZRem(ctx, "{sut}", outpointKey)                              // nft:sell:all:utxo, sort by time
			pipe.ZRem(ctx, "{suta"+strAddressPkh+"}", outpointKey)            // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, "{sutc"+strGenesisId+strCodeHash+"}", outpointKey) // nft:sell

			pipe.ZRem(ctx, "{sup}", outpointKey)                              // nft:sell:all:utxo, sort by price
			pipe.ZRem(ctx, "{supa"+strAddressPkh+"}", outpointKey)            // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, "{supc"+strGenesisId+strCodeHash+"}", outpointKey) // nft:sell

			pipe.ZRem(ctx, "{sui}", outpointKey)                              // nft:sell:all:utxo, sort by token index
			pipe.ZRem(ctx, "{suia"+strAddressPkh+"}", outpointKey)            // nft:sell:seller-address:utxo
			pipe.ZRem(ctx, "{suic"+strGenesisId+strCodeHash+"}", outpointKey) // nft:sell

		} else if data.Data.CodeType == scriptDecoder.CodeType_FT {
			pipe.ZRem(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, outpointKey)                       // ft:utxo
			pipe.ZIncrBy(ctx, "{fb"+strGenesisId+strCodeHash+"}", -float64(data.Data.FT.Amount), strAddressPkh) // ft:balance
			pipe.ZIncrBy(ctx, "{fs"+strAddressPkh+"}", -float64(data.Data.FT.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.Data.CodeType == scriptDecoder.CodeType_UNIQUE {
			pipe.ZRem(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, outpointKey) // uniq:utxo
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = struct{}{}
		addrToRemove[strAddressPkh] = struct{}{}
	}

	// 删除balance 为0的记录
	for codeKey := range tokenToRemove {
		pipe.ZRemRangeByScore(ctx, "{no"+codeKey+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "{fb"+codeKey+"}", "0", "0")
	}

	// 删除summary 为0的记录
	for addr := range addrToRemove {
		pipe.ZRemRangeByScore(ctx, "{ns"+addr+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "{fs"+addr+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "{nas"+addr+"}", "0", "0")
	}

	logger.Log.Info("UpdateUtxoInRedis finished")
}

func DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds map[string]*redis.IntCmd) bool {
	if len(addressBalanceCmds) == 0 {
		return true
	}
	pipe := rdb.RedisClient.TxPipeline()
	// 删除balance为0的记录
	// 要求整个函数单线程处理，否则可能删除非0数据
	for keyString, cmd := range addressBalanceCmds {
		balance, err := cmd.Result()
		if err == redis.Nil {
			logger.Log.Error("redis not found balance", zap.String("key", hex.EncodeToString([]byte(keyString))))
			continue
		} else if err != nil {
			logger.Log.Error("DeleteKeysWhitchAddressBalanceZero get failed", zap.Error(err))
			return false
		}

		if balance == 0 {
			pipe.Del(ctx, keyString)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Log.Error("DeleteKeysWhitchAddressBalanceZero failed", zap.Error(err))
		return false
	}
	return true
}
