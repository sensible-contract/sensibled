package serial

import (
	"context"
	"fmt"
	"satoblock/logger"
	"satoblock/model"

	redis "github.com/go-redis/redis/v8"
	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	rdb      *redis.Client
	rdbBlock *redis.Client
	ctx      = context.Background()
)

func init() {
	viper.SetConfigFile("conf/redis.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	address := viper.GetString("address")
	password := viper.GetString("password")
	database := viper.GetInt("database")
	databaseBlock := viper.GetInt("database_block")
	dialTimeout := viper.GetDuration("dialTimeout")
	readTimeout := viper.GetDuration("readTimeout")
	writeTimeout := viper.GetDuration("writeTimeout")
	poolSize := viper.GetInt("poolSize")
	rdb = redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     password,
		DB:           database,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})

	rdbBlock = redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     password,
		DB:           databaseBlock,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})
}

func PublishBlockSyncFinished() {
	rdbBlock.Publish(ctx, "channel_block_sync", "finished")
}

func FlushdbInRedis() {
	logger.Log.Info("FlushdbInRedis start")
	rdb.FlushDB(ctx)
	rdbBlock.FlushDB(ctx)
	logger.Log.Info("FlushdbInRedis finish")
}

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用
// 部分utxo信息在程序内存，missing的utxo将从redis查询
// 区块同步结束时会批量更新缓存的utxo到redis
func ParseGetSpentUtxoDataFromRedisSerial(block *model.ProcessBlock) {
	pipe := rdbBlock.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	for key := range block.SpentUtxoKeysMap {
		// 检查是否是区块内自产自花
		if data, ok := block.NewUtxoDataMap[key]; ok {
			block.SpentUtxoDataMap[key] = data
			delete(block.NewUtxoDataMap, key)
			continue
		}
		// 检查是否在本地全局缓存
		if data, ok := GlobalNewUtxoDataMap[key]; ok {
			block.SpentUtxoDataMap[key] = data
			delete(GlobalNewUtxoDataMap, key)
			continue
		}
		// 剩余utxo需要查询redis
		needExec = true
		m[key] = pipe.Get(ctx, key)
	}

	if !needExec {
		return
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		panic(err)
	}
	for key, v := range m {
		res, err := v.Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			panic(err)
		}
		d := &model.TxoData{}
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = scriptDecoder.GetLockingScriptType(d.Script)
		txo := scriptDecoder.ExtractPkScriptForTxo(d.Script, d.ScriptType)

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

		block.SpentUtxoDataMap[key] = d
		GlobalSpentUtxoDataMap[key] = d
	}
}

// UpdateUtxoInMapSerial 顺序更新当前区块的utxo信息变化到程序全局缓存
func UpdateUtxoInMapSerial(block *model.ProcessBlock) {
	// 更新到本地新utxo存储
	for key, data := range block.NewUtxoDataMap {
		GlobalNewUtxoDataMap[key] = data
	}
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(utxoToRestore, utxoToRemove map[string]*model.TxoData, isReorg bool) (err error) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))
	if len(utxoToRestore) == 0 && len(utxoToRemove) == 0 {
		return
	}
	pipe := rdb.Pipeline()
	pipeBlock := rdbBlock.Pipeline()
	for key, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis全局utxo数据添加
		pipeBlock.Set(ctx, key, buf, 0)
		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，只记录utxo
			if err := pipe.ZAdd(ctx, "utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			if err := pipe.ZAdd(ctx, "au"+string(data.AddressPkh), &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}

			// balance of address
			if err := pipe.ZIncrBy(ctx, "balance", float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			continue
		}

		// contract satoshi balance of address
		if err := pipe.ZIncrBy(ctx, "contract-balance", float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			if !isReorg {
				// skip if reorg
				// nft:info
				pipe.HSet(ctx, "ni"+string(data.CodeHash)+string(data.GenesisId),
					"metatxid", data.MetaTxId,
					"metavout", data.MetaOutputIndex,
					"supply", data.TokenSupply,
					"sensibleid", data.SensibleId,
				)
			}

			nftId := float64(data.TokenIndex)
			// nft:utxo
			if err := pipe.ZAdd(ctx, "nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}
			// nft:utxo-detail
			if err := pipe.ZAdd(ctx, "nd"+string(data.CodeHash)+string(data.GenesisId),
				&redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			if err := pipe.ZIncrBy(ctx, "no"+string(data.CodeHash)+string(data.GenesisId),
				1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// nft:summary
			if err := pipe.ZIncrBy(ctx, "ns"+string(data.AddressPkh),
				1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else if data.CodeType == scriptDecoder.CodeType_FT {
			if !isReorg {
				// skip if reorg
				// ft:info
				pipe.HSet(ctx, "fi"+string(data.CodeHash)+string(data.GenesisId),
					"decimal", data.Decimal,
					"name", data.Name,
					"symbol", data.Symbol,
					"sensibleid", data.SensibleId,
				)
			}
			// ft:utxo
			if err := pipe.ZAdd(ctx, "fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			// ft:balance
			if err := pipe.ZIncrBy(ctx, "fb"+string(data.CodeHash)+string(data.GenesisId),
				float64(data.Amount),
				string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// ft:summary
			if err := pipe.ZIncrBy(ctx, "fs"+string(data.AddressPkh),
				float64(data.Amount),
				string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			if !isReorg {
				// skip if reorg
				// ft:info
				pipe.HSet(ctx, "fi"+string(data.CodeHash)+string(data.GenesisId),
					"decimal", data.Decimal,
					"name", data.Name,
					"symbol", data.Symbol,
					"sensibleid", data.SensibleId,
				)
			}

			// ft:utxo
			if err := pipe.ZAdd(ctx, "fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
		}
	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		pipeBlock.Del(ctx, key)
		// redis有序utxo数据清除
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，只记录utxo
			if err := pipe.ZRem(ctx, "utxo", key).Err(); err != nil {
				panic(err)
			}
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据清除
			if err := pipe.ZRem(ctx, "au"+string(data.AddressPkh), key).Err(); err != nil {
				panic(err)
			}

			// balance of address
			if err := pipe.ZIncrBy(ctx, "balance", -float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			continue
		}

		// contract satoshi balance of address
		if err := pipe.ZIncrBy(ctx, "contract-balance", -float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据清除
		if data.CodeType == scriptDecoder.CodeType_NFT {
			// nft:utxo
			if err := pipe.ZRem(ctx, "nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				key).Err(); err != nil {
				panic(err)
			}
			// nft:utxo-detail
			if err := pipe.ZRem(ctx, "nd"+string(data.CodeHash)+string(data.GenesisId),
				key).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			if err := pipe.ZIncrBy(ctx, "no"+string(data.CodeHash)+string(data.GenesisId),
				-1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// nft:summary
			if err := pipe.ZIncrBy(ctx, "ns"+string(data.AddressPkh),
				-1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else if data.CodeType == scriptDecoder.CodeType_FT {
			// ft:utxo
			if err := pipe.ZRem(ctx, "fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh), key).Err(); err != nil {
				panic(err)
			}
			// ft:balance
			if err := pipe.ZIncrBy(ctx, "fb"+string(data.CodeHash)+string(data.GenesisId),
				-float64(data.Amount),
				string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// ft:summary
			if err := pipe.ZIncrBy(ctx, "fs"+string(data.AddressPkh),
				-float64(data.Amount),
				string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			// ft:utxo
			if err := pipe.ZRem(ctx, "fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh), key).Err(); err != nil {
				panic(err)
			}
		}

		// 记录key以备删除
		tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
		addrToRemove[string(data.AddressPkh)] = true
	}

	// 删除balance 为0的记录
	for codeKey := range tokenToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "no"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "fb"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除summary 为0的记录
	for addr := range addrToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "ns"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "fs"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	if err := pipe.ZRemRangeByScore(ctx, "balance", "0", "0").Err(); err != nil {
		panic(err)
	}

	// 删除contract balance 为0的记录
	if err := pipe.ZRemRangeByScore(ctx, "contract-balance", "0", "0").Err(); err != nil {
		panic(err)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	_, err = pipeBlock.Exec(ctx)
	if err != nil {
		panic(err)
	}

	logger.Log.Info("UpdateUtxoInRedis finished")

	return nil
}
