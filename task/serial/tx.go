package serial

import (
	"blkparser/logger"
	"blkparser/model"
	"blkparser/script"
	"context"
	"encoding/binary"
	"fmt"

	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	rdb *redis.Client
	ctx = context.Background()
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
}

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(block *model.ProcessBlock, withMap bool) {
	pipe := rdb.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	for key := range block.SpentUtxoKeysMap {
		if _, ok := block.NewUtxoDataMap[key]; ok {
			continue
		}

		if withMap {
			if data, ok := GlobalNewUtxoDataMap[key]; ok {
				block.SpentUtxoDataMap[key] = data
				delete(GlobalNewUtxoDataMap, key)
				continue
			}
		}

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
		d := model.TxoDataPool.Get().(*model.TxoData)
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = script.GetLockingScriptType(d.Script)
		d.IsNFT, d.CodeHash, d.GenesisId, d.AddressPkh, d.DataValue = script.ExtractPkScriptForTxo(d.Script, d.ScriptType)

		block.SpentUtxoDataMap[key] = d
		if withMap {
			GlobalSpentUtxoDataMap[key] = d
		}
	}
}

// UpdateUtxoInMapSerial 顺序更新当前区块的utxo信息变化到程序全局缓存
func UpdateUtxoInMapSerial(block *model.ProcessBlock) {
	insideTxo := make([]string, len(block.SpentUtxoKeysMap))
	for key := range block.SpentUtxoKeysMap {
		if data, ok := block.NewUtxoDataMap[key]; !ok {
			continue
		} else {
			model.TxoDataPool.Put(data)
		}
		insideTxo = append(insideTxo, key)
	}
	for _, key := range insideTxo {
		delete(block.NewUtxoDataMap, key)
		delete(block.SpentUtxoKeysMap, key)
	}

	for key, data := range block.NewUtxoDataMap {
		GlobalNewUtxoDataMap[key] = data
	}

	for _, data := range block.SpentUtxoDataMap {
		model.TxoDataPool.Put(data)
	}
}

// UpdateUtxoInRedisSerial 顺序更新当前区块的utxo信息变化到redis
func UpdateUtxoInRedisSerial(block *model.ProcessBlock) {
	insideTxo := make([]string, len(block.SpentUtxoKeysMap))
	for key := range block.SpentUtxoKeysMap {
		if _, ok := block.NewUtxoDataMap[key]; !ok {
			continue
		}
		insideTxo = append(insideTxo, key)
	}
	for _, key := range insideTxo {
		delete(block.NewUtxoDataMap, key)
		delete(block.SpentUtxoKeysMap, key)
	}

	UpdateUtxoInRedis(block.NewUtxoDataMap, block.SpentUtxoDataMap)
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(utxoToRestore, utxoToRemove map[string]*model.TxoData) (err error) {
	pipe := rdb.Pipeline()
	for key, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis全局utxo数据添加
		pipe.Set(ctx, key, buf, 0)
		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if err := pipe.ZAdd(ctx, "utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
			panic(err)
		}
		// redis有序address utxo数据添加
		if len(data.AddressPkh) == 20 {
			if err := pipe.ZAdd(ctx, "a"+string(data.AddressPkh), &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}

			if err := pipe.ZIncrBy(ctx, "balance", float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}

		}
		// redis有序genesis utxo数据添加
		if len(data.GenesisId) > 32 {
			if data.IsNFT {
				// nft:utxo
				nftId := make([]byte, 8)
				binary.LittleEndian.PutUint64(nftId, data.DataValue) // 8
				if err := pipe.ZAdd(ctx, "nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
					&redis.Z{Score: score, Member: key + string(nftId)}).Err(); err != nil {
					panic(err)
				}
				// nft:summary
				if err := pipe.ZIncrBy(ctx, "ns"+string(data.CodeHash)+string(data.GenesisId),
					1, string(data.AddressPkh)).Err(); err != nil {
					panic(err)
				}
				// nft:owner
				if err := pipe.ZIncrBy(ctx, "no"+string(data.AddressPkh),
					1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
					panic(err)
				}
			} else {
				// ft:utxo
				if err := pipe.ZAdd(ctx, "fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
					&redis.Z{Score: score, Member: key}).Err(); err != nil {
					panic(err)
				}
				// ft:summary
				if err := pipe.ZIncrBy(ctx, "fs"+string(data.CodeHash)+string(data.GenesisId),
					float64(data.DataValue),
					string(data.AddressPkh)).Err(); err != nil {
					panic(err)
				}
				// ft:balance
				if err := pipe.ZIncrBy(ctx, "fb"+string(data.AddressPkh),
					float64(data.DataValue),
					string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
					panic(err)
				}
			}
		}
	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		pipe.Del(ctx, key)
		// redis有序utxo数据清除
		if err := pipe.ZRem(ctx, "utxo", key).Err(); err != nil {
			panic(err)
		}

		if len(data.AddressPkh) == 20 {
			// redis有序address utxo数据清除
			if err := pipe.ZRem(ctx, "a"+string(data.AddressPkh), key).Err(); err != nil {
				panic(err)
			}

			if err := pipe.ZIncrBy(ctx, "balance", -float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}

		}

		// redis有序genesis utxo数据清除
		if len(data.GenesisId) > 32 {
			if data.IsNFT {
				// nft:utxo
				nftId := make([]byte, 8)
				binary.LittleEndian.PutUint64(nftId, data.DataValue) // 8
				if err := pipe.ZRem(ctx, "nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
					key+string(nftId)).Err(); err != nil {
					panic(err)
				}

				// nft:summary
				if err := pipe.ZIncrBy(ctx, "ns"+string(data.CodeHash)+string(data.GenesisId),
					-1, string(data.AddressPkh)).Err(); err != nil {
					panic(err)
				}
				// nft:owner
				if err := pipe.ZIncrBy(ctx, "no"+string(data.AddressPkh),
					-1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
					panic(err)
				}
			} else {
				// ft:utxo
				if err := pipe.ZRem(ctx, "nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
					key).Err(); err != nil {
					panic(err)
				}
				// ft:summary
				if err := pipe.ZIncrBy(ctx, "fs"+string(data.CodeHash)+string(data.GenesisId),
					-float64(data.DataValue),
					string(data.AddressPkh)).Err(); err != nil {
					panic(err)
				}
				// ft:balance
				if err := pipe.ZIncrBy(ctx, "fb"+string(data.AddressPkh),
					-float64(data.DataValue),
					string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
					panic(err)
				}
			}

			tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
			addrToRemove[string(data.AddressPkh)] = true
		}
	}

	// 删除summary 为0的记录
	for codeKey := range tokenToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "ns"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "fs"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	for addr := range addrToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "no"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "fb"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	if err := pipe.ZRemRangeByScore(ctx, "balance", "0", "0").Err(); err != nil {
		panic(err)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return nil
}

// DumpLockingScriptType  信息
func DumpLockingScriptType(block *model.Block) {
	for _, tx := range block.Txs {
		for idx, output := range tx.TxOuts {
			if output.Satoshi == 0 || !output.LockingScriptMatch {
				continue
			}

			key := string(output.LockingScriptType)

			if value, ok := calcMap[key]; ok {
				calcMap[key] = value + 1
			} else {
				calcMap[key] = 1
			}

			logger.Log.Info("pkscript",
				zap.String("tx", tx.HashHex),
				zap.Int("vout", idx),
				zap.Uint64("v", output.Satoshi),
				zap.String("type", output.LockingScriptTypeHex),
			)
		}
	}
}
