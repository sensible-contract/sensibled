package serial

import (
	"blkparser/logger"
	"blkparser/model"
	"blkparser/script"
	"context"
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

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。
// 稍慢但占用内存较少
func ParseGetSpentUtxoDataFromRedisSerial(block *model.ProcessBlock) {
	pipe := rdb.Pipeline()

	m := map[string]*redis.StringCmd{}
	for key := range block.SpentUtxoKeysMap {
		if _, ok := block.NewUtxoDataMap[key]; ok {
			continue
		}
		m[key] = pipe.Get(ctx, key)
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
		d := model.CalcDataPool.Get().(*model.CalcData)
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = script.GetLockingScriptType(d.Script)
		d.GenesisId, d.AddressPkh = script.ExtractPkScriptAddressPkh(d.Script, d.ScriptType)
		if d.AddressPkh == nil {
			d.GenesisId, d.AddressPkh = script.ExtractPkScriptGenesisIdAndAddressPkh(d.Script)
		}

		block.SpentUtxoDataMap[key] = d
	}
}

// ParseGetSpentUtxoDataFromMapSerial 部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromMapSerial(block *model.ProcessBlock) {
	pipe := rdb.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	for key := range block.SpentUtxoKeysMap {
		if _, ok := block.NewUtxoDataMap[key]; ok {
			continue
		}
		if data, ok := GlobalNewUtxoDataMap[key]; ok {
			block.SpentUtxoDataMap[key] = data
			delete(GlobalNewUtxoDataMap, key)
		} else {
			needExec = true
			m[key] = pipe.Get(ctx, key)
		}
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
		d := model.CalcDataPool.Get().(*model.CalcData)
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = script.GetLockingScriptType(d.Script)
		d.GenesisId, d.AddressPkh = script.ExtractPkScriptAddressPkh(d.Script, d.ScriptType)
		if d.AddressPkh == nil {
			d.GenesisId, d.AddressPkh = script.ExtractPkScriptGenesisIdAndAddressPkh(d.Script)
		}

		block.SpentUtxoDataMap[key] = d
		GlobalSpentUtxoDataMap[key] = d
	}
}

// UpdateUtxoInMapSerial 顺序更新当前区块的utxo信息变化到程序全局缓存
func UpdateUtxoInMapSerial(block *model.ProcessBlock) {
	insideTxo := make([]string, len(block.SpentUtxoKeysMap))
	for key := range block.SpentUtxoKeysMap {
		if data, ok := block.NewUtxoDataMap[key]; !ok {
			continue
		} else {
			model.CalcDataPool.Put(data)
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
		model.CalcDataPool.Put(data)
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
func UpdateUtxoInRedis(utxoToRestore, utxoToRemove map[string]*model.CalcData) (err error) {
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
		}
		// redis有序genesis utxo数据添加
		if len(data.GenesisId) > 32 {
			if err := pipe.ZAdd(ctx, "g"+string(data.GenesisId), &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
		}
	}
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		pipe.Del(ctx, key)
		// redis有序utxo数据清除
		if err := pipe.ZRem(ctx, "utxo", key).Err(); err != nil {
			panic(err)
		}
		// redis有序address utxo数据清除
		if err := pipe.ZRem(ctx, "a"+string(data.AddressPkh), key).Err(); err != nil {
			panic(err)
		}
		// redis有序genesis utxo数据清除
		if err := pipe.ZRem(ctx, "g"+string(data.GenesisId), key).Err(); err != nil {
			panic(err)
		}
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
			if output.Value == 0 || !output.LockingScriptMatch {
				continue
			}

			key := string(output.LockingScriptType)

			if data, ok := calcMap[key]; ok {
				data.Value += 1
				calcMap[key] = data
			} else {
				calcMap[key] = &model.CalcData{Value: 1}
			}

			logger.Log.Info("pkscript",
				zap.String("tx", tx.HashHex),
				zap.Int("vout", idx),
				zap.Uint64("v", output.Value),
				zap.String("type", output.LockingScriptTypeHex),
			)
		}
	}
}
