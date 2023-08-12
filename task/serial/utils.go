package serial

import (
	"context"
	"encoding/gob"
	"encoding/hex"
	"os"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/rdb"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// WriteDownUtxoToFile 批量更新redis utxo
func WriteDownUtxoToFile(utxoToRestore, utxoToRemove map[string]*model.TxoData) {
	logger.Log.Info("WriteDownUtxoToFile",
		zap.Int("add", len(utxoToRestore)),
		zap.Int("del", len(utxoToRemove)))

	outpointKeyToDel := make([]string, len(utxoToRemove))

	idx := 0
	for outpointKey := range utxoToRemove {
		outpointKeyToDel[idx] = outpointKey
		idx++
	}

	gobFile, err := os.OpenFile("./cmd/utxoToRemove.gob", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		logger.Log.Error("open outpointKeyToDel file failed", zap.Error(err))
		return
	}
	defer gobFile.Close()

	enc := gob.NewEncoder(gobFile)
	if err := enc.Encode(outpointKeyToDel); err != nil {
		logger.Log.Error("save outpointKeyToDel failed", zap.Error(err))
	}
	logger.Log.Info("save outpointKeyToDel ok")

	/////////////////////////////////////////////////////////////////

	idx = 0
	utxoBufToRestore := make([][]byte, len(utxoToRestore))
	for outpointKey, data := range utxoToRestore {
		buf := make([]byte, 36+20+len(data.PkScript))
		length := data.Marshal(buf)

		buf = append(buf[:length], []byte(outpointKey)...)
		utxoBufToRestore[idx] = buf[:length+36]
		idx++
	}

	gobFile1, err := os.OpenFile("./cmd/utxoToRestore.gob", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		logger.Log.Error("open utxoBufToRestore file failed", zap.Error(err))
		return
	}
	defer gobFile1.Close()

	enc1 := gob.NewEncoder(gobFile1)
	if err := enc1.Encode(utxoBufToRestore); err != nil {
		logger.Log.Error("save utxoBufToRestore failed", zap.Error(err))
	}
	logger.Log.Info("save utxoBufToRestore ok")
}

func DeleteKeysWhitchAddressBalanceZero(addressBalanceCmds map[string]*redis.IntCmd) bool {
	if len(addressBalanceCmds) == 0 {
		return true
	}
	ctx := context.Background()
	pipe := rdb.RdbBalanceClient.TxPipeline()
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
