// go build -v sensibled/tools/delete_ft_utxo
// ./delete_ft_utxo

package main

import (
	"context"
	"encoding/hex"
	"flag"
	_ "net/http/pprof"
	"sensibled/logger"
	"sensibled/rdb"
	"sensibled/utils"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var (
	ctx = context.Background()

	codeHashHex    string
	genesisIdHex   string
	addressFT      string
	outpointKeyHex string
	amount         int

	query bool
)

func init() {
	flag.StringVar(&codeHashHex, "codehash", "777e4dd291059c9f7a0fd563f7204576dcceb791", "codehash")
	flag.StringVar(&genesisIdHex, "genesis", "8764ede9fa7bf81ba1eec5e1312cf67117d47930", "genesis")
	flag.StringVar(&addressFT, "address", "166xnb84AQ4XDtcjPTDuJoYpGXMPkSLdU5", "address")
	flag.IntVar(&amount, "amount", 0, "token amount to remove")
	flag.StringVar(&outpointKeyHex, "outpoint", "8764ede9fa7bf81ba1eec5e1312cf67117d47930", "utxo outpoint to remove")

	flag.BoolVar(&query, "query", false, "qeury FT only, not remove utxo")
	flag.Parse()

	rdb.RdbBalanceClient = rdb.Init("conf/rdb_balance.yaml")
	rdb.RdbUtxoClient = rdb.Init("conf/rdb_utxo.yaml")
}

func main() {
	codeHash, _ := hex.DecodeString(codeHashHex)
	genesisId, _ := hex.DecodeString(genesisIdHex)
	addressPkh, _ := utils.DecodeAddress(addressFT)
	outpointKey, _ := hex.DecodeString(outpointKeyHex)

	strCodeHash := string(codeHash)
	strGenesisId := string(genesisId)
	strAddressPkh := string(addressPkh)
	strOutpointKey := string(outpointKey)

	pipe := rdb.RdbBalanceClient.Pipeline()
	balanceCmd := pipe.ZScore(ctx, "{fb"+strGenesisId+strCodeHash+"}", strAddressPkh)
	summaryCmd := pipe.ZScore(ctx, "{fs"+strAddressPkh+"}", strCodeHash+strGenesisId)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		panic(err)
	}

	balance, err := balanceCmd.Result()
	if err != nil && err != redis.Nil {
		panic(err)
	}

	summary, err := summaryCmd.Result()
	if err != nil && err != redis.Nil {
		panic(err)
	}

	logger.Log.Info("query",
		zap.Float64("balance", balance),
		zap.Float64("summary", summary),
	)

	utxoOutpoints, err := rdb.RdbBalanceClient.ZRevRange(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, 0, -1).Result()
	if err == redis.Nil {
		utxoOutpoints = nil
	} else if err != nil {
		logger.Log.Info("GetUtxoOutpointsByAddress redis failed", zap.Error(err))
		return
	}

	pipeUtxo := rdb.RdbUtxoClient.Pipeline()
	m := map[string]*redis.IntCmd{}
	for _, utxo := range utxoOutpoints {
		m[utxo] = pipeUtxo.Exists(ctx, "u"+utxo)
	}

	if _, err := pipeUtxo.Exec(ctx); err != nil && err != redis.Nil {
		panic(err)
	}
	for utxo, v := range m {
		if _, err := v.Result(); err == redis.Nil {
			logger.Log.Info("missing utxo",
				zap.String("txid", hex.EncodeToString(utils.ReverseBytes([]byte(utxo[:32])))),
				zap.String("utxo", hex.EncodeToString([]byte(utxo))))
		}
	}

	if query {
		return
	}
	pipe = rdb.RdbBalanceClient.Pipeline()
	pipe.ZRem(ctx, "{fu"+strAddressPkh+"}"+strCodeHash+strGenesisId, strOutpointKey)
	if amount > 0 {
		pipe.ZIncrBy(ctx, "{fb"+strGenesisId+strCodeHash+"}", -float64(amount), strAddressPkh)
		pipe.ZIncrBy(ctx, "{fs"+strAddressPkh+"}", -float64(amount), strCodeHash+strGenesisId)
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		panic(err)
	}
	logger.Log.Info("writed.")
}
