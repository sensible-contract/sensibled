package rdb

import (
	"context"
	"fmt"
	"sensibled/logger"

	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	useCluster  bool
	RedisClient redis.UniversalClient
	PikaClient  redis.UniversalClient
	ctx         = context.Background()
)

// "conf/redis.yaml"
func Init(filename string) (rds redis.UniversalClient) {
	viper.SetConfigFile(filename)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	addrs := viper.GetStringSlice("addrs")
	password := viper.GetString("password")
	database := viper.GetInt("database")
	dialTimeout := viper.GetDuration("dialTimeout")
	readTimeout := viper.GetDuration("readTimeout")
	writeTimeout := viper.GetDuration("writeTimeout")
	poolSize := viper.GetInt("poolSize")
	rds = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        addrs,
		Password:     password,
		DB:           database,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})

	if len(addrs) > 1 {
		useCluster = true
	}
	return rds
}

func FlushdbInRedis() {
	logger.Log.Info("FlushdbInRedis start")

	var err error
	if useCluster {
		rdbc := RedisClient.(*redis.ClusterClient)
		err = rdbc.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		// todo: pika cluster flushdb
	} else {
		err = RedisClient.FlushDB(ctx).Err()
		err = PikaClient.FlushDB(ctx).Err()
	}

	if err != nil {
		logger.Log.Info("FlushdbInRedis err", zap.Error(err))
	} else {
		logger.Log.Info("FlushdbInRedis finish")
	}
}
