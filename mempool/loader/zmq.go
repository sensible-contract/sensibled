package loader

import (
	"encoding/hex"
	"fmt"
	"sensibled/logger"
	"sensibled/utils"

	"github.com/spf13/viper"
	"github.com/zeromq/goczmq"
	"go.uber.org/zap"
)

var (
	NewBlockNotify = make(chan string, 1)
	RawTxNotify    = make(chan []byte, 1000)
)

func init() {
	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	zmqEndpoint := viper.GetString("zmq")

	// 监听新Tx
	go func() {
		zmqNotify(zmqEndpoint)
	}()
}

func zmqNotify(endpoint string) {
	logger.Log.Info("ZeroMQ started to listen for txs")
	subscriber, err := goczmq.NewSub(endpoint, "hashblock,rawtx")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}
	defer subscriber.Destroy()

	for {
		msg, _, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Error ZMQ RecFrame: %s", zap.Error(err))
			continue
		}

		if len(msg) == 4 {
			// id
			// logger.Log.Info("id: %d, %d", zap.Int("n", n), zap.Int("id", binary.LittleEndian.Uint32(msg)))

		} else if len(msg) == 5 || len(msg) == 6 || len(msg) == 9 {
			// topic
			// logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))
		} else if len(msg) == 32 {
			select {
			case NewBlockNotify <- utils.HashString(msg):
			default:
			}
			logger.Log.Info("received", zap.String("blkid", hex.EncodeToString(msg)))

		} else {
			// rawtx
			RawTxNotify <- msg
			// logger.Log.Info("tx received", zap.Int("n", n), zap.Int("rawtxLen", len(msg)))
		}
	}
}
