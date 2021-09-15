package loader

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sensibled/logger"

	"github.com/spf13/viper"
	"github.com/zeromq/goczmq"
	"go.uber.org/zap"
)

var (
	NewBlockNotify = make(chan string, 1)
	RawTxNotify    = make(chan []byte, 1000)
	NewTxNotify    = make(chan string, 1000)
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
	subscriber, err := goczmq.NewSub(endpoint, "hashblock,hashtx")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}
	defer subscriber.Destroy()

	subscriber.SetTcpKeepalive(1)
	subscriber.SetTcpKeepaliveIdle(120)
	subscriber.SetTcpKeepaliveCnt(10)
	subscriber.SetTcpKeepaliveIntvl(3)

	logger.Log.Info("zmq conf",
		zap.Int("keepalive", subscriber.TcpKeepalive()),
		zap.Int("keepalive idle", subscriber.TcpKeepaliveIdle()),
		zap.Int("keepalive count", subscriber.TcpKeepaliveCnt()),
		zap.Int("keepalive intvl", subscriber.TcpKeepaliveIntvl()),
	)
	hashBlock := false
	for {
		msg, n, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Error ZMQ RecFrame", zap.Error(err))
			continue
		}

		if len(msg) == 4 {
			// id
			logger.Log.Info("zmq id", zap.Int("n", n), zap.Uint32("id", binary.LittleEndian.Uint32(msg)))

		} else if len(msg) == 5 || len(msg) == 6 {
			logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))
		} else if len(msg) == 9 {
			// topic
			hashBlock = true
			logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))
		} else if len(msg) == 32 {
			hashIdHex := hex.EncodeToString(msg)
			if hashBlock {
				hashBlock = false
				select {
				case NewBlockNotify <- hashIdHex:
				default:
				}
				logger.Log.Info("new block received", zap.String("blkid", hashIdHex))
			} else {
				NewTxNotify <- hashIdHex
				logger.Log.Info("new tx received", zap.String("txid", hashIdHex))
			}

		} else {
			// rawtx
			// RawTxNotify <- msg
			logger.Log.Info("new tx received", zap.Int("n", n), zap.Int("rawtxLen", len(msg)))
		}
	}
}
