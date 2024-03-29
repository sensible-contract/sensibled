package loader

import (
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
)

func InitZmq() {
	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	zmqEndpointBlock := viper.GetString("zmq_block")
	zmqEndpointTx := viper.GetString("zmq_tx")

	logger.Log.Info("ZeroMQ started to listen for blocks")
	subscriberBlock, err := goczmq.NewSub(zmqEndpointBlock, "hashblock")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}

	logger.Log.Info("ZeroMQ started to listen for txs")
	subscriberTx, err := goczmq.NewSub(zmqEndpointTx, "rawtx")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}

	// 监听新Block
	go func() {
		zmqNotifyBlock(subscriberBlock)
	}()

	// 监听新Tx
	go func() {
		zmqNotifyTx(subscriberTx)
	}()
}

func zmqNotifyBlock(subscriber *goczmq.Sock) {
	defer subscriber.Destroy()

	subscriber.SetTcpKeepalive(1)
	subscriber.SetTcpKeepaliveIdle(120)
	subscriber.SetTcpKeepaliveCnt(10)
	subscriber.SetTcpKeepaliveIntvl(3)

	logger.Log.Info("zmq block conf",
		zap.Int("keepalive", subscriber.TcpKeepalive()),
		zap.Int("keepalive idle", subscriber.TcpKeepaliveIdle()),
		zap.Int("keepalive count", subscriber.TcpKeepaliveCnt()),
		zap.Int("keepalive intvl", subscriber.TcpKeepaliveIntvl()),
	)
	for {
		msg, n, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Block Error ZMQ RecFrame", zap.Error(err))
			continue
		}

		if len(msg) == 4 {
			// id
			// logger.Log.Info("zmq id", zap.Int("n", n), zap.Uint32("id", binary.LittleEndian.Uint32(msg)))

		} else if len(msg) == 9 {
			// topic
			// logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))

		} else if len(msg) == 32 {
			blockIdHex := hex.EncodeToString(msg)
			logger.Log.Info("new block received", zap.String("blkid", blockIdHex))
			NewBlockNotify <- blockIdHex
		} else {
			logger.Log.Info("bytes received", zap.Int("n", n), zap.Int("len", len(msg)))
		}
	}
}

func zmqNotifyTx(subscriber *goczmq.Sock) {
	defer subscriber.Destroy()

	subscriber.SetTcpKeepalive(1)
	subscriber.SetTcpKeepaliveIdle(120)
	subscriber.SetTcpKeepaliveCnt(10)
	subscriber.SetTcpKeepaliveIntvl(3)

	logger.Log.Info("zmq tx conf",
		zap.Int("keepalive", subscriber.TcpKeepalive()),
		zap.Int("keepalive idle", subscriber.TcpKeepaliveIdle()),
		zap.Int("keepalive count", subscriber.TcpKeepaliveCnt()),
		zap.Int("keepalive intvl", subscriber.TcpKeepaliveIntvl()),
	)
	for {
		msg, n, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Tx Error ZMQ RecFrame", zap.Int("n", n), zap.Error(err))
			continue
		}

		if len(msg) == 4 {
			// id
			// logger.Log.Info("zmq id", zap.Int("n", n), zap.Uint32("id", binary.LittleEndian.Uint32(msg)))

		} else if len(msg) == 5 || len(msg) == 6 {
			// topic
			// logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))

		} else {
			// rawtx
			RawTxNotify <- msg
			// logger.Log.Info("new tx received", zap.Int("n", n), zap.Int("rawtxLen", len(msg)))
		}
	}
}
