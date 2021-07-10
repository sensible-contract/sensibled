package loader

import (
	"encoding/hex"
	"satoblock/logger"
	"satoblock/utils"

	"github.com/zeromq/goczmq"
	"go.uber.org/zap"
)

func ZmqNotify(endpoint string, block chan string) {
	logger.Log.Info("ZeroMQ started to listen for blocks")
	subscriber, err := goczmq.NewSub(endpoint, "hashblock")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}
	defer subscriber.Destroy()

	for {
		msg, _, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Error ZMQ RecFrame", zap.Error(err))
		}

		if len(msg) == 32 {
			block <- utils.HashString(msg)
		}
		logger.Log.Info("received", zap.String("blkid", hex.EncodeToString(msg)))
	}
}
