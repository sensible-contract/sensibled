package utils

import (
	"satoblock/logger"
	"time"

	"go.uber.org/zap"
)

var (
	start            time.Time = time.Now()
	lastLogTime      time.Time
	lastBlockHeight  int
	lastBlockTxCount int
)

func ParseBlockSpeed(nTx, lenGlobalNewUtxoDataMap, lenGlobalSpentUtxoDataMap, nextBlockHeight, maxBlockHeight int) {
	msg := "parsing block"
	if nTx == 0 {
		// parsing header
		msg = "parsing header"
	}
	lastBlockTxCount += nTx

	if nextBlockHeight != maxBlockHeight-1 && time.Since(lastLogTime) < time.Second {
		return
	}

	if nextBlockHeight < lastBlockHeight {
		lastBlockHeight = 0
	}

	lastLogTime = time.Now()

	timeLeft := 0
	if maxBlockHeight > 0 && (nextBlockHeight-lastBlockHeight) != 0 {
		timeLeft = (maxBlockHeight - nextBlockHeight) / (nextBlockHeight - lastBlockHeight)
	}

	logger.Log.Info(msg,
		zap.Int("height", nextBlockHeight),
		zap.Int("nblk", nextBlockHeight-lastBlockHeight),
		zap.Int("ntx", lastBlockTxCount),
		zap.Int("txo", lenGlobalSpentUtxoDataMap),
		zap.Int("utxo", lenGlobalNewUtxoDataMap),
		zap.Int("time", timeLeft),
		zap.Duration("elapse", time.Since(start)),
	)

	lastBlockHeight = nextBlockHeight
	lastBlockTxCount = 0
}
