package serial

import (
	"blkparser/utils"
	"time"

	"go.uber.org/zap"
)

var (
	lastLogTime      time.Time
	lastBlockHeight  int
	lastBlockTxCount int
)

func ParseBlockSpeed(nTx int, nextBlockHeight, blockCountInBuffer, maxBlockHeightParallel, maxBlockHeight int) {
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

	utils.LogErr.Info("parsing",
		zap.Int("height", nextBlockHeight),
		zap.Int("~height", maxBlockHeightParallel-nextBlockHeight),
		zap.Int("buff", blockCountInBuffer),
		zap.Int("nblk", nextBlockHeight-lastBlockHeight),
		zap.Int("ntx", lastBlockTxCount),
		// zap.Int("+u", lastUtxoMapAddCount),
		// zap.Int("-u", lastUtxoMapRemoveCount),
		// zap.Int("=u", lastUtxoMapAddCount-lastUtxoMapRemoveCount),
		zap.Int("utxo", len(utxoMap)),
		// zap.Int("calc", len(calcMap)),
		zap.Int("time", timeLeft),
	)

	lastBlockHeight = nextBlockHeight
	lastBlockTxCount = 0
	lastUtxoMapAddCount = 0
	lastUtxoMapRemoveCount = 0
}
