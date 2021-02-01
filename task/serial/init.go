package serial

import (
	"blkparser/model"
	"sync"
)

var (
	calcMap   map[string]model.CalcData
	calcMutex sync.Mutex

	utxoMap map[string]model.CalcData
)

func init() {
	calcMap = make(map[string]model.CalcData, 0)
	utxoMap = make(map[string]model.CalcData, 0)

	// loadUtxoFromGobFile()
}

func Parse(block *model.Block) {
	dumpBlock(block)
	dumpBlockTx(block)
	dumpBlockTxInfo(block)

	// parseBlockCount(block)
	// parseUtxoSerial(block.ParseData)

}
