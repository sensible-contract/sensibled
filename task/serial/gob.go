package serial

import (
	"blkparser/model"
	"blkparser/utils"
	"encoding/binary"
	"encoding/hex"
	"runtime"
	"sync"

	"go.uber.org/zap"
)

var (
	calcMap   map[string]*model.CalcData
	calcMutex sync.Mutex

	GlobalSpentUtxoDataMap map[string]*model.CalcData
	GlobalNewUtxoDataMap   map[string]*model.CalcData
)

func init() {
	calcMap = make(map[string]*model.CalcData, 0)
	GlobalNewUtxoDataMap = make(map[string]*model.CalcData, 0)
	GlobalSpentUtxoDataMap = make(map[string]*model.CalcData, 0)
	// loadUtxoFromGobFile()
}

func CleanUtxoMap() {
	GlobalNewUtxoDataMap = nil
	runtime.GC()
}

func ParseEndDumpUtxo(log *zap.Logger) {
	for keyStr, data := range GlobalNewUtxoDataMap {
		key := []byte(keyStr)

		log.Info("utxo",
			zap.Uint32("h", data.BlockHeight),
			zap.String("tx", utils.HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.Int("n", len(data.ScriptType)),
		)
	}
}

func ParseEndDumpScriptType(log *zap.Logger) {
	for keyStr, data := range calcMap {
		key := []byte(keyStr)

		log.Info("script type",
			zap.String("s", hex.EncodeToString(key)),
			zap.Int("n", len(keyStr)),
			zap.Uint64("num", data.Value),
		)
	}
}
