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
	calcMap   map[string]model.CalcData
	calcMutex sync.Mutex

	utxoMap map[string]model.CalcData
)

func init() {
	calcMap = make(map[string]model.CalcData, 0)
	utxoMap = make(map[string]model.CalcData, 0)

	// loadUtxoFromGobFile()
}

func CleanUtxoMap() {
	utxoMap = nil
	runtime.GC()
}

func ParseEndDumpUtxo(log *zap.Logger) {
	for keyStr, data := range utxoMap {
		key := []byte(keyStr)

		log.Info("utxo",
			zap.Int("h", data.BlockHeight),
			zap.String("tx", utils.HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.Object("type", data.ScriptType),
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
