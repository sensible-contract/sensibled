package serial

import (
	"blkparser/utils"
	"encoding/binary"
	"encoding/hex"

	"go.uber.org/zap"
)

func parseEndDumpUtxo(log *zap.Logger) {
	for keyStr, data := range utxoMap {
		key := []byte(keyStr)

		log.Info("utxo",
			zap.Int("h", data.BlockHeight),
			zap.String("tx", utils.HashString(key[:32])),
			zap.Uint32("i", binary.LittleEndian.Uint32(key[32:])),
			zap.Uint64("v", data.Value),
			zap.String("type", data.ScriptType),
			zap.Int("n", len(data.ScriptType)),
		)
	}
}

func parseEndDumpScriptType(log *zap.Logger) {
	for keyStr, data := range calcMap {
		key := []byte(keyStr)

		log.Info("script type",
			zap.String("s", hex.EncodeToString(key)),
			zap.Int("n", len(keyStr)),
			zap.Uint64("num", data.Value),
		)
	}
}
