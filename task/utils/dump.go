package utils

import (
	"blkparser/model"
	"blkparser/utils"
	"encoding/binary"
	"encoding/hex"

	"go.uber.org/zap"
)

func ParseEndDumpUtxo(log *zap.Logger, newUtxoDataMap map[string]*model.CalcData) {
	for keyStr, data := range newUtxoDataMap {
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

func ParseEndDumpScriptType(log *zap.Logger, calcMap map[string]*model.CalcData) {
	for keyStr, data := range calcMap {
		key := []byte(keyStr)

		log.Info("script type",
			zap.String("s", hex.EncodeToString(key)),
			zap.Int("n", len(keyStr)),
			zap.Uint64("num", data.Value),
		)
	}
}
