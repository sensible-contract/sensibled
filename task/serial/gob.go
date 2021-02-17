package serial

import (
	"blkparser/model"
	"blkparser/utils"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"os"
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

func loadUtxoFromGobFile() {
	utxoFile, err := os.Open("/data/utxo.gob")
	if err != nil {
		utils.LogErr.Info("dump utxo",
			zap.String("log", "open utxo gob failed"),
		)
		return
	}
	utxoDec := gob.NewDecoder(utxoFile)
	utils.LogErr.Info("load utxo",
		zap.String("log", "loading utxo"),
	)
	if err := utxoDec.Decode(&utxoMap); err != nil {
		utils.LogErr.Info("load utxo",
			zap.String("log", "load utxo failed"),
		)
	}
}

func dumpUtxoToGobFile() {
	utxoFile, err := os.OpenFile("/data/utxo.gob", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		utils.LogErr.Info("dump utxo",
			zap.String("log", "dump utxo file failed"),
		)
		return
	}
	defer utxoFile.Close()

	enc := gob.NewEncoder(utxoFile)
	if err := enc.Encode(utxoMap); err != nil {
		utils.LogErr.Info("dump utxo",
			zap.String("log", "dump utxo failed"),
		)
	}
	utils.LogErr.Info("dump utxo",
		zap.String("log", "dump utxo ok"),
	)
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
