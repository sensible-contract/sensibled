package serial

import (
	"blkparser/utils"
	"encoding/gob"
	"os"

	"go.uber.org/zap"
)

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
