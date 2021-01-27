package blkparser

import (
	"encoding/gob"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ProcessBlock struct {
	Height         int
	UtxoMap        map[string]CalcData
	UtxoMissingMap map[string]bool
}

type CalcData struct {
	Value       uint64
	ScriptType  string
	BlockHeight int
}

var (
	calcMap   map[string]CalcData
	calcMutex sync.Mutex

	utxoMap map[string]CalcData

	logger    *zap.Logger
	loggerErr *zap.Logger
)

func loadUtxoFromGobFile() {
	utxoFile, err := os.Open("/data/utxo.gob")
	if err != nil {
		loggerErr.Info("dump utxo",
			zap.String("log", "open utxo gob failed"),
		)
		return
	}
	utxoDec := gob.NewDecoder(utxoFile)
	loggerErr.Info("load utxo",
		zap.String("log", "loading utxo"),
	)
	if err := utxoDec.Decode(&utxoMap); err != nil {
		loggerErr.Info("load utxo",
			zap.String("log", "load utxo failed"),
		)
	}
}

func init() {
	// logger, _ = zap.NewProduction()
	logger, _ = zap.Config{
		Encoding:    "console",                               // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel), // 输出级别
		OutputPaths: []string{"/data/output.log"},            // 输出目的地
	}.Build()

	loggerErr, _ = zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"stderr"},                       // 输出目的地
	}.Build()

	calcMap = make(map[string]CalcData, 0)
	utxoMap = make(map[string]CalcData, 0)

	// loadUtxoFromGobFile()
}
