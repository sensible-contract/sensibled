package blkparser

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type CalcData struct {
	Value      uint64
	ScriptType string
}

var (
	calcMap   map[string]*CalcData
	calcMutex sync.Mutex

	utxoMissingMap sync.Map
	utxoMap        sync.Map

	logger    *zap.Logger
	loggerErr *zap.Logger
)

func init() {
	calcMap = make(map[string]*CalcData, 0)

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
}
