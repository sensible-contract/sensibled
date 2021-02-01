package utils

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Log    *zap.Logger
	LogErr *zap.Logger
)

func init() {
	// logger, _ = zap.NewProduction()
	Log, _ = zap.Config{
		Encoding:    "console",                               // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel), // 输出级别
		OutputPaths: []string{"/data/output.log"},            // 输出目的地
	}.Build()

	LogErr, _ = zap.Config{
		Encoding:    "console",                                // 配置编码方式（json 或 console）
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel), // 输出级别
		OutputPaths: []string{"stderr"},                       // 输出目的地
	}.Build()

}
