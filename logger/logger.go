package logger

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Log    *zap.Logger
	LogErr *zap.Logger
)

func init() {
	viper.SetConfigFile("conf/log.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	logFile := viper.GetString("logFile")

	zap.RegisterEncoder("row-binary", constructRowBinaryEncoder)
	zap.RegisterEncoder("row-binary-debug", constructRowBinaryEncoderDebug)

	Log, _ = zap.Config{
		Encoding:          "console",
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{logFile},
	}.Build()

	LogErr, _ = zap.Config{
		Encoding:          "console",
		Level:             zap.NewAtomicLevelAt(zapcore.DebugLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{"stderr"},
	}.Build()
}

func SyncLog() {
	Log.Sync()
	LogErr.Sync()
}
