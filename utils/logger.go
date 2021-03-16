package utils

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Log    *zap.Logger
	LogErr *zap.Logger

	LogBlk      *zap.Logger
	LogTx       *zap.Logger
	LogTxIn     *zap.Logger
	LogTxInFull *zap.Logger
	LogTxOut    *zap.Logger
	DEBUG       = true
)

func init() {
	viper.SetConfigFile("conf/dump.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	dumpEncoding := viper.GetString("encoding")
	logFile := viper.GetString("logFile")
	pathPrefix := viper.GetString("pathPrefix")
	pathSurfix := viper.GetString("pathSurfix")

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

	LogBlk, _ = zap.Config{
		Encoding:          dumpEncoding,
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{pathPrefix + "/blk" + pathSurfix},
	}.Build()

	LogTx, _ = zap.Config{
		Encoding:          dumpEncoding,
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{pathPrefix + "/tx" + pathSurfix},
	}.Build()

	LogTxIn, _ = zap.Config{
		Encoding:          dumpEncoding,
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{pathPrefix + "/txin" + pathSurfix},
	}.Build()

	LogTxOut, _ = zap.Config{
		Encoding:          dumpEncoding,
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		OutputPaths:       []string{pathPrefix + "/txout" + pathSurfix},
	}.Build()

}

func SyncLog() {
	Log.Sync()
	LogErr.Sync()
	LogBlk.Sync()
	LogTx.Sync()
	LogTxIn.Sync()
	LogTxOut.Sync()
}
