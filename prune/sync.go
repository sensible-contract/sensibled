package prune

import (
	"fmt"

	"github.com/spf13/viper"
)

var (
	IsTxrawPrune     bool
	IsPkScriptPrune  bool
	IsScriptSigPrune bool
	IsOpReturnPrune  bool
	IsHistoryPrune   bool
)

func Init() {
	viper.SetConfigFile("conf/prune.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	// 清理非sensible的txraw
	IsTxrawPrune = viper.GetBool("txraw")

	// 清理非sensible的锁定脚本
	// 清理无地址的锁定脚本，若要保留，可以设置为20位空地址
	IsPkScriptPrune = viper.GetBool("pkscript")

	// 清理所有的解锁脚本
	IsScriptSigPrune = viper.GetBool("scriptsig")

	// 清理所有非sensible交易的false opreturn输出
	IsOpReturnPrune = viper.GetBool("opreturn")

	IsHistoryPrune = viper.GetBool("history")
}
