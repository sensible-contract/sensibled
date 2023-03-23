package store

import (
	"unisatd/loader/clickhouse"
	"unisatd/logger"

	"go.uber.org/zap"
)

var (
	processAllSQLs = []string{
		// 删除mempool数据
		// (2**22-1)/2100 = 1997
		"ALTER TABLE blktx_height DROP PARTITION '1997'",
		"ALTER TABLE blknft_height DROP PARTITION '1997'",
		"ALTER TABLE blkbrc20_height DROP PARTITION '1997'",
		"ALTER TABLE txin_spent DROP PARTITION '1997'",
		"ALTER TABLE txin DROP PARTITION '1997'",
		"ALTER TABLE txout DROP PARTITION '1997'",
	}

	createPartSQLs = []string{
		"DROP TABLE IF EXISTS blktx_height_mempool_new",
		"DROP TABLE IF EXISTS blknft_height_mempool_new",
		"DROP TABLE IF EXISTS blkbrc20_height_mempool_new",
		"DROP TABLE IF EXISTS txout_mempool_new",
		"DROP TABLE IF EXISTS txin_mempool_new",

		"CREATE TABLE IF NOT EXISTS blktx_height_mempool_new AS blktx_height",
		"CREATE TABLE IF NOT EXISTS blknft_height_mempool_new AS blknft_height",
		"CREATE TABLE IF NOT EXISTS blkbrc20_height_mempool_new AS blkbrc20_height",
		"CREATE TABLE IF NOT EXISTS txout_mempool_new AS txout",
		"CREATE TABLE IF NOT EXISTS txin_mempool_new AS txin",
	}

	// 更新现有基础数据表blktx_height、blknft_height, blkbrc20_height, txin、txout
	processPartSQLsForTxIn = []string{
		"INSERT INTO txin SELECT * FROM txin_mempool_new",
		// 更新txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, substring(utxid, 1, 12), vout FROM txin_mempool_new",

		"DROP TABLE IF EXISTS txin_mempool_new",
	}
	processPartSQLsForTxOut = []string{
		"INSERT INTO txout SELECT * FROM txout_mempool_new;",

		"DROP TABLE IF EXISTS txout_mempool_new",
	}

	processPartSQLs = []string{
		"INSERT INTO blktx_height SELECT * FROM blktx_height_mempool_new;",
		"INSERT INTO blknft_height SELECT * FROM blknft_height_mempool_new;",
		"INSERT INTO blkbrc20_height SELECT * FROM blkbrc20_height_mempool_new;",

		"DROP TABLE IF EXISTS blktx_height_mempool_new",
		"DROP TABLE IF EXISTS blknft_height_mempool_new",
		"DROP TABLE IF EXISTS blkbrc20_height_mempool_new",
	}
)

func ProcessAllSyncCk() bool {
	logger.Log.Info("sync mempool sql: all")
	return ProcessSyncCk(processAllSQLs)
}

func CreatePartSyncCk() bool {
	logger.Log.Info("create mempool sql: part")
	return ProcessSyncCk(createPartSQLs)
}

func ProcessPartSyncCk() bool {
	logger.Log.Info("sync mempool sql: part")
	if !ProcessSyncCk(processPartSQLs) {
		return false
	}
	if !ProcessSyncCk(processPartSQLsForTxIn) {
		return false
	}
	return ProcessSyncCk(processPartSQLsForTxOut)
}

func ProcessSyncCk(processSQLs []string) bool {
	for _, psql := range processSQLs {
		partLen := len(psql)
		if partLen > 96 {
			partLen = 96
		}
		// logger.Log.Info("sync exec: " + psql[:partLen])
		if _, err := clickhouse.CK.Exec(psql); err != nil {
			logger.Log.Info("sync exec err",
				zap.String("sql", psql[:partLen]), zap.Error(err))
			return false
		}
	}
	return true
}
