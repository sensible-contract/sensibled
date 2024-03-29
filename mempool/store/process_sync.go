package store

import (
	"sensibled/loader/clickhouse"
	"sensibled/logger"

	"go.uber.org/zap"
)

var (
	processAllSQLs = []string{
		// 删除mempool数据
		"ALTER TABLE blktx_contract_height DROP PARTITION '2045222'",
		"ALTER TABLE blktx_height DROP PARTITION '2045222'",
		"ALTER TABLE txin_spent DROP PARTITION '2045222'",
		"ALTER TABLE txin DROP PARTITION '2045222'",
		"ALTER TABLE txout DROP PARTITION '2045222'",
	}

	createPartSQLs = []string{
		"DROP TABLE IF EXISTS blktx_contract_height_mempool_new",
		"DROP TABLE IF EXISTS blktx_height_mempool_new",
		"DROP TABLE IF EXISTS txout_mempool_new",
		"DROP TABLE IF EXISTS txin_mempool_new",

		"CREATE TABLE IF NOT EXISTS blktx_contract_height_mempool_new AS blktx_contract_height",
		"CREATE TABLE IF NOT EXISTS blktx_height_mempool_new AS blktx_height",
		"CREATE TABLE IF NOT EXISTS txout_mempool_new AS txout",
		"CREATE TABLE IF NOT EXISTS txin_mempool_new AS txin",
	}

	// 更新现有基础数据表blktx_contract_height、blktx_height、txin、txout
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
		"INSERT INTO blktx_contract_height SELECT * FROM blktx_contract_height_mempool_new;",
		"INSERT INTO blktx_height SELECT * FROM blktx_height_mempool_new;",

		"DROP TABLE IF EXISTS blktx_contract_height_mempool_new",
		"DROP TABLE IF EXISTS blktx_height_mempool_new",
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
