package store

import (
	"sensibled/loader/clickhouse"
	"sensibled/logger"
	"strconv"

	"go.uber.org/zap"
)

var (
	createAllSQLs = []string{
		// block list
		// ================================================================
		"DROP TABLE IF EXISTS blk_height",
		`
CREATE TABLE IF NOT EXISTS blk_height (
	height       UInt32,
	blkid        FixedString(32),
	previd       FixedString(32),
	merkle       FixedString(32),
	ntx          UInt64,
	nftnew       UInt64,        -- without coinbase, 新创建nft个数, 包括无效重复创建
	nftin        UInt64,        -- without coinbase, 输入nft个数
	nftout       UInt64,        -- without coinbase, 输出nft个数, 创建nft个数 = nftlost+out-in，无效创建不计数
	nftlost      UInt64,        -- 在普通交易中丢弃，在coinbase中收集的的nft个数
	invalue      UInt64,        -- without coinbase
	outvalue     UInt64,        -- without coinbase
	coinbase_out UInt64,
	blocktime    UInt32,
	bits         UInt32,
	blocksize    UInt32
) engine=MergeTree()
ORDER BY height
PARTITION BY intDiv(height, 2100)
`,

		"DROP TABLE IF EXISTS blk",
		`
CREATE TABLE IF NOT EXISTS blk (
	height       UInt32,
	blkid        FixedString(32),
	previd       FixedString(32),
	merkle       FixedString(32),
	ntx          UInt64,
	nftnew       UInt64,        -- without coinbase, 新创建nft个数, 包括无效创建
	nftin        UInt64,        -- without coinbase, 输入nft个数
	nftout       UInt64,        -- without coinbase, 输出nft个数, 创建nft个数 = nftlost+out-in，无效创建不计数
	nftlost      UInt64,        -- 在普通交易中丢弃，在coinbase中收集的的nft个数
	invalue      UInt64,        -- without coinbase
	outvalue     UInt64,        -- without coinbase
	coinbase_out UInt64,
	blocktime    UInt32,
	bits         UInt32,
	blocksize    UInt32
) engine=MergeTree()
ORDER BY blkid
PARTITION BY intDiv(height, 2100)
`,

		// tx list
		// ================================================================
		// 区块包含的交易列表，分区内按区块高度height排序、索引。按blk height查询时可确定分区 (快)
		"DROP TABLE IF EXISTS blktx_height",
		`
CREATE TABLE IF NOT EXISTS blktx_height (
	txid         FixedString(32),
	nin          UInt32,
	nout         UInt32,
	txsize       UInt32,
	witoffset    UInt32,
	locktime     UInt32,
	nftnew       UInt64,        -- 新创建nft个数, 包括无效创建
	nftin        UInt64,        -- 输入nft个数
	nftout       UInt64,        -- 输出nft个数, 创建nft个数 = out-in，无效创建不计数
	nftlost      UInt64,        -- 随Fee丢失的nft个数，如果是在coinbase中丢弃，则真正丢失
	invalue      UInt64,
	outvalue     UInt64,
	rawtx        String,
	height       UInt32,
	txidx        UInt64
) engine=MergeTree()
ORDER BY (height, txid)
PARTITION BY intDiv(height, 2100)
`,

		// txout
		// ================================================================
		// 交易输出列表，分区内按交易txid+idx排序、索引，单条记录包括输出的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height，可配合tx_height表查询
		"DROP TABLE IF EXISTS txout",
		`
CREATE TABLE IF NOT EXISTS txout (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	code_type    UInt32,      -- 0: none, 1: ft, 2: unique, 3: nft
	data_value   UInt64,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	nftout       UInt64,      -- utxo输出nft个数, 创建nft个数 = out-in，无效创建不计数
	nftpoints    String,      -- 序列化后的所有输出nftpoints列表
	height       UInt32,
	utxidx       UInt64
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
`,

		// txin
		// ================================================================
		// 交易输入的outpoint列表，分区内按outpoint txid+idx排序、索引。用于查询某txo被哪个tx花费，需遍历所有分区（慢）
		// 查询需附带height，需配合txout_spent_height表查询
		"DROP TABLE IF EXISTS txin_spent",
		`
CREATE TABLE IF NOT EXISTS txin_spent (
	height       UInt32,
	txid         FixedString(32),
	idx          UInt32,
	utxid        FixedString(12),
	vout         UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
`,

		// 交易输入列表，分区内按交易txid+idx排序、索引，单条记录包括输入的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height。可配合tx_height表查询
		"DROP TABLE IF EXISTS txin",
		`
CREATE TABLE IF NOT EXISTS txin (
	height       UInt32,
	txidx        UInt64,
	txid         FixedString(32),
	idx          UInt32,
	script_sig   String,
	script_wits  String,
	nsequence    UInt32,
	nftnew       UInt64,        -- 新创建nft个数, 包括无效创建

	height_txo   UInt32,
	utxidx       UInt64,
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	code_type    UInt32,      -- 0: none, 1: ft, 2: unique, 3: nft
	data_value   UInt64,
	satoshi      UInt64,
	script_type  String,
	script_pk    String
	nftin        UInt64,      -- 输入utxo包含的nft个数, 创建nft个数 = out-in，无效创建不计数
	nftpoints    String,      -- 序列化后的所有输入nftpoints列表

) engine=MergeTree()
ORDER BY (txid, idx)
PARTITION BY intDiv(height, 2100)
`,

		// ================================================================
		// tx在哪个高度被打包，按txid首字节分区，分区内按交易txid排序、索引。按txid查询时可确定分区（快）
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从txid确定所在区块height。配合其他表查询
		// todo: ADD COLUMN txidx        UInt64,         // new

		"DROP TABLE IF EXISTS tx_height",
		`
CREATE TABLE IF NOT EXISTS tx_height (
	txid         FixedString(12),
	height       UInt32
) engine=MergeTree()
ORDER BY txid
PARTITION BY substring(txid, 1, 1)
`,

		// txout在哪个高度被花费，按txid首字节分区，分区内按交易txid+idx排序、索引。按txid+idx查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从txid+idx确定花费所在区块height。配合其他表查询
		"DROP TABLE IF EXISTS txout_spent_height",
		`
CREATE TABLE IF NOT EXISTS txout_spent_height (
	height       UInt32,
	utxid        FixedString(12),
	vout         UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY substring(utxid, 1, 1)
`,
	}

	processAllSQLs = []string{
		// 生成区块id索引
		"INSERT INTO blk SELECT * FROM blk_height",

		// 生成tx到区块高度索引
		"INSERT INTO tx_height SELECT substring(txid, 1, 12), height FROM blktx_height",

		// 生成txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, substring(utxid, 1, 12), vout FROM txin",
		// 生成txo被花费的tx区块高度索引
		"INSERT INTO txout_spent_height SELECT height, utxid, vout FROM txin_spent",
	}

	removeOrphanPartSQLs = []string{
		// ================ 如果没有孤块，则无需处理
		"ALTER TABLE blk_height DELETE WHERE height >= ",
		"ALTER TABLE blk DELETE WHERE height >= ",

		"ALTER TABLE blktx_height DELETE WHERE height >= ",

		"ALTER TABLE txin_spent DELETE WHERE height >= ",

		"ALTER TABLE txin DELETE WHERE height >= ",
		"ALTER TABLE txout DELETE WHERE height >= ",
	}

	createPartSQLs = []string{
		"DROP TABLE IF EXISTS blk_height_new",
		"DROP TABLE IF EXISTS blktx_height_new",
		"DROP TABLE IF EXISTS txout_new",
		"DROP TABLE IF EXISTS txin_new",

		"CREATE TABLE IF NOT EXISTS blk_height_new AS blk_height",
		"CREATE TABLE IF NOT EXISTS blktx_height_new AS blktx_height",
		"CREATE TABLE IF NOT EXISTS txout_new AS txout",
		"CREATE TABLE IF NOT EXISTS txin_new AS txin",
	}

	// 更新现有基础数据表txin、txout
	processPartSQLsForTxIn = []string{
		"INSERT INTO txin SELECT * FROM txin_new",
		// 更新txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, substring(utxid, 1, 12), vout FROM txin_new",
		// 更新txo被花费的tx区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txout_spent_height SELECT height, substring(utxid, 1, 12), vout FROM txin_new ORDER BY utxid",

		"DROP TABLE IF EXISTS txin_new",
	}
	processPartSQLsForTxOut = []string{
		"INSERT INTO txout SELECT * FROM txout_new;",

		"DROP TABLE IF EXISTS txout_new",
	}

	processPartSQLs = []string{
		"INSERT INTO blk_height SELECT * FROM blk_height_new;",
		"INSERT INTO blktx_height SELECT * FROM blktx_height_new;",

		// 优化blk表，以便统一按height排序查询
		// "OPTIMIZE TABLE blk_height FINAL",

		// 更新区块id索引
		"INSERT INTO blk SELECT * FROM blk_height_new",

		// 更新tx到区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO tx_height SELECT substring(txid, 1, 12), height FROM blktx_height_new ORDER BY txid",

		"DROP TABLE IF EXISTS blk_height_new",
		"DROP TABLE IF EXISTS blktx_height_new",
	}
)

func CreateAllSyncCk() bool {
	logger.Log.Info("create sql: all")
	return ProcessSyncCk(createAllSQLs)
}

func ProcessAllSyncCk() bool {
	logger.Log.Info("sync sql: all")
	return ProcessSyncCk(processAllSQLs)
}

func RemoveOrphanPartSyncCk(startBlockHeight int) bool {
	logger.Log.Info("remove sql: part")
	removeOrphanPartSQLsWithHeight := []string{}
	for _, psql := range removeOrphanPartSQLs {
		removeOrphanPartSQLsWithHeight = append(removeOrphanPartSQLsWithHeight,
			psql+strconv.Itoa(startBlockHeight),
		)
	}
	return ProcessSyncCk(removeOrphanPartSQLsWithHeight)
}

func CreatePartSyncCk() bool {
	logger.Log.Info("create sql: part")
	return ProcessSyncCk(createPartSQLs)
}

func ProcessPartSyncCk() bool {
	logger.Log.Info("sync sql: part")
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
