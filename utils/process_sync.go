package utils

import (
	"blkparser/loader/clickhouse"
	"log"
	"strconv"
)

var (
	createAllSQLs = []string{
		"DROP TABLE IF EXISTS blk_height",
		`\
CREATE TABLE IF NOT EXISTS blk_height (
	height       UInt32,
	blkid        FixedString(32),
	previd       FixedString(32),
	merkle       FixedString(32),
	ntx          UInt64,
	blocktime    UInt32,
	bits         UInt32,
	blocksize    UInt32
) engine=MergeTree()
ORDER BY height
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		"DROP TABLE IF EXISTS blk",
		`\
CREATE TABLE IF NOT EXISTS blk (
	height       UInt32,
	blkid        FixedString(32),
	previd       FixedString(32),
	merkle       FixedString(32),
	ntx          UInt64,
	blocktime    UInt32,
	bits         UInt32,
	blocksize    UInt32
) engine=MergeTree()
ORDER BY blkid
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// tx list
		// ================================================================
		// 区块包含的交易列表，分区内按区块高度height排序、索引。按blk height查询时可确定分区 (快)
		"DROP TABLE IF EXISTS blktx_height",
		`\
CREATE TABLE IF NOT EXISTS blktx_height (
	txid         FixedString(32),
	nin          UInt32,
	nout         UInt32,
	txsize       UInt32,
	locktime     UInt32,
	height       UInt32,
	blkid        FixedString(32),
	idx          UInt64
) engine=MergeTree()
ORDER BY height
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// 区块包含的交易列表，分区内按交易txid排序、索引。仅按txid查询时将遍历所有分区 (慢)
		// 查询需附带height。可配合tx_height表查询
		"DROP TABLE IF EXISTS tx",
		`\
CREATE TABLE IF NOT EXISTS tx (
	txid         FixedString(32),
	nin          UInt32,
	nout         UInt32,
	txsize       UInt32,
	locktime     UInt32,
	height       UInt32,
	blkid        FixedString(32),
	idx          UInt64
) engine=MergeTree()
ORDER BY txid
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// txout
		// ================================================================
		// 交易输出列表，分区内按交易txid+idx排序、索引，单条记录包括输出的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height，可配合tx_height表查询
		"DROP TABLE IF EXISTS txout",
		`\
CREATE TABLE IF NOT EXISTS txout (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	height       UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,
		// load

		// txin
		// ================================================================
		// 交易输入列表，分区内按交易txid+idx排序、索引，单条记录包括输入的细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height。可配合tx_height表查询
		"DROP TABLE IF EXISTS txin",
		`\
CREATE TABLE IF NOT EXISTS txin (
	txid         FixedString(32),
	idx          UInt32,
	utxid        FixedString(32),
	vout         UInt32,
	script_sig   String,
	nsequence    UInt32,
	height       UInt32         
) engine=MergeTree()
ORDER BY (txid, idx)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// 交易输入的outpoint列表，分区内按outpoint txid+idx排序、索引。用于查询某txo被哪个tx花费，需遍历所有分区（慢）
		// 查询需附带height，需配合txout_spent_height表查询
		"DROP TABLE IF EXISTS txin_spent",
		`\
CREATE TABLE IF NOT EXISTS txin_spent (
	height       UInt32,
	txid         FixedString(32),
	idx          UInt32,
	utxid        FixedString(32),
	vout         UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// 交易输入列表，分区内按交易txid+idx排序、索引，单条记录包括输入的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height。可配合tx_height表查询
		"DROP TABLE IF EXISTS txin_full",
		`\
CREATE TABLE IF NOT EXISTS txin_full (
	height       UInt32,         
	txid         FixedString(32),
	idx          UInt32,
	script_sig   String,
	nsequence    UInt32,

	height_txo   UInt32,         
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String,
	satoshi      UInt64,
	script_type  String,
	script_pk    String
) engine=MergeTree()
ORDER BY (txid, idx)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// tx在哪个高度被打包，按txid首字节分区，分区内按交易txid排序、索引。按txid查询时可确定分区（快）
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从txid确定所在区块height。配合其他表查询
		"DROP TABLE IF EXISTS tx_height",
		`\
CREATE TABLE IF NOT EXISTS tx_height (
	txid         FixedString(32),
	height       UInt32
) engine=MergeTree()
ORDER BY txid
PARTITION BY substring(txid, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// txout在哪个高度被花费，按txid首字节分区，分区内按交易txid+idx排序、索引。按txid+idx查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从txid+idx确定花费所在区块height。配合其他表查询
		"DROP TABLE IF EXISTS txout_spent_height",
		`\
CREATE TABLE IF NOT EXISTS txout_spent_height (
	height       UInt32,
	utxid        FixedString(32),
	vout         UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY substring(utxid, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// address在哪些高度的tx中出现，按address首字节分区，分区内按address+genesis+height排序，按address索引。按address查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从address确定所在区块height。配合txin_full源表查询
		"DROP TABLE IF EXISTS txin_address_height",
		`\
CREATE TABLE IF NOT EXISTS txin_address_height (
	height       UInt32,
	txid         FixedString(32),
	idx          UInt32,
	address      String,
	genesis      String
) engine=MergeTree()
PRIMARY KEY address
ORDER BY (address, genesis, height)
PARTITION BY substring(address, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// genesis在哪些高度的tx中出现，按genesis首字节分区，分区内按genesis+address+height排序，按genesis索引。按genesis查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从genesis确定所在区块height。配合txin_full源表查询
		"DROP TABLE IF EXISTS txin_genesis_height",
		`\
CREATE TABLE IF NOT EXISTS txin_genesis_height (
	height       UInt32,
	txid         FixedString(32),
	idx          UInt32,
	address      String,
	genesis      String
) engine=MergeTree()
PRIMARY KEY genesis
ORDER BY (genesis, address, height)
PARTITION BY substring(genesis, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// address在哪些高度的tx中出现，按address首字节分区，分区内按address+genesis+height排序，按address索引。按address查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从address确定所在区块height。配合txout源表查询
		"DROP TABLE IF EXISTS txout_address_height",
		`\
CREATE TABLE IF NOT EXISTS txout_address_height (
	height       UInt32,
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String
) engine=MergeTree()
PRIMARY KEY address
ORDER BY (address, genesis, height)
PARTITION BY substring(address, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// genesis在哪些高度的tx中出现，按genesis首字节分区，分区内按genesis+address+height排序，按genesis索引。按genesis查询时可确定分区 (快)
		// 此数据表不能保证和最长链一致，而是包括所有已打包tx的height信息，其中可能存在已被孤立的块高度
		// 主要用于从genesis确定所在区块height。配合txout源表查询
		"DROP TABLE IF EXISTS txout_genesis_height",
		`\
CREATE TABLE IF NOT EXISTS txout_genesis_height (
	height       UInt32,
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String
) engine=MergeTree()
PRIMARY KEY genesis
ORDER BY (genesis, address, height)
PARTITION BY substring(genesis, 1, 1)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// sign mergeTree
		"DROP TABLE IF EXISTS utxo",
		`\
CREATE TABLE IF NOT EXISTS utxo (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	height       UInt32,
    sign         Int8
) engine=CollapsingMergeTree(sign)
ORDER BY (utxid, vout)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// utxo address
		"DROP TABLE IF EXISTS utxo_address",
		`\
CREATE TABLE IF NOT EXISTS utxo_address (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	height       UInt32,
    sign         Int8
) engine=CollapsingMergeTree(sign)
PRIMARY KEY address
ORDER BY (address, genesis, height)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// utxo genesis
		"DROP TABLE IF EXISTS utxo_genesis",
		`\
CREATE TABLE IF NOT EXISTS utxo_genesis (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	genesis      String,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	height       UInt32,
    sign         Int8
) engine=CollapsingMergeTree(sign)
PRIMARY KEY genesis
ORDER BY (genesis, address, height)
SETTINGS storage_policy = 'prefer_nvme_policy'`,
	}

	processAllSQLs = []string{
		// 生成区块id索引
		"INSERT INTO blk SELECT * FROM blk_height",

		// 生成区块内tx索引
		"INSERT INTO tx SELECT * FROM blktx_height",
		// 生成tx到区块高度索引
		"INSERT INTO tx_height SELECT txid, height FROM tx",

		// 生成txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, utxid, vout FROM txin",
		// 生成txo被花费的tx区块高度索引
		"INSERT INTO txout_spent_height SELECT height, utxid, vout FROM txin_spent",

		// 生成地址参与的输出索引
		"INSERT INTO txout_address_height SELECT height, utxid, vout, address, genesis FROM txout",
		// 生成溯源ID参与的输出索引
		"INSERT INTO txout_genesis_height SELECT height, utxid, vout, address, genesis FROM txout",

		// 生成地址参与输入的相关tx区块高度索引
		"INSERT INTO txin_address_height SELECT height, txid, idx, address, genesis FROM txin_full",
		// 生成溯源ID参与输入的相关tx区块高度索引
		"INSERT INTO txin_genesis_height SELECT height, txid, idx, address, genesis FROM txin_full",

		// 全量生成utxo
		`INSERT INTO utxo
  SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height, 1 FROM txout
  ANTI LEFT JOIN txin_spent
  USING (utxid, vout)
  WHERE txout.satoshi > 0 AND
        NOT startsWith(script_type, char(0x6a)) AND
        NOT startsWith(script_type, char(0x00, 0x6a))`,

		// 生成地址相关的utxo索引
		"INSERT INTO utxo_address SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height, 1 FROM utxo",
		// 生成溯源ID相关的utxo索引
		"INSERT INTO utxo_genesis SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height, 1 FROM utxo",
	}

	removeOrphanPartSQLs = []string{
		// ================ 如果没有孤块，则无需处理
		"ALTER TABLE blk_height DELETE WHERE height > ",
		"ALTER TABLE blk DELETE WHERE height > ",

		"ALTER TABLE blktx_height DELETE WHERE height > ",
		"ALTER TABLE tx DELETE WHERE height > ",

		"ALTER TABLE txin DELETE WHERE height > ",
		"ALTER TABLE txin_spent DELETE WHERE height > ",

		// 回滚已被花费的utxo_address
		`INSERT INTO utxo
  SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height_txo, 1 FROM txin_full
  WHERE satoshi > 0 AND
      height > `,
		// 删除新添加的utxo_address˜
		`INSERT INTO utxo_address
  SELECT utxid, vout,'', '', 0, '', '', 0, -1 FROM txout
  WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a)) AND
      height > `,

		// 回滚已被花费的utxo_genesis
		`INSERT INTO utxo_genesis
  SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height_txo, 1 FROM txin_full
  WHERE satoshi > 0 AND
      height > `,
		// 删除新添加的utxo_genesis
		`INSERT INTO utxo_genesis
  SELECT utxid, vout,'', '', 0, '', '', 0, -1 FROM txout
  WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a)) AND
      height > `,

		"ALTER TABLE txin_full DELETE WHERE height > ",
		"ALTER TABLE txout DELETE WHERE height > ",
	}

	createPartSQLs = []string{
		"DROP TABLE IF EXISTS blk_height_new",
		"DROP TABLE IF EXISTS blktx_height_new",
		"DROP TABLE IF EXISTS txin_new",
		"DROP TABLE IF EXISTS txout_new",
		"DROP TABLE IF EXISTS txin_full_new",

		"CREATE TABLE IF NOT EXISTS blk_height_new AS blk_height",
		"CREATE TABLE IF NOT EXISTS blktx_height_new AS blktx_height",
		"CREATE TABLE IF NOT EXISTS txin_new AS txin",
		"CREATE TABLE IF NOT EXISTS txout_new AS txout",
		"CREATE TABLE IF NOT EXISTS txin_full_new AS txin_full",
	}

	//  更新现有基础数据表blk_height、blktx_height、txin、txout
	processPartSQLs = []string{
		"INSERT INTO blk_height SELECT * FROM blk_height_new;",
		"INSERT INTO blktx_height SELECT * FROM blktx_height_new;",
		"INSERT INTO txin SELECT * FROM txin_new;",
		"INSERT INTO txout SELECT * FROM txout_new;",

		//  优化blk表，以便统一按height排序查询
		"OPTIMIZE TABLE blk_height FINAL",

		//  更新区块id索引
		"INSERT INTO blk SELECT * FROM blk_height_new",

		//  更新区块内tx索引
		"INSERT INTO tx SELECT * FROM blktx_height_new",
		//  更新tx到区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO tx_height SELECT txid, height FROM blktx_height_new ORDER BY txid",

		//  更新txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, utxid, vout FROM txin_new",
		//  更新txo被花费的tx区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txout_spent_height SELECT height, utxid, vout FROM txin_new ORDER BY utxid",

		//  更新输入详情, 到新表txin_full_new
		`INSERT INTO txin_full_new
  SELECT height, txid, idx, script_sig, nsequence,
         txo.height, txo.utxid, txo.vout, txo.address, txo.genesis, txo.satoshi, txo.script_type, txo.script_pk FROM txin_new
  LEFT JOIN (
      SELECT height, utxid, vout, address, genesis, satoshi, script_type, script_pk FROM txout
      WHERE (height, utxid, vout) IN (
          SELECT height, txid, txin.vout FROM tx_height
          JOIN (
              SELECT utxid, vout FROM txin_new
          ) AS txin
          ON tx_height.txid = txin.utxid
          WHERE txid in (
              SELECT utxid FROM txin_new
          )
      )
  ) AS txo
  USING (utxid, vout)`,

		"INSERT INTO txin_full SELECT * FROM txin_full_new",

		//  更新地址参与的输出索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txout_address_height SELECT height, utxid, vout, address, genesis FROM txout_new ORDER BY address",
		//  更新溯源ID参与的输出索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txout_genesis_height SELECT height, utxid, vout, address, genesis FROM txout_new ORDER BY genesis",

		//  更新地址参与输入的相关tx区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txin_address_height SELECT height, txid, idx, address, genesis FROM txin_full_new ORDER BY address",
		//  更新溯源ID参与输入的相关tx区块高度索引，注意这里并未清除孤立区块的数据
		"INSERT INTO txin_genesis_height SELECT height, txid, idx, address, genesis FROM txin_full_new ORDER BY genesis",

		//  更新地址相关的utxo索引
		//  增量添加utxo_address
		`INSERT INTO utxo_address
  SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height, 1 FROM txout_new
  WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a))`,
		//  已花费txo标记清除
		`INSERT INTO utxo_address
  SELECT utxid, vout,'', '', 0, '', '', 0, -1 FROM txin_new`,
		//  如果一个satoshi=0的txo被花费(早期有这个现象)，就可能遗留一个sign=-1的数据，需要删除
		"ALTER TABLE utxo_address DELETE WHERE sign=-1",

		"OPTIMIZE TABLE utxo_address FINAL",

		//  更新溯源ID相关的utxo索引
		//  增量添加utxo_genesis
		`INSERT INTO utxo_genesis
  SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height, 1 FROM txout_new
  WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a))`,
		//  已花费txo标记清除
		`INSERT INTO utxo_genesis
  SELECT utxid, vout,'', '', 0, '', '', 0, -1 FROM txin_new`,
		//  如果一个satoshi=0的txo被花费(早期有这个现象)，就可能遗留一个sign=-1的数据，需要删除
		"ALTER TABLE utxo_genesis DELETE WHERE sign=-1",

		"OPTIMIZE TABLE utxo_genesis FINAL",

		"DROP TABLE IF EXISTS blk_height_new",
		"DROP TABLE IF EXISTS blktx_height_new",
		"DROP TABLE IF EXISTS txin_new",
		"DROP TABLE IF EXISTS txout_new",
		"DROP TABLE IF EXISTS txin_full_new",
	}
)

func CreateAllSyncCk() bool {
	return ProcessSyncCk(createAllSQLs)
}

func ProcessAllSyncCk() bool {
	return ProcessSyncCk(processAllSQLs)
}

func RemoveOrphanPartSyncCk(startBlockHeight int) bool {
	removeOrphanPartSQLsWithHeight := []string{}
	for _, psql := range removeOrphanPartSQLs {
		removeOrphanPartSQLsWithHeight = append(removeOrphanPartSQLsWithHeight,
			psql+strconv.Itoa(startBlockHeight),
		)
	}
	return ProcessSyncCk(removeOrphanPartSQLsWithHeight)
}

func CreatePartSyncCk() bool {
	return ProcessSyncCk(createPartSQLs)
}

func ProcessPartSyncCk() bool {
	return ProcessSyncCk(processPartSQLs)
}

func ProcessSyncCk(processSQLs []string) bool {
	for _, psql := range processSQLs {
		partLen := len(psql)
		if partLen > 64 {
			partLen = 64
		}
		log.Println("sync exec:", psql[:partLen])
		if _, err := clickhouse.CK.Exec(psql); err != nil {
			log.Println("sync exec err", psql, err.Error())
			return false
		}
	}
	return true
}
