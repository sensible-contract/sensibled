package loader

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"unisatd/loader/clickhouse"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"
	"unisatd/utils"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func blockResultSRF(rows *sql.Rows) (interface{}, error) {
	var ret model.BlockDO
	err := rows.Scan(&ret.Height, &ret.BlockId)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func GetLatestBlockFromDB() (blkRsp *model.BlockDO, err error) {
	psql := "SELECT height, blkid FROM blk_height ORDER BY height DESC LIMIT 1"

	blkRet, err := clickhouse.ScanOne(psql, blockResultSRF)
	if err != nil {
		logger.Log.Info("query blk failed", zap.Error(err))
		return nil, err
	}
	if blkRet == nil {
		return nil, errors.New("not exist")
	}
	blkRsp = blkRet.(*model.BlockDO)
	return blkRsp, nil
}

func GetBestBlockHeightFromRedis() (height int, err error) {
	// get decimal from f info
	ctx := context.Background()
	height, err = rdb.RdbBalanceClient.HGet(ctx, "info", "blocks_total").Int()
	if err == redis.Nil {
		height = 0
		logger.Log.Info("GetBestBlockHeightFromRedis, but info missing")
	} else if err != nil {
		logger.Log.Info("GetBestBlockHeightFromRedis, but redis failed", zap.Error(err))
		return
	}

	return height, nil
}

func utxoResultSRF(rows *sql.Rows) (interface{}, error) {
	var nftPointsBuf []byte
	var ret model.TxoData
	err := rows.Scan(&ret.UTxid, &ret.Vout, &ret.Satoshi, &ret.ScriptType, &ret.PkScript, &nftPointsBuf, &ret.BlockHeight, &ret.TxIdx)
	if err != nil {
		return nil, err
	}

	ret.AddressData = scriptDecoder.ExtractPkScriptForTxo(ret.PkScript, ret.ScriptType)
	ret.LoadNFTCreatePointsFromRaw(nftPointsBuf)
	return &ret, nil
}

// 已花费的utxo需要回滚, 除了coinbase之外
func GetSpentUTXOAfterBlockHeight(start, end int) (utxosMapRsp map[string]*model.TxoData, err error) {
	if end == 0 {
		end = model.MEMPOOL_HEIGHT
	}

	psql := fmt.Sprintf(`
SELECT utxid, vout, satoshi, script_type, script_pk, nftpoints, height_txo, utxidx FROM txin
   WHERE satoshi > 0 AND
      txidx > 0 AND
      height >= %d AND
      height < %d`, start, end)
	return getUtxoBySql(psql)
}

// 新产生的utxo需要删除
func GetNewUTXOAfterBlockHeight(start, end int) (utxosMapRsp map[string]*model.TxoData, err error) {
	if end == 0 {
		end = model.MEMPOOL_HEIGHT
	}
	psql := fmt.Sprintf(`
SELECT utxid, vout, satoshi, script_type, script_pk, nftpoints, 0, 0 FROM txout
   WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x00, 0x6a)) AND
      NOT startsWith(script_type, char(0x6a)) AND
      height >= %d AND
      height < %d`, start, end)
	return getUtxoBySql(psql)
}

func getUtxoBySql(psql string) (utxosMapRsp map[string]*model.TxoData, err error) {
	utxosRet, err := clickhouse.ScanAll(psql, utxoResultSRF)
	if err != nil {
		logger.Log.Info("query blk failed", zap.Error(err))
		return nil, err
	}
	if utxosRet == nil {
		return nil, nil
	}
	utxosRsp := utxosRet.([]*model.TxoData)

	utxosMapRsp = make(map[string]*model.TxoData, len(utxosRsp))
	for _, data := range utxosRsp {
		key := make([]byte, 36)
		copy(key, data.UTxid)
		binary.LittleEndian.PutUint32(key[32:], data.Vout) // 4
		utxosMapRsp[string(key)] = data
	}

	return utxosMapRsp, nil
}

////////////////////////////////////////////////////////////////

func rawtxResultSRF(rows *sql.Rows) (interface{}, error) {
	var ret model.TxData
	err := rows.Scan(&ret.TxId, &ret.Raw)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func GetAllMempoolRawTx(txs map[string]*model.TxData) (err error) {
	psql := fmt.Sprintf("SELECT txid, rawtx FROM blktx_height WHERE height = %d", model.MEMPOOL_HEIGHT)

	txsRet, err := clickhouse.ScanAll(psql, rawtxResultSRF)
	if err != nil {
		logger.Log.Info("query tx failed", zap.Error(err))
		return err
	}
	if txsRet == nil {
		return errors.New("not exist")
	}

	txsRsp := txsRet.([]*model.TxData)
	for _, tx := range txsRsp {
		txs[utils.HashString(tx.TxId)] = tx
	}

	return nil
}
