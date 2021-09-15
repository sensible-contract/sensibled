package loader

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sensibled/loader/clickhouse"
	"sensibled/logger"
	"sensibled/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
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

func utxoResultSRF(rows *sql.Rows) (interface{}, error) {
	var ret model.TxoData
	err := rows.Scan(&ret.UTxid, &ret.Vout, &ret.Satoshi, &ret.ScriptType, &ret.PkScript, &ret.BlockHeight, &ret.TxIdx)
	if err != nil {
		return nil, err
	}

	ret.Data = scriptDecoder.ExtractPkScriptForTxo(ret.PkScript, ret.ScriptType)
	return &ret, nil
}

func GetSpentUTXOAfterBlockHeight(height int) (utxosMapRsp map[string]*model.TxoData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, satoshi, script_type, script_pk, height_txo, utxidx FROM txin
   WHERE satoshi > 0 AND
      height >= %d AND
      height < %d`, height, model.MEMPOOL_HEIGHT)
	return getUtxoBySql(psql)
}

func GetNewUTXOAfterBlockHeight(height int) (utxosMapRsp map[string]*model.TxoData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, satoshi, script_type, script_pk, 0, 0 FROM txout
   WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a)) AND
      height >= %d AND
      height < %d`, height, model.MEMPOOL_HEIGHT)
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
	var ret []byte
	err := rows.Scan(&ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func GetRawTxByIdFromMempool(txidHex string) (txRsp []byte, err error) {
	psql := fmt.Sprintf(`
SELECT rawtx FROM blktx_height
WHERE height = 4294967295 AND txid = reverse(unhex('%s'))
LIMIT 1`, txidHex)

	txRet, err := clickhouse.ScanOne(psql, rawtxResultSRF)
	if err != nil {
		logger.Log.Info("query tx failed", zap.Error(err))
		return nil, err
	}
	if txRet == nil {
		return nil, errors.New("not exist")
	}
	txRsp = txRet.([]byte)

	return txRsp, nil
}
