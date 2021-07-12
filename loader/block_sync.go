package loader

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"satoblock/loader/clickhouse"
	"satoblock/logger"
	"satoblock/model"

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
	var dataValue uint64
	err := rows.Scan(&ret.UTxid, &ret.Vout, &ret.AddressPkh, &ret.CodeHash, &ret.GenesisId, &ret.CodeType, &dataValue, &ret.Satoshi, &ret.ScriptType, &ret.Script, &ret.BlockHeight, &ret.TxIdx)
	if err != nil {
		return nil, err
	}
	if ret.CodeType == scriptDecoder.CodeType_NFT {
		ret.TokenIndex = dataValue
	} else if ret.CodeType == scriptDecoder.CodeType_FT {
		ret.Amount = dataValue
	}

	return &ret, nil
}

func GetSpentUTXOAfterBlockHeight(height int) (utxosMapRsp map[string]*model.TxoData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, script_type, script_pk, height_txo, utxidx FROM txin
   WHERE satoshi > 0 AND
      height >= %d AND
      height < %d`, height, model.MEMPOOL_HEIGHT)
	return getUtxoBySql(psql)
}

func GetNewUTXOAfterBlockHeight(height int) (utxosMapRsp map[string]*model.TxoData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, '', '', 0, 0 FROM txout
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
