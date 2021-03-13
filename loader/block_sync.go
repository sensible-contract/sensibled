package loader

import (
	"blkparser/loader/clickhouse"
	"blkparser/model"
	"database/sql"
	"errors"
	"fmt"
	"log"
)

const (
	SQL_FIELEDS_BLOCK = "height, blkid, previd, merkle, ntx, blocktime, bits, blocksize"
)

func blockResultSRF(rows *sql.Rows) (interface{}, error) {
	var ret model.BlockDO
	err := rows.Scan(&ret.Height, &ret.BlockId, &ret.PrevBlockId, &ret.MerkleRoot, &ret.TxCount, &ret.BlockTime, &ret.Bits, &ret.BlockSize)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func GetLatestBlocks() (blksRsp []*model.BlockDO, err error) {
	psql := fmt.Sprintf("SELECT %s FROM blk_height ORDER BY height DESC LIMIT 1000", SQL_FIELEDS_BLOCK)

	blksRet, err := clickhouse.ScanAll(psql, blockResultSRF)
	if err != nil {
		log.Printf("query blk failed: %v", err)
		return nil, err
	}
	if blksRet == nil {
		return nil, errors.New("not exist")
	}
	blksRsp = blksRet.([]*model.BlockDO)
	return blksRsp, nil
}

func utxoResultSRF(rows *sql.Rows) (interface{}, error) {
	var ret model.CalcData
	err := rows.Scan(&ret.UTxid, &ret.Vout, &ret.AddressPkh, &ret.GenesisId, &ret.Value, &ret.ScriptType, &ret.Script, &ret.BlockHeight, &ret.TxIdx)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func GetSpentUTXOAfterBlockHeight(height int) (utxosRsp []*model.CalcData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, address, genesis, satoshi, script_type, script_pk, height_txo, utxidx FROM txin_full
   WHERE satoshi > 0 AND
      height >= %d`, height)

	utxosRet, err := clickhouse.ScanAll(psql, utxoResultSRF)
	if err != nil {
		log.Printf("query blk failed: %v", err)
		return nil, err
	}
	if utxosRet == nil {
		return nil, nil
	}
	utxosRsp = utxosRet.([]*model.CalcData)
	return utxosRsp, nil
}

func GetNewUTXOAfterBlockHeight(height int) (utxosRsp []*model.CalcData, err error) {
	psql := fmt.Sprintf(`
SELECT utxid, vout, address, genesis, 0, '', '', 0, 0 FROM txout
   WHERE satoshi > 0 AND
      NOT startsWith(script_type, char(0x6a)) AND
      NOT startsWith(script_type, char(0x00, 0x6a)) AND
      height >= %d`, height)

	utxosRet, err := clickhouse.ScanAll(psql, utxoResultSRF)
	if err != nil {
		log.Printf("query blk failed: %v", err)
		return nil, err
	}
	if utxosRet == nil {
		return nil, nil
	}
	utxosRsp = utxosRet.([]*model.CalcData)
	return utxosRsp, nil
}
