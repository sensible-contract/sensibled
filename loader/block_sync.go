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
	psql := fmt.Sprintf("SELECT %s FROM blk_height WHERE ORDER BY height DESC LIMIT 1000", SQL_FIELEDS_BLOCK)

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

	// for _, block := range blocks {
	// 	blksRsp = append(blksRsp, &model.BlockInfoResp{
	// 		Height:         int(block.Height),
	// 		BlockIdHex:     utils.HashString(block.BlockId),
	// 		PrevBlockIdHex: utils.HashString(block.PrevBlockId),
	// 		MerkleRootHex:  utils.HashString(block.MerkleRoot),
	// 		TxCount:        int(block.TxCount),
	// 		BlockTime:      int(block.BlockTime),
	// 		Bits:           int(block.Bits),
	// 		BlockSize:      int(block.BlockSize),
	// 	})
	// }
	// return
}
