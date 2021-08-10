package store

import (
	"database/sql"
	"fmt"
	"satoblock/loader/clickhouse"
	"satoblock/logger"

	"go.uber.org/zap"
)

var (
	SyncStmtBlk         *sql.Stmt
	SyncStmtBlkCodeHash *sql.Stmt
	SyncStmtTx          *sql.Stmt
	SyncStmtTxOut       *sql.Stmt
	SyncStmtTxIn        *sql.Stmt

	syncTxBlk         *sql.Tx
	syncTxBlkCodeHash *sql.Tx
	syncTxTx          *sql.Tx
	syncTxTxOut       *sql.Tx
	syncTxTxIn        *sql.Tx
)

const (
	sqlBlkPattern         string = "INSERT INTO %s (height, blkid, previd, merkle, ntx, invalue, outvalue, coinbase_out, blocktime, bits, blocksize) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlBlkCodeHashPattern string = "INSERT INTO %s (height, codehash, genesis, code_type, nft_idx, in_data_value, out_data_value, invalue, outvalue, blkid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxPattern          string = "INSERT INTO %s (txid, nin, nout, txsize, locktime, invalue, outvalue, rawtx, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern       string = "INSERT INTO %s (utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern        string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk(isFull bool) bool {
	sqlBlk := fmt.Sprintf(sqlBlkPattern, "blk_height_new")
	sqlBlkCodeHash := fmt.Sprintf(sqlBlkCodeHashPattern, "blk_codehash_height_new")
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_new")
	if isFull {
		sqlBlk = fmt.Sprintf(sqlBlkPattern, "blk_height")
		sqlBlkCodeHash = fmt.Sprintf(sqlBlkCodeHashPattern, "blk_codehash_height")
		sqlTx = fmt.Sprintf(sqlTxPattern, "blktx_height")
		sqlTxOut = fmt.Sprintf(sqlTxOutPattern, "txout")
		sqlTxIn = fmt.Sprintf(sqlTxInPattern, "txin")
	}
	var err error
	syncTxBlk, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-blk", zap.Error(err))
		return false
	}
	SyncStmtBlk, err = syncTxBlk.Prepare(sqlBlk)
	if err != nil {
		logger.Log.Error("sync-prepare-blk", zap.Error(err))
		return false
	}

	syncTxBlkCodeHash, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-blk-code", zap.Error(err))
		return false
	}
	SyncStmtBlkCodeHash, err = syncTxBlkCodeHash.Prepare(sqlBlkCodeHash)
	if err != nil {
		logger.Log.Error("sync-prepare-blk-code", zap.Error(err))
		return false
	}

	syncTxTx, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-tx", zap.Error(err))
		return false
	}
	SyncStmtTx, err = syncTxTx.Prepare(sqlTx)
	if err != nil {
		logger.Log.Error("sync-prepare-tx", zap.Error(err))
		return false
	}

	syncTxTxOut, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-txout", zap.Error(err))
		return false
	}
	SyncStmtTxOut, err = syncTxTxOut.Prepare(sqlTxOut)
	if err != nil {
		logger.Log.Error("sync-prepare-txout", zap.Error(err))
		return false
	}

	syncTxTxIn, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-txinfull", zap.Error(err))
		return false
	}
	SyncStmtTxIn, err = syncTxTxIn.Prepare(sqlTxIn)
	if err != nil {
		logger.Log.Error("sync-prepare-txinfull", zap.Error(err))
		return false
	}

	return true
}

func PrepareFullSyncCk() bool {
	return prepareSyncCk(true)
}

func PreparePartSyncCk() bool {
	return prepareSyncCk(false)
}

func CommitSyncCk() {
	defer SyncStmtBlk.Close()
	defer SyncStmtTx.Close()
	defer SyncStmtTxOut.Close()

	logger.Log.Info("sync-commit-blk...")
	if err := syncTxBlk.Commit(); err != nil {
		logger.Log.Error("sync-commit-blk", zap.Error(err))
	}
	logger.Log.Info("sync-commit-tx...")
	if err := syncTxTx.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx", zap.Error(err))
	}
	logger.Log.Info("sync-commit-txout...")
	if err := syncTxTxOut.Commit(); err != nil {
		logger.Log.Error("sync-commit-txout", zap.Error(err))
	}
}

func CommitFullSyncCk(needCommit bool) {
	defer SyncStmtTxIn.Close()

	if !needCommit {
		syncTxTxIn.Rollback()
		return
	}
	logger.Log.Info("sync-commit-txin...")
	if err := syncTxTxIn.Commit(); err != nil {
		logger.Log.Error("sync-commit-txin", zap.Error(err))
	}
}

func CommitCodeHashSyncCk(needCommit bool) {
	defer SyncStmtBlkCodeHash.Close()

	if !needCommit {
		syncTxBlkCodeHash.Rollback()
		return
	}

	if err := syncTxBlkCodeHash.Commit(); err != nil {
		logger.Log.Error("sync-commit-blk-code", zap.Error(err))
	}
}
