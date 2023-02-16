package store

import (
	"database/sql"
	"fmt"
	"unisatd/loader/clickhouse"
	"unisatd/logger"

	"go.uber.org/zap"
)

var (
	SyncStmtBlk   *sql.Stmt
	SyncStmtTx    *sql.Stmt
	SyncStmtTxOut *sql.Stmt
	SyncStmtTxIn  *sql.Stmt

	syncBlk   *sql.Tx
	syncTx    *sql.Tx
	syncTxOut *sql.Tx
	syncTxIn  *sql.Tx
)

const (
	sqlBlkPattern   string = "INSERT INTO %s (height, blkid, previd, merkle, ntx, nftnew, nftin, nftout, nftlost, invalue, outvalue, coinbase_out, blocktime, bits, blocksize) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxPattern    string = "INSERT INTO %s (txid, nin, nout, txsize, witoffset, locktime, nftnew, nftin, nftout, nftlost, invalue, outvalue, rawtx, height, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern string = "INSERT INTO %s (utxid, vout, address, code_type, satoshi, script_type, script_pk, nftout, nftpoints, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern  string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, script_wits, nsequence, nftnew, height_txo, utxidx, utxid, vout, address, code_type, satoshi, script_type, script_pk, nftin, nftpoints) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk(isFull bool) bool {
	sqlBlk := fmt.Sprintf(sqlBlkPattern, "blk_height_new")
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_new")
	if isFull {
		sqlBlk = fmt.Sprintf(sqlBlkPattern, "blk_height")
		sqlTx = fmt.Sprintf(sqlTxPattern, "blktx_height")
		sqlTxOut = fmt.Sprintf(sqlTxOutPattern, "txout")
		sqlTxIn = fmt.Sprintf(sqlTxInPattern, "txin")
	}
	var err error
	syncBlk, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-blk", zap.Error(err))
		return false
	}
	SyncStmtBlk, err = syncBlk.Prepare(sqlBlk)
	if err != nil {
		logger.Log.Error("sync-prepare-blk", zap.Error(err))
		return false
	}

	syncTx, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-tx", zap.Error(err))
		return false
	}
	SyncStmtTx, err = syncTx.Prepare(sqlTx)
	if err != nil {
		logger.Log.Error("sync-prepare-tx", zap.Error(err))
		return false
	}

	syncTxOut, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-txout", zap.Error(err))
		return false
	}
	SyncStmtTxOut, err = syncTxOut.Prepare(sqlTxOut)
	if err != nil {
		logger.Log.Error("sync-prepare-txout", zap.Error(err))
		return false
	}

	syncTxIn, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-txinfull", zap.Error(err))
		return false
	}
	SyncStmtTxIn, err = syncTxIn.Prepare(sqlTxIn)
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

func CommitSyncCk() bool {
	logger.Log.Info("sync commit...")
	defer SyncStmtBlk.Close()
	defer SyncStmtTx.Close()
	defer SyncStmtTxOut.Close()
	defer SyncStmtTxIn.Close()

	isOK := true
	if err := syncBlk.Commit(); err != nil {
		logger.Log.Error("sync-commit-blk", zap.Error(err))
		isOK = false
	}
	if err := syncTx.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx", zap.Error(err))
		isOK = false
	}
	if err := syncTxOut.Commit(); err != nil {
		logger.Log.Error("sync-commit-txout", zap.Error(err))
		isOK = false
	}
	if err := syncTxIn.Commit(); err != nil {
		logger.Log.Error("sync-commit-txin", zap.Error(err))
		isOK = false
	}
	return isOK
}
