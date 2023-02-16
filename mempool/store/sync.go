package store

import (
	"database/sql"
	"fmt"
	"sensibled/loader/clickhouse"
	"sensibled/logger"

	"go.uber.org/zap"
)

var (
	SyncStmtTx    *sql.Stmt
	SyncStmtTxOut *sql.Stmt
	SyncStmtTxIn  *sql.Stmt

	syncTx    *sql.Tx
	syncTxOut *sql.Tx
	syncTxIn  *sql.Tx
)

const (
	sqlTxPattern    string = "INSERT INTO %s (txid, nin, nout, txsize, witoffset, locktime, invalue, outvalue, rawtx, height, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern string = "INSERT INTO %s (utxid, vout, address, code_type, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern  string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, script_wits, nsequence, height_txo, utxidx, utxid, vout, address, code_type, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk() bool {
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_mempool_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_mempool_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_mempool_new")

	var err error

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

func PreparePartSyncCk() bool {
	return prepareSyncCk()
}

func CommitSyncCk() bool {
	logger.Log.Info("sync commit...")
	defer SyncStmtTx.Close()
	defer SyncStmtTxOut.Close()
	defer SyncStmtTxIn.Close()

	isOk := true
	if err := syncTx.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx", zap.Error(err))
		isOk = false
	}
	if err := syncTxOut.Commit(); err != nil {
		logger.Log.Error("sync-commit-txout", zap.Error(err))
		isOk = false
	}
	if err := syncTxIn.Commit(); err != nil {
		logger.Log.Error("sync-commit-txin", zap.Error(err))
		isOk = false
	}
	return isOk
}
