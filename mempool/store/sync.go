package store

import (
	"database/sql"
	"fmt"
	"sensibled/loader/clickhouse"
	"sensibled/logger"

	"go.uber.org/zap"
)

var (
	SyncStmtTxContract *sql.Stmt
	SyncStmtTx         *sql.Stmt
	SyncStmtTxOut      *sql.Stmt
	SyncStmtTxIn       *sql.Stmt

	syncTxContract *sql.Tx
	syncTx         *sql.Tx
	syncTxOut      *sql.Tx
	syncTxIn       *sql.Tx
)

const (
	sqlTxContractPattern string = "INSERT INTO %s (height, codehash, genesis, code_type, operation, in_value1, in_value2, in_value3, out_value1, out_value2, out_value3, blkid, txidx, txid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxPattern         string = "INSERT INTO %s (txid, nin, nout, txsize, locktime, invalue, outvalue, rawtx, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern      string = "INSERT INTO %s (utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern       string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, codehash, genesis, code_type, data_value, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk() bool {
	sqlTxContract := fmt.Sprintf(sqlTxContractPattern, "blktx_contract_height_mempool_new")
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_mempool_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_mempool_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_mempool_new")

	var err error

	syncTxContract, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-blk-contract", zap.Error(err))
		return false
	}
	SyncStmtTxContract, err = syncTxContract.Prepare(sqlTxContract)
	if err != nil {
		logger.Log.Error("sync-prepare-blk-contract", zap.Error(err))
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

func PreparePartSyncCk() bool {
	return prepareSyncCk()
}

func CommitSyncCk() {
	defer SyncStmtTx.Close()
	defer SyncStmtTxOut.Close()
	defer SyncStmtTxIn.Close()
	defer SyncStmtTxContract.Close()

	logger.Log.Info("sync-commit-tx...")
	if err := syncTx.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx", zap.Error(err))
	}
	logger.Log.Info("sync-commit-txout...")
	if err := syncTxOut.Commit(); err != nil {
		logger.Log.Error("sync-commit-txout", zap.Error(err))
	}

	logger.Log.Info("sync-commit-txin...")
	if err := syncTxIn.Commit(); err != nil {
		logger.Log.Error("sync-commit-txin", zap.Error(err))
	}

	logger.Log.Info("sync-commit-tx-contract...")
	if err := syncTxContract.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx-contract", zap.Error(err))
	}
}
