package store

import (
	"database/sql"
	"fmt"
	"unisatd/loader/clickhouse"
	"unisatd/logger"

	"go.uber.org/zap"
)

var (
	SyncStmtTx    *sql.Stmt
	SyncStmtNFT   *sql.Stmt
	SyncStmtBRC20 *sql.Stmt
	SyncStmtTxOut *sql.Stmt
	SyncStmtTxIn  *sql.Stmt

	syncTx    *sql.Tx
	syncNFT   *sql.Tx
	syncBRC20 *sql.Tx
	syncTxOut *sql.Tx
	syncTxIn  *sql.Tx
)

const (
	sqlTxPattern    string = "INSERT INTO %s (txid, nin, nout, txsize, witoffset, locktime, nftnew, nftin, nftout, nftlost, invalue, outvalue, rawtx, height, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlNFTPattern   string = "INSERT INTO %s (txid, idx, vin, vout, offset, satoshi, script_pk, invalue, outvalue, content_type, content_len, content, height, txidx, blocktime, nftidx, nftnumber) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlBRC20Pattern string = "INSERT INTO %s (txid, idx, vin, vout, offset, satoshi, script_pk, invalue, outvalue, content_type, content_len, content, height, txidx, blocktime, nftidx, nftnumber, nftheight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern string = "INSERT INTO %s (utxid, vout, address, code_type, satoshi, script_type, script_pk, nftout, nftpoints, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern  string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, script_wits, nsequence, nftnew, height_txo, utxidx, utxid, vout, address, code_type, satoshi, script_type, script_pk, nftin, nftpoints) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk() bool {
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_mempool_new")
	sqlNFT := fmt.Sprintf(sqlNFTPattern, "blknft_height_mempool_new")
	sqlBRC20 := fmt.Sprintf(sqlBRC20Pattern, "blkbrc20_height_mempool_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_mempool_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_mempool_new")

	var err error
	// tx
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
	// nft
	syncNFT, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-nft", zap.Error(err))
		return false
	}
	SyncStmtNFT, err = syncNFT.Prepare(sqlNFT)
	if err != nil {
		logger.Log.Error("sync-prepare-nft", zap.Error(err))
		return false
	}
	// brc20
	syncBRC20, err = clickhouse.CK.Begin()
	if err != nil {
		logger.Log.Error("sync-begin-brc20", zap.Error(err))
		return false
	}
	SyncStmtBRC20, err = syncBRC20.Prepare(sqlBRC20)
	if err != nil {
		logger.Log.Error("sync-prepare-brc20", zap.Error(err))
		return false
	}
	// txout
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
	// txin
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
	defer SyncStmtNFT.Close()
	defer SyncStmtBRC20.Close()
	defer SyncStmtTxOut.Close()
	defer SyncStmtTxIn.Close()

	isOk := true
	if err := syncTx.Commit(); err != nil {
		logger.Log.Error("sync-commit-tx", zap.Error(err))
		isOk = false
	}
	if err := syncNFT.Commit(); err != nil {
		logger.Log.Error("sync-commit-nft", zap.Error(err))
		isOk = false
	}
	if err := syncBRC20.Commit(); err != nil {
		logger.Log.Error("sync-commit-brc20", zap.Error(err))
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
