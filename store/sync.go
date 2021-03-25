package store

import (
	"blkparser/loader/clickhouse"
	"database/sql"
	"fmt"
	"log"
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

	sqlBlkPattern         string = "INSERT INTO %s (height, blkid, previd, merkle, ntx, invalue, outvalue, coinbase_out, blocktime, bits, blocksize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	sqlBlkCodeHashPattern string = "INSERT INTO %s (height, codehash, genesis, code_type, in_data_value, out_data_value, invalue, outvalue, blkid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxPattern          string = "INSERT INTO %s (txid, nin, nout, txsize, locktime, invalue, outvalue, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern       string = "INSERT INTO %s (utxid, vout, address, codehash, genesis, data_value, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern        string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, codehash, genesis, data_value, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
		log.Println("sync-begin-blk", err.Error())
		return false
	}
	SyncStmtBlk, err = syncTxBlk.Prepare(sqlBlk)
	if err != nil {
		log.Println("sync-prepare-blk", err.Error())
		return false
	}

	syncTxBlkCodeHash, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-blk-code", err.Error())
		return false
	}
	SyncStmtBlkCodeHash, err = syncTxBlkCodeHash.Prepare(sqlBlkCodeHash)
	if err != nil {
		log.Println("sync-prepare-blk-code", err.Error())
		return false
	}

	syncTxTx, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-tx", err.Error())
		return false
	}
	SyncStmtTx, err = syncTxTx.Prepare(sqlTx)
	if err != nil {
		log.Println("sync-prepare-tx", err.Error())
		return false
	}

	syncTxTxOut, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txout", err.Error())
		return false
	}
	SyncStmtTxOut, err = syncTxTxOut.Prepare(sqlTxOut)
	if err != nil {
		log.Println("sync-prepare-txout", err.Error())
		return false
	}

	syncTxTxIn, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txinfull", err.Error())
		return false
	}
	SyncStmtTxIn, err = syncTxTxIn.Prepare(sqlTxIn)
	if err != nil {
		log.Println("sync-prepare-txinfull", err.Error())
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

	if err := syncTxBlk.Commit(); err != nil {
		log.Println("sync-commit-blk", err.Error())
	}
	if err := syncTxTx.Commit(); err != nil {
		log.Println("sync-commit-tx", err.Error())
	}
	if err := syncTxTxOut.Commit(); err != nil {
		log.Println("sync-commit-txout", err.Error())
	}
}

func CommitFullSyncCk(needCommit bool) {
	defer SyncStmtTxIn.Close()

	if !needCommit {
		syncTxTxIn.Rollback()
		return
	}

	if err := syncTxTxIn.Commit(); err != nil {
		log.Println("sync-commit-txinfull", err.Error())
	}
}

func CommitCodeHashSyncCk(needCommit bool) {
	defer SyncStmtBlkCodeHash.Close()

	if !needCommit {
		syncTxBlkCodeHash.Rollback()
		return
	}

	if err := syncTxBlkCodeHash.Commit(); err != nil {
		log.Println("sync-commit-blk-code", err.Error())
	}
}
