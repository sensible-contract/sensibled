package utils

import (
	"blkparser/loader/clickhouse"
	"database/sql"
	"log"
)

var (
	SyncStmtBlk   *sql.Stmt
	SyncStmtTx    *sql.Stmt
	SyncStmtTxOut *sql.Stmt
	SyncStmtTxIn  *sql.Stmt

	syncTxBlk   *sql.Tx
	syncTxTx    *sql.Tx
	syncTxTxOut *sql.Tx
	syncTxTxIn  *sql.Tx

	// full sync
	sqlBlk   string = "INSERT INTO blk_height (height, blkid, previd, merkle, ntx, invalue, outvalue, coinbase_out, blocktime, bits, blocksize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTx    string = "INSERT INTO blktx_height (txid, nin, nout, txsize, locktime, invalue, outvalue, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOut string = "INSERT INTO txout (utxid, vout, address, genesis, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxIn  string = "INSERT INTO txin (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, genesis, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	// part sync
	sqlBlkNew   string = "INSERT INTO blk_height_new (height, blkid, previd, merkle, ntx, invalue, outvalue, coinbase_out, blocktime, bits, blocksize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxNew    string = "INSERT INTO blktx_height_new (txid, nin, nout, txsize, locktime, invalue, outvalue, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutNew string = "INSERT INTO txout_new (utxid, vout, address, genesis, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInNew  string = "INSERT INTO txin_new (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, genesis, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func PrepareFullSyncCk() bool {
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

func PreparePartSyncCk() bool {
	var err error
	syncTxBlk, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-blk", err.Error())
		return false
	}
	SyncStmtBlk, err = syncTxBlk.Prepare(sqlBlkNew)
	if err != nil {
		log.Println("sync-prepare-blk", err.Error())
		return false
	}

	syncTxTx, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-tx", err.Error())
		return false
	}
	SyncStmtTx, err = syncTxTx.Prepare(sqlTxNew)
	if err != nil {
		log.Println("sync-prepare-tx", err.Error())
		return false
	}

	syncTxTxOut, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txout", err.Error())
		return false
	}
	SyncStmtTxOut, err = syncTxTxOut.Prepare(sqlTxOutNew)
	if err != nil {
		log.Println("sync-prepare-txout", err.Error())
		return false
	}

	syncTxTxIn, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txinfull", err.Error())
		return false
	}
	SyncStmtTxIn, err = syncTxTxIn.Prepare(sqlTxInNew)
	if err != nil {
		log.Println("sync-prepare-txinfull", err.Error())
		return false
	}

	return true
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
