package parser

import (
	"encoding/binary"
	"satoblock/model"
	"satoblock/utils"
)

func NewTxs(txsraw []byte) (txs []*model.Tx) {
	offset := uint(0)
	txcnt, txcnt_size := utils.DecodeVarIntForBlock(txsraw[offset:])
	offset += txcnt_size

	txs = make([]*model.Tx, txcnt)

	txoffset := uint(0)
	for i := range txs {
		txs[i], txoffset = NewTx(txsraw[offset:])
		txs[i].Raw = txsraw[offset : offset+txoffset]
		txs[i].Hash = utils.GetHash256(txs[i].Raw)
		txs[i].HashHex = utils.HashString(txs[i].Hash)

		txs[i].Size = uint32(txoffset)
		offset += txoffset
	}
	return txs
}

func NewTx(rawtx []byte) (tx *model.Tx, offset uint) {
	tx = new(model.Tx)
	tx.Version = binary.LittleEndian.Uint32(rawtx[0:4])
	offset = 4

	txincnt, txincntsize := utils.DecodeVarIntForBlock(rawtx[offset:])
	offset += txincntsize

	tx.TxInCnt = uint32(txincnt)
	tx.TxIns = make([]*model.TxIn, txincnt)

	txoffset := uint(0)
	for i := range tx.TxIns {
		tx.TxIns[i], txoffset = NewTxIn(rawtx[offset:])
		offset += txoffset
	}

	txoutcnt, txoutcntsize := utils.DecodeVarIntForBlock(rawtx[offset:])
	offset += txoutcntsize

	tx.TxOutCnt = uint32(txoutcnt)
	tx.TxOuts = make([]*model.TxOut, txoutcnt)
	for i := range tx.TxOuts {
		tx.TxOuts[i], txoffset = NewTxOut(rawtx[offset:])
		offset += txoffset
	}

	tx.LockTime = binary.LittleEndian.Uint32(rawtx[offset : offset+4])
	offset += 4
	return
}

func NewTxIn(txinraw []byte) (txin *model.TxIn, offset uint) {
	txin = new(model.TxIn)
	txin.InputHash = txinraw[0:32]
	txin.InputHashHex = utils.HashString(txin.InputHash)
	txin.InputVout = binary.LittleEndian.Uint32(txinraw[32:36])
	offset = 36

	scriptsig, scriptsigsize := utils.DecodeVarIntForBlock(txinraw[offset:])
	offset += scriptsigsize

	txin.ScriptSig = txinraw[offset : offset+scriptsig]
	offset += scriptsig

	txin.Sequence = binary.LittleEndian.Uint32(txinraw[offset : offset+4])
	offset += 4

	// process Parallel
	txin.InputOutpointKey = string(txinraw[0:36])
	txin.InputOutpoint = txinraw[0:36]
	return
}

func NewTxOut(txoutraw []byte) (txout *model.TxOut, offset uint) {
	txout = new(model.TxOut)
	txout.Satoshi = binary.LittleEndian.Uint64(txoutraw[0:8])
	offset = 8

	pkscript, pkscriptsize := utils.DecodeVarIntForBlock(txoutraw[offset:])
	offset += pkscriptsize

	txout.PkScript = make([]byte, pkscript)
	copy(txout.PkScript, txoutraw[offset:offset+pkscript])

	offset += pkscript
	return
}
