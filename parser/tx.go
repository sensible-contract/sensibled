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
		if txs[i].WitOffset > 0 {
			txs[i].Raw = txsraw[offset : offset+txoffset]
			txs[i].Hash = utils.GetWitnessHash256(txs[i].Raw, txs[i].WitOffset)
			txs[i].HashHex = utils.HashString(txs[i].Hash)
		} else {
			txs[i].Raw = txsraw[offset : offset+txoffset]
			txs[i].Hash = utils.GetHash256(txs[i].Raw)
			txs[i].HashHex = utils.HashString(txs[i].Hash)
		}

		txs[i].Size = uint32(txoffset)
		offset += txoffset
	}
	return txs
}

func NewTx(rawtx []byte) (tx *model.Tx, offset uint) {
	tx = new(model.Tx)
	tx.Version = binary.LittleEndian.Uint32(rawtx[0:4])
	offset = 4

	// check witness
	isWit := false
	if rawtx[4] == 0 && rawtx[5] == 1 {
		isWit = true
		offset += 2
	}

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

	if isWit {
		tx.WitOffset = offset
		tx.TxWits = make([]*model.TxWit, txincnt)
		for i := range tx.TxWits {
			tx.TxWits[i], txoffset = NewTxWit(rawtx[offset:])
			offset += txoffset
		}
	}

	tx.LockTime = binary.LittleEndian.Uint32(rawtx[offset : offset+4])
	offset += 4
	return
}

func NewTxWit(txwitraw []byte) (txwit *model.TxWit, offset uint) {
	txwit = new(model.TxWit)
	txWitcnt, txWitcntsize := utils.DecodeVarIntForBlock(txwitraw[0:])
	offset = txWitcntsize

	for witIndex := uint(0); witIndex < txWitcnt; witIndex++ {
		txWitScriptcnt, txWitScriptcntsize := utils.DecodeVarIntForBlock(txwitraw[offset:])
		offset += txWitScriptcntsize

		// txwit.Pkscript = txwitraw[offset : offset+pkscript]
		offset += txWitScriptcnt
	}
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

	txout.Pkscript = make([]byte, pkscript)
	copy(txout.Pkscript, txoutraw[offset:offset+pkscript])
	// txout.Pkscript = txoutraw[offset : offset+pkscript]
	offset += pkscript

	return
}
