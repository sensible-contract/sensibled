package parser

import (
	"encoding/binary"
	"satoblock/model"
	"satoblock/utils"
)

func NewTx(rawtx []byte) (tx *model.Tx, offset uint) {
	txLen := len(rawtx)
	if txLen < 4+1+32+4+1+1+1+8+1+1+4 {
		return nil, 0
	}

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
		// failed
		if txoffset == 0 {
			return nil, 0
		}
		offset += txoffset
		// invalid
		if offset >= uint(txLen) {
			return nil, 0
		}
	}

	txoutcnt, txoutcntsize := utils.DecodeVarIntForBlock(rawtx[offset:])
	offset += txoutcntsize

	tx.TxOutCnt = uint32(txoutcnt)
	tx.TxOuts = make([]*model.TxOut, txoutcnt)
	for i := range tx.TxOuts {
		tx.TxOuts[i], txoffset = NewTxOut(rawtx[offset:])
		// failed
		if txoffset == 0 {
			return nil, 0
		}
		offset += txoffset
		// invalid
		if offset >= uint(txLen) {
			return nil, 0
		}
	}

	// invalid
	if offset+4 != uint(txLen) {
		return nil, 0
	}

	tx.LockTime = binary.LittleEndian.Uint32(rawtx[offset : offset+4])
	offset += 4
	return
}

func NewTxIn(txinraw []byte) (txin *model.TxIn, offset uint) {
	inLen := len(txinraw)
	if inLen < 32+4+1+1+4 {
		return nil, 0
	}
	txin = new(model.TxIn)
	txin.InputHash = txinraw[0:32]
	txin.InputHashHex = utils.HashString(txin.InputHash)
	txin.InputVout = binary.LittleEndian.Uint32(txinraw[32:36])
	offset = 36

	scriptsig, scriptsigsize := utils.DecodeVarIntForBlock(txinraw[offset:])
	offset += scriptsigsize

	txin.ScriptSig = txinraw[offset : offset+scriptsig]
	offset += scriptsig

	// invalid
	if offset+4 > uint(inLen) {
		return nil, 0
	}
	txin.Sequence = binary.LittleEndian.Uint32(txinraw[offset : offset+4])
	offset += 4

	// process Parallel
	txin.InputOutpointKey = string(txinraw[0:36])
	txin.InputOutpoint = txinraw[0:36]
	return
}

func NewTxOut(txoutraw []byte) (txout *model.TxOut, offset uint) {
	outLen := len(txoutraw)
	if outLen < 8+1+1 {
		return nil, 0
	}
	txout = new(model.TxOut)
	txout.Satoshi = binary.LittleEndian.Uint64(txoutraw[0:8])
	offset = 8

	pkscript, pkscriptsize := utils.DecodeVarIntForBlock(txoutraw[offset:])
	offset += pkscriptsize

	// invalid
	if offset+pkscript > uint(outLen) {
		return nil, 0
	}

	txout.PkScript = make([]byte, pkscript)
	copy(txout.PkScript, txoutraw[offset:offset+pkscript])
	// txout.PkScript = txoutraw[offset : offset+pkscript]
	offset += pkscript

	return
}
