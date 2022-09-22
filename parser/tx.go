package parser

import (
	"encoding/binary"
	"sensibled/model"
	"sensibled/utils"
)

func NewTxs(txsraw []byte) (txs []*model.Tx) {
	offset := uint(0)
	txcnt, txcnt_size := utils.DecodeVarIntForBlock(txsraw[offset:])
	offset += txcnt_size

	txs = make([]*model.Tx, txcnt)

	txoffset := uint(0)
	for i := range txs {
		// fmt.Println("offset:", offset)
		txs[i], txoffset = NewTx(txsraw[offset:])
		txs[i].Raw = txsraw[offset : offset+txoffset]
		txs[i].TxId = GetTxId(txs[i])
		txs[i].TxIdHex = utils.HashString(txs[i].TxId)
		txs[i].Size = uint32(txoffset)
		offset += txoffset
	}
	return txs
}

func GetTxId(tx *model.Tx) []byte {
	return utils.GetHash256(tx.Raw)
}

func NewTx(rawtx []byte) (tx *model.Tx, offset uint) {
	tx = new(model.Tx)
	tx.Version = binary.LittleEndian.Uint32(rawtx[0:4])
	offset = 4
	// fmt.Println("version:", offset)

	txincnt, txincntsize := utils.DecodeVarIntForBlock(rawtx[offset:])
	offset += txincntsize
	// fmt.Println("in:", offset)

	tx.TxInCnt = uint32(txincnt)
	tx.TxIns = make([]*model.TxIn, txincnt)

	txoffset := uint(0)
	for i := range tx.TxIns {
		tx.TxIns[i], txoffset = NewTxIn(rawtx[offset:])
		offset += txoffset
	}

	txoutcnt, txoutcntsize := utils.DecodeVarIntForBlock(rawtx[offset:])
	offset += txoutcntsize
	// fmt.Println("out:", offset)

	tx.TxOutCnt = uint32(txoutcnt)
	tx.TxOuts = make([]*model.TxOut, txoutcnt)
	for i := range tx.TxOuts {
		tx.TxOuts[i], txoffset = NewTxOut(rawtx[offset:])
		offset += txoffset
	}

	// fmt.Println("lock:", offset)
	tx.LockTime = binary.LittleEndian.Uint32(rawtx[offset : offset+4])
	offset += 4
	return
}

func NewTxIn(txinraw []byte) (txin *model.TxIn, offset uint) {
	txin = new(model.TxIn)
	txin.InputHash = make([]byte, 32)
	copy(txin.InputHash, txinraw[0:32])
	txin.InputHashHex = utils.HashString(txin.InputHash)
	txin.InputVout = binary.LittleEndian.Uint32(txinraw[32:36])
	offset = 36

	scriptsig, scriptsigsize := utils.DecodeVarIntForBlock(txinraw[offset:])
	offset += scriptsigsize

	txin.ScriptSig = make([]byte, scriptsig)
	copy(txin.ScriptSig, txinraw[offset:offset+scriptsig])
	offset += scriptsig

	txin.Sequence = binary.LittleEndian.Uint32(txinraw[offset : offset+4])
	offset += 4

	// process Parallel
	txin.InputOutpointKey = string(txinraw[0:36])
	txin.InputOutpoint = make([]byte, 36)
	copy(txin.InputOutpoint, txinraw[0:36])
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

////////////////

func NewRawTx(tx *model.Tx, rawtx []byte) (offset int) {
	binary.LittleEndian.PutUint32(rawtx[0:4], tx.Version)
	offset = 4
	// fmt.Println("version:", offset)

	txincntsize := utils.EncodeVarIntForBlock(uint64(tx.TxInCnt), rawtx[offset:])
	offset += txincntsize
	// fmt.Println("in:", offset)

	txoffset := 0
	for _, txin := range tx.TxIns {
		txoffset = NewRawTxIn(txin, rawtx[offset:])
		offset += txoffset
	}

	txoutcntsize := utils.EncodeVarIntForBlock(uint64(tx.TxOutCnt), rawtx[offset:])
	offset += txoutcntsize
	// fmt.Println("out:", offset)

	for _, txout := range tx.TxOuts {
		txoffset = NewRawTxOut(txout, rawtx[offset:])
		offset += txoffset
	}

	// fmt.Println("lock:", offset)
	binary.LittleEndian.PutUint32(rawtx[offset:offset+4], tx.LockTime)
	offset += 4
	return
}

func NewRawTxIn(txin *model.TxIn, txinraw []byte) (offset int) {
	copy(txinraw[0:36], txin.InputOutpoint)
	offset = 36
	txinraw[offset] = 0x00
	offset += 1
	binary.LittleEndian.PutUint32(txinraw[offset:offset+4], txin.Sequence)
	offset += 4
	return
}

func NewRawTxOut(txout *model.TxOut, txoutraw []byte) (offset int) {
	binary.LittleEndian.PutUint64(txoutraw[0:8], txout.Satoshi)
	offset = 8

	if len(txout.PkScript) == 0 {
		txoutraw[offset] = 0x00
		offset += 1
	} else if txout.PkScript[0] == 0x6a {
		txoutraw[offset] = 0x01
		txoutraw[offset+1] = 0x6a
		offset += 2
	} else if len(txout.PkScript) >= 2 && txout.PkScript[0] == 0x00 && txout.PkScript[1] == 0x6a {
		txoutraw[offset] = 0x02
		txoutraw[offset+1] = 0x00
		txoutraw[offset+2] = 0x6a
		offset += 3
	} else {
		pkscript := len(txout.PkScript)
		pkscriptsize := utils.EncodeVarIntForBlock(uint64(pkscript), txoutraw[offset:])
		offset += pkscriptsize

		copy(txoutraw[offset:offset+pkscript], txout.PkScript)
		offset += pkscript
	}
	return
}
