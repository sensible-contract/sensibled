package blkparser

import (
	"encoding/binary"
)

type Tx struct {
	HashHex   string // 32
	Hash      []byte // 32
	Size      uint32
	WitOffset uint
	LockTime  uint32
	Version   uint32
	TxInCnt   uint32
	TxOutCnt  uint32
	TxIns     []*TxIn
	TxOuts    []*TxOut
	TxWits    []*TxWit
}

type TxIn struct {
	InputHashHex string // 32
	InputHash    []byte // 32
	InputVout    uint32
	// ScriptSig []byte
	Sequence uint32

	// other:
	InputOutpointKey string // 32 + 4
}

type TxOut struct {
	// Addr     string
	Value    uint64
	Pkscript []byte

	// other:
	OutpointKey          string // 32 + 4
	LockingScriptType    []byte
	LockingScriptTypeHex string
	LockingScriptMatch   bool
}

type TxWit struct {
	// Addr     string
	Value uint64
	// Pkscript []byte
}

func NewTx(rawtx []byte) (tx *Tx, offset uint) {
	tx = new(Tx)
	tx.Version = binary.LittleEndian.Uint32(rawtx[0:4])
	offset = 4

	// check witness
	isWit := false
	if rawtx[4] == 0 && rawtx[5] == 1 {
		isWit = true
		offset += 2
	}

	txincnt, txincntsize := DecodeVariableLengthInteger(rawtx[offset:])
	offset += txincntsize

	tx.TxInCnt = uint32(txincnt)
	tx.TxIns = make([]*TxIn, txincnt)

	txoffset := uint(0)
	for i := range tx.TxIns {
		tx.TxIns[i], txoffset = NewTxIn(rawtx[offset:])
		offset += txoffset

	}

	txoutcnt, txoutcntsize := DecodeVariableLengthInteger(rawtx[offset:])
	offset += txoutcntsize

	tx.TxOutCnt = uint32(txoutcnt)
	tx.TxOuts = make([]*TxOut, txoutcnt)
	for i := range tx.TxOuts {
		tx.TxOuts[i], txoffset = NewTxOut(rawtx[offset:])
		offset += txoffset
	}

	if isWit {
		tx.WitOffset = offset
		tx.TxWits = make([]*TxWit, txincnt)
		for i := range tx.TxWits {
			tx.TxWits[i], txoffset = NewTxWit(rawtx[offset:])
			offset += txoffset
		}
	}

	tx.LockTime = binary.LittleEndian.Uint32(rawtx[offset : offset+4])
	offset += 4
	return
}

func NewTxWit(txwitraw []byte) (txwit *TxWit, offset uint) {
	txwit = new(TxWit)
	txWitcnt, txWitcntsize := DecodeVariableLengthInteger(txwitraw[0:])
	offset = txWitcntsize

	for witIndex := uint(0); witIndex < txWitcnt; witIndex++ {
		txWitScriptcnt, txWitScriptcntsize := DecodeVariableLengthInteger(txwitraw[offset:])
		offset += txWitScriptcntsize

		// txwit.Pkscript = txwitraw[offset : offset+pkscript]
		offset += txWitScriptcnt
	}
	return
}

func NewTxIn(txinraw []byte) (txin *TxIn, offset uint) {
	txin = new(TxIn)
	txin.InputHash = txinraw[0:32]
	txin.InputHashHex = HashString(txin.InputHash)
	txin.InputVout = binary.LittleEndian.Uint32(txinraw[32:36])
	offset = 36

	scriptsig, scriptsigsize := DecodeVariableLengthInteger(txinraw[offset:])
	offset += scriptsigsize

	// txin.ScriptSig = txinraw[offset : offset+scriptsig]
	offset += scriptsig

	txin.Sequence = binary.LittleEndian.Uint32(txinraw[offset : offset+4])
	offset += 4

	// other init
	txin.InputOutpointKey = string(txinraw[0:36])
	return
}

func NewTxOut(txoutraw []byte) (txout *TxOut, offset uint) {
	txout = new(TxOut)
	txout.Value = binary.LittleEndian.Uint64(txoutraw[0:8])
	offset = 8

	pkscript, pkscriptsize := DecodeVariableLengthInteger(txoutraw[offset:])
	offset += pkscriptsize

	txout.Pkscript = txoutraw[offset : offset+pkscript]
	offset += pkscript

	return

	// address
	// _, addrhash, _, err := txscript.ExtractPkScriptAddrs(txout.Pkscript, &chaincfg.MainNetParams)
	// if err != nil {
	// 	return
	// }
	// if len(addrhash) != 0 {
	// 	txout.Addr = addrhash[0].EncodeAddress()
	// } else {
	// 	txout.Addr = ""
	// }
	// return
}
