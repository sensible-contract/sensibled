package parser

import (
	"encoding/binary"
	"sensibled/mempool/parser"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/utils"
)

func NewTxs(stripMode bool, txsraw []byte) (txs []*model.Tx) {
	offset := uint(0)
	txcnt, txcnt_size := utils.DecodeVarIntForBlock(txsraw[offset:])
	offset += txcnt_size

	txs = make([]*model.Tx, txcnt)

	for i := range txs {
		txoffset := uint(0)
		txs[i], txoffset = NewTx(txsraw[offset:])

		tx := txs[i]
		tx.Raw = txsraw[offset : offset+txoffset]
		offset += txoffset

		if stripMode {
			tx.TxId = make([]byte, 32)
			copy(tx.TxId, txsraw[offset:offset+32])
			offset += 32

			tx.Size = binary.LittleEndian.Uint32(txsraw[offset : offset+4])
			offset += 4
		} else {
			if tx.WitOffset > 0 {
				tx.TxId = utils.GetWitnessHash256(tx.Raw, tx.WitOffset)
			} else {
				tx.TxId = utils.GetHash256(tx.Raw)
			}
			tx.Size = uint32(txoffset)
		}
		tx.TxIdHex = utils.HashString(tx.TxId)

		// nft decode
		if tx.WitOffset == 0 {
			continue
		}
		isNFTInLastInput := true
		for _, input := range tx.TxIns {
			// 只支持第一个输入的NFT
			if !isNFTInLastInput {
				break
			}
			if len(input.ScriptWitness) == 0 {
				break
			}

			wits, offset := parser.NewTxWit(input.ScriptWitness)
			if len(input.ScriptWitness) != int(offset) {
				break
			}

			// 只支持p2tr格式的见证，单NFT，多段OP_FALSE/OP_IF仅识别第一个。
			// 跳过没有脚本的wits
			if len(wits) < 2 {
				break
			}

			// 附件
			hasAnnex := (wits[len(wits)-1].Script[0] == 0x50)
			// 跳过P2WPKH
			if len(wits) < 3 && hasAnnex {
				break
			}

			nftScript := wits[len(wits)-2].Script
			if hasAnnex {
				// fixme: -1 at official impliment
				nftScript = wits[len(wits)-3].Script
			}

			if nft, ok := scriptDecoder.ExtractPkScriptForNFT(nftScript); ok {
				satOffset := len(input.CreatePointOfNewNFTs)
				input.CreatePointOfNewNFTs = append(input.CreatePointOfNewNFTs, &model.NFTCreatePoint{
					Idx:    uint64(len(tx.CreateNFTData)),
					Offset: uint64(satOffset),
				})
				tx.CreateNFTData = append(tx.CreateNFTData, nft)

			} else {
				isNFTInLastInput = false
				break
			}
		}
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
		tx.WitOffset = uint32(offset)
		for i := range tx.TxIns {
			tx.TxIns[i].ScriptWitness, txoffset = NewTxWits(rawtx[offset:])
			offset += txoffset
		}
	}

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

func NewTxWit(txwitraw []byte) (wits []*model.TxWit, offset uint) {
	txWitcnt, txWitcntsize := utils.DecodeVarIntForBlock(txwitraw[0:])
	offset = txWitcntsize

	wits = make([]*model.TxWit, txWitcnt)
	for witIndex := uint(0); witIndex < txWitcnt; witIndex++ {
		txWitScriptcnt, txWitScriptcntsize := utils.DecodeVarIntForBlock(txwitraw[offset:])
		offset += txWitScriptcntsize

		txwit := new(model.TxWit)
		txwit.Script = txwitraw[offset : offset+txWitScriptcnt]

		wits[witIndex] = txwit
		offset += txWitScriptcnt
	}
	return
}

func NewTxWits(txwitraw []byte) (wits []byte, offset uint) {
	txWitcnt, txWitcntsize := utils.DecodeVarIntForBlock(txwitraw[0:])
	offset = txWitcntsize

	for witIndex := uint(0); witIndex < txWitcnt; witIndex++ {
		txWitScriptcnt, txWitScriptcntsize := utils.DecodeVarIntForBlock(txwitraw[offset:])
		offset += txWitScriptcntsize
		offset += txWitScriptcnt
	}

	wits = txwitraw[:offset]

	return
}

// striped tx
func NewRawTx(tx *model.Tx, rawtx []byte) (offset int) {
	binary.LittleEndian.PutUint32(rawtx[0:4], tx.Version)
	offset = 4

	txincntsize := utils.EncodeVarIntForBlock(uint64(tx.TxInCnt), rawtx[offset:])
	offset += txincntsize

	txoffset := 0
	for _, txin := range tx.TxIns {
		txoffset = NewRawTxIn(txin, rawtx[offset:])
		offset += txoffset
	}

	txoutcntsize := utils.EncodeVarIntForBlock(uint64(tx.TxOutCnt), rawtx[offset:])
	offset += txoutcntsize

	for _, txout := range tx.TxOuts {
		txoffset = NewRawTxOut(txout, rawtx[offset:])
		offset += txoffset
	}

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
	} else if scriptDecoder.IsOpreturn(txout.PkScript) {
		txoutraw[offset] = 0x01
		txoutraw[offset+1] = 0x6a
		offset += 2
	} else {
		pkscript := len(txout.PkScript)
		pkscriptsize := utils.EncodeVarIntForBlock(uint64(pkscript), txoutraw[offset:])
		offset += pkscriptsize

		copy(txoutraw[offset:offset+pkscript], txout.PkScript)
		offset += pkscript
	}
	return
}
