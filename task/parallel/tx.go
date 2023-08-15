package parallel

import (
	"encoding/binary"
	"sensibled/model"
	"sensibled/prune"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
)

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxFirst(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	for idx, input := range tx.TxIns {
		key := make([]byte, 36)
		copy(key, tx.TxId)
		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		input.InputPoint = key
	}

	for idx, output := range tx.TxOuts {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(idx))

		output.OutpointIdxKey = string(key)
		output.ScriptType = scriptDecoder.GetLockingScriptType(output.PkScript)
		output.Data = scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)

		if output.Data.CodeType == scriptDecoder.CodeType_NONE || output.Data.CodeType == scriptDecoder.CodeType_SENSIBLE {
			// not token
			continue
		}

		// update token summary
		buf := make([]byte, 12, 12+20+40)
		binary.LittleEndian.PutUint32(buf, output.Data.CodeType)
		if output.Data.CodeType == scriptDecoder.CodeType_NFT {
			binary.LittleEndian.PutUint64(buf[4:], output.Data.NFT.TokenIndex)
		} else if output.Data.CodeType == scriptDecoder.CodeType_NFT_SELL {
			binary.LittleEndian.PutUint64(buf[4:], output.Data.NFTSell.TokenIndex)
		}

		buf = append(buf, output.Data.CodeHash[:]...)
		buf = append(buf, output.Data.GenesisId[:output.Data.GenesisIdLen]...)

		var tokenIndex uint64
		var decimal uint8
		switch output.Data.CodeType {
		case scriptDecoder.CodeType_NFT:
			tokenIndex = output.Data.NFT.TokenIndex
		case scriptDecoder.CodeType_NFT_SELL:
			tokenIndex = output.Data.NFTSell.TokenIndex
		case scriptDecoder.CodeType_FT:
			decimal = output.Data.FT.Decimal
		}

		tokenKey := string(buf)

		tokenSummary, ok := block.TokenSummaryMap[tokenKey]
		if !ok {
			tokenSummary = &model.TokenData{
				CodeType:  output.Data.CodeType,
				NFTIdx:    tokenIndex,
				Decimal:   decimal,
				CodeHash:  output.Data.CodeHash[:],
				GenesisId: output.Data.GenesisId[:output.Data.GenesisIdLen],
			}
			block.TokenSummaryMap[tokenKey] = tokenSummary
		}

		tokenSummary.OutSatoshi += output.Satoshi
		tokenSummary.OutDataValue += 1
	}
}

// ParseUpdateTxoSpendByTxParallel utxo被使用
func ParseUpdateTxoSpendByTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	if isCoinbase {
		return
	}
	for _, input := range tx.TxIns {
		block.SpentUtxoKeysMap[input.InputOutpointKey] = struct{}{}
	}
}

// ParseUpdateNewUtxoInTxParallel utxo 信息
func ParseUpdateNewUtxoInTxParallel(txIdx uint64, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if scriptDecoder.IsFalseOpreturn(output.ScriptType) {
			continue
		}

		// 从tx output提取utxo，以备程序使用
		d := &model.TxoData{}
		d.BlockHeight = block.Height
		d.TxIdx = txIdx
		d.Satoshi = output.Satoshi
		d.ScriptType = output.ScriptType
		d.PkScript = output.PkScript

		d.Data = output.Data

		block.NewUtxoDataMap[string(tx.TxId)+output.OutpointIdxKey] = d
	}
}

// ParseUpdateAddressInTxParallel address tx历史记录
func ParseUpdateAddressInTxParallel(txIdx uint64, tx *model.Tx, block *model.ProcessBlock) {
	if prune.IsHistoryPrune {
		return
	}
	for _, output := range tx.TxOuts {
		if output.Data.HasAddress {
			address := string(output.Data.AddressPkh[:])
			block.AddrPkhInTxMap[address] = append(block.AddrPkhInTxMap[address], int(txIdx))
		}
	}
}
