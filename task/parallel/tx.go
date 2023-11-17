package parallel

import (
	"encoding/binary"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/prune"
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
		output.AddressData = scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)

		if output.AddressData.CodeType == scriptDecoder.CodeType_NONE || output.AddressData.CodeType == scriptDecoder.CodeType_SENSIBLE {
			// not token
			continue
		}

		// update token summary
		buf := make([]byte, 12, 12+20+40)
		binary.LittleEndian.PutUint32(buf, output.AddressData.CodeType)
		if output.AddressData.CodeType == scriptDecoder.CodeType_NFT {
			binary.LittleEndian.PutUint64(buf[4:], output.AddressData.SensibleData.NFT.TokenIndex)
		} else if output.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
			binary.LittleEndian.PutUint64(buf[4:], output.AddressData.SensibleData.NFTSell.TokenIndex)
		}

		buf = append(buf, output.AddressData.SensibleData.CodeHash[:]...)
		buf = append(buf, output.AddressData.SensibleData.GenesisId[:output.AddressData.SensibleData.GenesisIdLen]...)

		var tokenIndex uint64
		var decimal uint8
		switch output.AddressData.CodeType {
		case scriptDecoder.CodeType_NFT:
			tokenIndex = output.AddressData.SensibleData.NFT.TokenIndex
		case scriptDecoder.CodeType_NFT_SELL:
			tokenIndex = output.AddressData.SensibleData.NFTSell.TokenIndex
		case scriptDecoder.CodeType_FT:
			decimal = output.AddressData.SensibleData.FT.Decimal
		}

		tokenKey := string(buf)

		tokenSummary, ok := block.TokenSummaryMap[tokenKey]
		if !ok {
			tokenSummary = &model.TokenData{
				CodeType:  output.AddressData.CodeType,
				NFTIdx:    tokenIndex,
				Decimal:   decimal,
				CodeHash:  output.AddressData.SensibleData.CodeHash[:],
				GenesisId: output.AddressData.SensibleData.GenesisId[:output.AddressData.SensibleData.GenesisIdLen],
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

		d.AddressData = output.AddressData

		block.NewUtxoDataMap[string(tx.TxId)+output.OutpointIdxKey] = d
	}
}

// ParseUpdateAddressInTxParallel address tx历史记录
func ParseUpdateAddressInTxParallel(txIdx uint64, tx *model.Tx, block *model.ProcessBlock) {
	if prune.IsHistoryPrune {
		return
	}
	for _, output := range tx.TxOuts {
		if output.AddressData.HasAddress {
			address := string(output.AddressData.AddressPkh[:])
			block.AddrPkhInTxMap[address] = append(block.AddrPkhInTxMap[address], int(txIdx))
		}
	}
}
