package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
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
		key := make([]byte, 36)
		copy(key, tx.TxId)

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)
		output.Outpoint = key

		output.ScriptType = scriptDecoder.GetLockingScriptType(output.PkScript)
		output.ScriptTypeHex = hex.EncodeToString(output.ScriptType)

		if scriptDecoder.IsOpreturn(output.ScriptType) {
			output.LockingScriptUnspendable = true
		}

		output.AddressData = scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)

		if output.AddressData.CodeType == scriptDecoder.CodeType_NONE || output.AddressData.CodeType == scriptDecoder.CodeType_SENSIBLE {
			// not token
			continue
		}

		// update token summary
		// buf := make([]byte, 12, 12+20+40)
		// binary.LittleEndian.PutUint32(buf, output.AddressData.CodeType)
		// if output.AddressData.CodeType == scriptDecoder.CodeType_NFT {
		// 	binary.LittleEndian.PutUint64(buf[4:], output.AddressData.NFT.TokenIndex)
		// } else if output.AddressData.CodeType == scriptDecoder.CodeType_NFT_SELL {
		// 	binary.LittleEndian.PutUint64(buf[4:], output.AddressData.NFTSell.TokenIndex)
		// }

		// buf = append(buf, output.AddressData.CodeHash[:]...)
		// buf = append(buf, output.AddressData.GenesisId[:output.AddressData.GenesisIdLen]...)

		// var tokenIndex uint64
		// var decimal uint8
		// switch output.AddressData.CodeType {
		// case scriptDecoder.CodeType_NFT:
		// 	tokenIndex = output.AddressData.NFT.TokenIndex
		// case scriptDecoder.CodeType_NFT_SELL:
		// 	tokenIndex = output.AddressData.NFTSell.TokenIndex
		// case scriptDecoder.CodeType_FT:
		// 	decimal = output.AddressData.FT.Decimal
		// }

		// tokenKey := string(buf)

		// tokenSummary, ok := block.TokenSummaryMap[tokenKey]
		// if !ok {
		// 	tokenSummary = &model.TokenData{
		// 		CodeType:  output.AddressData.CodeType,
		// 		NFTIdx:    tokenIndex,
		// 		Decimal:   decimal,
		// 		CodeHash:  output.AddressData.CodeHash[:],
		// 		GenesisId: output.AddressData.GenesisId[:output.AddressData.GenesisIdLen],
		// 	}
		// 	block.TokenSummaryMap[tokenKey] = tokenSummary
		// }

		// tokenSummary.OutSatoshi += output.Satoshi
		// tokenSummary.OutDataValue += 1
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

// ParseUpdateNewUtxoInTxParallel utxo 信息, 输出初始化时缺少NFT数据，会在查询utxo后、写入input db前补齐
func ParseUpdateNewUtxoInTxParallel(txIdx uint64, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.LockingScriptUnspendable {
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

		block.NewUtxoDataMap[output.OutpointKey] = d
	}
}

// ParseUpdateAddressInTxParallel address tx历史记录
func ParseUpdateAddressInTxParallel(txIdx uint64, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.AddressData.HasAddress {
			address := string(output.AddressData.AddressPkh[:])
			block.AddrPkhInTxMap[address] = append(block.AddrPkhInTxMap[address], int(txIdx))
		}
	}
}
