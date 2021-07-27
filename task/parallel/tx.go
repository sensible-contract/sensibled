package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"satoblock/model"
	"strconv"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
)

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxFirst(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	for idx, input := range tx.TxIns {
		key := make([]byte, 36)
		copy(key, tx.Hash)
		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		input.InputPoint = key
	}

	for idx, output := range tx.TxOuts {
		key := make([]byte, 36)
		copy(key, tx.Hash)

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)
		output.Outpoint = key

		output.LockingScriptType = scriptDecoder.GetLockingScriptType(output.Pkscript)
		output.LockingScriptTypeHex = hex.EncodeToString(output.LockingScriptType)

		if scriptDecoder.IsOpreturn(output.LockingScriptType) {
			output.LockingScriptUnspendable = true
		}

		txo := scriptDecoder.ExtractPkScriptForTxo(output.Pkscript, output.LockingScriptType)
		output.CodeType = txo.CodeType
		output.CodeHash = txo.CodeHash
		output.GenesisId = txo.GenesisId
		output.SensibleId = txo.SensibleId
		output.AddressPkh = txo.AddressPkh

		// nft
		output.MetaTxId = txo.MetaTxId
		output.MetaOutputIndex = txo.MetaOutputIndex
		output.TokenIndex = txo.TokenIndex
		output.TokenSupply = txo.TokenSupply

		// ft
		output.Name = txo.Name
		output.Symbol = txo.Symbol
		output.Amount = txo.Amount
		output.Decimal = txo.Decimal

		if len(output.CodeHash) < 20 || len(output.GenesisId) < 20 {
			// not token
			continue
		}

		// update token summary
		tokenKey := string(output.CodeHash) + string(output.GenesisId)
		if output.CodeType == scriptDecoder.CodeType_NFT {
			tokenKey += strconv.FormatUint(output.TokenIndex, 10)
		}
		tokenSummary, ok := block.TokenSummaryMap[tokenKey]
		if !ok {
			tokenSummary = &model.TokenData{
				CodeType:  output.CodeType,
				NFTIdx:    output.TokenIndex,
				Decimal:   output.Decimal,
				CodeHash:  output.CodeHash,
				GenesisId: output.GenesisId,
			}
			block.TokenSummaryMap[tokenKey] = tokenSummary
		}

		tokenSummary.OutSatoshi += output.Satoshi
		tokenSummary.OutDataValue += 1
	}
}

// ParseTxoSpendByTxParallel utxo被使用
func ParseTxoSpendByTxParallel(tx *model.Tx, isCoinbase bool, block *model.ProcessBlock) {
	if isCoinbase {
		return
	}
	for _, input := range tx.TxIns {
		block.SpentUtxoKeysMap[input.InputOutpointKey] = true
	}
}

// ParseNewUtxoInTxParallel utxo 信息
func ParseNewUtxoInTxParallel(txIdx int, tx *model.Tx, block *model.ProcessBlock) {
	for _, output := range tx.TxOuts {
		if output.LockingScriptUnspendable {
			continue
		}

		d := &model.TxoData{}
		d.BlockHeight = block.Height
		d.TxIdx = uint64(txIdx)
		d.AddressPkh = output.AddressPkh
		d.CodeType = output.CodeType
		d.CodeHash = output.CodeHash
		d.GenesisId = output.GenesisId
		d.SensibleId = output.SensibleId

		// nft
		d.MetaTxId = output.MetaTxId
		d.MetaOutputIndex = output.MetaOutputIndex
		d.TokenIndex = output.TokenIndex
		d.TokenSupply = output.TokenSupply

		// ft
		d.Amount = output.Amount
		d.Decimal = output.Decimal
		d.Name = output.Name
		d.Symbol = output.Symbol

		d.Satoshi = output.Satoshi
		d.ScriptType = output.LockingScriptType
		d.Script = output.Pkscript

		block.NewUtxoDataMap[output.OutpointKey] = d
	}
}
