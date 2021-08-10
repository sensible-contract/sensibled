package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"satoblock/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
)

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxFirst(tx *model.Tx) {
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

		output.ScriptType = scriptDecoder.GetLockingScriptType(output.PkScript)
		output.ScriptTypeHex = hex.EncodeToString(output.ScriptType)

		if scriptDecoder.IsOpreturn(output.ScriptType) {
			output.LockingScriptUnspendable = true
		}

		txo := scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)
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
	}
}

// ParseTxoSpendByTxParallel utxo被使用
func ParseTxoSpendByTxParallel(tx *model.Tx, spentUtxoKeysMap map[string]struct{}) {
	for _, input := range tx.TxIns {
		spentUtxoKeysMap[input.InputOutpointKey] = struct{}{}
	}
}

// ParseNewUtxoInTxParallel utxo 信息
func ParseNewUtxoInTxParallel(txIdx int, tx *model.Tx, mpNewUtxo map[string]*model.TxoData) {
	for _, output := range tx.TxOuts {
		if output.LockingScriptUnspendable {
			continue
		}

		d := model.TxoDataPool.Get().(*model.TxoData)
		d.BlockHeight = model.MEMPOOL_HEIGHT
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
		d.ScriptType = output.ScriptType
		d.PkScript = output.PkScript

		mpNewUtxo[output.OutpointKey] = d
	}
}
