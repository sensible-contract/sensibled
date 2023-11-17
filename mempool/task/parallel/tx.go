package parallel

import (
	"encoding/binary"
	"sensibled/model"

	scriptDecoder "sensibled/parser/script"
)

// ParseTx 同区块内串行
func ParseTxFirst(tx *model.Tx) {
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
	}
}

// ParseUpdateTxoSpendByTxParallel utxo被使用
func ParseUpdateTxoSpendByTxParallel(tx *model.Tx, spentUtxoKeysMap map[string]struct{}) {
	for _, input := range tx.TxIns {
		spentUtxoKeysMap[input.InputOutpointKey] = struct{}{}
	}
}

// ParseUpdateNewUtxoInTxParallel utxo 信息
func ParseUpdateNewUtxoInTxParallel(txIdx uint64, tx *model.Tx, mpNewUtxo map[string]*model.TxoData) {
	for _, output := range tx.TxOuts {
		// LockingScriptUnspendable
		if scriptDecoder.IsFalseOpreturn(output.ScriptType) {
			continue
		}

		d := &model.TxoData{}
		d.BlockHeight = model.MEMPOOL_HEIGHT
		d.TxIdx = txIdx
		d.Satoshi = output.Satoshi
		d.PkScript = output.PkScript
		d.ScriptType = output.ScriptType
		d.AddressData = output.AddressData

		mpNewUtxo[string(tx.TxId)+output.OutpointIdxKey] = d
	}
}

// ParseUpdateAddressInTxParallel address tx历史记录
func ParseUpdateAddressInTxParallel(txIdx uint64, tx *model.Tx, addrPkhInTxMap map[string][]int) {
	for _, output := range tx.TxOuts {
		if output.AddressData.HasAddress {
			address := string(output.AddressData.AddressPkh[:])
			addrPkhInTxMap[address] = append(addrPkhInTxMap[address], int(txIdx))
		}
	}
}
