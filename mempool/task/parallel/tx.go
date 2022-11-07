package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"sensibled/model"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
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
		key := make([]byte, 36)
		copy(key, tx.TxId)

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)
		output.Outpoint = key

		output.ScriptType = scriptDecoder.GetLockingScriptType(output.PkScript)
		output.ScriptTypeHex = hex.EncodeToString(output.ScriptType)

		if scriptDecoder.IsFalseOpreturn(output.ScriptType) {
			output.LockingScriptUnspendable = true
		}

		output.Data = scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)
	}
}

// ParseUpdateTxoSpendByTxParallel utxo被使用
func ParseUpdateTxoSpendByTxParallel(tx *model.Tx, spentUtxoKeysMap map[string]struct{}) {
	for _, input := range tx.TxIns {
		spentUtxoKeysMap[input.InputOutpointKey] = struct{}{}
	}
}

// ParseUpdateNewUtxoInTxParallel utxo 信息
func ParseUpdateNewUtxoInTxParallel(txIdx int, tx *model.Tx, mpNewUtxo map[string]*model.TxoData) {
	for _, output := range tx.TxOuts {
		if output.LockingScriptUnspendable {
			continue
		}

		d := &model.TxoData{}
		d.BlockHeight = model.MEMPOOL_HEIGHT
		d.TxIdx = uint64(txIdx)
		d.Satoshi = output.Satoshi
		d.PkScript = output.PkScript
		d.ScriptType = output.ScriptType
		d.Data = output.Data

		mpNewUtxo[output.OutpointKey] = d
	}
}

// ParseUpdateAddressInTxParallel address tx历史记录
func ParseUpdateAddressInTxParallel(txIdx int, tx *model.Tx, addrPkhInTxMap map[string][]int) {
	for _, output := range tx.TxOuts {
		if output.Data.HasAddress {
			address := string(output.Data.AddressPkh[:])
			addrPkhInTxMap[address] = append(addrPkhInTxMap[address], txIdx)
		}
	}
}
