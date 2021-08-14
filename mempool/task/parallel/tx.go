package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"sensibled/model"

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

		output.Data = scriptDecoder.ExtractPkScriptForTxo(output.PkScript, output.ScriptType)
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
		d.Satoshi = output.Satoshi
		d.PkScript = output.PkScript
		d.ScriptType = output.ScriptType
		d.Data = output.Data

		mpNewUtxo[output.OutpointKey] = d
	}
}
