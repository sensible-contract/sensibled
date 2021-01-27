package blkparser

import (
	"encoding/binary"
	"encoding/hex"

	"go.uber.org/zap"
)

func parseTxParallel(tx *Tx, isCoinbase bool, block *ProcessBlock) {
	key := make([]byte, 36)
	copy(key, tx.Hash)
	for idx, output := range tx.TxOuts {
		if output.Value == 0 {
			continue
		}

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)

		output.LockingScriptType = getLockingScriptType(output.Pkscript)
		output.LockingScriptTypeHex = hex.EncodeToString(output.LockingScriptType)

		// test locking script
		output.LockingScriptMatch = true

		// if isLockingScriptOnlyEqual(output.Pkscript) {
		// 	output.LockingScriptMatch = true
		// }
	}

	// dumpLockingScriptType(tx)
	// parseTxoSpendByTxParallel(tx, isCoinbase, block)
	// parseUtxoParallel(tx, block)
}

func isLockingScriptOnlyEqual(pkscript []byte) bool {
	// test locking script
	// "0b 3c4b616e7965323032303e 87"

	length := len(pkscript)
	if length == 0 {
		return true
	}
	if pkscript[length-1] != 0x87 {
		return false
	}
	cnt, cntsize := SafeDecodeVarIntForScript(pkscript)
	if length == int(cnt+cntsize+1) {
		return true
	}
	return false
}

func getLockingScriptType(pkscript []byte) (scriptType []byte) {
	length := len(pkscript)
	if length == 0 {
		return
	}
	scriptType = make([]byte, 0)

	lenType := 0
	p := uint(0)
	e := uint(length)

	for p < e && lenType < 32 {
		c := pkscript[p]
		if 0 < c && c < 0x4f {
			cnt, cntsize := SafeDecodeVarIntForScript(pkscript[p:])
			p += cnt + cntsize
		} else {
			p += 1
		}
		scriptType = append(scriptType, c)
		lenType += 1
	}
	return
}

// dumpLockingScriptType  信息
func dumpLockingScriptType(tx *Tx) {
	for idx, output := range tx.TxOuts {
		if output.Value == 0 || !output.LockingScriptMatch {
			continue
		}

		key := string(output.LockingScriptType)

		calcMutex.Lock()
		if data, ok := calcMap[key]; ok {
			data.Value += 1
			calcMap[key] = data
		} else {
			calcMap[key] = CalcData{Value: 1}
		}
		calcMutex.Unlock()

		logger.Debug("pkscript",
			zap.String("tx", tx.HashHex),
			zap.Int("vout", idx),
			zap.Uint64("v", output.Value),
			zap.String("type", output.LockingScriptTypeHex),
		)
	}
}

// parseUtxoParallel utxo 信息
func parseUtxoParallel(tx *Tx, block *ProcessBlock) {
	for idx, output := range tx.TxOuts {
		if output.Value == 0 || !output.LockingScriptMatch {
			continue
		}

		if _, ok := block.UtxoMissingMap[output.OutpointKey]; ok {
			delete(block.UtxoMissingMap, output.OutpointKey)
		} else {
			block.UtxoMap[output.OutpointKey] = CalcData{
				Value:       output.Value,
				ScriptType:  output.LockingScriptTypeHex,
				BlockHeight: block.Height,
			}
		}

		logger.Debug("utxo",
			zap.String("tx", tx.HashHex),
			zap.Int("vout", idx),
			zap.Uint64("v", output.Value),
			zap.String("type", output.LockingScriptTypeHex),
		)
	}
}

// parseTxoSpendByTxParallel utxo被使用
func parseTxoSpendByTxParallel(tx *Tx, isCoinbase bool, block *ProcessBlock) {
	if isCoinbase {
		return
	}
	for idx, input := range tx.TxIns {
		if _, ok := block.UtxoMap[input.InputOutpointKey]; !ok {
			block.UtxoMissingMap[input.InputOutpointKey] = true
		} else {
			delete(block.UtxoMap, input.InputOutpointKey)
		}

		logger.Debug("spend",
			zap.String("tx", input.InputHashHex),
			zap.Uint32("vout", input.InputVout),
			zap.Int("idx", idx),
		)
	}
	logger.Debug("by",
		zap.String("tx", tx.HashHex),
	)
}
